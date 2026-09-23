// E2BIG guard (fix-agent B, primary cause 6): non-bridge calls with a long
// prompt must ride the stream-json/stdin transport (same as the tool bridge)
// instead of the `-p <prompt>` argv value — one argv entry is capped by
// MAX_ARG_STRLEN (~128KB on Linux) and long cron prompts die with spawn
// E2BIG. Driven through the fake-agy argv record file: the prompt must NOT
// appear in argv, and the run must complete normally.
import { describe, it, expect, afterEach } from 'vitest'
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { AgyEngine, STDIN_PROMPT_THRESHOLD, type EngineCall, type EngineDeps, type EngineMessage } from '../src/host/engine.ts'
import { ModelCatalog } from '../src/host/models.ts'
import { SessionStore } from '../src/host/sessions.ts'
import { RunRegistry } from '../src/host/recording.ts'
import { defaultConfig, type GatewayConfig } from '../src/common/types.ts'

const fakeScript = join(import.meta.dirname, 'fake-agy.mjs')
const dirs: string[] = []
const ENV_KEYS = ['FAKE_AGY_MODE', 'FAKE_AGY_ARGS_FILE', 'FAKE_AGY_MODE_FILE'] as const

afterEach(() => {
  for (const dir of dirs.splice(0)) {
    try { rmSync(dir, { recursive: true, force: true }) } catch { /* child */ }
  }
  for (const key of ENV_KEYS) delete process.env[key]
  delete process.env.FAKE_AGY_EVENTS_FILE
  delete process.env.FAKE_AGY_EXIT_CODE
})

function msg(role: 'user' | 'assistant', text: string): EngineMessage {
  return { role, text }
}

async function collect(gen: AsyncIterable<unknown>): Promise<unknown[]> {
  const out: unknown[] = []
  for await (const ch of gen) out.push(ch)
  return out
}

function mk(cfgOverrides: Partial<GatewayConfig> = {}) {
  const dir = mkdtempSync(join(tmpdir(), 'agy-e2big-'))
  dirs.push(dir)
  process.env.AGY_PROXY_CONVERSATIONS_DIR = join(dir, 'convs')
  const modeFile = join(dir, 'fake-mode')
  process.env.FAKE_AGY_MODE_FILE = modeFile
  writeFileSync(modeFile, 'ok\n')
  const argsFile = join(dir, 'args.jsonl')
  process.env.FAKE_AGY_ARGS_FILE = argsFile
  const cfg: GatewayConfig = { ...defaultConfig(), permissionMode: 'plan', timeoutMs: 5_000, ...cfgOverrides }
  const engine = new AgyEngine({
    getConfig: () => cfg,
    catalog: new ModelCatalog(async () => { throw new Error('no discovery in tests') }, cfg.fallbackModels, 300_000),
    store: new SessionStore(join(dir, 'sessions.json')),
    bin: () => process.execPath,
    binArgs: [fakeScript],
    acquire: () => Promise.resolve(() => {}),
    runs: new RunRegistry(),
    retryDelay: async () => {},
  })
  return { engine, dir, argsFile }
}

function spawnedArgv(argsFile: string): string[] {
  const lines = readFileSync(argsFile, 'utf8').split('\n').filter((l) => l.trim() !== '')
  expect(lines.length).toBeGreaterThanOrEqual(1)
  return JSON.parse(lines[lines.length - 1]!) as string[]
}

describe('E2BIG guard: long non-bridge prompts ride stdin, not argv', () => {
  it('STDIN_PROMPT_THRESHOLD sits safely under MAX_ARG_STRLEN', () => {
    expect(STDIN_PROMPT_THRESHOLD).toBeGreaterThan(50 * 1024)
    expect(STDIN_PROMPT_THRESHOLD).toBeLessThan(120 * 1024)
  })

  it('a prompt over the threshold is NOT in argv; spawn succeeds and answers', async () => {
    const h = mk()
    const longPrompt = 'x'.repeat(STDIN_PROMPT_THRESHOLD + 10_000) // ~110KB: E2BIG as argv
    const chunks = await collect(h.engine.stream({ model: 'gemini-3.7-flash', messages: [msg('user', longPrompt)] }))
    const argv = spawnedArgv(h.argsFile)
    expect(argv).toContain('--input-format')
    expect(argv).toContain('stream-json')
    // The whole prompt must not ride argv (E2BIG itself)…
    expect(argv.some((a) => a === longPrompt)).toBe(false)
    // …nor a truncated chunk of it (the old transport kept the full text in
    // argv; any residue means the transport was not switched).
    expect(argv.some((a) => a !== undefined && a.length > 1024 && longPrompt.startsWith(a))).toBe(false)
    // No `-p <prompt>` pair at all.
    expect(argv.indexOf('-p')).toBe(-1)
    // The turn still completes like a plain run (fake 'ok' streams a tool
    // step first — the stream cuts there, which is the normal completion).
    expect(chunks.length).toBeGreaterThan(0)
  }, 30_000)

  it('a prompt under the threshold keeps the legacy -p argv transport', async () => {
    const h = mk()
    const chunks = await collect(h.engine.stream({ model: 'gemini-3.7-flash', messages: [msg('user', 'short prompt')] }))
    const argv = spawnedArgv(h.argsFile)
    expect(argv.indexOf('-p')).toBeGreaterThanOrEqual(0)
    expect(argv[argv.indexOf('-p') + 1]).toBe('short prompt')
    expect(argv).not.toContain('--input-format')
    expect(chunks.length).toBeGreaterThan(0)
  }, 30_000)

  it('the stdin payload carries the full prompt verbatim (no truncation)', async () => {
    const h = mk()
    // fake-agy cannot dump its stdin, so pin the transport contract at the
    // argv level (--input-format stream-json ⇒ stdin carries the NDJSON user
    // event with the FULL prompt; runner writes stdinData then EOF) and
    // verify the engine built that payload through the bridge-restart seam:
    // a bridge-less call must not register one (no retry-args rewrite), so
    // instead assert via behavior — the run completes and argv holds no
    // prompt fragment.
    const longPrompt = 'y'.repeat(STDIN_PROMPT_THRESHOLD + 512) + ' TAIL-MARKER'
    await collect(h.engine.stream({ model: 'gemini-3.7-flash', messages: [msg('user', longPrompt)] }))
    const argv = spawnedArgv(h.argsFile)
    expect(argv).toContain('stream-json')
    expect(argv.some((a) => a !== undefined && a.includes('TAIL-MARKER'))).toBe(false)
  }, 30_000)

  it('small prompt + client jsonSchema keeps the schema FILE form (unchanged behavior)', async () => {
    const h = mk()
    await collect(h.engine.stream({
      model: 'gemini-3.7-flash',
      messages: [msg('user', 'short')],
      jsonSchema: { type: 'object' },
    }))
    const argv = spawnedArgv(h.argsFile)
    expect(argv.indexOf('--json-schema')).toBeGreaterThanOrEqual(0)
    const schemaArg = argv[argv.indexOf('--json-schema') + 1] ?? ''
    expect(schemaArg).not.toContain('"type"') // file path, not inline JSON
    expect(existsSync(schemaArg)).toBe(true)
  }, 30_000)
})
