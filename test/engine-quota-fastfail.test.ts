// t_1e2de674 — live quota fast-fail (agy-gateway).
//
// Real shape: on an exhausted quota agy retries INTERNALLY for ~2.5min before
// it prints 'Individual quota reached' and exits; the bridge used to learn the
// class only AFTER the process was gone (stderr tail), so the single pool slot
// was held for the whole wait and the client paid a full spawn. The fix wires a
// live stderr hook (runner onStderr) into a NARROW hard-quota detector that
// kills the spawn the moment the server-issued line lands and reclassifies the
// run onto the quota branch — so the settle-time cooldown still fires (a bare
// kill would settle ABORTED, no cooldown).
//
// fake-agy `rate-limit-delayed` emits nothing but init, sleeps
// FAKE_AGY_QUOTA_AFTER_MS, THEN prints the quota line + ERROR envelope + exit 1.
// With the detector the run ends in single-digit seconds at the line; without
// it the run waits out the whole delay. Driven end-to-end through the engine +
// fake-agy (no live LLM).
import { describe, it, expect, afterEach, vi } from 'vitest'
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { AgyEngine, type EngineMessage, type EngineDeps } from '../src/host/engine.ts'
import { ModelCatalog } from '../src/host/models.ts'
import { SessionStore } from '../src/host/sessions.ts'
import { RunRegistry } from '../src/host/recording.ts'
import { AccountPoolManager } from '../src/host/pool.ts'
import type { StreamChunk } from '../src/host/stream-types.ts'
import { defaultConfig, Err } from '../src/common/types.ts'

const fakeBin = process.execPath
const fakeScript = join(import.meta.dirname, 'fake-agy.mjs')
const dirs: string[] = []
const ENV_KEYS = ['FAKE_AGY_MODE', 'FAKE_AGY_MODE_FILE', 'FAKE_AGY_ARGS_FILE', 'FAKE_AGY_FAIL_HOME', 'FAKE_AGY_QUOTA_AFTER_MS', 'FAKE_AGY_QUOTA_RETRY_MS'] as const

afterEach(() => {
  for (const dir of dirs.splice(0)) {
    try { rmSync(dir, { recursive: true, force: true }) } catch { /* fake-agy child */ }
  }
  for (const key of ENV_KEYS) delete process.env[key]
})

type OnRunInfo = Parameters<NonNullable<EngineDeps['onRun']>>[0]

function msg(role: 'user' | 'assistant', text: string): EngineMessage {
  return { role, text }
}

async function collect(gen: AsyncIterable<StreamChunk>): Promise<StreamChunk[]> {
  const out: StreamChunk[] = []
  for await (const ch of gen) out.push(ch)
  return out
}

function finishOf(chunks: StreamChunk[]): { reason: { kind: string; failure?: { code: string; message: string } } } {
  const last = chunks[chunks.length - 1] as { type: string; reason: { kind: string; failure?: { code: string; message: string } } }
  if (last === undefined || last.type !== 'finish') throw new Error('stream ended without a finish chunk')
  return last
}

interface Harness {
  engine: AgyEngine
  pool: AccountPoolManager
  setMode: (mode: string) => void
  onRunCalls: OnRunInfo[]
}

function mk(): Harness {
  const dir = mkdtempSync(join(tmpdir(), 'agy-fastfail-'))
  dirs.push(dir)
  process.env.AGY_PROXY_CONVERSATIONS_DIR = join(dir, 'convs')
  const modeFile = join(dir, 'fake-mode')
  process.env.FAKE_AGY_MODE_FILE = modeFile
  writeFileSync(modeFile, 'ok\n')
  process.env.FAKE_AGY_ARGS_FILE = join(dir, 'args.jsonl')
  // Long enough that a run WITHOUT the detector would clearly wait it out; the
  // detector must finish far sooner. timeoutMs stays above the delay so a
  // missing detector would surface as a real slow run, not a watchdog timeout.
  const cfg = { ...defaultConfig(), permissionMode: 'plan' as const, timeoutMs: 30_000 }
  const pool = new AccountPoolManager(join(dir, 'accounts'))
  const accA = pool.createAccountSlot('alpha')
  const accB = pool.createAccountSlot('beta')
  if (pool.getAccounts()[0]?.id !== accA.id) pool.reorderAccounts([accA.id, accB.id])
  const onRunCalls: OnRunInfo[] = []
  const engine = new AgyEngine({
    getConfig: () => cfg,
    catalog: new ModelCatalog(async () => { throw new Error('no discovery in tests') }, cfg.fallbackModels, 300_000),
    store: new SessionStore(join(dir, 'sessions.json')),
    pool,
    bin: () => fakeBin,
    binArgs: [fakeScript],
    acquire: () => Promise.resolve(() => {}),
    runs: new RunRegistry(),
    onRun: (i) => { onRunCalls.push(i) },
    retryDelay: async () => {},
  })
  return { engine, pool, setMode: (m) => writeFileSync(modeFile, m + '\n'), onRunCalls }
}

describe('t_1e2de674: live quota fast-fail', () => {
  it('kills the spawn at the live quota line: single-digit seconds, AGY_ERROR (not ABORTED), account cooled', async () => {
    const h = mk()
    const first = h.pool.getAccounts()[0]!
    // The first hard signature lands at ~1500ms; the fake would otherwise keep
    // "retrying" for another 20s before it gives up on its own. The live
    // detector must end the run right at the line.
    process.env.FAKE_AGY_QUOTA_AFTER_MS = '1500'
    process.env.FAKE_AGY_QUOTA_RETRY_MS = '20000'
    h.setMode('rate-limit-delayed')
    const started = Date.now()
    const chunks = await collect(h.engine.stream({ model: 'gemini-3.7-flash', messages: [msg('user', 'exhausted ask')] }))
    const elapsed = Date.now() - started
    // Far under the 20s the fake would have waited on its own.
    expect(elapsed).toBeLessThan(4000)
    const fin = finishOf(chunks)
    expect(fin.reason.kind).toBe('error')
    // NOT ABORTED: the whole point — a bare kill would lose the class and skip
    // the cooldown.
    expect(fin.reason.failure?.code).toBe(Err.AGY_ERROR)
    expect(fin.reason.failure?.message ?? '').toContain('quota')
    // AGY_ERROR is not retryable: exactly one attempt, settled final.
    expect(h.onRunCalls.length).toBe(1)
    expect(h.onRunCalls[0]?.final).toBe(true)
    expect(h.onRunCalls[0]?.code).toBe(Err.AGY_ERROR)
    // Settle-time cooldown still fired for the exhausted account.
    await vi.waitFor(() => {
      expect(h.pool.getAccount(first.id)?.cooldowns.google).toBeDefined()
    }, { timeout: 5_000, interval: 20 })
  }, 30_000)

  it('regression: the INSTANT rate-limit mode stays on the quota branch', async () => {
    const h = mk()
    h.setMode('rate-limit')
    const chunks = await collect(h.engine.stream({ model: 'gemini-3.7-flash', messages: [msg('user', 'instant quota')] }))
    const fin = finishOf(chunks)
    expect(fin.reason.kind).toBe('error')
    expect(fin.reason.failure?.code).toBe(Err.AGY_ERROR)
    expect(fin.reason.failure?.message ?? '').toContain('quota')
  }, 30_000)
})
