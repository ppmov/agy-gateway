// Eligibility/geo class (t_a5691f67): the upstream gate refuses the account —
// loadCodeAssist 502, "not eligible for Antigravity", "User location is not
// supported". The class must (1) classify as its own error code instead of a
// generic PROCESS_EXIT, (2) never consume a blind respawn (PROCESS_EXIT is
// retryable), and (3) quarantine the account out of rotation after a repeated
// streak — while a SINGLE transient failure leaves a healthy account in place.
// Driven end-to-end through the engine + fake-agy mode files (no live LLM).
import { describe, it, expect, afterEach } from 'vitest'
import { mkdtempSync, readFileSync, rmSync, writeFileSync, existsSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { AgyEngine, type EngineCall, type EngineDeps, type EngineMessage } from '../src/host/engine.ts'
import { ModelCatalog } from '../src/host/models.ts'
import { SessionStore } from '../src/host/sessions.ts'
import { RunRegistry } from '../src/host/recording.ts'
import { AccountPoolManager } from '../src/host/pool.ts'
import type { StreamChunk } from '../src/host/stream-types.ts'
import { defaultConfig, Err, looksLikeEligibilityBlocked, looksLikeValidationRequired } from '../src/common/types.ts'

const fakeBin = process.execPath
const fakeScript = join(import.meta.dirname, 'fake-agy.mjs')
const dirs: string[] = []
const ENV_KEYS = ['FAKE_AGY_MODE', 'FAKE_AGY_MODE_FILE', 'FAKE_AGY_ARGS_FILE'] as const

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

function call(messages: EngineMessage[], extra: Partial<EngineCall> = {}): EngineCall {
  return { model: 'gemini-3.7-flash', messages, ...extra }
}

async function collect(gen: AsyncIterable<StreamChunk>): Promise<StreamChunk[]> {
  const out: StreamChunk[] = []
  for await (const ch of gen) out.push(ch)
  return out
}

function finishOf(chunks: StreamChunk[]): { type: string; reason: { kind: string; failure?: { code: string; message: string } } } {
  const last = chunks[chunks.length - 1] as { type: string; reason: { kind: string; failure?: { code: string; message: string } } }
  if (last === undefined || last.type !== 'finish') throw new Error('stream ended without a finish chunk')
  return last
}

interface Harness {
  engine: AgyEngine
  pool: AccountPoolManager
  dir: string
  setMode: (mode: string) => void
  argsFile: string
  onRunCalls: OnRunInfo[]
}

function mk(): Harness {
  const dir = mkdtempSync(join(tmpdir(), 'agy-elig-'))
  dirs.push(dir)
  process.env.AGY_PROXY_CONVERSATIONS_DIR = join(dir, 'convs')
  const cfg = { ...defaultConfig(), permissionMode: 'plan' as const, timeoutMs: 5_000 }

  const modeFile = join(dir, 'fake-mode')
  process.env.FAKE_AGY_MODE_FILE = modeFile
  const argsFile = join(dir, 'args.jsonl')
  process.env.FAKE_AGY_ARGS_FILE = argsFile
  const setMode = (mode: string): void => { writeFileSync(modeFile, mode + '\n') }
  setMode('ok')

  const pool = new AccountPoolManager(join(dir, 'accounts'))
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
  return { engine, pool, dir, setMode, argsFile, onRunCalls }
}

describe('eligibility/geo classification', () => {
  it('matches the server-issued eligibility/geo phrases', () => {
    expect(looksLikeEligibilityBlocked('Eligibility check failed: loadCodeAssist: Bad Gateway')).toBe(true)
    expect(looksLikeEligibilityBlocked('Error: not eligible for Antigravity')).toBe(true)
    expect(looksLikeEligibilityBlocked('User location is not supported for the API use')).toBe(true)
  })

  it('stays narrow — unrelated failures and a bare "not eligible" do not match', () => {
    expect(looksLikeEligibilityBlocked(undefined)).toBe(false)
    expect(looksLikeEligibilityBlocked('')).toBe(false)
    // Catalog code uses "eligible account" for pool selection: not this class.
    expect(looksLikeEligibilityBlocked('no eligible account: tick skipped')).toBe(false)
    expect(looksLikeEligibilityBlocked('429 RESOURCE_EXHAUSTED Resets in 21m25s')).toBe(false)
    // Distinct from the re-validation challenge.
    expect(looksLikeValidationRequired('Eligibility check failed: loadCodeAssist')).toBe(false)
    expect(looksLikeEligibilityBlocked('403 VALIDATION_REQUIRED additional verification')).toBe(false)
  })
})

describe('eligibility/geo at the engine: no blind respawn, quarantine with a streak', () => {
  it('classifies as ELIGIBILITY_BLOCKED and never respawns the same account', async () => {
    const h = mk()
    h.setMode('eligibility')
    const chunks = await collect(h.engine.stream(call([msg('user', 'hi')])))
    const spawns = existsSync(h.argsFile) ? readFileSync(h.argsFile, 'utf8').split('\n').filter((l) => l.trim() !== '').length : 0
    // One spawn, no retry: the code is not in RETRYABLE_CODES.
    expect(spawns).toBe(1)
    expect(finishOf(chunks).reason.failure?.code).toBe(Err.ELIGIBILITY_BLOCKED)
    expect(h.onRunCalls[0]?.final).toBe(true)
    expect(h.onRunCalls[0]?.code).toBe(Err.ELIGIBILITY_BLOCKED)
  })

  it('a SINGLE transient failure does not quarantine a healthy account', async () => {
    const pool = new AccountPoolManager(mkdtempSync(join(tmpdir(), 'agy-elig-pool-')))
    const acc = pool.createAccountSlot('alpha')
    expect(pool.recordEligibilityFailure(acc.id, 'Eligibility check failed: loadCodeAssist: Bad Gateway')).toBe(false)
    expect(pool.getAccount(acc.id)?.authRequired).toBeFalsy()
    expect(pool.getAccount(acc.id)?.eligibilityFailures?.consecutive).toBe(1)
  })

  it('a repeated streak arms the quarantine and takes the account out of rotation', () => {
    const pool = new AccountPoolManager(mkdtempSync(join(tmpdir(), 'agy-elig-pool2-')))
    const acc = pool.createAccountSlot('alpha')
    expect(pool.recordEligibilityFailure(acc.id, 'Eligibility check failed: loadCodeAssist: Bad Gateway')).toBe(false)
    expect(pool.recordEligibilityFailure(acc.id, 'Eligibility check failed: loadCodeAssist: Bad Gateway')).toBe(true)
    const after = pool.getAccount(acc.id)
    expect(after?.authRequired).toBe(true)
    expect(after?.eligibilityFailures?.consecutive).toBe(2)
    // Quarantined accounts are filtered out of selection.
    expect(pool.selectAccount('google')).toBeNull()
  })

  it('the second request after quarantine does not spend a spawn on the dead account', async () => {
    const h = mk()
    h.setMode('eligibility')
    const acc = h.pool.createAccountSlot('alpha')
    // Two failures arm the quarantine (threshold 2), matching the observed
    // series (22 spawns on one account within 2h with zero successes).
    await collect(h.engine.stream(call([msg('user', 'hi')])))
    await collect(h.engine.stream(call([msg('user', 'hi')])))
    expect(h.pool.getAccount(acc.id)?.authRequired).toBe(true)
    const spawnsBefore = readFileSync(h.argsFile, 'utf8').split('\n').filter((l) => l.trim() !== '').length
    // Third call: no healthy account remains → the engine fails pre-flight
    // (POOL_EXHAUSTED thrown, no finish chunk) instead of spawning the dead
    // credential again.
    await expect(collect(h.engine.stream(call([msg('user', 'hi')])))).rejects.toMatchObject({ code: Err.POOL_EXHAUSTED })
    const spawnsAfter = readFileSync(h.argsFile, 'utf8').split('\n').filter((l) => l.trim() !== '').length
    expect(spawnsAfter).toBe(spawnsBefore)
  })
})
