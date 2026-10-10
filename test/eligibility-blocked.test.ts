// Eligibility/geo class (t_a5691f67) + its transport-only split (t_a1cb54bc).
// The upstream gate refuses a session in two shapes that share the agy
// `Eligibility check failed:` prefix but are fundamentally different:
//   (1) the REAL geo/identity refusal ("not eligible for Antigravity" /
//       "not currently available in your location") → ELIGIBILITY_BLOCKED, the
//       account leaves rotation — now with a TTL so the pool always recovers;
//   (2) a transport-only failure of the eligibility PROBE ("Bad Gateway" 502 /
//       EOF / 503 UNAVAILABLE) → UPSTREAM_UNAVAILABLE: a ROUTE state shared by
//       every account behind the relay, which must NEVER quarantine the account
//       (a relay blip used to exile the whole pool for hours while quotas were
//       full). Driven end-to-end through the engine + fake-agy mode files.
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
import { defaultConfig, Err, looksLikeEligibilityBlocked, looksLikeTransientEligibilityTransport, looksLikeValidationRequired } from '../src/common/types.ts'
import { getAccountHealth, shouldPollAccount } from '../src/common/pool-types.ts'

const fakeBin = process.execPath
const fakeScript = join(import.meta.dirname, 'fake-agy.mjs')
const dirs: string[] = []
const ENV_KEYS = ['FAKE_AGY_MODE', 'FAKE_AGY_MODE_FILE', 'FAKE_AGY_ARGS_FILE', 'FAKE_AGY_ELIG_TRANSPORT_TEXT'] as const

/** The exact transport-only probe failure observed in the 07.10 incident. */
const TRANSPORT_BAD_GATEWAY = 'Eligibility check failed: Post "https://daily-cloudcode-pa.googleapis.com/v1internal:loadCodeAssist": Bad Gateway'
/** The REAL geo refusal (ledger 07–09.10). */
const GEO_REFUSAL = 'Eligibility check failed: Your current account is not eligible for Antigravity, because it is not currently available in your location.'

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

describe('eligibility/geo vs transient-transport classification', () => {
  it('keeps the geo class on the REAL identity/geo phrases only', () => {
    expect(looksLikeEligibilityBlocked('Error: not eligible for Antigravity')).toBe(true)
    expect(looksLikeEligibilityBlocked(GEO_REFUSAL)).toBe(true)
    expect(looksLikeEligibilityBlocked('User location is not supported for the API use')).toBe(true)
    // The transport-only probe failure carries the SAME prefix but is not geo.
    expect(looksLikeEligibilityBlocked(TRANSPORT_BAD_GATEWAY)).toBe(false)
    expect(looksLikeEligibilityBlocked('Eligibility check failed: Post "…:loadCodeAssist": unexpected EOF')).toBe(false)
  })

  it('classifies a transport-only probe failure as transient (probe && transport)', () => {
    expect(looksLikeTransientEligibilityTransport(TRANSPORT_BAD_GATEWAY)).toBe(true)
    expect(looksLikeTransientEligibilityTransport('Eligibility check failed: Post "…:loadCodeAssist": unexpected EOF')).toBe(true)
    expect(looksLikeTransientEligibilityTransport('Eligibility check failed: fetchUserInfo: UNAVAILABLE (code 503): The service is currently unavailable.')).toBe(true)
    // The real geo refusal is NOT transient.
    expect(looksLikeTransientEligibilityTransport(GEO_REFUSAL)).toBe(false)
    // A bare transport token WITHOUT the eligibility-probe context is not this
    // class (generation-path transport rides the PROCESS_EXIT/retryable branch).
    expect(looksLikeTransientEligibilityTransport('Post "…:generateContent": Bad Gateway')).toBe(false)
    expect(looksLikeTransientEligibilityTransport('upstream: EOF')).toBe(false)
    expect(looksLikeTransientEligibilityTransport(undefined)).toBe(false)
    expect(looksLikeTransientEligibilityTransport('')).toBe(false)
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

describe('eligibility/geo at the engine: no blind respawn, quarantine with a streak + TTL', () => {
  it('the geo refusal classifies as ELIGIBILITY_BLOCKED and never respawns the same account', async () => {
    const h = mk()
    h.setMode('eligibility')
    h.pool.createAccountSlot('alpha')
    const chunks = await collect(h.engine.stream(call([msg('user', 'hi')])))
    const spawns = existsSync(h.argsFile) ? readFileSync(h.argsFile, 'utf8').split('\n').filter((l) => l.trim() !== '').length : 0
    // One spawn, no retry: ELIGIBILITY_BLOCKED is not in RETRYABLE_CODES.
    expect(spawns).toBe(1)
    expect(finishOf(chunks).reason.failure?.code).toBe(Err.ELIGIBILITY_BLOCKED)
    expect(h.onRunCalls[0]?.final).toBe(true)
    expect(h.onRunCalls[0]?.code).toBe(Err.ELIGIBILITY_BLOCKED)
  })

  it('a transport probe failure classifies as UPSTREAM_UNAVAILABLE and does NOT quarantine', async () => {
    const h = mk()
    h.setMode('eligibility-transport')
    const acc = h.pool.createAccountSlot('alpha')
    const chunks = await collect(h.engine.stream(call([msg('user', 'hi')])))
    expect(finishOf(chunks).reason.failure?.code).toBe(Err.UPSTREAM_UNAVAILABLE)
    expect(finishOf(chunks).reason.failure?.message).toContain('Bad Gateway')
    // A ROUTE state never touches account state — the account stays in rotation.
    expect(h.pool.getAccount(acc.id)?.authRequired).toBeFalsy()
    expect(h.pool.getAccount(acc.id)?.eligibilityFailures).toBeUndefined()
  })

  it('repeated transport failures never arm the quarantine', async () => {
    const h = mk()
    h.setMode('eligibility-transport')
    const acc = h.pool.createAccountSlot('alpha')
    // Well past ELIGIBILITY_QUARANTINE_THRESHOLD (2): each run still settles as
    // the transient class, so nothing accumulates on the account.
    for (let i = 0; i < 3; i++) {
      const chunks = await collect(h.engine.stream(call([msg('user', 'hi')])))
      expect(finishOf(chunks).reason.failure?.code).toBe(Err.UPSTREAM_UNAVAILABLE)
    }
    expect(h.pool.getAccount(acc.id)?.authRequired).toBeFalsy()
    expect(h.pool.getAccount(acc.id)?.eligibilityFailures).toBeUndefined()
    // The pool is still routable — no POOL_EXHAUSTED.
    expect(h.pool.selectAccount('google')?.id).toBe(acc.id)
  })

  it('the EOF variant of the transport failure is also transient, not geo', async () => {
    const h = mk()
    h.setMode('eligibility-transport')
    process.env.FAKE_AGY_ELIG_TRANSPORT_TEXT = 'Post "…:loadCodeAssist": unexpected EOF'
    const acc = h.pool.createAccountSlot('alpha')
    const chunks = await collect(h.engine.stream(call([msg('user', 'hi')])))
    expect(finishOf(chunks).reason.failure?.code).toBe(Err.UPSTREAM_UNAVAILABLE)
    expect(h.pool.getAccount(acc.id)?.authRequired).toBeFalsy()
  })

  it('the second request after a geo quarantine does not spend a spawn on the dead account', async () => {
    const h = mk()
    h.setMode('eligibility')
    const acc = h.pool.createAccountSlot('alpha')
    // Two failures arm the quarantine (threshold 2), matching the observed
    // series (22 spawns on one account within 2h with zero successes).
    await collect(h.engine.stream(call([msg('user', 'hi')])))
    await collect(h.engine.stream(call([msg('user', 'hi')])))
    const armed = h.pool.getAccount(acc.id)
    expect(armed?.authRequired).toBe(true)
    // ...and the quarantine now carries a TTL (t_a1cb54bc).
    expect(armed?.eligibilityFailures?.until).toBeGreaterThan(Date.now())
    const spawnsBefore = readFileSync(h.argsFile, 'utf8').split('\n').filter((l) => l.trim() !== '').length
    // Third call: no healthy account remains → the engine fails pre-flight
    // (POOL_EXHAUSTED thrown, no finish chunk) instead of spawning the dead
    // credential again.
    await expect(collect(h.engine.stream(call([msg('user', 'hi')])))).rejects.toMatchObject({ code: Err.POOL_EXHAUSTED })
    const spawnsAfter = readFileSync(h.argsFile, 'utf8').split('\n').filter((l) => l.trim() !== '').length
    expect(spawnsAfter).toBe(spawnsBefore)
  })
})

describe('eligibility quarantine TTL + auto-recovery (pool)', () => {
  it('a geo streak arms the quarantine with a TTL', () => {
    const pool = new AccountPoolManager(mkdtempSync(join(tmpdir(), 'agy-elig-pool2-')))
    const acc = pool.createAccountSlot('alpha')
    expect(pool.recordEligibilityFailure(acc.id, GEO_REFUSAL)).toBe(false)
    const afterFirst = pool.getAccount(acc.id)
    expect(afterFirst?.authRequired).toBeFalsy()
    expect(afterFirst?.eligibilityFailures?.consecutive).toBe(1)
    expect(pool.recordEligibilityFailure(acc.id, GEO_REFUSAL)).toBe(true)
    const armed = pool.getAccount(acc.id)
    expect(armed?.authRequired).toBe(true)
    expect(armed?.eligibilityFailures?.consecutive).toBe(2)
    expect(armed?.eligibilityFailures?.until).toBeGreaterThan(Date.now())
    // Quarantined accounts are filtered out of selection while the window is live.
    expect(pool.selectAccount('google')).toBeNull()
  })

  it('a SINGLE transient failure does not quarantine a healthy account', () => {
    const pool = new AccountPoolManager(mkdtempSync(join(tmpdir(), 'agy-elig-pool-')))
    const acc = pool.createAccountSlot('alpha')
    expect(pool.recordEligibilityFailure(acc.id, TRANSPORT_BAD_GATEWAY)).toBe(false)
    expect(pool.getAccount(acc.id)?.authRequired).toBeFalsy()
    expect(pool.getAccount(acc.id)?.eligibilityFailures?.consecutive).toBe(1)
  })

  it('the armed quarantine auto-recovers when its window lapses', () => {
    const pool = new AccountPoolManager(mkdtempSync(join(tmpdir(), 'agy-elig-ttl-')))
    const acc = pool.createAccountSlot('alpha')
    pool.recordEligibilityFailure(acc.id, GEO_REFUSAL)
    pool.recordEligibilityFailure(acc.id, GEO_REFUSAL)
    const armed = pool.getAccount(acc.id)!
    expect(pool.selectAccount('google')).toBeNull()
    expect(shouldPollAccount(armed)).toBe(false)
    expect(getAccountHealth(armed, 'google').status).toBe('auth_required')

    // Window lapses (inject the clock): the quarantine is lifted lazily.
    armed.eligibilityFailures!.until = Date.now() - 1
    expect(pool.selectAccount('google')?.id).toBe(acc.id)
    const healed = pool.getAccount(acc.id)!
    expect(healed.authRequired).toBeUndefined()
    expect(healed.authError).toBeUndefined()
    expect(healed.eligibilityFailures).toBeUndefined()
    expect(shouldPollAccount(healed)).toBe(true)
    expect(getAccountHealth(healed, 'google').status).toBe('healthy')
  })

  it('a legacy eligibility record without a TTL stays an active quarantine', () => {
    // Backward compatibility: a pool.json written before the TTL existed has
    // no `until`; it must read as quarantined (`?? Infinity`), never silently
    // released by the lazy gate.
    const pool = new AccountPoolManager(mkdtempSync(join(tmpdir(), 'agy-elig-legacy-')))
    const acc = pool.createAccountSlot('alpha')
    const live = pool.getAccount(acc.id)!
    live.authRequired = true
    live.authError = GEO_REFUSAL
    live.eligibilityFailures = { consecutive: 2, lastAt: Date.now() }
    expect(pool.selectAccount('google')).toBeNull()
    expect(live.authRequired).toBe(true)
    expect(shouldPollAccount(live)).toBe(false)
  })
})
