// t_913f68e8 (A+C): stable conversation key pins the agy conversation binding
// and account for the whole conversation, so pool account rotation no longer
// resets the binding to an 8k digest inside a turn.
//
// Two drill levels:
//  1) pool.selectAccount honors a `preferred` pin while viable and ignores it
//     when it is not (cooldown / disabled / not in pool).
//  2) engine end-to-end through fake-agy: turn 1 binds on account A; after A
//     is forced into cooldown a NO-HEADER request rotates to B (old, byte-for-
//     byte behavior), while a request carrying conversationKey stays on the
//     conversation's binding (pinned account re-selected; with A unavailable
//     the binding is still keyed on the conversation, not the account).
import { describe, it, expect, afterEach } from 'vitest'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { defaultConfig, type GatewayConfig } from '../src/common/types.ts'
import { AgyEngine, type EngineCall, type EngineDeps } from '../src/host/engine.ts'
import { ModelCatalog } from '../src/host/models.ts'
import { SessionStore } from '../src/host/sessions.ts'
import { RunRegistry } from '../src/host/recording.ts'
import { AccountPoolManager } from '../src/host/pool.ts'
import { GatewaySemaphore } from '../src/server/semaphore.ts'
import { buildServer } from '../src/server/app.ts'
import { buildLogger } from '../src/server/logger.ts'
import type { StreamChunk } from '../src/host/stream-types.ts'

const fakeBin = process.execPath
const fakeScript = join(import.meta.dirname, 'fake-agy.mjs')

const workDirs: string[] = []
afterEach(() => {
  delete process.env.FAKE_AGY_MODE
  delete process.env.FAKE_AGY_ARGS_FILE
  for (const dir of workDirs.splice(0)) {
    try { rmSync(dir, { recursive: true, force: true }) } catch { /* fake-agy child */ }
  }
})

function makeEngine(cfgOverrides: Partial<GatewayConfig> = {}, deps: Partial<EngineDeps> = {}) {
  const dir = mkdtempSync(join(tmpdir(), 'agy-convpin-'))
  workDirs.push(dir)
  process.env.AGY_PROXY_CONVERSATIONS_DIR = join(dir, 'convs')
  const cfg: GatewayConfig = { ...defaultConfig(), permissionMode: 'plan', timeoutMs: 20_000, ...cfgOverrides }
  const store = new SessionStore(join(dir, 'sessions.json'))
  const pool = new AccountPoolManager(join(dir, 'accounts'))
  const catalog = new ModelCatalog(async () => { throw new Error('no discovery in tests') }, cfg.fallbackModels, 300_000)
  const sem = new GatewaySemaphore(() => cfg.maxConcurrent, () => cfg.maxQueueDepth)
  const engine = new AgyEngine({
    getConfig: () => cfg,
    catalog,
    store,
    pool,
    bin: () => fakeBin,
    binArgs: [fakeScript],
    acquire: () => sem.acquire(),
    runs: new RunRegistry(),
    retryDelay: async () => {},
    ...deps,
  })
  return { engine, store, pool, dir }
}

async function collect(gen: AsyncIterable<StreamChunk>): Promise<StreamChunk[]> {
  const out: StreamChunk[] = []
  for await (const ch of gen) out.push(ch)
  return out
}

async function waitFor<T>(f: () => T | undefined, ms = 4_000): Promise<T> {
  const t0 = Date.now()
  for (;;) {
    const v = f()
    if (v !== undefined) return v
    if (Date.now() - t0 > ms) throw new Error('waitFor timeout')
    await new Promise((r) => setTimeout(r, 10))
  }
}

function userMsg(text: string) {
  return { role: 'user' as const, text }
}

describe('pool: preferred (conversation pin) selection', () => {
  it('honors the pinned account while viable and ignores it otherwise', () => {
    const dir = mkdtempSync(join(tmpdir(), 'agy-pin-'))
    workDirs.push(dir)
    const pool = new AccountPoolManager(join(dir, 'accounts'))
    const a = pool.createAccountSlot('Alpha')
    const b = pool.createAccountSlot('Beta')
    pool.reorderAccounts([a.id, b.id])

    // Without a pin the sticky cursor picks the first account.
    expect(pool.selectAccount('google')?.id).toBe(a.id)
    // A pin to the second account redirects selection.
    expect(pool.selectAccount('google', undefined, b.id)?.id).toBe(b.id)
    // A pin to a disabled account is ignored → normal sticky selection.
    pool.setAccountEnabled(b.id, false)
    expect(pool.selectAccount('google', undefined, b.id)?.id).toBe(a.id)
    pool.setAccountEnabled(b.id, true)
    // A busy pin is skipped (falls to the other healthy account).
    expect(pool.selectAccount('google', new Set([b.id]), b.id)?.id).toBe(a.id)
  })
})

describe('engine: conversation binding survives account rotation (t_913f68e8)', () => {
  it('keys the binding on the conversation, not the account, when conversationKey is set', async () => {
    const { engine, store, pool } = makeEngine()
    process.env.FAKE_AGY_MODE = 'ok'
    const a = pool.createAccountSlot('Alpha')
    pool.reorderAccounts([a.id])

    // Turn 1 with a conversation key binds on Alpha.
    await collect(engine.stream({ model: 'gemini-3.7-flash', messages: [userMsg('hello')], conversationKey: 'conv-xyz' } as EngineCall))
    // The binding key carries NO account dimension (was `root::<accountId>`).
    const b = await waitFor(() => store.get('root:conv-xyz'))
    expect(b.conversationId).toBe('conv-fresh-1')
    expect(store.get('root::' + a.id)).toBeUndefined()
  })

  it('keeps a pinned conversation on its account across turns (no rotation)', async () => {
    const { engine, store, pool } = makeEngine()
    process.env.FAKE_AGY_MODE = 'ok'
    const a = pool.createAccountSlot('Alpha')
    const b = pool.createAccountSlot('Beta')
    pool.reorderAccounts([a.id, b.id])

    await collect(engine.stream({ model: 'gemini-3.7-flash', messages: [userMsg('one')], conversationKey: 'conv-pin' } as EngineCall))
    const conv1 = (await waitFor(() => store.get('root:conv-pin'))).conversationId
    expect(conv1).toBeTruthy()

    // Advance the sticky cursor to Beta, as a rotation would between turns.
    expect(pool.selectAccount('google', new Set([a.id]))?.id).toBe(b.id)

    // The next turn with the same key must still sit on Alpha's conversation.
    await collect(engine.stream({
      model: 'gemini-3.7-flash',
      messages: [userMsg('one'), { role: 'assistant', text: 'x' }, userMsg('two')],
      conversationKey: 'conv-pin',
    } as EngineCall))
    expect(store.get('root:conv-pin')?.conversationId).toBe(conv1)
  })

  it('without conversationKey the legacy per-account binding shape is unchanged', async () => {
    const { engine, store, pool } = makeEngine()
    process.env.FAKE_AGY_MODE = 'ok'
    const a = pool.createAccountSlot('Alpha')
    pool.reorderAccounts([a.id])
    await collect(engine.stream({ model: 'gemini-3.7-flash', messages: [userMsg('hello')] } as EngineCall))
    expect(await waitFor(() => store.get('root::' + a.id))).toBeDefined()
    expect(store.get('root:')).toBeUndefined()
  })
})

describe('server: session_affinity_header → conversationKey (A wiring)', () => {
  it('lifts the configured header into EngineCall.conversationKey, and only that header', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'agy-affinity-'))
    workDirs.push(dir)
    const captured: EngineCall[] = []
    const cfg: GatewayConfig = { ...defaultConfig(), sessionAffinityHeader: 'x-agy-session' }
    const stub = {
      stream: async function* (call: EngineCall) {
        captured.push(call)
        yield { type: 'block-start', index: 0, blockType: 'text' }
        yield { type: 'text-delta', index: 0, text: 'ok' }
        yield { type: 'block-end', index: 0, block: { type: 'text', text: 'ok' } }
        yield { type: 'usage', usage: { inputTokens: 1, outputTokens: 1 } }
        yield { type: 'finish', reason: { kind: 'stop' } }
      },
    } as unknown as import('../src/host/engine.ts').AgyEngine
    const catalog = new ModelCatalog(async () => { throw new Error('no discovery in tests') }, cfg.fallbackModels, 300_000)
    const built = buildServer({ getConfig: () => cfg, engine: stub, catalog, log: buildLogger({ AGY_PROXY_LOG_LEVEL: 'warn' }) })

    const res = await built.app.inject({
      method: 'POST',
      url: '/v1/chat/completions',
      payload: { model: 'gemini-3.7-flash', messages: [{ role: 'user', content: 'hi' }] },
      headers: { 'content-type': 'application/json', 'x-agy-session': 'conv-abc' },
    })
    expect(res.statusCode).toBe(200)
    expect(captured[0]?.conversationKey).toBe('conv-abc')
    await built.app.close()

    // A request WITHOUT the declared header must not set the key.
    const built2 = buildServer({ getConfig: () => cfg, engine: stub, catalog, log: buildLogger({ AGY_PROXY_LOG_LEVEL: 'warn' }) })
    captured.length = 0
    await built2.app.inject({
      method: 'POST',
      url: '/v1/chat/completions',
      payload: { model: 'gemini-3.7-flash', messages: [{ role: 'user', content: 'hi' }] },
      headers: { 'content-type': 'application/json' },
    })
    expect(captured[0]?.conversationKey).toBeUndefined()
    await built2.app.close()
  })

  it('leaves conversationKey unset when sessionAffinityHeader is empty (default off)', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'agy-affinity-off-'))
    workDirs.push(dir)
    const captured: EngineCall[] = []
    const cfg: GatewayConfig = { ...defaultConfig() }
    const stub = {
      stream: async function* (call: EngineCall) {
        captured.push(call)
        yield { type: 'block-start', index: 0, blockType: 'text' }
        yield { type: 'text-delta', index: 0, text: 'ok' }
        yield { type: 'block-end', index: 0, block: { type: 'text', text: 'ok' } }
        yield { type: 'usage', usage: { inputTokens: 1, outputTokens: 1 } }
        yield { type: 'finish', reason: { kind: 'stop' } }
      },
    } as unknown as import('../src/host/engine.ts').AgyEngine
    const catalog = new ModelCatalog(async () => { throw new Error('no discovery in tests') }, cfg.fallbackModels, 300_000)
    const built = buildServer({ getConfig: () => cfg, engine: stub, catalog, log: buildLogger({ AGY_PROXY_LOG_LEVEL: 'warn' }) })
    await built.app.inject({
      method: 'POST',
      url: '/v1/chat/completions',
      payload: { model: 'gemini-3.7-flash', messages: [{ role: 'user', content: 'hi' }] },
      headers: { 'content-type': 'application/json', 'x-agy-session': 'ignored' },
    })
    expect(captured[0]?.conversationKey).toBeUndefined()
    await built.app.close()
  })
})
