// Non-stream client-disconnect drill (t_3e6d3726 regression): a client that
// destroys its socket mid-turn on /v1/chat/completions WITHOUT `stream:true`
// must cascade the abort into the agy process tree, exactly like the streaming
// leg (S-H2 drill in http-disconnect.test.ts). Pre-fix the non-stream routes
// called `engine.stream(call)` with no `signal`, so the reply.raw 'close'
// abort fired but the engine never saw it: the fake-agy `hang` run survived
// until the 120s watchdog and held the single pool slot (AGY_PROXY_MAX_
// CONCURRENT=1) — head-of-line blocking, the class in t_3e6d3726.
//
// The settle code observed here is the exact value index.ts feeds to the
// usage ledger (`status: i.code`), so `code === 'ABORTED'` == the ABORTED
// ledger row the acceptance criterion demands. Slot release is proven by a
// second request completing once the first disconnects.
import { describe, it, expect, afterEach } from 'vitest'
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { connect } from 'node:net'
import { request as httpRequest } from 'node:http'
import { defaultConfig, type GatewayConfig } from '../src/common/types.ts'
import { AgyEngine, type EngineDeps } from '../src/host/engine.ts'
import { ModelCatalog } from '../src/host/models.ts'
import { SessionStore } from '../src/host/sessions.ts'
import { RunRegistry } from '../src/host/recording.ts'
import { buildServer } from '../src/server/app.ts'
import { buildLogger } from '../src/server/logger.ts'
import { GatewaySemaphore } from '../src/server/semaphore.ts'

const fakeBin = process.execPath
const fakeScript = join(import.meta.dirname, 'fake-agy.mjs')

const dirs: string[] = []
const ENV_KEYS = ['FAKE_AGY_MODE', 'FAKE_AGY_MODE_FILE', 'FAKE_AGY_PID_FILE', 'AGY_PROXY_CONVERSATIONS_DIR'] as const

type OnRunInfo = Parameters<NonNullable<EngineDeps['onRun']>>[0]

function makeServer(onRunCalls: OnRunInfo[]) {
  const cfg: GatewayConfig = { ...defaultConfig(), permissionMode: 'plan', timeoutMs: 120_000, maxConcurrent: 1 }
  const dir = mkdtempSync(join(tmpdir(), 'agy-disconnect-ns-'))
  dirs.push(dir)
  process.env.AGY_PROXY_CONVERSATIONS_DIR = join(dir, 'convs')
  const catalog = new ModelCatalog(async () => { throw new Error('no discovery in tests') }, cfg.fallbackModels, 300_000)
  const sem = new GatewaySemaphore(() => cfg.maxConcurrent, () => cfg.maxQueueDepth)
  const engine = new AgyEngine({
    getConfig: () => cfg,
    catalog,
    store: new SessionStore(join(dir, 'sessions.json')),
    bin: () => fakeBin,
    binArgs: [fakeScript],
    acquire: () => sem.acquire(),
    runs: new RunRegistry(),
    onRun: (i) => { onRunCalls.push(i) },
    retryDelay: async () => {},
  })
  const built = buildServer({ getConfig: () => cfg, engine, catalog, log: buildLogger({ AGY_PROXY_LOG_LEVEL: 'warn' }) })
  return { built, dir }
}

function alive(pid: number): boolean {
  try { process.kill(pid, 0); return true } catch { return false }
}

afterEach(() => {
  for (const dir of dirs.splice(0)) {
    try { rmSync(dir, { recursive: true, force: true }) } catch { /* lingering handles */ }
  }
  for (const key of ENV_KEYS) delete process.env[key]
})

describe('t_3e6d3726 regression: non-stream client disconnect kills the agy run', () => {
  it('destroying the client socket on a non-stream POST ends the run, settles ABORTED, frees the slot', async () => {
    const onRunCalls: OnRunInfo[] = []
    const { built, dir } = makeServer(onRunCalls)
    const modeFile = join(dir, 'fake-mode')
    process.env.FAKE_AGY_MODE_FILE = modeFile
    writeFileSync(modeFile, 'hang\n')
    const pidFile = join(dir, 'pids.json')
    process.env.FAKE_AGY_PID_FILE = pidFile

    await built.app.listen({ port: 0, host: '127.0.0.1' })
    const address = built.app.server.address()
    if (address === null || typeof address === 'string') throw new Error('no listen address')
    const port = address.port

    // Request 1 — NON-STREAM (no `stream:true`): holds the single slot.
    const body = JSON.stringify({ model: 'gemini-3.7-flash', messages: [{ role: 'user', content: 'hi' }] })
    const socket = connect(port, '127.0.0.1')
    socket.on('data', () => undefined)
    socket.on('error', () => undefined)
    socket.write(
      'POST /v1/chat/completions HTTP/1.1\r\n' +
      'Host: 127.0.0.1\r\n' +
      'Content-Type: application/json\r\n' +
      `Content-Length: ${Buffer.byteLength(body)}\r\n` +
      'Connection: close\r\n\r\n' +
      body,
    )

    // The hang run is alive: wait for its pid, then confirm it stays up.
    let pid = 0
    const deadline = Date.now() + 10_000
    while (pid === 0 && Date.now() < deadline) {
      try { pid = Number(readFileSync(pidFile, 'utf8').trim().split('\n').at(-1)) } catch { /* not written yet */ }
      if (pid) break
      await new Promise((r) => setTimeout(r, 25))
    }
    expect(pid).toBeGreaterThan(0)
    expect(alive(pid)).toBe(true)

    // Disconnect mid-turn. Next spawn serves 'ok' so the slot-release probe
    // can complete (mode is read at each process start; the hang process
    // already snapshotted its mode).
    writeFileSync(modeFile, 'ok\n')
    socket.destroy()
    const killDeadline = Date.now() + 8_000
    let dead = false
    while (Date.now() < killDeadline) {
      if (!alive(pid)) { dead = true; break }
      await new Promise((r) => setTimeout(r, 50))
    }
    // Pre-fix the run stayed alive until the 120s watchdog (test deadline 8s).
    expect(dead).toBe(true)

    // The settle hook — the ledger's status source — must book ABORTED.
    const aborted = onRunCalls.find((c) => c.final && c.code === 'ABORTED')
    expect(aborted, `onRun codes: ${onRunCalls.map((c) => `${c.code}/${c.final}`).join(', ')}`).toBeDefined()

    // Slot release probe: a fresh non-stream request must complete promptly
    // (pre-fix it would wait ~186s behind the stuck slot; 5s deadline here).
    const okBody = await new Promise<string>((resolve, reject) => {
      const req = httpRequest(
        { host: '127.0.0.1', port, path: '/v1/chat/completions', method: 'POST', headers: { 'content-type': 'application/json' } },
        (res) => {
          let data = ''
          res.on('data', (d) => { data += d.toString('utf8') })
          res.on('end', () => resolve(data))
        },
      )
      req.on('error', reject)
      req.setTimeout(5_000, () => { req.destroy(new Error('slot not released (probe timed out)')) })
      req.end(JSON.stringify({ model: 'gemini-3.7-flash', messages: [{ role: 'user', content: 'hi' }] }))
    })
    // A live completion body (any valid chat.completion JSON) proves the slot
    // was released and a real run served the probe.
    expect(okBody).toContain('"object":"chat.completion"')

    await built.app.close()
  }, 25_000)
})
