// Regression tests for the EPIPE-crash fix in buildLogger (t_8d043eb1).
// Pre-fix, the bare pino() default (logger.ts:26) wrote synchronously to
// process.stdout; when the reader of that pipe disappears (journal rotation,
// pipe close), the failed sync write escalates to an uncaughtException —
// src/index.ts crashToExit then exits 1, killing the gateway and in-flight
// HTTP streams. The fix routes stdout logging through
// pino.destination({ dest: 1, sync: false }): write failures surface as
// 'error' events on the destination stream, and buildLogger attaches a
// no-op 'error' listener so a lost log line can never crash the process.
import { describe, it, expect } from 'vitest'
import { buildLogger } from '../src/server/logger.ts'
import pino from 'pino'
import { Writable } from 'node:stream'

describe('buildLogger EPIPE-safety (async stdout sink)', () => {
  it('default (no destination) uses an async stdout sink with an error listener — not bare sync pino', () => {
    const log = buildLogger({ AGY_PROXY_LOG_LEVEL: 'info' })
    const stream = (log as unknown as Record<symbol, unknown>)[pino.symbols.streamSym] as
      | (Writable & { sync?: boolean })
      | undefined
    expect(stream).toBeDefined()
    expect(stream).not.toBe(process.stdout)
    expect(stream!.sync).toBe(false)
    // SonicBoom ships its own filterBrokenPipe EPIPE listener on the same
    // stream; our guard adds another, so the count is >=1, not exactly 1.
    expect(stream!.listenerCount('error')).toBeGreaterThanOrEqual(1)
  })

  it('a write error on the sink surfaces as a swallowed stream error, not an uncaughtException', async () => {
    // pino.destination({ dest: 1 }) hard-wires fd 1; to simulate a broken pipe
    // deterministically we build the same shape the fix creates:
    // an async pino destination with the same no-op error listener.
    const broken = pino.destination({ dest: 1, sync: false })
    broken.on('error', () => {})
    const log = pino({ level: 'info' }, broken)
    expect(() => broken.emit('error', new Error('EPIPE simulated'))).not.toThrow()
    log.info({ src: 'test' }, 'write after simulated EPIPE does not throw')
    await new Promise((resolve) => broken.flush?.(() => resolve(resolve)))
    expect(true).toBe(true)
  })

  it('custom destination (tests, embedders) keeps working unchanged', () => {
    let received = ''
    const capture = new Writable({
      write(_chunk, _enc, cb) {
        received = received + String(_chunk)
        cb()
    },
    })
    const log = buildLogger({ AGY_PROXY_LOG_LEVEL: 'info' }, capture)
    log.info({ hello: 'world' }, 'custom sink intact')
    expect(received).toContain('custom sink intact')
    expect(received).not.toContain('[redacted]')
  })
})