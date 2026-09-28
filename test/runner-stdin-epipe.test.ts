// Regression tests for the EPIPE guard on child.stdin (t_8d043eb1).
// An agy child that dies while a large prompt (100KB+ cron payloads ride
// stdin) is still draining into the pipe makes the pending stdin write fail
// asynchronously with `write EPIPE`. The sync throw from write() is caught
// by the try/catch in startAgyProcess, but the deferred 'error' event was
// not listened to — it escalated to an uncaughtException, and the gateway's
// crashToExit exits 1, killing in-flight HTTP streams (observed 25.09 13:55,
// journal window lost to rotation). The guard attaches a no-op 'error'
// listener on child.stdin so a lost prompt write can never crash the
// gateway; the child's fate is still reported through its own exit handling.
import { describe, it, expect } from 'vitest'
import { startAgyProcess } from '../src/host/runner.ts'

const NODE = process.execPath

describe('runner stdin EPIPE guard (t_8d043eb1)', () => {
  it('child closing stdin read-end mid-prompt-write does not throw EPIPE out of the runner', async () => {
    // The child closes its stdin read end immediately, then stays alive
    // long enough for a 200KB prompt payload to be queued and flushed into
    // the now-unwritable pipe — the deterministic reproduction of the
    // deferred `write EPIPE` escalation.
    const p = startAgyProcess({
      bin: NODE,
      args: ['-e', 'process.stdin.destroy(); setTimeout(() => process.exit(0), 300)'],
      stdinData: 'x'.repeat(200_000),
    })
    const out = await p.outcome
    expect(out.code).toBe(0)
  }, 15_000)

  it('child dying shortly after spawn while stdin still drains does not crash either', async () => {
    // Mirrors the prod pattern (agy PROCESS_EXIT mid-drain): the child
    // reads stdin briefly, then exits; the payload is still flushing.
    const p = startAgyProcess({
      bin: NODE,
      args: ['-e', 'setTimeout(() => process.exit(7), 120)'],
      stdinData: 'x'.repeat(200_000),
    })
    const out = await p.outcome
    // The runner reports the child's fate (non-zero exit), no crash.
    expect(out.code).toBe(7)
  }, 15_000)

  it('an unguarded stdin write escalates to uncaughtException `write EPIPE` (differential)', async () => {
    // The exact mechanism the guard shuts off: a pipe write whose read end
    // is closed fails ASYNCHRONOUSLY (WriteWrap.onWriteComplete) — the
    // error is not a sync throw from write(), it surfaces as an
    // uncaughtException `write EPIPE` when no 'error' listener exists.
    // This pins the physics of the crash class the guard addresses.
    const { spawn } = await import('node:child_process')
    const caught: Error[] = []
    const onUncaught = (e: Error): void => { caught.push(e) }
    process.on('uncaughtException', onUncaught)
    try {
      const child = spawn('sh', ['-c', 'exec 0<&-; sleep 0.2'], { stdio: ['pipe', 'ignore', 'ignore'] })
      child.stdin!.write('x'.repeat(200_000))
      await new Promise((resolve) => setTimeout(resolve, 400))
      child.kill()
      const epipe = caught.find((e) => (e as NodeJS.ErrnoException).code === 'EPIPE')
      expect(epipe).toBeDefined()
      expect((epipe as NodeJS.ErrnoException).syscall).toBe('write')
    } finally {
      process.removeListener('uncaughtException', onUncaught)
    }
  }, 15_000)

  it('normal stdin transport still completes (no regression on the happy path)', async () => {
    const p = startAgyProcess({
      bin: NODE,
      args: ['-e', 'let d=""; process.stdin.on("data", (c) => (d += c)); process.stdin.on("end", () => { process.stdout.write("len:" + d.length); process.exit(0) })'],
      stdinData: 'y'.repeat(50_000),
    })
    const out = await p.outcome
    expect(out.code).toBe(0)
    expect(out.stdout).toBe('len:50000')
  }, 15_000)
})