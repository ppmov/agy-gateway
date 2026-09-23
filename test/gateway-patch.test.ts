// Gateway-patch unit tests (fix-agent B, audit gap #4): the startup
// account-dir guard (CRITICAL phase-1 protection — admin DELETE rm -rf's
// account.dir, so a dir outside /root/.agy-accounts must refuse to boot) and
// the 429 cooldown router the engine consults at settle time.
import { describe, it, expect, afterEach } from 'vitest'
import { mkdtempSync, mkdirSync, rmSync, symlinkSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { ACCOUNTS_ROOT, decideEngineCooldown, validateAccountDirs } from '../src/patch/gateway-patch.ts'

const dirs: string[] = []
afterEach(() => {
  for (const dir of dirs.splice(0)) {
    try { rmSync(dir, { recursive: true, force: true }) } catch { /* ok */ }
  }
})

function tmpDir(): string {
  const dir = mkdtempSync(join(tmpdir(), 'agy-dirguard-'))
  dirs.push(dir)
  return dir
}

describe('validateAccountDirs (audit gap: CRITICAL guard had no unit tests)', () => {
  it('accepts an empty pool and legacy entries without a dir', () => {
    expect(validateAccountDirs({})).toEqual({ ok: true })
    expect(validateAccountDirs({ accounts: [] })).toEqual({ ok: true })
    expect(validateAccountDirs({ accounts: [{ id: 'a' }, { id: 'b', dir: '   ' }] })).toEqual({ ok: true })
  })

  it('accepts dirs inside the accounts root (real directories)', () => {
    const root = tmpDir()
    const inPool = join(root, 'acc_a')
    mkdirSync(inPool, { recursive: true })
    expect(validateAccountDirs({ accounts: [{ id: 'a', dir: inPool }] }, root)).toEqual({ ok: true })
  })

  it('accepts a not-yet-created in-pool dir (boot precedes first agy write)', () => {
    const root = tmpDir()
    expect(validateAccountDirs({ accounts: [{ id: 'a', dir: join(root, 'acc_missing') }] }, root)).toEqual({ ok: true })
  })

  it('refuses a dir outside the accounts root (the rm -rf blast-radius rule)', () => {
    const root = tmpDir()
    const outside = tmpDir() // a sibling temp dir: exists, real, outside root
    const res = validateAccountDirs({ accounts: [{ id: 'a', dir: outside }] }, root)
    expect(res.ok).toBe(false)
    expect(res.reason ?? '').toContain('outside')
    expect(res.reason ?? '').toContain(outside)
  })

  it('refuses a dir that merely LOOKS in-root but resolves outside (.. traversal)', () => {
    const root = tmpDir()
    const outside = tmpDir()
    const tricky = join(root, '..', outside.split('/').pop()!)
    const res = validateAccountDirs({ accounts: [{ id: 'a', dir: tricky }] }, root)
    expect(res.ok).toBe(false)
  })

  it('refuses an in-root SYMLINK pointing outside (realpath escape)', () => {
    const root = tmpDir()
    const outside = tmpDir()
    const link = join(root, 'acc_link')
    symlinkSync(outside, link)
    const res = validateAccountDirs({ accounts: [{ id: 'a', dir: link }] }, root)
    expect(res.ok).toBe(false)
    expect(res.reason ?? '').toContain('resolves to')
  })

  it('refuses the root itself (an rm -rf on the whole pool dir must never be exposed)', () => {
    const root = tmpDir()
    const res = validateAccountDirs({ accounts: [{ id: 'a', dir: root }] }, root)
    expect(res.ok).toBe(false)
  })

  it('fails on the FIRST offending entry and names the production accounts root by default', () => {
    const res = validateAccountDirs({ accounts: [{ id: 'a', dir: '/etc' }, { id: 'b', dir: '/usr' }] })
    expect(res.ok).toBe(false)
    expect(res.reason ?? '').toContain('/etc')
    expect(ACCOUNTS_ROOT).toBe('/root/.agy-accounts')
  })
})

describe('decideEngineCooldown (429 full integration: window → action)', () => {
  it('free-text hard limit with a long window → full cooldown (pool.recordFailure path)', () => {
    const v = decideEngineCooldown('429 RESOURCE_EXHAUSTED: Individual quota reached for this model. Resets in 21m25s.')
    expect(v.kind).toBe('full_cooldown')
  })

  it('free-text hard limit with a <5m window → short cooldown covering the window', () => {
    const v = decideEngineCooldown('429 RESOURCE_EXHAUSTED: quota exceeded. Resets in 2m30s.')
    expect(v.kind).toBe('short_cooldown')
    expect(v.cooldownMs).toBe(150_000)
  })

  it('free-text hard limit with a <3s window → instant retry, no cooldown', () => {
    const v = decideEngineCooldown('429 RESOURCE_EXHAUSTED: too fast. Resets in 2s.')
    expect(v.kind).toBe('instant_retry')
    expect(v.cooldownMs).toBe(0)
  })

  it('JSON RetryInfo bodies route by the parsed window (2s instant / 4m short / 10m full)', () => {
    const retry = (delay: string, msg: string): string =>
      JSON.stringify({ error: { code: 429, status: 'RESOURCE_EXHAUSTED', message: msg, details: [{ '@type': 'type.googleapis.com/google.rpc.RetryInfo', retryDelay: delay }] } })
    expect(decideEngineCooldown(retry('2s', 'rate limit exceeded')).kind).toBe('instant_retry')
    const short = decideEngineCooldown(retry('4m', 'rate limit exceeded'))
    expect(short.kind).toBe('short_cooldown')
    expect(short.cooldownMs).toBe(240_000)
    expect(decideEngineCooldown(retry('10m', 'rate limit exceeded')).kind).toBe('full_cooldown')
  })

  it('JSON quota_exhausted → full cooldown even without a reset time', () => {
    const body = JSON.stringify({ error: { code: 429, status: 'RESOURCE_EXHAUSTED', message: 'quota exhausted', details: [{ '@type': 'type.googleapis.com/google.rpc.ErrorInfo', reason: 'quota_exhausted' }] } })
    expect(decideEngineCooldown(body).kind).toBe('full_cooldown')
  })

  it('JSON 429 without any delay (classifier soft_retry) → instant retry', () => {
    const body = JSON.stringify({ error: { code: 429, status: 'RESOURCE_EXHAUSTED', message: 'slow down' } })
    expect(decideEngineCooldown(body).kind).toBe('instant_retry')
  })

  it('hard signature with NO parseable window → full cooldown (conservative default)', () => {
    expect(decideEngineCooldown('429 Too Many Requests').kind).toBe('full_cooldown')
  })

  it('the classifier never owns the SOFT decision: overloaded stays out of cooldown routing', () => {
    // The engine's blunt looksLikeHardRateLimit gate decides WHETHER to call
    // this; 'model overloaded' never passes that gate. The router itself
    // would see no window → full_cooldown, so it must only ever be consulted
    // on hard signatures. Pinned here so a future gate change is conscious.
    expect(decideEngineCooldown('model overloaded').kind).toBe('full_cooldown')
  })
})
