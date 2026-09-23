// Quota-skip scheduling tests (fix-agent B, audit gap #4): an account whose
// live quota is at/below the preventive thresholds (5h ≤2%, weekly ≤1%) with a
// FUTURE reset time must be skipped by selectAccount — the preventive
// rotation that keeps a near-dead account from eating the next request.
// Drive through AccountPoolManager with quota state injected the same way the
// quota poller writes it (updateAccountQuotas — the production seam; the
// original plan phrased it as a mock quota-service on the shared interface).
import { describe, it, expect } from 'vitest'
import { mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { AccountPoolManager } from '../src/host/pool.ts'

function mkPool(): { pool: AccountPoolManager; a: string; b: string } {
  const dir = mkdtempSync(join(tmpdir(), 'agy-quota-skip-'))
  const pool = new AccountPoolManager(dir)
  const accA = pool.createAccountSlot('alpha')
  const accB = pool.createAccountSlot('beta')
  // Deterministic order: A first, B second.
  if (pool.getAccounts()[0]?.id !== accA.id) pool.reorderAccounts([accA.id, accB.id])
  return { pool, a: accA.id, b: accB.id }
}

function futureReset(minutes = 30): string {
  return new Date(Date.now() + minutes * 60_000).toISOString()
}

describe('pool: preventive quota skip in selectAccount', () => {
  it('skips an account with 5h quota ≤2% and a future resetTime', () => {
    const { pool, a, b } = mkPool()
    pool.updateAccountQuotas(a, { google: { remainingFraction: 0.02, resetTime: futureReset() } })
    expect(pool.selectAccount('google')?.id).toBe(b)
  })

  it('skips an account with 5h quota just ABOVE 2% only when healthy (stays available)', () => {
    const { pool, a } = mkPool()
    pool.updateAccountQuotas(a, { google: { remainingFraction: 0.0200001, resetTime: futureReset() } })
    // Above the threshold → NOT quota-skipped; sequential mode sticks to A.
    expect(pool.selectAccount('google')?.id).toBe(a)
  })

  it('skips an account with weekly quota ≤1% and a future weeklyResetTime', () => {
    const { pool, a, b } = mkPool()
    pool.updateAccountQuotas(a, { google: { weeklyFraction: 0.01, weeklyResetTime: futureReset() } })
    expect(pool.selectAccount('google')?.id).toBe(b)
  })

  it('a past resetTime clears the skip: the account returns to rotation', () => {
    const { pool, a } = mkPool()
    pool.updateAccountQuotas(a, { google: { remainingFraction: 0.0, resetTime: new Date(Date.now() - 60_000).toISOString() } })
    expect(pool.selectAccount('google')?.id).toBe(a)
  })

  it('a ≤2% account WITHOUT a resetTime stays in rotation (conservative: unknown reset)', () => {
    const { pool, a } = mkPool()
    pool.updateAccountQuotas(a, { google: { remainingFraction: 0.01 } })
    expect(pool.selectAccount('google')?.id).toBe(a)
  })

  it('a malformed resetTime does not suppress the account', () => {
    const { pool, a, b } = mkPool()
    pool.updateAccountQuotas(a, { google: { remainingFraction: 0.01, resetTime: 'not-a-date' } })
    // resetTime unparseable → the skip branch must not fire on NaN.
    expect([a, b]).toContain(pool.selectAccount('google')?.id)
    expect(pool.selectAccount('google')?.id).toBe(a)
  })

  it('quota skip is family-scoped: a drained google quota does not touch anthropic', () => {
    const { pool, a } = mkPool()
    pool.updateAccountQuotas(a, { google: { remainingFraction: 0.0, resetTime: futureReset() } })
    expect(pool.selectAccount('google')?.id).not.toBe(a)
    expect(pool.selectAccount('anthropic')?.id).toBe(a)
  })

  it('the whole-pool-exhausted-by-quota case selects nothing', () => {
    const { pool, a, b } = mkPool()
    pool.updateAccountQuotas(a, { google: { remainingFraction: 0.0, resetTime: futureReset() } })
    pool.updateAccountQuotas(b, { google: { remainingFraction: 0.0, resetTime: futureReset() } })
    expect(pool.selectAccount('google')).toBeNull()
  })

  it('exactly-at-threshold values skip (≤ is the documented comparison)', () => {
    const { pool, a, b } = mkPool()
    pool.updateAccountQuotas(a, { google: { remainingFraction: 0.02, resetTime: futureReset() } })
    expect(pool.selectAccount('google')?.id).toBe(b)
  })
})
