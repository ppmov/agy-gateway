// Domain types and models for the Antigravity Multi-Account Pool & Sequential Drain.
// Ported from dsh-agy-link src/common/pool-types.ts @ 46984db (verbatim).

export type ModelFamily = 'google' | 'anthropic' | 'openai' | 'unknown'

/** Map a model slug to its backend quota counter family. */
export function modelFamilyOf(modelId?: string): ModelFamily {
  if (!modelId) return 'unknown'
  const id = modelId.toLowerCase()
  if (id.startsWith('claude-') || id.includes('claude')) return 'anthropic'
  if (id.startsWith('gemini-') || id.startsWith('gemma-') || id.includes('gemini')) return 'google'
  if (id.startsWith('gpt-') || id.startsWith('openai/') || id.includes('gpt-oss')) return 'openai'
  return 'unknown'
}

export interface ModelQuotaInfo {
  modelId: string
  displayName?: string
  remainingFraction?: number
  resetTime?: string
}

export interface FamilyQuotaInfo {
  /** 5-hour rolling remaining quota fraction: 0.0 (exhausted) to 1.0 (full). */
  remainingFraction?: number
  /** ISO-8601 timestamp for when the 5-hour quota window resets. */
  resetTime?: string
  /** Weekly remaining quota fraction: 0.0 (exhausted) to 1.0 (full). */
  weeklyFraction?: number
  /** ISO-8601 timestamp for when the weekly quota resets. */
  weeklyResetTime?: string
  /** Description / tooltip from upstream backend */
  description?: string
  /** Timestamp when this quota was last updated locally. */
  updatedAt?: number
  /** Optional model-level breakdown in this family. */
  models?: ModelQuotaInfo[]
}

export interface FamilyCooldownState {
  /** Timestamp when cooldown expires and account becomes available again. */
  cooldownUntil: number
  /** Reason for cooldown (e.g. "429 Rate Limit", "Quota Exhausted"). */
  reason: string
  /** Consecutive rate-limit failure count (for exponential/tiered backoff). */
  consecutiveFailures: number
}

export interface ManagedAccount {
  id: string
  alias: string
  email?: string
  /**
   * Absolute path to the isolated account home directory. Empty for a
   * primary/system account; agy-proxy always isolates (the container has no
   * desktop login), but the field stays optional to keep the pool file shape
   * upstream-compatible.
   */
  dir: string
  /**
   * True when this account rides the real system HOME (no HOME injection).
   * agy-proxy never creates systemHome accounts (VPS containers have no
   * desktop login), but the flag stays for pool-file compatibility.
   */
  systemHome?: boolean
  /** Optional custom proxy URL override (e.g. "socks5://127.0.0.1:7890"). */
  proxyUrl?: string
  enabled: boolean
  createdAt: number
  lastUsedAt?: number
  /** Set to true when OAuth token refresh fails with invalid_grant or authentication is rejected. */
  authRequired?: boolean
  /** Last authentication failure reason. */
  authError?: string
  /**
   * Eligibility/geo failure streak (t_a5691f67) — consecutive upstream
   * eligibility refusals and the time of the last one. Drives the quarantine
   * threshold in AccountPoolManager.recordEligibilityFailure; cleared by
   * recordSuccess (and by clearAuthRequired, an explicit operator reset).
   * `until` (t_a1cb54bc) = epoch ms at which the quarantine window expires:
   * even a REAL geo refusal must not block the pool forever, so the account
   * auto-recovers when the window lapses (see isEligibilityQuarantined). A
   * legacy record without `until` is treated as an ACTIVE quarantine
   * (`?? Infinity`) for backward compatibility; new records always carry it.
   */
  eligibilityFailures?: { consecutive: number; lastAt: number; until?: number }
  /** Cooldown state tracked per model family. */
  cooldowns: Partial<Record<ModelFamily, FamilyCooldownState>>
  /** Cached real-time quota statistics per model family. */
  quotas: Partial<Record<ModelFamily, FamilyQuotaInfo>>
}

export type AccountHealthStatus = 'healthy' | 'cooldown' | 'auth_required' | 'disabled'

export interface AccountHealthInfo {
  status: AccountHealthStatus
  message?: string
  cooldownMs?: number
}

/**
 * Whether the account is CURRENTLY quarantined out of rotation (t_a1cb54bc).
 * A bare `authRequired` (AUTH / VALIDATION_REQUIRED — no eligibility record)
 * stays quarantined until an operator clear or a success. An eligibility
 * quarantine expires with its window (`until`), so even a real geo refusal
 * auto-recovers; a legacy record without `until` reads as an active
 * quarantine (`?? Infinity`) for backward compatibility. Pure predicate — the
 * lazy removal of an expired window lives in AccountPoolManager.
 */
export function isEligibilityQuarantined(account: ManagedAccount): boolean {
  return account.authRequired === true && (account.eligibilityFailures?.until ?? Infinity) > Date.now()
}

/**
 * Whether the background quota poller should touch this account at all.
 * Disabled, auth-quarantined and cooldown accounts are skipped so automatic
 * polling never hammers Google endpoints for accounts already known to be
 * restricted (risk-control exposure minimization). Manual force refresh
 * from the UI bypasses this gate. An eligibility quarantine is skipped only
 * while its window is live — after it lapses the account is polled again
 * (otherwise it could never re-earn quota and would stay out forever).
 */
export function shouldPollAccount(account: ManagedAccount): boolean {
  if (!account.enabled) return false
  if (isEligibilityQuarantined(account)) return false
  const now = Date.now()
  for (const cd of Object.values(account.cooldowns)) {
    if (cd && cd.cooldownUntil > now) return false
  }
  return true
}

/** Compute real-time health indicator for an account. */
export function getAccountHealth(account: ManagedAccount, family: ModelFamily = 'google'): AccountHealthInfo {
  if (!account.enabled) return { status: 'disabled', message: 'Account disabled' }
  if (isEligibilityQuarantined(account)) return { status: 'auth_required', message: account.authError || 'Authentication required' }
  const cd = account.cooldowns[family]
  if (cd && cd.cooldownUntil > Date.now()) {
    return {
      status: 'cooldown',
      message: cd.reason,
      cooldownMs: cd.cooldownUntil - Date.now(),
    }
  }
  return { status: 'healthy' }
}

export interface AccountPoolData {
  version: 1
  mode: 'sequential' | 'round-robin'
  defaultCooldownMs: number
  maxCooldownMs: number
  primaryAccountId?: string
  /** Currently active account id per model family for sticky sequential drain */
  activeAccountIds?: Partial<Record<ModelFamily, string>>
  accounts: ManagedAccount[]
}

export function defaultPoolData(): AccountPoolData {
  return {
    version: 1,
    mode: 'sequential',
    defaultCooldownMs: 15 * 60 * 1000, // 15 minutes (hardened from 10m)
    maxCooldownMs: 60 * 60 * 1000,    // 60 minutes
    accounts: [],
  }
}
