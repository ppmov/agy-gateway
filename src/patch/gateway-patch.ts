// Gateway-patch integration seam (ppmov/agy-gateway Phase 0-2):
// wires the CLIProxyAPI port modules into the Sakiko15 engine pipeline.
//
//  1. Schema sanitizer  — cleans Gemini-incompatible schema keys in the OUTGOING
//     structured-output schema (--json-schema path, engine.ts) — declaration /
//     generation locations only, NEVER conversation history.
//  2. 429-classifier    — classifies hard rate-limit error text BEFORE the blunt
//     looksLikeHardRateLimit cooldown, feeding ShortCooldownStore (<5m windows
//     switch auth instead of a full 15m account cooldown).
//  3. Account-dir guard — startup validator: refuses pool entries whose `dir`
//     lies outside /root/.agy-accounts, so admin DELETE can never rm -rf an
//     arbitrary server path (charter CRITICAL).
import { cleanJSONSchemaForAntigravity } from './antigravity-schema-sanitize.ts'
import { decideAntigravity429, type Decision429 } from './antigravity-429-classifier.ts'
import { parseResetDurationMs } from '../common/types.ts'
import { realpathSync } from 'node:fs'

/** Canonical home of all account profile dirs (rm -rf blast radius boundary). */
export const ACCOUNTS_ROOT = '/root/.agy-accounts'

export function classifyAgy429(rawErrText: string | object): Decision429 {
  return decideAntigravity429(rawErrText)
}

/** Clean an outbound structured-output schema (tool/response declarations). */
export function sanitizeOutgoingSchema(schema: unknown): unknown {
  const cleaned = cleanJSONSchemaForAntigravity(schema)
  try {
    return JSON.parse(cleaned)
  } catch {
    return schema
  }
}

export interface DirGuardResult {
  ok: boolean
  reason?: string
}

/**
 * Startup validator for pool.json account dirs. Fails the boot when a `dir`
 * escapes the accounts root (symlink or .. traversal included), so the admin
 * DELETE endpoint (`rm -rf account.dir`, pool.ts deleteAccount) can never
 * delete server material outside the account pool.
 */
export function validateAccountDirs(
  pool: { accounts?: ReadonlyArray<{ id?: string; dir?: string }> },
  accountsRoot: string = ACCOUNTS_ROOT,
): DirGuardResult {
  for (const acc of pool.accounts ?? []) {
    if (typeof acc.dir !== 'string' || acc.dir.trim() === '') continue // legacy/optional dir
    const resolved = acc.dir.startsWith(accountsRoot + '/')
      ? checkRealPath(acc.dir, accountsRoot)
      : raiseOutsideRoot(acc.dir, accountsRoot)
    if (!resolved.ok) return resolved
  }
  return { ok: true }
}

/** Symlink escape check: the pool dir must resolve inside accountsRoot. */
function checkRealPath(dir: string, accountsRoot: string): DirGuardResult {
  try {
    const rp = realpathSync(dir)
    if (rp === accountsRoot || rp.startsWith(accountsRoot + '/')) return { ok: true }
    return raiseOutsideRoot(`${dir} (resolves to ${rp})`, accountsRoot)
  } catch {
    // non-existent dir is fine at boot — it only becomes deletable after agy
    // writes to it, and agy writes inside the dir itself
    return { ok: true }
  }
}

function raiseOutsideRoot(dir: string, accountsRoot: string): DirGuardResult {
  return {
    ok: false,
    reason:
      `account dir '${dir}' is outside '${accountsRoot}' — refusing to start: ` +
      'admin DELETE rm -rf’s account.dir; only in-pool directories may be exposed',
  }
}

/** CLIProxyAPI short-cooldown semantics: <5m rate-limit → cooldown that auth
 *  switch covers; >=5m → treat as full quota exhaustion (account cooldown). */
export const SHORT_COOLDOWN_MS = 5 * 60_000

/** What the engine should do with a hard-rate-limit failure text:
 *  - instant_retry — the window is <3s (or a JSON body says retry now):
 *    no cooldown of any kind, the same auth retries immediately.
 *  - short_cooldown — the window is <5m: mark a ShortCooldownStore entry and
 *    switch auth for the next selection.
 *  - full_cooldown — quota exhausted / window >=5m / unrecognized hard
 *    signature: the account leaves rotation (pool.recordFailure). */
export type EngineCooldownKind = 'instant_retry' | 'short_cooldown' | 'full_cooldown'

/** Free-text hard signatures carry no parseable window in the classifier's
 *  JSON paths; the pool's own reset-duration parser covers agy's
 *  "Resets in 21m25s" / "retry after 30 seconds" shapes. */
function freeTextWindowMs(rawErrText: string): number | null {
  try {
    const parsed = parseResetDurationMs(rawErrText)
    return typeof parsed === 'number' && parsed > 0 ? parsed : null
  } catch {
    return null
  }
}

/** Decide what the engine should do with a hard-rate-limit failure text.
 *  Augments (not replaces) the blunt hard-limit gate: the gate still decides
 *  WHETHER the text is a server-issued 429; this refines HOW to react using
 *  the classifier's retry-delay parsing plus the engine's free-text windows.
 *  A hard signature with NO window anywhere cools the account down fully
 *  (pool default) — the pre-integration behavior for free-text limits. */
export function decideEngineCooldown(rawErrText: string): {
  kind: EngineCooldownKind
  cooldownMs: number
  decision: Decision429
} {
  const trimmed = rawErrText.trim()
  const maybeJson: object | null = trimmed.startsWith('{') ? safeJson(trimmed) : null
  const decision = decideAntigravity429(maybeJson ?? trimmed)
  // Window discovery order: the classifier's RetryInfo/quotaResetDelay/'after
  // Ns' paths first, then the engine's free-text "Resets in …" shapes.
  const windowMs = decision.retryAfterMs ?? freeTextWindowMs(rawErrText)
  if (windowMs !== null) {
    if (windowMs < 3_000) return { kind: 'instant_retry', cooldownMs: 0, decision }
    if (windowMs < SHORT_COOLDOWN_MS) return { kind: 'short_cooldown', cooldownMs: windowMs, decision }
    return { kind: 'full_cooldown', cooldownMs: Number.POSITIVE_INFINITY, decision }
  }
  if (maybeJson !== null) {
    // A real 429 JSON body without any retry-delay: the classifier's
    // soft_retry verdict means "safe to retry right away"; a body that
    // says quota_exhausted without a reset time cools the account fully.
    if (decision.kind === 'full_quota_exhausted') {
      return { kind: 'full_cooldown', cooldownMs: Number.POSITIVE_INFINITY, decision }
    }
    return { kind: 'instant_retry', cooldownMs: 0, decision }
  }
  return { kind: 'full_cooldown', cooldownMs: Number.POSITIVE_INFINITY, decision }

  function safeJson(t: string): object | null {
    try {
      return JSON.parse(t) as object
    } catch {
      return null
    }
  }
}
