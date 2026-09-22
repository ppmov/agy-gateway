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

/** Decide what the engine should do with a hard-rate-limit failure text. */
export function decideEngineCooldown(rawErrText: string): {
  kind: 'short_cooldown' | 'full_cooldown'
  cooldownMs: number
  decision: Decision429
} {
  const decision = decideAgy429Checked(rawErrText)
  if (
    decision.kind === 'soft_retry' ||
    (decision.kind === 'instant_retry_same_auth' && (decision.retryAfterMs ?? 0) < 3_000) ||
    decision.kind === 'short_cooldown_switch_auth'
  ) {
    return { kind: 'short_cooldown', cooldownMs: SHORT_COOLDOWN_MS, decision }
  }
  return { kind: 'full_cooldown', cooldownMs: Number.POSITIVE_INFINITY, decision }

  function decideAgy429Checked(text: string): Decision429 {
    // engines surface the error as free text (stderr / result envelope), not
    // always the raw 429 body: try a JSON object first, fall back to using the
    // text verbatim (classifier regex 'after Ns' + keyword paths match it).
    const maybe: string | object = text.trim().startsWith('{') ? safeJson(text) ?? text : text
    return decideAntigravity429(maybe)
  }

  function safeJson(t: string): object | null {
    try {
      return JSON.parse(t) as object
    } catch {
      return null
    }
  }
}
