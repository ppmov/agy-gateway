// Port of CLIProxyAPI antigravity_executor_credits.go 429 classifier + helps/json_retry_helpers.go ParseRetryDelay.
// Pure functions: classify429/decide429 take the parsed (or raw JSON) 429 body.

export type Decision429Kind =
  | "soft_retry"
  | "instant_retry_same_auth"
  | "short_cooldown_switch_auth"
  | "full_quota_exhausted";

export interface Decision429 {
  kind: Decision429Kind;
  retryAfterMs: number | null;
  reason: string;
}

export type Category429 = "unknown" | "rate_limited" | "quota_exhausted" | "soft_rate_limit";

export const INSTANT_RETRY_THRESHOLD_MS = 3_000;       // antigravityInstantRetryThreshold = 3s
export const SHORT_COOLDOWN_THRESHOLD_MS = 5 * 60_000; // antigravityShortQuotaCooldownThreshold = 5m

const ERROR_INFO_TYPE = "type.googleapis.com/google.rpc.ErrorInfo";
const RETRY_INFO_TYPE = "type.googleapis.com/google.rpc.RetryInfo";
const QUOTA_KEYWORDS = ["quota_exhausted", "quota exhausted"];

interface ErrBody {
  error?: {
    code?: number;
    message?: string;
    status?: string;
    details?: Array<Record<string, unknown>>;
  };
}

// Port of helps.ParseRetryDelay: RetryInfo.retryDelay first, then ErrorInfo metadata.quotaResetDelay,
// then "after Ns" / "after 1h2m3s" regexes in error.message.
export function parseRetryDelayMs(body: unknown): number | null {
  const b = body as ErrBody;
  const details = b?.error?.details;
  if (Array.isArray(details)) {
    for (const d of details) {
      if (d["@type"] !== RETRY_INFO_TYPE) continue;
      const v = d["retryDelay"];
      if (typeof v !== "string" || v === "") continue;
      const ms = goParseDurationMs(v);
      if (ms === null) return null;
      return ms;
    }
    for (const d of details) {
      if (d["@type"] !== ERROR_INFO_TYPE) continue;
      const meta = d["metadata"] as Record<string, unknown> | undefined;
      const v = meta && typeof meta === "object" ? meta["quotaResetDelay"] : undefined;
      if (typeof v !== "string" || v === "") continue;
      const ms = goParseDurationMs(v);
      if (ms !== null) return ms;
    }
  }
  const message = typeof b?.error?.message === "string" ? b.error.message : "";
  if (message) {
    let m = /after\s+(\d+)s\.?/i.exec(message);
    if (m) return Number(m[1]) * 1000;
    m = /after\s+((?:\d+h)?(?:\d+m)?(?:\d+s)?)\.?/i.exec(message.toLowerCase());
    if (m) {
      const ms = goParseDurationMs(String(m[1]));
      if (ms !== null && ms > 0) return ms;
    }
  }
  return null;
}

// Go time.ParseDuration subset, including composite ("1h2m", "2m30s")
export function goParseDurationMs(s: string): number | null {
const neg = s.startsWith("-");
let body = neg ? s.slice(1) : s;
if (body === "") return null;
let total = 0;
const re = /([\d.]+)(ns|us|μs|ms|s|m|h)/g;
let matched = "";
let m: RegExpExecArray | null;
while ((m = re.exec(body)) !== null) {
  matched += m[0];
  const num = Number(m[1]);
  if (!Number.isFinite(num)) return null;
  const unit = m[2];
  const mult = unit === "ns" ? 1e-6 : unit === "us" || unit === "μs" ? 1e-3 : unit === "ms" ? 1 : unit === "s" ? 1000 : unit === "m" ? 60_000 : 3_600_000;
  total += num * mult;
}
if (matched !== body) return null;
return neg ? -total : total;
}

export function asBody(input: string | object): ErrBody {
  if (typeof input === "string") {
    try { return JSON.parse(input) as ErrBody; } catch { return {}; }
  }
  return input as ErrBody;
}

// Port of decideAntigravity429.
export function decideAntigravity429(rawBody: string | object): Decision429 {
  const decision: Decision429 = { kind: "soft_retry", retryAfterMs: null, reason: "" };
  const body = asBody(rawBody);
  if (!body || typeof body !== "object") return decision;

  const retryAfter = parseRetryDelayMs(body);
  if (retryAfter !== null) decision.retryAfterMs = retryAfter;

  const status = typeof body?.error?.status === "string" ? body.error.status.trim() : "";
  if (status.toLowerCase() !== "resource_exhausted") return decision;

  const details = body?.error?.details;
  if (Array.isArray(details)) {
    for (const detail of details) {
      if (detail?.["@type"] !== ERROR_INFO_TYPE) continue;
      const reason = typeof detail["reason"] === "string" ? detail["reason"].trim() : "";
      decision.reason = reason;
      if (reason.toLowerCase() === "quota_exhausted") {
        decision.kind = "full_quota_exhausted";
        return decision;
      }
    if (reason.toLowerCase() === "rate_limit_exceeded") {
      if (retryAfter === null) {
        decision.kind = "soft_retry";
        return decision;
      }
      if (retryAfter < INSTANT_RETRY_THRESHOLD_MS) {
        decision.kind = "instant_retry_same_auth";
      } else if (retryAfter < SHORT_COOLDOWN_THRESHOLD_MS) {
        decision.kind = "short_cooldown_switch_auth";
      } else {
        decision.kind = "full_quota_exhausted";
      }
      return decision;
    }
    }
  }

  const lower = JSON.stringify(body).toLowerCase();
  for (const kw of QUOTA_KEYWORDS) {
    if (lower.includes(kw)) {
      decision.kind = "full_quota_exhausted";
      decision.reason = "quota_exhausted";
      return decision;
    }
  }

  decision.kind = "soft_retry";
  return decision;
}

// Port of classifyAntigravity429.
export function classifyAntigravity429(rawBody: string | object): Category429 {
  switch (decideAntigravity429(rawBody).kind) {
    case "instant_retry_same_auth":
    case "short_cooldown_switch_auth":
      return "rate_limited";
    case "full_quota_exhausted":
      return "quota_exhausted";
    case "soft_retry":
      return "soft_rate_limit";
    default:
      return "unknown";
  }
}

// Port of markAntigravityShortCooldown semantics: cooldown window keyed by auth|model,
// stored with an explicit "until" timestamp; expired entries are self-cleaning.
export class ShortCooldownStore {
  private untilByKey = new Map<string, number>();

  static key(authId: string, modelName: string): string {
    if (!authId.trim() || !modelName.trim()) return "";
    return `${authId.trim()}|${modelName.trim()}|sc`;
  }

  mark(authId: string, modelName: string, nowMs: number, durationMs: number): void {
    const key = ShortCooldownStore.key(authId, modelName);
    if (!key || durationMs <= 0) return;
    this.untilByKey.set(key, nowMs + durationMs);
  }

  remaining(authId: string, modelName: string, nowMs: number): number {
    const key = ShortCooldownStore.key(authId, modelName);
    if (!key) return 0;
    const until = this.untilByKey.get(key);
    if (until === undefined) return 0;
    const remaining = until - nowMs;
    if (remaining <= 0) {
      this.untilByKey.delete(key);
      return 0;
    }
    return remaining;
  }

  isInCooldown(authId: string, modelName: string, nowMs: number): boolean {
    return this.remaining(authId, modelName, nowMs) > 0;
  }
}
