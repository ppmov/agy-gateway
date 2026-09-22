// Ports of antigravity_executor_credits_test.go — TestClassifyAntigravity429 cases,
// plus threshold-boundary and cooldown-window tests.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  classifyAntigravity429, decideAntigravity429, parseRetryDelayMs,
  goParseDurationMs, ShortCooldownStore,
  INSTANT_RETRY_THRESHOLD_MS, SHORT_COOLDOWN_THRESHOLD_MS,
} from "./antigravity-429-classifier.ts";

function rc429() {
  return {
    "error": {
      "code": 429,
      "message": "You have exhausted your capacity on this model. Your quota will reset after 0s.",
      "status": "RESOURCE_EXHAUSTED",
      "details": [
        {
          "@type": "type.googleapis.com/google.rpc.ErrorInfo",
          "reason": "RATE_LIMIT_EXCEEDED",
          "domain": "cloudcode-pa.googleapis.com",
          "metadata": {
            "model": "claude-opus-4-6-thinking",
            "quotaResetDelay": "479.417207ms",
            "quotaResetTimeStamp": "2026-04-20T09:19:49Z",
            "uiMessage": "true",
          },
        },
        { "@type": "type.googleapis.com/google.rpc.RetryInfo", "retryDelay": "0.479417207s" },
      ],
    },
  };
}

test("classify: quota exhausted via message keyword", () => {
  const body = { error: { status: "RESOURCE_EXHAUSTED", message: "QUOTA_EXHAUSTED" } };
  assert.equal(classifyAntigravity429(body), "quota_exhausted");
});

test("classify: standard antigravity rate limit with ui message stays rate limited", () => {
  const body = rc429();
  assert.equal(classifyAntigravity429(body), "rate_limited");
  const d = decideAntigravity429(body);
  assert.equal(d.kind, "instant_retry_same_auth");
  assert.ok(d.retryAfterMs !== null && d.retryAfterMs > 0);
  assert.ok(Math.abs(d.retryAfterMs - 479.417207) < 1);
});

test("classify: structured rate limit (retryDelay 0.5s → instant retry)", () => {
  const body = { error: { status: "RESOURCE_EXHAUSTED", details: [
    { "@type": "type.googleapis.com/google.rpc.ErrorInfo", reason: "RATE_LIMIT_EXCEEDED" },
    { "@type": "type.googleapis.com/google.rpc.RetryInfo", retryDelay: "0.5s" },
  ] } };
  assert.equal(classifyAntigravity429(body), "rate_limited");
  assert.equal(decideAntigravity429(body).kind, "instant_retry_same_auth");
});

test("classify: structured quota exhausted (ErrorInfo reason QUOTA_EXHAUSTED)", () => {
  const body = { error: { status: "RESOURCE_EXHAUSTED", details: [
    { "@type": "type.googleapis.com/google.rpc.ErrorInfo", reason: "QUOTA_EXHAUSTED" },
  ] } };
  assert.equal(classifyAntigravity429(body), "quota_exhausted");
  assert.equal(decideAntigravity429(body).reason, "QUOTA_EXHAUSTED");
});

test("classify: unstructured 429 defaults to soft rate limit", () => {
  const body = { error: { message: "too many requests" } };
  assert.equal(classifyAntigravity429(body), "soft_rate_limit");
});

test("RATE_LIMIT_EXCEEDED without retry delay → soft_retry", () => {
  const body = { error: { status: "RESOURCE_EXHAUSTED", details: [
    { "@type": "type.googleapis.com/google.rpc.ErrorInfo", reason: "RATE_LIMIT_EXCEEDED" },
  ] } };
  const d = decideAntigravity429(body);
  assert.equal(d.kind, "soft_retry");
  assert.equal(d.retryAfterMs, null);
});

test("short cooldown branch: RATE_LIMIT_EXCEEDED with retry after 2m (<5m, ≥3s)", () => {
  const body = { error: { status: "RESOURCE_EXHAUSTED", details: [
    { "@type": "type.googleapis.com/google.rpc.ErrorInfo", reason: "RATE_LIMIT_EXCEEDED" },
    { "@type": "type.googleapis.com/google.rpc.RetryInfo", retryDelay: "2m" },
  ] } };
  assert.equal(decideAntigravity429(body).kind, "short_cooldown_switch_auth");
});

test("long retry after (≥5m) collapses to full quota exhausted", () => {
  const body = { error: { status: "RESOURCE_EXHAUSTED", details: [
    { "@type": "type.googleapis.com/google.rpc.ErrorInfo", reason: "RATE_LIMIT_EXCEEDED" },
    { "@type": "type.googleapis.com/google.rpc.RetryInfo", retryDelay: "300s" },
  ] } };
  assert.equal(decideAntigravity429(body).kind, "full_quota_exhausted");
});

test("threshold boundaries: Go uses strict <; exactly 3s → short cooldown", () => {
  const mk = (delay: string) => ({ error: { status: "RESOURCE_EXHAUSTED", details: [
    { "@type": "type.googleapis.com/google.rpc.ErrorInfo", reason: "RATE_LIMIT_EXCEEDED" },
    { "@type": "type.googleapis.com/google.rpc.RetryInfo", retryDelay: delay },
  ] } });
  assert.equal(decideAntigravity429(mk("2.99999s")).kind, "instant_retry_same_auth");
  assert.equal(decideAntigravity429(mk("3s")).kind, "short_cooldown_switch_auth");
  assert.equal(decideAntigravity429(mk("299.99s")).kind, "short_cooldown_switch_auth");
  assert.equal(decideAntigravity429(mk("300s")).kind, "full_quota_exhausted");
});

test("quotaResetDelay metadata fallback in ErrorInfo", () => {
  const body = { error: { status: "RESOURCE_EXHAUSTED", details: [
    { "@type": "type.googleapis.com/google.rpc.ErrorInfo", reason: "RATE_LIMIT_EXCEEDED",
      metadata: { quotaResetDelay: "45.000s" } },
  ] } };
  assert.equal(parseRetryDelayMs(body), 45_000);
});

test("message regex 'after 30s' fallback", () => {
  assert.equal(parseRetryDelayMs({ error: { message: "Please retry after 30s." } }), 30_000);
  assert.equal(parseRetryDelayMs({ error: { message: "Please retry after 1h2m." } }), 3_720_000);
});

test("non-RESOURCE_EXHAUSTED status stays soft_retry", () => {
  const body = { error: { status: "INVALID_ARGUMENT", details: [
    { "@type": "type.googleapis.com/google.rpc.ErrorInfo", reason: "QUOTA_EXHAUSTED" },
  ] } };
  assert.equal(decideAntigravity429(body).kind, "soft_retry");
});

test("empty body → soft_retry", () => {
  assert.equal(decideAntigravity429("").kind, "soft_retry");
});

test("goParseDurationMs formats", () => {
  assert.equal(goParseDurationMs("300ms"), 300);
  assert.equal(goParseDurationMs("1.5h"), 5_400_000);
  assert.equal(goParseDurationMs("479.417207ms"), 479.417207);
  assert.equal(goParseDurationMs("bogus"), null);
});

test("ShortCooldownStore: mark/check/expiry/self-clean", () => {
  const store = new ShortCooldownStore();
  store.mark("auth1", "claude-sonnet-4-6", 1_000, 60_000);
  assert.ok(store.isInCooldown("auth1", "claude-sonnet-4-6", 2_000));
  assert.equal(store.remaining("auth1", "claude-sonnet-4-6", 2_000), 59_000);
  assert.ok(!store.isInCooldown("auth1", "other-model", 2_000));
  assert.ok(!store.isInCooldown("auth1", "claude-sonnet-4-6", 61_001));
  // expired entry cleans itself; re-check returns 0
  assert.equal(store.remaining("auth1", "claude-sonnet-4-6", 62_000), 0);
  store.mark("", "model", 0, 1000);
  store.mark("auth", "", 0, 1000);
  assert.ok(!store.isInCooldown("", "model", 0));
});

test("constants mirror Go thresholds", () => {
  assert.equal(INSTANT_RETRY_THRESHOLD_MS, 3_000);
  assert.equal(SHORT_COOLDOWN_THRESHOLD_MS, 300_000);
});
