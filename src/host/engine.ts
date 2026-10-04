// Engine — the heart of agy-proxy (charter §4). Translates one gateway model
// call into a short-lived `agy -p --output-format stream-json` child process,
// maps the NDJSON event stream onto StreamChunks, and binds sessions to agy
// conversations for multi-turn continuity. Only the trailing user messages
// become the prompt; earlier context rides agy-native history plus a digest
// prefix on first bind (ADR-7 discipline inherited from dsh-agy-link).
//
// Rewritten from dsh-agy-link src/host/adapter.ts @ 46984db: the dsh-llm
// LlmAdapter base class and its auxiliary-call machinery (compaction/title,
// allowAuxiliary, forwardSystemPrompt toggle) have no host in a standalone
// gateway. The public surface is a self-owned vocabulary: stream(EngineCall)
// -> AsyncIterable<StreamChunk>. Retained verbatim in behavior: ChunkQueue,
// buildArgs, buildDigest, sliding-window rate limit, account selection +
// autoFallbackModel, session binding (model switch / compaction invalidation),
// media staging, telemetry env, isolated HOMEs, per-account spawn spacing,
// settlement classification order, binding persistence/drop rules, steer
// preemption, duplicate submission debounce, and continuation spans.
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { mkdirSync } from 'node:fs'
import { randomUUID } from 'node:crypto'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import PQueue from 'p-queue'
import { Err, extractValidationUrl, hasDeniedActions, hasStructuredContent, looksLikeAuthFailure, looksLikeEligibilityBlocked, looksLikeHardRateLimit, looksLikeRateLimit, looksLikeValidationRequired, type GatewayConfig } from '../common/types.ts'
import { modelFamilyOf, type ManagedAccount, type ModelFamily } from '../common/pool-types.ts'
import type { AccountPoolManager } from './pool.ts'
import { diffConversations, snapshotConversations } from './discovery.ts'
import { EventMapper, usageFromRaw } from './mapper.ts'
import { parseMirrorCallId, type RunRecording, type RunRegistry } from './recording.ts'
import { defaultEffortFor, findEntry, ModelCatalog, resolveModelSlug } from './models.ts'
import { StreamJsonParser } from './parser.ts'
import { defaultMediaDir, stageImages, type ImageRefLike } from './media.ts'
import { isolatedHomeEnv, sanitizeChildEnv, startAgyProcess, type RunOutcome } from './runner.ts'
import { dataDir, stateDir } from '../common/config.ts'
import type { SessionStore } from './sessions.ts'
import { CallId, type StreamChunk, type TokenUsage } from './stream-types.ts'
import * as gatewayPatch from '../patch/gateway-patch.ts'
import { ShortCooldownStore } from '../patch/antigravity-429-classifier.ts'
import { MIRROR_TOOL_NAME } from './mirror.ts'
import {
  buildToolBridgePrompt,
  buildToolBridgeResponseSchema,
  needsToolBridgeFollowUp,
  parseToolBridgeAnswer,
  isRetryableToolBridgeError,
  resolveToolBridgePolicy,
  TOOL_BRIDGE_MAX_RETRIES,
  ToolBridgeError,
  type OpenAiFunctionTool,
  type OpenAiToolCall,
  type ToolBridgeMessage,
  type ToolBridgeResult,
} from '../patch/tool-bridge.ts'

/** Engine failure with a stable routing code (maps 1:1 onto the Err table). */
export class EngineError extends Error {
  readonly code: string
  /** Seconds the client should wait before retrying (POOL_EXHAUSTED → Retry-After). */
  readonly retryAfterSec?: number
  constructor(message: string, code: string, retryAfterSec?: number) {
    super(message)
    this.name = 'EngineError'
    this.code = code
    this.retryAfterSec = retryAfterSec
  }
}

/** Codes the service layer may retry once (ADR-11 discipline, engine-owned). */
export const RETRYABLE_CODES: readonly string[] = [Err.TIMEOUT, Err.PROCESS_EXIT, Err.INVALID_OUTPUT]

/** Retry policy constants for the service layer (inherited from ADR-11). */
export const RETRY_POLICY = {
  maxRetries: 1,
  initialDelayMs: 2_000,
  maxDelayMs: 10_000,
  jitterRatio: 0.1,
} as const

/** Jittered backoff for one retry: base ± jitterRatio (±10% of 2s ⇒ 1.8–2.2s),
 *  capped at maxDelayMs. Pure so tests can pin the bounds without sleeping. */
export function computeRetryDelayMs(
  baseMs: number,
  rand: () => number = Math.random,
  maxMs: number = RETRY_POLICY.maxDelayMs,
): number {
  const jittered = baseMs * (1 + (2 * rand() - 1) * RETRY_POLICY.jitterRatio)
  return Math.max(0, Math.min(maxMs, Math.round(jittered)))
}

/**
 * Prompts longer than this ride the stream-json stdin transport instead of the
 * `-p <prompt>` argv value: one argv entry is capped by MAX_ARG_STRLEN
 * (~128KB on Linux) and a long cron prompt dies with spawn E2BIG. The stdin
 * transport (the same one the tool bridge uses, verified against agy 1.6.5)
 * has no such ceiling. 100KB keeps a safe margin under it — the full argv
 * also carries flag words and the env block.
 */
export const STDIN_PROMPT_THRESHOLD = 100 * 1024

/** Default retry-delay implementation used when deps.retryDelay is absent. */
export function defaultRetryDelay(ms: number): Promise<void> {
  return new Promise((resolve) => {
    const t = setTimeout(resolve, computeRetryDelayMs(ms))
    t.unref()
  })
}

/**
 * Abort-aware, unref-ed sleep (A-M2/A-L5): resolves on timeout or as soon as
 * `signal` aborts, never holding the event loop open at shutdown. Callers
 * must re-check `signal.aborted` (and run liveness flags) after the wait.
 */
function sleepUnref(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const onAbort = () => {
      if (timer !== null) {
        clearTimeout(timer)
        timer = null
      }
      signal?.removeEventListener('abort', onAbort)
      resolve()
    }
    let timer: NodeJS.Timeout | null = setTimeout(() => {
      timer = null
      signal?.removeEventListener('abort', onAbort)
      resolve()
    }, Math.max(0, ms))
    timer.unref()
    if (signal?.aborted === true) onAbort()
    else signal?.addEventListener('abort', onAbort, { once: true })
  })
}

export interface EngineMessageImage {
  name?: string
  mediaType: 'image/png' | 'image/jpeg' | 'image/webp' | 'image/gif'
  bytes: number
}

export interface EngineMessage {
  role: 'user' | 'assistant' | 'system' | 'tool'
  text: string
  /** Images attached to a user message (staged to disk for agy). */
  images?: EngineMessageImage[]
  /** For role='tool': callId of the mirrored agy tool call this answers. */
  toolCallId?: string
  /**
   * Phase 3 tool-bridge: for role='assistant', the client-visible tool calls
   * that opened the turn. Carried into the bridge conversation view so the
   * model sees which functions were already requested (argument text stays a
   * JSON string — never re-parsed as data).
   */
  toolCalls?: readonly OpenAiToolCall[]
  /**
   * Provider that produced an assistant turn. Multi-model gateway sessions
   * mark foreign replies so the digest keeps their turns; unmarked assistant
   * messages are treated as foreign (agy's native history lacks them).
   */
  provider?: string
}

export interface EngineCall {
  model: string
  reasoningEffort?: string
  messages: readonly EngineMessage[]
  /** System prompt; always forwarded (prefixed as system instructions). */
  system?: string
  signal?: AbortSignal
  /** Gateway session key for binding + steer/debounce scoping. */
  sessionKey?: string
  /**
   * t_913f68e8 (A): stable per-conversation id (Hermes
   * `providers.<name>.session_affinity_header`) used ONLY for the agy
   * conversation binding + sticky account pinning. Never feeds steer/debounce
   * (that would preempt a sibling run sharing the conversation root). Empty =
   * fall back to sessionKey (byte-for-byte old behavior).
   */
  conversationKey?: string
  /** JSON schema enforcing the final result (--json-schema). */
  jsonSchema?: unknown
  /**
   * Phase 3 tool-bridge: client function definitions (OpenAI `tools[]`).
   * When present, the bridge takes over the prompt (definitions + conversation
   * packed into one agy prompt, envelope `--json-schema`) and the recut layer
   * unpacks the model's JSON answer into OpenAI `message.tool_calls`. See
   * src/patch/tool-bridge.ts.
   */
  tools?: readonly OpenAiFunctionTool[]
  /** OpenAI tool_choice ('auto' | 'none' | 'required' | {function:{name}}). */
  toolChoice?: unknown
  /** Per-call image byte reader; wins over deps.readImage (request-scoped). */
  readImage?: (ref: ImageRefLike) => Promise<Uint8Array | null>
  /**
   * Server-provided observability passthrough (request id, key id, protocol),
   * echoed verbatim in the settle-time onRun callback so a single enriched
   * hook can write the usage ledger for every spawn without per-route wiring.
   */
  meta?: Readonly<Record<string, unknown>>
}

export interface EngineDeps {
  getConfig: () => GatewayConfig
  catalog: ModelCatalog
  store: SessionStore
  pool?: AccountPoolManager
  bin: () => string | null
  /** A-M1: drops the wiring-level bin cache so a retry (or the next run)
   *  re-scans PATH. The engine fires it after any failed attempt — a spawn
   *  failure may stem from a stale/missing binary, and the per-attempt
   *  re-read seam only recovers if the cache lets go. */
  invalidateBin?: () => void
  /**
   * Fixed argv prefix inserted before the call's own arguments (after them
   * nothing is appended by the engine). Test harnesses point `bin` at the
   * node executable and pass the fake CLI script here; production leaves it
   * empty (the real agy binary takes the args directly).
   */
  binArgs?: readonly string[]
  /** Shared semaphore for cross-session concurrency. B3/P4: takes the call's
   *  AbortSignal — a parked waiter is rejected (ABORTED) on disconnect
   *  instead of occupying the queue until a slot frees up, so a pile-up of
   *  dead connections cannot starve live ones. The fast path (a free slot)
   *  ignores the signal: an already-disconnected call still acquires and
   *  hits the post-acquire check below. */
  acquire: (signal?: AbortSignal) => Promise<() => void>
  log?: (msg: string) => void
  /** Recordings shared with the agy_tool mirror (continuation spans). */
  runs: RunRegistry
  /** Last-run telemetry + usage-ledger input, fired once per actual agy
   *  spawn attempt (settle time — never for continuation spans, so tool
   *  round trips do not double count). With dispatch-level retries (M5) an
   *  attempt that will be retried fires with final=false; the host gates
   *  ledger/push bookkeeping on final=true because the ledger's
   *  INSERT OR IGNORE is first-wins — a failed attempt's row would otherwise
   *  swallow the retry's successful usage. Pre-flight throws
   *  (POOL_EXHAUSTED/BUSY/…) never fire it: no upstream consumption.
   *  failureMessage carries the terminal error text (schema v2 error_text). */
  onRun?: (info: {
    ok: boolean
    code: string
    /** 0-based spawn attempt within the logical run. */
    attempt: number
    /** true for the one attempt whose outcome settles the run. */
    final: boolean
    failureMessage?: string
    durationMs: number
    model: string
    /** Model slug actually driven (after fallback-model resolution). */
    providerModel: string
    usage: TokenUsage | null
    accountId?: string
    family?: string
    conversationId?: string
    meta?: Readonly<Record<string, unknown>>
  }) => void
  /** Retry backoff seam for dispatch-level retries (M5); tests inject a
   *  resolved promise to keep failing-run suites fast. Absent = the
   *  RETRY_POLICY jittered delay. */
  retryDelay?: (ms: number) => Promise<void>
  /**
   * Per-key model whitelist (M5): resolve a managed key id to its allowed
   * model ids, or null when the key has no whitelist (unrestricted). Absent
   * callback = the feature is off. The engine checks the model actually
   * SERVED (post-fallback resolution) and rejects with Err.MODEL_NOT_ALLOWED
   * (403, both protocols). The root key (keyId=null) must resolve null —
   * charter red line: the bootstrap key is unrestricted. Readonly: the
   * engine only compares membership — key-store hands back its frozen cache.
   */
  getScopes?: (keyId: string | null) => readonly string[] | null
  /** Reads image bytes from protocol-layer attachment storage. */
  readImage?: (ref: ImageRefLike) => Promise<Uint8Array | null>
  /**
   * Call-level override for readImage (see EngineCall.readImage): protocol
   * adapters stage request-scoped byte buffers without shared mutable state.
   */
  /** Called with each run's parser so the host keeps the last stdout ring for the doctor report. */
  onParser?: (parser: StreamJsonParser) => void
}

// ---- stream() -------------------------------------------------------------

class ChunkQueue {
  private chunks: StreamChunk[] = []
  /** A-L1: consumed-prefix cursor — shift() moved the whole array per chunk
   *  on 20k-event runs; the head index advances O(1) and the consumed prefix
   *  is compacted once per wake cycle instead. */
  private head = 0
  private wake: (() => void) | null = null
  private closed = false

  push(ch: StreamChunk): void {
    this.chunks.push(ch)
    this.wake?.()
    this.wake = null
  }

  close(): void {
    this.closed = true
    this.wake?.()
    this.wake = null
  }

  async *drain(): AsyncIterable<StreamChunk> {
    for (;;) {
      while (this.head < this.chunks.length) {
        const ch = this.chunks[this.head]
        this.head++
        if (ch !== undefined) yield ch
      }
      if (this.head > 0) {
        this.chunks.splice(0, this.head)
        this.head = 0
      }
      if (this.closed) return
      await new Promise<void>((resolve) => {
        this.wake = resolve
      });
    }
  }
}

function brief(s: string): string {
  const flat = s.trim().replace(/\s+/g, ' ')
  return flat.length > 300 ? flat.slice(0, 300) + '...' : flat
}

/** Whether one finished span cut on a completed tool step (A-M4): true = the
 *  client holds a mirror tool call and a continuation span may still resume
 *  the recording, so it must stay registered. */
interface SpanEnd {
  cutOnTool: boolean
}

/** Gateway provider id; assistant turns marked with it rode OUR agy history. */
const PROVIDER_ID = 'antigravity'

/**
 * Phase 3 tool-bridge recut: replace the RAW agy turn with the OpenAI-shaped
 * one. `rec` is the run recording; `events.slice(from)` are the events the raw
 * span streamed (all mapped chunks were buffered, nothing reached the client).
 *
 * Success → the chunks `block-start/text/text-delta/block-end … usage/finish`
 * with `finish.reason.kind='tool-calls'` when the model asked for functions.
 * Failure → a terminal error chunk (`invalid_output`, 502-class) — an answer
 * without structure is NEVER a silent 200.
 */
function* recutToolBridgeSpan(
  rec: RunRecording,
  rawOverride: string | Record<string, unknown> | undefined,
  policy: ReturnType<typeof resolveToolBridgePolicy>,
  model: string,
): Generator<StreamChunk> {
  const raw = rawOverride ?? lastResultResponse(rec, 0)
  const failure = (message: string): StreamChunk[] => [
    { type: 'usage', usage: { inputTokens: 0, outputTokens: 0 } },
    { type: 'finish', reason: { kind: 'error', failure: { message, code: Err.INVALID_OUTPUT } } },
  ]
  // Client-side bridge failures keep their own status through the recut:
  // overflow (413) and malformed tools[] (400) are NOT upstream 502-class
  // failures — the HTTP mapping reads the failure code off this chunk.
  const bridgeFailure = (err: ToolBridgeError): StreamChunk[] => [
    { type: 'usage', usage: { inputTokens: 0, outputTokens: 0 } },
    { type: 'finish', reason: { kind: 'error', failure: { message: err.message, code: err.code === 'prompt_overflow' ? Err.PROMPT_OVERFLOW : err.code === 'invalid_tools' ? Err.INVALID_TOOLS : Err.INVALID_OUTPUT } } },
  ]

  if (raw === undefined) {
    // No result envelope: the run failed upstream (quota/auth/process/empty).
    // Surfacing NOTHING here would produce an empty 200 — the exact silent
    // success the bridge must never return. Prefer the recording's own
    // failure (it carries the real cause + routing code); otherwise report
    // the missing structure.
    const f = rec.failureInfo
    if (f !== null) {
      yield { type: 'usage', usage: { inputTokens: 0, outputTokens: 0 } }
      yield { type: 'finish', reason: f.kind === 'aborted' ? { kind: 'aborted', failure: { message: f.message, code: f.code } } : { kind: 'error', failure: { message: f.message, code: f.code } } }
      return
    }
    yield* failure('tool bridge: agy produced no usable answer (no result envelope)')
    return
  }

  let turn: ToolBridgeResult
  try {
    turn = parseToolBridgeAnswer(raw, policy, model)
  } catch (err) {
    if (err instanceof ToolBridgeError && err.code !== 'invalid_output') {
      yield* bridgeFailure(err)
      return
    }
    yield* failure(err instanceof ToolBridgeError ? err.message : 'tool bridge: ' + brief(String(err)))
    return
  }

  const usage: TokenUsage = usageFromRaw(rec.finalUsage(rec.getResultRawUsage() ?? {}))
  let idx = 0
  if (turn.content !== '') {
    yield { type: 'block-start', index: idx, blockType: 'text' }
    yield { type: 'text-delta', index: idx, text: turn.content }
    yield { type: 'block-end', index: idx, block: { type: 'text', text: turn.content } }
    idx++
  }
  for (const call of turn.toolCalls) {
    yield { type: 'block-start', index: idx, blockType: 'tool-call' }
    yield {
      type: 'block-end',
      index: idx,
      block: { type: 'tool-call', id: CallId(call.id), name: call.function.name, arguments: call.function.arguments },
    }
    idx++
  }
  yield { type: 'usage', usage }
  yield {
    type: 'finish',
    reason: turn.toolCalls.length > 0 ? { kind: 'tool-calls' } : { kind: 'stop' },
  }
}

/** The last result envelope's answer among `events[from..]`: the structured
 *  envelope object (agy >=1.6 `result.structured_output`, preferred when the
 *  bridge schema is in play) or the plain `response` text. */
function lastResultResponse(rec: RunRecording, from: number): string | Record<string, unknown> | undefined {
  for (let i = rec.length - 1; i >= from; i--) {
    const ev = rec.eventAt(i)
    if (ev === undefined || ev.kind !== 'result') continue
    if (hasStructuredContent(ev.structuredOutput)) {
      return typeof ev.structuredOutput === 'string'
        ? ev.structuredOutput
        : (ev.structuredOutput as Record<string, unknown>)
    }
    if (ev.response.trim() !== '') return ev.response
  }
  return undefined
}

/**
 * Phase 3 tool-bridge retry loop: when the recut failed on a STRUCTURED
 * shape problem, ask agy up to TOOL_BRIDGE_MAX_RETRIES more times (a fresh
 * spawn on the same prompt) before the client sees anything — retries stay
 * invisible by construction (the raw chunks were buffered, only the final
 * recut is emitted). Returns the successful raw answer or the last error; the
 * recording keeps the LAST attempt only when it produced a usable answer, so
 * the caller can recut from the fresh events.
 *
 * Contract on the `undefined` input: NO retry is issued (the first guard
 * returns it verbatim). That is deliberate — the caller only reaches the recut
 * with `undefined` once the run has settled without an envelope (the wait for
 * settle runs first, t_03a5ca20), so there is nothing left to respawn against.
 */
async function respawnForToolBridge(
  engine: AgyEngine,
  rec: RunRecording,
  policy: ReturnType<typeof resolveToolBridgePolicy>,
  model: string,
  family: ModelFamily,
  firstRaw: string | Record<string, unknown> | undefined,
): Promise<string | Record<string, unknown> | undefined> {
  let raw = firstRaw
  const isBlank = (v: string | Record<string, unknown> | undefined): boolean => typeof v === 'string' && v.trim() === ''
  for (let attempt = 0; attempt < TOOL_BRIDGE_MAX_RETRIES; attempt++) {
    let parsed: ToolBridgeResult | undefined
    let err: unknown
    try {
      parsed = raw === undefined || isBlank(raw)
        ? undefined
        : parseToolBridgeAnswer(raw, policy, model)
    } catch (e) {
      err = e
    }
    if (parsed !== undefined) return raw
    // An EMPTY answer is the probabilistic headless shape too (agy burned the
    // turn on an auto-denied native tool and finished with an empty response
    // while reporting SUCCESS): retry it exactly like a broken envelope, so
    // the client gets a real attempt instead of an immediate 502.
    const emptyAnswer = raw !== undefined && isBlank(raw)
    if (raw === undefined || (!emptyAnswer && !isRetryableToolBridgeError(err))) return raw
    const fresh = await engine.respawnToolBridgeAttempt(rec, family)
    if (fresh === undefined) return raw
    raw = fresh
  }
  return raw
}

function isForeignAssistant(m: EngineMessage): boolean {
  if (m.role !== 'assistant') return false
  return m.provider !== PROVIDER_ID
}

function sawAuthFailure(parser: StreamJsonParser, outcome: { stderrTail: string; stdout: string }): boolean {
  if (parser.stats.sawAuthFailure) return true
  return looksLikeAuthFailure(outcome.stderrTail) || looksLikeAuthFailure(outcome.stdout.slice(0, 4000))
}

/**
 * Project the engine's message list into the tool-bridge conversation view.
 * Assistant turns keep their tool calls (so the model sees what it already
 * asked for), tool turns keep the answered call id, and any client tool
 * result that arrived WITHOUT the mirror-native `role:'tool'` shape is folded
 * into an explicit `[tool result …]` user turn — otherwise a client that
 * follows its own continuation shape would dead-end against the gateway's
 * mirror-cursor rule.
 */
export function toolBridgeMessages(messages: readonly EngineMessage[]): ToolBridgeMessage[] {
  const out: ToolBridgeMessage[] = []
  for (const m of messages) {
    if (m.role === 'tool') {
      out.push({
        role: 'tool',
        text: m.text,
        ...(m.toolCallId !== undefined ? { toolCallId: m.toolCallId } : {}),
      })
      continue
    }
    if (m.role === 'assistant') {
      out.push({
        role: 'assistant',
        text: m.text,
        ...(m.toolCalls !== undefined ? { toolCalls: m.toolCalls } : {}),
      })
      continue
    }
    out.push({ role: m.role, text: m.text })
  }
  return out
}

/** Rolling digest of turns this agy conversation has not seen (ADR-7). */
export function buildDigest(messages: readonly EngineMessage[], fromIdx: number, maxChars: number): string {
  const parts: string[] = []
  let budget = maxChars
  for (let i = messages.length - 1; i >= fromIdx; i--) {
    const m = messages[i]
    if (m === undefined || m.role === 'system') continue
    if (m.text === '') continue
    const line = (m.role === 'user' ? 'User: ' : 'Assistant: ') + m.text
    if (line.length > budget) {
      parts.unshift(line.slice(0, Math.max(0, budget)))
      break
    }
    budget -= line.length
    parts.unshift(line)
  }
  if (parts.length === 0) return ''
  return '[conversation so far]\n' + parts.join('\n\n') + '\n[end of conversation so far]\n\n'
}/** Width/height are unknown at the protocol boundary; agy only uses the path. */
const UNKNOWN_DIM = 0

export class AgyEngine {
  /** sessionKey -> in-flight run, for steer-time preemption. */
  private readonly activeRuns = new Map<string, RunRecording>()
  /** sessionKey -> prompt info for duplicate submission debounce */
  private readonly activeSessionPrompts = new Map<string, { prompt: string; startedAt: number }>()
  /**
   * t_913f68e8 (C): conversationKey -> pinned account id. Keeps one
   * conversation on one account for its whole life so the agy conversation
   * binding survives (account rotation inside a turn was resetting it to an
   * 8k digest and silently truncating context). Cleared organically when the
   * pinned account stops being a viable candidate — then normal sticky
   * selection takes over.
   */
  private readonly conversationAccount = new Map<string, string>()
  /** accountId -> timestamp of last spawn for spacing throttling */
  private readonly lastAccountSpawnTime = new Map<string, number>()
  private readonly minSpawnIntervalMs = 500
  /** Global sliding window timestamps for batch rate-limiting protection */
  private readonly requestTimestamps: number[] = []
  /** accountId -> serial queue (concurrency:1, charter §6) around the agy
   *  spawn→outcome segment: two concurrent requests on the same sticky account
   *  must not interleave on one conversation binding. Continuations bypass the
   *  queue (no spawn). Inactive without a pool. */
  private readonly accountQueues = new Map<string, PQueue>()
  /** Accounts with in-flight spawns, tracked ENGINE-LEVEL until each request's
   *  dispatch finally: selectAccount skips these so concurrent same-family
   *  arrivals spread across the pool (M5 acceptance ≥2.5×). Per-call tracking
   *  was the original M5 bug — each request only ever saw its own account, so
   *  the busy filter filtered nothing. */
  private readonly busyAccounts = new Set<string>()
  private readonly inFlightCount = new Map<string, number>()
  /**
   * Phase 3 tool-bridge retry restart info: the argv/env/cwd of the LAST
   * bridge spawn so respawnToolBridgeAttempt() can re-run the SAME prompt
   * once when the model's answer had no usable structure. Cleared by the
   * non-bridge path so a retry can never replay an unrelated call.
   */
  private bridgeRestart: { args: string[]; env: NodeJS.ProcessEnv; cwd: string; stdinData?: string; signal?: AbortSignal } | undefined
  /**
   * Live agy child processes spawned by this engine. The tool-bridge retry
   * must never add a second concurrent process, and the per-request busy MARK
   * is not a usable proxy (it is held by the very request doing the retry).
   */
  private liveAgyChildren = 0

  /**
   * CLIProxyAPI short-cooldown store (gateway-patch 429 integration): the
   * account|model windows for `short_cooldown_switch_auth` rate limits —
   * lighter than a full recordFailure cooldown, consulted alongside the pool
   * cooldowns so a <5m limit window switches auth instead of freezing the
   * account out of rotation for the blunt 15m default.
   */
  private readonly shortCooldowns = new ShortCooldownStore()

  constructor(private readonly deps: EngineDeps) {}

  /**
   * Full 429-integration gate (replaces the blunt looksLikeHardRateLimit
   * cooldown switch). `decideEngineCooldown` refines the hard-limit signal
   * into two action classes; the engine additionally honors the classifier's
   * instant-retry / short-cooldown kinds, which the blunt gate conflated:
   *  - full_cooldown  (full_quota_exhausted / >=5m windows): the account
   *    leaves rotation via pool.recordFailure (the default 15m cooldown, or
   *    the upstream reset time when the error text carries one).
   *  - short_cooldown (<5m windows): a ShortCooldownStore window keyed by
   *    account|model marks the account unavailable for just that window, and
   *    `switchAuth: true` tells the retry loop to move to the next account.
   * Soft signals (model overloaded) still never cool an account down.
   */
  private applyRateLimitCooldown(accountId: string, family: ModelFamily, rawErrText: string): { switchAuth: boolean } {
    const verdict = gatewayPatch.decideEngineCooldown(rawErrText)
    if (verdict.kind === 'full_cooldown') {
      // The pool parses its own reset duration out of the reason text
      // ("Resets in 21m25s") — pass the raw signature through unchanged.
      this.deps.pool?.recordFailure(accountId, family, rawErrText)
      return { switchAuth: true }
    }
    if (verdict.kind === 'instant_retry') {
      // The upstream window is under 3s — no cooldown of any kind; the same
      // auth is good to go again immediately (the pool never learns of it).
      return { switchAuth: false }
    }
    // short_cooldown: mark a <5m window and switch accounts for the next
    // selection (keyed account|family, self-cleaning after the window).
    this.shortCooldowns.mark(accountId, family, Date.now(), verdict.cooldownMs)
    return { switchAuth: true }
  }

  /**
   * Selection filter seam: folds the pool's own cooldown/quota skip logic
   * with the engine's short-cooldown store (a <5m window must read as "in
   * cooldown" even though the pool itself never recorded one).
   */
  private selectAccountFor(family: ModelFamily, busy?: ReadonlySet<string>, preferred?: string): ManagedAccount | null | undefined {
    const pool = this.deps.pool
    if (!pool) return undefined
    const acc = pool.selectAccount(family, busy, preferred)
    if (acc === null || !this.shortCooldowns.isInCooldown(acc.id, family, Date.now())) return acc
    // The picked account sits in a short-cooldown window: exclude it (as
    // busy) and re-select so the request switches auth within the same call.
    // When the pool is exhausted this falls through to the busy-less
    // selection (same tradeoff as the busy filter) and may hand the same
    // account back — the request still routes.
    const exclude = new Set(busy ?? [])
    exclude.add(acc.id)
    const next = pool.selectAccount(family, exclude)
    if (next !== null && next.id !== acc.id && !this.shortCooldowns.isInCooldown(next.id, family, Date.now())) {
      return next
    }
    return acc
  }

  private trackBusy(id: string | undefined): void {
    if (id === undefined) return
    this.inFlightCount.set(id, (this.inFlightCount.get(id) ?? 0) + 1)
    this.busyAccounts.add(id)
  }

  private untrackBusy(id: string | undefined): void {
    if (id === undefined) return
    const left = (this.inFlightCount.get(id) ?? 1) - 1
    if (left <= 0) {
      this.inFlightCount.delete(id)
      this.busyAccounts.delete(id)
    } else {
      this.inFlightCount.set(id, left)
    }
  }

  /**
   * Earliest reset signal for a family across the pool: min over live cooldown
   * clocks and future quota reset times (5h + weekly). Feeds the
   * POOL_EXHAUSTED message and its Retry-After; null = every clock unknown.
   */
  private earliestResetMs(family: ModelFamily): number | null {
    const pool = this.deps.pool
    if (!pool) return null
    let earliest: number | null = null
    const consider = (isoMs: number | null): void => {
      if (isoMs !== null && (earliest === null || isoMs < earliest)) earliest = isoMs
    }
    const countdown = pool.getEarliestResetCountdown(family)
    if (countdown !== null) consider(Date.now() + countdown)
    for (const acc of pool.getAccounts()) {
      if (!acc.enabled) continue
      const fq = acc.quotas?.[family]
      if (fq?.resetTime) {
        const t = Date.parse(fq.resetTime)
        if (Number.isFinite(t) && t > Date.now()) consider(t)
      }
      if (fq?.weeklyResetTime) {
        const t = Date.parse(fq.weeklyResetTime)
        if (Number.isFinite(t) && t > Date.now()) consider(t)
      }
    }
    return earliest !== null ? Math.max(0, earliest - Date.now()) : null
  }

  /** Build the agy argv for one call. Exported for tests. */
  buildArgs(opts: {
    prompt: string
    model: string
    effort?: string
    conversationId?: string
    permissionMode: GatewayConfig['permissionMode']
    timeoutMs: number
    extraArgs: readonly string[]
    addDirs?: readonly string[]
    printTimeoutMinutes?: number
  }): string[] {
    const ptMins = opts.printTimeoutMinutes ?? Math.max(1, Math.ceil(opts.timeoutMs / 60_000))
    const args: string[] = ['--output-format', 'stream-json', '--print-timeout', ptMins + 'm']
    if (opts.permissionMode === 'skip') args.push('--dangerously-skip-permissions')
    else args.push('--mode', opts.permissionMode)
    const effectiveModel = resolveModelSlug(opts.model)
    if (effectiveModel !== '') args.push('--model', effectiveModel)
    const isGemini = effectiveModel === '' || effectiveModel.toLowerCase().startsWith('gemini')
    if (isGemini && opts.effort && opts.effort !== '') args.push('--effort', opts.effort)
    if (opts.conversationId) args.push('--conversation', opts.conversationId)
    for (const d of opts.addDirs ?? []) args.push('--add-dir', d)
    args.push(...opts.extraArgs)
    args.push('-p', opts.prompt)
    return args
  }

  async *stream(call: EngineCall): AsyncIterable<StreamChunk> {
    const cfg = this.deps.getConfig()
    const bin = this.deps.bin()
    if (!bin) throw new EngineError('agy binary not found on PATH — install it via https://antigravity.google/docs/cli/install', Err.AGY_NOT_INSTALLED)
    const sessionKey = call.sessionKey !== undefined ? String(call.sessionKey) : ''
    // t_913f68e8 (A): a stable per-conversation id from Hermes' opt-in
    // `session_affinity_header` rides EngineCall.conversationKey. It keys the
    // agy conversation binding so rotation of the pool account no longer
    // resets the binding to an 8k digest (the observed in= 152k→18k collapse).
    const convKey = call.conversationKey !== undefined && call.conversationKey !== '' ? String(call.conversationKey) : ''
    // Tenant scoping (S-H2): the gateway never sets sessionKey, so without a
    // tenant dimension the conversation binding below degrades to
    // `:<accountId>` — shared by EVERY caller of that account, letting one
    // key's chat continue another key's agy conversation (and collide staged
    // media files). meta.keyId (set by withCallMeta; absent = bootstrap root
    // key) namespaces both.
    const metaTenant = (call.meta ?? {}) as { keyId?: unknown }
    const tenantKey = typeof metaTenant.keyId === 'string' && metaTenant.keyId !== '' ? metaTenant.keyId : 'root'
    // Steer/debounce maps get the same tenant prefix: an identical prompt from
    // another key must neither be debounced nor preempt this run. Gateway
    // calls never set sessionKey, so these stay off for them (empty key).
    const steerKey = sessionKey !== '' ? `${tenantKey}:${sessionKey}` : ''

    // Workspace precedence: explicit config > <dataDir>/workspace. Unlike the
    // upstream adapter there is no process.cwd() fallback — a gateway has no
    // meaningful cwd, and silently running agy in the server's install
    // directory would be a wrong-workspace write path.
    let workspaceRoot = cfg.workspaceRoot
    if (workspaceRoot === '') {
      workspaceRoot = join(dataDir(), 'workspace')
      ensureWorkspaceDir(this.deps.log, workspaceRoot)
    }

    // ---- native tool mirroring: continuation spans (v1) ----
    // When the previous span cut on a completed agy tool step, the client
    // received the mirror tool call (and its replayed output); it is now
    // calling us again to continue the SAME run. Resume the recording from
    // the cursor encoded in the trailing tool-result callId — no new process,
    // no prompt assembly, no digest.
    const continuation = detectContinuation(call.messages)
    if (continuation !== null) {
      const rec = this.deps.runs.get(continuation.runId)
      if (rec === undefined) {
        yield { type: 'usage', usage: { inputTokens: 0, outputTokens: 0 } }
        yield {
          type: 'finish',
          reason: {
            kind: 'error',
            failure: {
              message: 'agy run ' + continuation.runId + ' is no longer available (server restarted?) — please resend your message',
              code: Err.AGY_ERROR,
            },
          },
        }
        return
      }
      const spanEnd = yield* this.driveSpan(rec, continuation.eventIndex + 1, true)
      // A-M4 lifecycle: a span that did NOT cut on a tool step ends the
      // client's interest in this recording — drop it once the run settled.
      rec.keepForContinuation = spanEnd.cutOnTool
      if (!rec.keepForContinuation && rec.isSettled) this.deps.runs.forget(rec.runId)
      return
    }
    // Mid-turn steer preemption: the session sends a new prompt while a run
    // is still alive (a NEW run, not a continuation). The previous run's agy
    // process would keep appending to the SAME conversation concurrently —
    // abort it first.
    if (steerKey !== '') {
      const prev = this.activeRuns.get(steerKey)
      if (prev !== undefined && !prev.isSettled) {
        if (prev.requestAbort != null) prev.requestAbort()
        // A queued (not yet spawned) run has no process to abort — settle it
        // so the per-account queue task skips the spawn entirely.
        else prev.settle({ kind: 'aborted', code: 'ABORTED', message: 'superseded by a newer prompt on the same session' })
      }
    }
    const catalog = this.deps.catalog.get()
    const rawModel = call.model
    const model = resolveModelSlug(rawModel)
    const entry = findEntry(catalog, model)
    const isGemini = (model === '' ? cfg.defaultModel : model).toLowerCase().startsWith('gemini')
    // The catalog is advisory (the fallback list may be stale): accept unknown
    // ids, but validate explicit reasoning efforts against known entries.
    let effort: string | undefined
    if (isGemini) {
      if (call.reasoningEffort !== undefined) {
        const wanted = String(call.reasoningEffort)
        if (entry && entry.efforts === null) {
          throw new EngineError('model ' + model + ' has no selectable reasoning efforts', Err.UNSUPPORTED_REASONING_EFFORT)
        }
        if (entry && entry.efforts && !entry.efforts.includes(wanted)) {
          throw new EngineError('reasoning effort ' + wanted + ' is not supported by ' + model, Err.UNSUPPORTED_REASONING_EFFORT)
        }
        effort = wanted
      } else if (entry && entry.efforts) {
        effort = defaultEffortFor(entry, cfg)
      }
    }

    // ---- prompt assembly (digest discipline, upstream ADR-7) ----
    const messages = call.messages
    let lastAssistantIdx = -1
    for (let i = messages.length - 1; i >= 0; i--) {
      const mm = messages[i]
      if (mm !== undefined && mm.role === 'assistant') {
        lastAssistantIdx = i
        break
      }
    }
    const trailingUser = messages.slice(lastAssistantIdx + 1).filter((m) => m.role === 'user')

    // Sliding-window rate limit protection per minute (gateway batch stability)
    if (cfg.rateLimitPerMinute > 0) {
      const now = Date.now()
      const windowStart = now - 60_000
      while (this.requestTimestamps.length > 0 && this.requestTimestamps[0]! < windowStart) {
        this.requestTimestamps.shift()
      }
      if (this.requestTimestamps.length >= cfg.rateLimitPerMinute) {
        const delayMs = this.requestTimestamps[0]! + 60_000 - now
        if (delayMs > 0) {
          await sleepUnref(delayMs, call.signal)
        }
      }
      if (call.signal?.aborted ?? false) {
        // Nothing is held yet (no busy mark, no semaphore slot, no window
        // entry): fail the aborted call before it consumes further work.
        throw new EngineError('request aborted before dispatch', 'ABORTED')
      }
      this.requestTimestamps.push(Date.now())
    }

    let activeModel = model === '' ? cfg.defaultModel : model
    let family = modelFamilyOf(activeModel)
    // Accounts with in-flight spawns (tracked at ENGINE level until settle) are
    // skipped by selectAccount so concurrent requests spread across the pool
    // (M5: acceptance §4 互不阻塞 / ≥2.5× throughput). Busy-but-healthy pools
    // fall through to the unfiltered selection inside the pool.
    let account = this.selectAccountFor(family, this.busyAccounts, convKey !== '' ? this.conversationAccount.get(convKey) : undefined)
    if (this.deps.pool && this.deps.pool.getAccounts().length > 0 && !account) {
      if (cfg.autoFallbackModel) {
        const fallbackSlugs = ['gemini-3.5-flash', 'gemini-3.6-flash']
        for (const fb of fallbackSlugs) {
          const fbFam = modelFamilyOf(fb)
          const fbAcc = this.selectAccountFor(fbFam)
          if (fbAcc) {
            account = fbAcc
            family = fbFam
            activeModel = fb
            break
          }
        }
      }
      if (!account) {
        // MA4 / M3 DoD: an exhausted pool is a rate-limit condition (429 +
        // Retry-After), not an upstream failure (502). The countdown covers
        // both cooldown clocks and future quota reset times.
        const countdownMs = this.earliestResetMs(family)
        const waitStr = countdownMs !== null ? ` (earliest reset in ${Math.ceil(countdownMs / 1000)}s)` : ''
        throw new EngineError(
          `All Antigravity accounts in pool are in cooldown for ${family}${waitStr}. Add an account or wait for reset.`,
          Err.POOL_EXHAUSTED,
          countdownMs !== null ? Math.max(1, Math.ceil(countdownMs / 1000)) : undefined,
        )
      }
    }

    // ---- per-key model whitelist (M5): the key's scopes constraint applies
    // to the model ACTUALLY SERVED — after fallback resolution, since the
    // fallback switch silently redirects the request — not to the request's
    // model label. Root (keyless) requests and an absent callback bypass. ----
    const getScopes = this.deps.getScopes
    if (getScopes !== undefined) {
      const metaScopes = (call.meta ?? {}) as { keyId?: unknown }
      const scopesKey = typeof metaScopes.keyId === 'string' && metaScopes.keyId !== '' ? metaScopes.keyId : null
      const allowedModels = getScopes(scopesKey)
      const servingModel = activeModel === '' ? cfg.defaultModel : activeModel
      if (allowedModels !== null && allowedModels.length > 0 && !allowedModels.includes(servingModel)) {
        throw new EngineError(
          `model ${servingModel} is not in this key's allowed model list — patch the key scopes in the admin UI (MODEL_NOT_ALLOWED)`,
          Err.MODEL_NOT_ALLOWED,
        )
      }
    }

    // t_913f68e8 (A): with a stable conversation key the binding is keyed on
    // the conversation ALONE (no account dimension), so it survives a forced
    // account switch. Without it the old `tenant:session:account` shape is
    // kept byte-for-byte (empty sessionKey → `root::<accountId>`).
    const sessionAccountKey = convKey !== ''
      ? `${tenantKey}:${convKey}`
      : (account
        ? `${tenantKey}:${sessionKey}:${account.id}`
        : (sessionKey !== '' ? `${tenantKey}:${sessionKey}` : ''))
    let binding = sessionAccountKey !== '' ? this.deps.store.get(sessionAccountKey) : undefined

    // Model switch detection: If model changed in the session, drop stale agy conversation binding
    const currentModel = activeModel === '' ? cfg.defaultModel : activeModel
    if (binding !== undefined && binding.model && binding.model !== currentModel) {
      if (sessionAccountKey !== '') this.deps.store.delete(sessionAccountKey)
      binding = undefined
    }

    // Compaction detection: If the client truncated history or cleared earlier
    // turns, messages.length drops below the recorded watermark. Invalidate the
    // stale agy conversation binding so a clean agy session is started and
    // seeded with a digest of the truncated history.
    if (binding !== undefined && messages.length < binding.lastMessageCount) {
      if (sessionAccountKey !== '') this.deps.store.delete(sessionAccountKey)
      binding = undefined
    }

    let prompt = ''
    const trailingText = trailingUser.map((m) => m.text).filter((s) => s !== '')
    prompt = trailingText.join('\n\n')
    // Digest budget (upstream default; the gateway config surface has no
    // dedicated knob — 8k chars covers ~4k tokens of context preamble).
    const digestMaxChars = 8_000
    if (binding === undefined && lastAssistantIdx >= 0) {
      // First contact: bring agy up to speed with a bounded digest.
      prompt = buildDigest(messages, 0, digestMaxChars) + prompt
    } else if (binding !== undefined) {
      // Returning session: digest only the foreign turns since our
      // watermark (the client may have talked to another model in between).
      // Our own agy replies ride the native conversation history, and the
      // trailing user run is already the live prompt above.
      const from = Math.min(binding.lastMessageCount, messages.length)
      const end = Math.max(from, lastAssistantIdx + 1)
      const span = messages
        .slice(from, end)
        .filter((m) => m.role !== 'assistant' || isForeignAssistant(m))
      if (span.some((m) => m.role === 'assistant')) {
        prompt = buildDigest(span, 0, digestMaxChars) + prompt
      }
    }
    if (call.system) {
      // The gateway ALWAYS forwards system prompts (no host-level toggle).
      prompt = 'System instructions:\n' + call.system + '\n\n' + prompt;
    }

    // ---- Phase 3 tool-bridge: client function calling ----
    // The client's own tools[] are NOT forwarded as agy tools. Instead the
    // definitions + the whole conversation are packed into ONE prompt and the
    // structured answer is requested through --json-schema (see
    // src/patch/tool-bridge.ts). The JSON envelope reaches `recut()` below,
    // which unpacks it into OpenAI tool_calls. A client that already has
    // mirror-native tool definitions is left on the native loop (parallel_tool
    // tools like agy_tool) — the bridge only serves function-shaped tools.
    const bridgeTools = (call.tools ?? []).filter((t) => t.function?.name !== MIRROR_TOOL_NAME)
    let bridgePolicy: ReturnType<typeof resolveToolBridgePolicy> | null = null
    if (bridgeTools.length > 0 || call.toolChoice !== undefined) {
      bridgePolicy = resolveToolBridgePolicy(bridgeTools, call.toolChoice, toolBridgeMessages(messages))
      if (bridgePolicy.allowedNames.length > 0 || bridgePolicy.tools.length > 0) {
        prompt = buildToolBridgePrompt(bridgePolicy, toolBridgeMessages(messages), call.system)
      } else {
        // tool_choice:'none' — nothing to offer; keep the plain prompt and let
        // the model answer as text.
        bridgePolicy = null
      }
    }

    // ---- multimodal staging: images ride as staged files ----
    let stagedDirs: string[] = []
    const imageRefs: ImageRefLike[] = []
    for (const m of trailingUser) {
      for (const img of m.images ?? []) {
        // width/height are not carried over the wire; agy only reads the file.
        imageRefs.push({
          attachmentId: img.name ?? 'image',
          mediaType: img.mediaType,
          bytes: img.bytes,
          width: UNKNOWN_DIM,
          height: UNKNOWN_DIM,
          ...(img.name !== undefined ? { name: img.name } : {}),
        })
      }
    }
    // M5 busy-spread: the mark is tracked synchronously with selection —
    // tracking only after the await-ed semaphore acquire let concurrent
    // same-tick arrivals all select from an empty busy set and pile onto
    // one account. H1 (no leak) is preserved by the try/catch around the
    // throw-capable pre-dispatch segment: its catch releases the mark, and
    // post-acquire untracking stays in the dispatch IIFE's finally.
    this.trackBusy(account?.id)
    let args: string[] = []
    let schemaCleanup: (() => Promise<void>) | null = null
    let release: () => void = () => undefined
    try {
      if (imageRefs.length > 0) {
        // Call-level reader wins: protocol adapters stage request-scoped
        // byte buffers (data: URLs) without shared mutable state.
        const readImage = call.readImage ?? this.deps.readImage
        if (readImage) {
          const dir = cfg.mediaDir !== '' ? cfg.mediaDir : defaultMediaDir(stateDir())
          // Tenant-prefixed + per-request random suffix (S-H2a): the old
          // `anon-<msgLen>` key was deterministic across callers — two users
          // sending the same message count overwrote each other's staged
          // files (cross-tenant image swap). Within one request the index
          // suffix stays deterministic, so a retried read reuses its own file.
          const key = tenantKey.replace(/[^a-zA-Z0-9_-]+/g, '_') + '-' + messages.length + '-' + randomUUID()
          const res = await stageImages({
            dir,
            key,
            images: imageRefs,
            readImage,
            maxImages: cfg.mediaMaxImages,
            maxBytes: cfg.mediaMaxBytes,
          })
          if (res.promptSuffix !== '') {
            prompt = prompt === ''
              ? (res.promptSuffix + '\n\n[Please inspect the attached image(s) using view_file and assist the user.]')
              : (prompt + '\n\n' + res.promptSuffix)
          }
          if (res.staged.length > 0) stagedDirs = [dir]
        }
      }
      if (prompt.trim() === '') {
        throw new EngineError('request carries no user text or images to forward to agy', Err.AGY_ERROR)
      }

      // In-flight duplicate submission debounce (prevents double-clicks / network repeat loops)
      if (steerKey !== '') {
        const activePrompt = this.activeSessionPrompts.get(steerKey)
        if (activePrompt !== undefined && activePrompt.prompt === prompt && Date.now() - activePrompt.startedAt < 3000) {
          throw new EngineError(
            'Duplicate request ignored: an identical request is already running for this session.',
            Err.BUSY,
          )
        }
        this.activeSessionPrompts.set(steerKey, { prompt, startedAt: Date.now() })
      }

      args = this.buildArgs({
        prompt,
        model: activeModel === '' ? cfg.defaultModel : activeModel,
        effort,
        conversationId: binding !== undefined ? binding.conversationId : undefined,
        permissionMode: cfg.permissionMode,
        timeoutMs: cfg.timeoutMs,
        printTimeoutMinutes: Math.max(240, Math.ceil(cfg.timeoutMs / 60_000)),
        extraArgs: cfg.extraArgs,
        addDirs: stagedDirs,
      })
      // Phase 3 tool-bridge: the bridge turn is a pure text→JSON conversion.
      // Two structural differences from a normal one-shot (both verified live
      // against agy 1.6.5 and mirrored from the star-cliproxy reference
      // tool-bridge-provider.buildAgyArgs):
      //  1. The prompt rides stdin as ONE NDJSON `{event:'user'}` event
      //     (`--input-format stream-json`) instead of a `-p <prompt>` argv
      //     value — same transport the reference uses, and it lifts the
      //     ARG_MAX/E2BIG ceiling off long prompts.
      //  2. The envelope schema is passed INLINE (`--json-schema '<json>'`),
      //     not as a temp file. With `-p`+file the model burned its turn on a
      //     native run_command (denied headless) and returned
      //     response:"" with no structured_output; the stdin+inline-schema
      //     form returns the envelope in result.structured_output.
      // `--sandbox` keeps native tools structurally off as a second guard.
      let stdinData: string | undefined
      if (bridgePolicy !== null && bridgePolicy.allowedNames.length > 0) {
        const envelopeSchema = gatewayPatch.sanitizeOutgoingSchema(
          call.jsonSchema !== undefined && call.jsonSchema !== null
            ? call.jsonSchema
            : buildToolBridgeResponseSchema(bridgePolicy),
        )
        // Rebuild the argv for stream-json: drop the `-p <prompt>` pair the
        // shared builder appended, drop any schema-file args already added.
        const stripped: string[] = []
        for (let i = 0; i < args.length; i++) {
          const a = args[i]
          if (a === '-p' || a === '--print') {
            i++ // skip the prompt value that follows
            continue
          }
          if (a === '--json-schema') {
            i++ // skip the schema-file path that follows
            continue
          }
          if (a === '--input-format' || a === '--output-format') {
            i++ // rewritten below
            continue
          }
          stripped.push(a as string)
        }
        args = [
          '--input-format', 'stream-json',
          '--output-format', 'stream-json',
          '--json-schema', JSON.stringify(envelopeSchema),
          ...stripped,
          '--sandbox',
        ]
        stdinData = JSON.stringify({ event: 'user', message: { role: 'user', content: prompt } }) + '\n'
      } else if (prompt.length >= STDIN_PROMPT_THRESHOLD) {
        // E2BIG guard (primary cause 6): the `-p <prompt>` argv value is one
        // entry, capped by MAX_ARG_STRLEN (~128KB on Linux) — a long cron
        // prompt dies with spawn E2BIG. Reroute ONLY the transport to the
        // same stream-json/stdin form the bridge uses (`--input-format
        // stream-json`, one `{event:'user'}` NDJSON line on stdin — the mode
        // verified live against agy 1.6.5), keeping plain-prompt semantics:
        // no envelope schema, no --sandbox, model/effort/conversation flags
        // untouched. Prompts under the threshold keep the argv form.
        const stripped: string[] = []
        for (let i = 0; i < args.length; i++) {
          const a = args[i]
          if (a === '-p' || a === '--print') {
            i++ // drop the prompt value that follows
            continue
          }
          if (a === '--input-format') {
            i++ // rewritten here — defensive; buildArgs never adds it
            continue
          }
          stripped.push(a as string)
        }
        args = [
          '--input-format', 'stream-json',
          ...stripped,
        ]
        stdinData = JSON.stringify({ event: 'user', message: { role: 'user', content: prompt } }) + '\n'
      }
      // A client-supplied jsonSchema on a NON-bridge request keeps the file
      // form (unchanged upstream behavior).
      if (bridgePolicy === null && call.jsonSchema !== undefined && call.jsonSchema !== null) {
        const dir = await mkdtemp(join(tmpdir(), 'agy-schema-'))
        const file = join(dir, 'schema.json')
        // gateway-patch: CLIProxyAPI schema sanitizer — declaration/generation
        // locations only; conversation history never passes through here
        // (the -p prompt text carries it as prose, not as JSON schemas).
        await writeFile(file, JSON.stringify(gatewayPatch.sanitizeOutgoingSchema(call.jsonSchema)), 'utf8')
        args.push('--json-schema', file)
        schemaCleanup = () => rm(dir, { recursive: true, force: true })
      }
      // Phase 3 tool-bridge: remember the exact spawn parameters so a retry
      // can replay the SAME turn before the client sees the shape failure.
      // (env is filled in below, once envFor() is defined.)
      if (bridgePolicy !== null && bridgePolicy.allowedNames.length > 0) {
        this.bridgeRestart = {
          args: [...args],
          env: {},
          cwd: workspaceRoot,
          ...(stdinData !== undefined ? { stdinData } : {}),
          ...(call.signal !== undefined ? { signal: call.signal } : {}),
        }
      } else {
        this.bridgeRestart = undefined
      }
      release = await this.deps.acquire(call.signal)
      if (call.signal?.aborted ?? false) {
        // A-M2: aborted while parked in the semaphore queue. The catch below
        // only untracks the busy mark — the semaphore slot (and any schema
        // temp dir) is ours to hand back here, post-acquire. No spawn, no
        // onRun: nothing was consumed.
        release()
        release = () => undefined
        if (schemaCleanup) void schemaCleanup().catch(() => undefined)
        schemaCleanup = null
        throw new EngineError('request aborted while waiting for a concurrency slot', 'ABORTED')
      }
    } catch (err) {
      // H1: the dispatch IIFE's finally only runs once it starts — a throw
      // before it (queue-full BUSY, staging/schema fs errors, the debounce)
      // must release the busy mark here, or the account is suppressed from
      // selection until restart.
      this.untrackBusy(account?.id)
      // B3/P4: every pre-dispatch throw also leaves the --json-schema temp
      // dir behind (the post-acquire branch above cleans its own) — a parked
      // acquire rejected by a disconnect, and the pre-existing queue-full
      // BUSY, leaked the staged schema file until the tmp volume was
      // cleaned by hand.
      if (schemaCleanup) void schemaCleanup().catch(() => undefined)
      schemaCleanup = null
      throw err
    }
    // ---- dispatch with a single engine-level retry (M5) ----
    // One recording per LOGICAL run: every spawn attempt appends into it in
    // order, so the span consumer sees one seamless event stream and a
    // retried spawn adds no visible seam. The recording is settled exactly
    // once — after the retry loop — which is what guarantees a failure frame
    // can never reach the client before the retries are exhausted.
    const rec = this.deps.runs.create()
    let released = false
    const releaseOnce = (): void => {
      if (released) return
      released = true
      release()
      if (schemaCleanup) void schemaCleanup().catch(() => undefined)
    }

    // Only pool accounts run with an isolated HOME (their token file lives in
    // account.dir/.gemini); injecting HOME for the real user profile would
    // hide its agy login. Attempt-scoped: a retry may re-select the account.
    // The composition is routed through sanitizeChildEnv: the gateway's own
    // AGY_PROXY_* env (root key, admin password) must never reach the child —
    // agy is a tool-executing agent, prompt injection can read its own env.
    const envFor = (acc: typeof account): NodeJS.ProcessEnv =>
      sanitizeChildEnv({
        ...process.env,
        ...(cfg.disableTelemetry
          ? {
              DO_NOT_TRACK: '1',
              DISABLE_TELEMETRY: '1',
              GOOGLE_CLOUD_DISABLE_TELEMETRY: '1',
              ANTIGRAVITY_DISABLE_TELEMETRY: '1',
            }
          : {}),
        ...(acc && acc.dir ? isolatedHomeEnv(acc.dir) : {}),
        ...(acc?.proxyUrl
          ? {
              ALL_PROXY: acc.proxyUrl,
              HTTPS_PROXY: acc.proxyUrl,
              HTTP_PROXY: acc.proxyUrl,
              all_proxy: acc.proxyUrl,
              https_proxy: acc.proxyUrl,
              http_proxy: acc.proxyUrl,
            }
          : {}),
      })

    // Steer preemption lives on the recording, but the abort now has to
    // survive beyond the current process (a kill between retry attempts must
    // not be followed by a fresh spawn), so it flips a flag as well.
    let proc: ReturnType<typeof startAgyProcess> | undefined
    let steerAborted = false
    rec.requestAbort = () => {
      steerAborted = true
      proc?.kill('abort')
    }

    /** One message per abort flavor: the ledger's errorText says WHICH caller
     *  gave up (steer preemption / client disconnect / shutdown drain) instead
     *  of a blanket 'aborted by caller'. The max-tokens budget cut is NOT a
     *  failure: the route already emitted the successful length/max_tokens
     *  finish before aborting, so settling it as ABORTED used to book a
     *  healthy run as a dashboard failure — it settles as a normal completion
     *  (failure null → ok/OK) instead. Only the route aborts with that reason
     *  (app.ts budget cut), so it is unreachable on pre-spawn/retry-gap paths. */
    const abortFailure = (): { kind: 'aborted'; code: string; message: string } | null => {
      if (steerAborted) return { kind: 'aborted', code: 'ABORTED', message: 'superseded by a newer prompt on the same session' }
      const reason = (call.signal?.reason as Error | undefined)?.message
      if (reason === 'output-budget') return null
      if (reason === 'client disconnected') return { kind: 'aborted', code: 'ABORTED', message: 'client disconnected mid-generation' }
      if (reason === 'server shutting down') return { kind: 'aborted', code: 'ABORTED', message: 'server shutting down (grace drain)' }
      return { kind: 'aborted', code: 'ABORTED', message: 'agy run aborted by caller' }
    }

    /** What one spawn attempt concluded. */
    interface AttemptResult {
      failure: { kind: 'error' | 'aborted'; code: string; message: string } | null
      outcome: RunOutcome
      rawErrText: string
      conversationId: string | null
      resultEvent: { ok: boolean; response: string; structuredOutput?: unknown; deniedActions?: unknown } | null
      account: typeof account
    }

    const spawnFailed = (msg: string, acc: typeof account): AttemptResult => ({
      failure: { kind: 'error', code: Err.PROCESS_EXIT, message: msg },
      outcome: { code: null, signal: null, timedOut: false, aborted: false, stdout: '', stderrTail: '', durationMs: 0 },
      rawErrText: '',
      conversationId: null,
      resultEvent: null,
      account: acc,
    })

    const runOnce = async (index: number, attemptAcc: typeof account): Promise<AttemptResult> => {
      const task = async (): Promise<AttemptResult> => {
        // Superseded while queued (steer/abort) — don't spawn. Also covers a
        // client that disconnected while its request sat in the per-account
        // queue (A-M2): the abort listener installed by the runner only fires
        // post-spawn, so the pre-spawn waits must check the signal themselves.
        if (rec.isSettled || steerAborted || (call.signal?.aborted ?? false)) {
          return spawnFailedAbort(attemptAcc)
        }
        // Per-account burst spacing throttle with randomized jitter (prevents high-frequency flood to Google endpoints)
        if (attemptAcc) {
          const lastSpawn = this.lastAccountSpawnTime.get(attemptAcc.id) ?? 0
          const elapsed = Date.now() - lastSpawn
          const jitter = Math.floor(Math.random() * 300) // 0~299ms organic jitter
          const targetInterval = this.minSpawnIntervalMs + jitter
          if (elapsed < targetInterval) {
            await sleepUnref(targetInterval - elapsed, call.signal)
            if (rec.isSettled || steerAborted || (call.signal?.aborted ?? false)) {
              return spawnFailedAbort(attemptAcc)
            }
          }
          this.lastAccountSpawnTime.set(attemptAcc.id, Date.now())
        }
        const parser = new StreamJsonParser()
        this.deps.onParser?.(parser)
        const before = snapshotConversations(attemptAcc?.dir || undefined)
        let streamCid: string | null = null
        // Phase 3 tool-bridge: the retry spawn must use the SAME account env
        // (isolated HOME) as this attempt, not the server's process.env.
        if (this.bridgeRestart !== undefined) this.bridgeRestart.env = envFor(attemptAcc)
        // The binary seam is re-read per attempt: a retry may find a
        // (re)installed binary even when attempt 0 failed to spawn.
        const attemptBin = this.deps.bin() ?? bin
        try {
          this.liveAgyChildren++
          proc = startAgyProcess({
            bin: attemptBin,
            args: [...(this.deps.binArgs ?? []), ...args],
            cwd: workspaceRoot,
            timeoutMs: cfg.timeoutMs,
            signal: call.signal,
            env: envFor(attemptAcc),
            // Phase 3 tool-bridge: the prompt rides stdin as one NDJSON event
            // when the bridge is active (see the argv rewrite above); the
            // native path keeps the `-p <prompt>` argv and an empty stdin.
            ...(this.bridgeRestart?.stdinData !== undefined ? { stdinData: this.bridgeRestart.stdinData } : {}),
            // A-M5: raw stdout chunks go straight into the parser — the old
            // onLine seam re-split every chunk into lines and re-appended
            // '\n' only for the parser to split them apart again, and the
            // runner kept a 4MB stdout copy behind it. onChunk hands the
            // utf8-decoded chunk over untouched; feed() owns line buffering.
            onChunk: (chunk) => {
              for (const ev of parser.feed(chunk)) {
                if (ev.kind === 'init' && ev.conversationId) streamCid = ev.conversationId
                if (ev.kind === 'result' && ev.conversationId !== '') streamCid = ev.conversationId
                rec.append(ev)
              }
            },
          })
        } catch (e) {
          this.liveAgyChildren--
          // startAgyProcess funnels spawn errors into the outcome promise
          // (child 'error' → code null); a synchronous throw is exceptional —
          // but it used to leave the recording unsettled forever. Normalize.
          return spawnFailed('failed to spawn agy: ' + brief(String(e)), attemptAcc)
        }
        if (steerKey !== '') this.activeRuns.set(steerKey, rec)
        const outcome = await proc.outcome
        this.liveAgyChildren--
        if (this.activeRuns.get(steerKey) === rec) this.activeRuns.delete(steerKey)
        if (steerKey !== '') this.activeSessionPrompts.delete(steerKey)
        for (const ev of parser.flush()) {
          if (ev.kind === 'result' && ev.conversationId !== '') streamCid = ev.conversationId
          rec.append(ev)
        }
        const diffed = diffConversations(before, attemptAcc?.dir || undefined).conversationId
        const conversationId = streamCid ?? diffed
        // A result envelope the mapper will finish on: ok, or an error that
        // still carries a usable response. Anything else leaves the live span
        // un-finished, so the failure below reaches it through the recording.
        const r = rec.getResultEvent()
        // CANCELED-with-empty-response guard (Google issue #902, ~10% of long
        // tool turns): agy ends with status=CANCELED / empty response yet exits
        // 0, and the parser classifies it ok. The client would see an empty
        // success; intercept it as a retryable INVALID_OUTPUT failure instead.
        //
        // DENY-empty guard (t_71dd6987): a headless turn that burned itself on
        // an auto-denied native tool ALSO ends ok + empty, but a respawn can
        // never change the denial. It is told apart by a NON-EMPTY
        // `denied_actions` on the envelope and classified DENIED_ACTIONS (not
        // in RETRYABLE_CODES), so the client gets the real cause at once
        // instead of a wasted spawn.
        const emptyShape = r !== null && r.ok && r.response === '' && !hasStructuredContent(r.structuredOutput) && !rec.sawTextBefore(rec.length)
        const deniedEmpty = emptyShape && hasDeniedActions(r.deniedActions)
        const canceledEmpty = emptyShape && !deniedEmpty
        const consumable = r !== null && (r.ok || r.response !== '') && !canceledEmpty && !deniedEmpty
        // Error classification scans ONLY stderr and the result envelope's
        // error field. stdout is model prose + event JSON: a run whose streamed
        // text merely MENTIONED "rate limit"/"quota" (or contained a hash with
        // "429") used to be misclassified as a quota failure, masking the real
        // error and slapping a ghost cooldown on a healthy account.
        const rawErrText = [outcome.stderrTail, parser.stats.lastResultError].filter(Boolean).join(' ')
        const isRateLimit = looksLikeRateLimit(rawErrText)
        let failure: { kind: 'error' | 'aborted'; code: string; message: string } | null = null
        if (outcome.aborted) {
          failure = abortFailure()
        } else if (outcome.timedOut) {
          failure = { kind: 'error', code: Err.TIMEOUT, message: 'agy run was idle for ' + cfg.timeoutMs + 'ms without output' }
        } else if (sawAuthFailure(parser, outcome)) {
          failure = { kind: 'error', code: Err.AUTH, message: 'agy is not signed in — add a Google account to the pool (admin UI) to login' }
        } else if (looksLikeValidationRequired(rawErrText)) {
          // Google 403 re-validation: surface the challenge URL verbatim
          // (M3 DoD) and quarantine the account like an expired login.
          const detail = parser.stats.lastResultError ?? (outcome.stderrTail !== '' ? brief(outcome.stderrTail) : 'upstream requires re-validation (VALIDATION_REQUIRED)')
          const url = extractValidationUrl(detail)
          failure = { kind: 'error', code: Err.VALIDATION_REQUIRED, message: url !== undefined ? detail + ' (validation_url: ' + url + ')' : detail }
        } else if (looksLikeEligibilityBlocked(rawErrText)) {
          // Google eligibility/geo gate: the account cannot obtain a session
          // (loadCodeAssist 502/Not Allowed, "not eligible for Antigravity",
          // "User location is not supported"). Classified distinctly from a
          // generic PROCESS_EXIT — which is retryable and re-spawns the same
          // dead account — so the settle-time quarantine takes it out of
          // rotation instead.
          const detail = parser.stats.lastResultError ?? (outcome.stderrTail !== '' ? brief(outcome.stderrTail) : 'upstream eligibility/geo gate refused the account (ELIGIBILITY_BLOCKED)')
          failure = { kind: 'error', code: Err.ELIGIBILITY_BLOCKED, message: detail }
        } else if (isRateLimit) {
          const bestMsg = parser.stats.lastResultError || (outcome.stderrTail ? brief(outcome.stderrTail) : 'Rate limit or quota reached')
          // gateway-patch: CLIProxyAPI 429-classifier drives the routing
          // decision (full integration). The decision itself is made at
          // settle time via applyRateLimitCooldown (full cooldown →
          // recordFailure; instant/short windows → ShortCooldownStore +
          // account switch); the message below still carries the raw kind so
          // clients/logs keep the refined signal.
          const d429 = gatewayPatch.classifyAgy429(bestMsg)
          failure = { kind: 'error', code: Err.AGY_ERROR, message: 'Google Antigravity quota / rate limit reached (' + d429.kind + '): ' + bestMsg }
        } else if (!consumable) {
          if (outcome.code !== 0) {
            // agy reports its failure on STDOUT as a result envelope and often
            // exits 1 with EMPTY stderr; dropping the envelope here used to
            // leave users with a bare "agy exited with code 1" and no cause.
            // Prefer the envelope's error text, then the stderr tail. A null
            // code separates the two no-envelope hazards (M5): a killed
            // process (signal present) and a spawn failure (runner sinks the
            // child 'error' string into stderrTail).
            const detail = parser.stats.lastResultError ?? (outcome.stderrTail !== '' ? brief(outcome.stderrTail) : '')
            const suffix = detail !== '' ? ': ' + detail : ''
            if (outcome.code === null) {
              failure = {
                kind: 'error',
                code: Err.PROCESS_EXIT,
                message: outcome.signal !== null
                  ? 'agy process was terminated (signal ' + outcome.signal + ')' + suffix
                  : 'failed to spawn agy: ' + (detail !== '' ? detail : 'unknown spawn error (binary missing or not executable?)'),
              }
            } else {
              failure = { kind: 'error', code: Err.PROCESS_EXIT, message: 'agy exited with code ' + outcome.code + suffix }
            }
          } else if (deniedEmpty) {
            failure = { kind: 'error', code: Err.DENIED_ACTIONS, message: 'agy auto-denied a native tool in headless mode and produced no answer (denied_actions)' }
          } else if (canceledEmpty) {
            failure = { kind: 'error', code: Err.INVALID_OUTPUT, message: 'agy ended with an empty response (CANCELED, google antigravity #902)' }
          } else if (parser.stats.lastResultError) {
            failure = { kind: 'error', code: Err.AGY_ERROR, message: 'agy reported an error: ' + parser.stats.lastResultError }
          } else {
            failure = { kind: 'error', code: Err.INVALID_OUTPUT, message: 'agy produced no result event (' + parser.stats.garbage + ' unparseable lines)' }
          }
        }
        return { failure, outcome, rawErrText, conversationId, resultEvent: r, account: attemptAcc }
      }

      // ---- per-account serial queue (charter §6, p-queue concurrency:1) ----
      // One agy process per account at a time: two concurrent requests on the
      // same sticky account would race the per-HOME conversation discovery
      // (two fresh .db files → ambiguous diff) and interleave on one session
      // binding. The queue task spans the whole spawn→outcome window, so the
      // next same-account spawn waits for the previous process to exit.
      // Pool-off behavior is unchanged (the task runs immediately). Each
      // retry attempt queues independently — a retry NEVER awaits inside
      // attempt A's queue for account B's queue (concurrency:1 deadlock).
      if (attemptAcc != null) {
        let q = this.accountQueues.get(attemptAcc.id)
        if (q === undefined) {
          q = new PQueue({ concurrency: 1 })
          this.accountQueues.set(attemptAcc.id, q)
        }
        return await q.add(task)
      }
      return await task()
    }

    function spawnFailedAbort(acc: typeof account): AttemptResult {
      // Pre-spawn/retry-gap aborts can never be a budget cut (no deltas were
      // streamed), so a null classification is impossible here — coalesce to
      // the generic text defensively.
      const message = abortFailure() ?? { kind: 'aborted' as const, code: 'ABORTED', message: 'agy run aborted by caller' }
      return {
        failure: message,
        outcome: { code: null, signal: null, timedOut: false, aborted: true, stdout: '', stderrTail: '', durationMs: 0 },
        rawErrText: '',
        conversationId: null,
        resultEvent: null,
        account: acc,
      }
    }

    void (async () => {
      let attempt = 0
      let attemptAccount = account
      const fireRun = (r: AttemptResult, idx: number, final: boolean): void => {
        const rawUsage = rec.getResultRawUsage()
        this.deps.onRun?.({
          ok: r.failure === null,
          code: r.failure !== null ? r.failure.code : 'OK',
          attempt: idx,
          final,
          ...(r.failure !== null ? { failureMessage: r.failure.message } : {}),
          durationMs: r.outcome.durationMs,
          model,
          providerModel: activeModel === '' ? cfg.defaultModel : activeModel,
          usage: rawUsage !== null ? usageFromRaw(rec.finalUsage(rawUsage)) : null,
          ...(r.account != null ? { accountId: r.account.id } : {}),
          ...(family !== undefined ? { family } : {}),
          ...(r.conversationId != null && r.conversationId !== '' ? { conversationId: r.conversationId } : {}),
          ...(call.meta !== undefined ? { meta: call.meta } : {}),
        })
      }
      let last: AttemptResult
      try {
        for (;;) {
          if (rec.isSettled) {
            // Steer settled the whole run as superseded — return the
            // semaphore slot (and temp-schema) before leaving, or it leaks
            // until the next same-account run releases for us.
            releaseOnce()
            return
          }
          last = await runOnce(attempt, attemptAccount)
          // A-M1: any failed attempt may stem from a stale or missing
          // binary — drop the wiring cache so the retry's per-attempt
          // re-read actually rescans (a no-op when no cache is wired).
          if (last.failure !== null) this.deps.invalidateBin?.()
          // Retry gate (M5): only outcome-level failures with nothing on the
          // wire. A result-shaped failure never emits span chunks (the mapper
          // returns passively on !ok with an empty response); the #902
          // CANCELED shape (ok + empty response, nothing streamed) is
          // retryable too — the mapper stays passive on it, so a retry is
          // invisible to the client. Any recorded step (text/reasoning/tool)
          // may have streamed to the client: no replay.
          const canceledEmptyShape =
            last.resultEvent !== null && last.resultEvent.ok && last.resultEvent.response === '' && !hasStructuredContent(last.resultEvent.structuredOutput) && !rec.sawTextBefore(rec.length)
          const shapeOnly =
            last.resultEvent === null ||
            (!last.resultEvent.ok && last.resultEvent.response === '') ||
            canceledEmptyShape
          const canRetry =
            last.failure !== null &&
            shapeOnly &&
            RETRYABLE_CODES.includes(last.failure.code) &&
            !rec.hasClientMappedEvents()
          const final = !canRetry || attempt >= RETRY_POLICY.maxRetries || steerAborted || (call.signal?.aborted ?? false)
          fireRun(last, attempt, final)
          if (final) break
          attempt++
          await (this.deps.retryDelay ?? defaultRetryDelay)(RETRY_POLICY.initialDelayMs)
          if (rec.isSettled || steerAborted || (call.signal?.aborted ?? false)) {
            last = spawnFailedAbort(attemptAccount)
            fireRun(last, attempt, true) // the aborted settle still books exactly one final event
            break
          }
          // Same sticky selection as the first spawn, for the fixed family, but
          // the account that just FAILED is excluded like a busy one
          // (t_67b7936c): a retryable failure never touches account state, so a
          // plain re-select used to hand the same account back and one broken
          // account doubled the client's wait on a deterministic failure.
          // When the failed account is the only healthy candidate the pool's
          // busy-less fallback still returns it (a single-account pool keeps
          // its self-healing retry); an operator disable mid-flight keeps the
          // settled failure instead of a secondhand POOL_EXHAUSTED.
          if ((this.deps.pool?.getAccounts().length ?? 0) > 0) {
            this.untrackBusy(attemptAccount?.id)
            const retryBusy = new Set(this.busyAccounts)
            if (attemptAccount != null) retryBusy.add(attemptAccount.id)
            const next = this.selectAccountFor(family, retryBusy) ?? undefined
            if (next === undefined) {
              this.trackBusy(attemptAccount?.id)
              break
            }
            attemptAccount = next
            this.trackBusy(next.id)
          }
        }
        releaseOnce()
        rec.settle(last.failure)
        // A-M4 lifecycle: a settled recording is only kept while a future
        // span may still resume it from a mirror-tool cursor (its last
        // completed span cut on a tool step). Otherwise nothing will ever
        // look it up again — drop it now instead of waiting for the
        // capacity eviction to hit an in-progress run.
        if (!rec.keepForContinuation) this.deps.runs.forget(rec.runId)
        if (last.failure === null) {
          if (last.account) this.deps.pool?.recordSuccess(last.account.id, family)
          // t_913f68e8 (C): pin the successful account to the conversation so
          // the next turn re-selects it instead of rotating mid-conversation.
          if (convKey !== '' && last.account) this.conversationAccount.set(convKey, last.account.id)
          if (sessionAccountKey !== '') {
            const finalId = binding !== undefined ? binding.conversationId : last.conversationId
            if (finalId) {
              this.deps.store.set(sessionAccountKey, {
                conversationId: finalId,
                lastMessageCount: messages.length,
                updatedAt: Date.now(),
                model: activeModel,
              })
            }
          }
        } else {
          const effectiveRateLimit = looksLikeRateLimit(last.rawErrText) || looksLikeRateLimit(last.failure.message)
          // Full 429 integration (gateway-patch): decideEngineCooldown splits
          // the blunt hard-limit signal into full_quota_exhausted (→ pool
          // cooldown, account leaves rotation) and the <5m/instant windows
          // (→ ShortCooldownStore + account switch on the next selection).
          // Soft signals (model overloaded) decide short_cooldown neither
          // records a window nor cools the account down — the message above
          // is all they shape.
          if (last.account && looksLikeHardRateLimit(last.rawErrText)) {
            this.applyRateLimitCooldown(last.account.id, family, last.rawErrText)
          }
          if (last.account && (last.failure.code === Err.AUTH || last.failure.code === Err.VALIDATION_REQUIRED || /invalid_grant|not signed in|auth/i.test(last.failure.message))) {
            this.deps.pool?.markAuthRequired(last.account.id, last.failure.message)
          }
          // Eligibility/geo class (t_a5691f67): the credential is intact but
          // the upstream gate refuses service. Distinct quarantine path (it is
          // NOT auth drift): the pool counts a streak and quarantines only
          // after repeated failures, so a single transient 502 does not exile
          // a healthy account. Quarantined accounts are filtered out of
          // selectAccount, so the next request never spends a full spawn on
          // the dead credential.
          if (last.account && last.failure.code === Err.ELIGIBILITY_BLOCKED) {
            this.deps.pool?.recordEligibilityFailure(last.account.id, last.failure.message)
          }
          if (sessionAccountKey !== '') {
            if (
              last.failure.code === Err.AUTH ||
              effectiveRateLimit ||
              (last.failure.message && /conversation.*(not found|invalid|not recognized|expired|does not exist)|session.*(expired|invalid)/i.test(last.failure.message))
            ) {
              // If auth expired or rate limit hit or conversation rejected, drop stale binding
              this.deps.store.delete(sessionAccountKey)
            }
          }
        }
      } catch (err) {
        // S1: the dispatch guard must be escape-proof — a throw inside
        // releaseOnce/settle/untrackBusy would reject this detached IIFE and
        // hard-crash the gateway (unhandled rejection = process exit by
        // default), bypassing ledger flush and SSE teardown. If settle itself
        // fails the recording stays unsettled; the span pump's end-of-stream
        // fallback below then serves the client a bounded terminal error
        // instead of a hang.
        try {
          releaseOnce()
        } catch (releaseErr) {
          this.deps.log?.('slot release after dispatch error failed: ' + brief(String(releaseErr)))
        }
        try {
          rec.settle({ kind: 'error', code: Err.PROCESS_EXIT, message: 'internal error: ' + brief(String(err)) })
        } catch (settleErr) {
          this.deps.log?.('settle after dispatch error failed: ' + brief(String(settleErr)))
        }
      } finally {
        try {
          this.untrackBusy(attemptAccount?.id)
        } catch {
          // busy-mark hygiene must never escape the dispatch IIFE
        }
      }
    })()

    // ---- Phase 3 tool-bridge: buffered span + OpenAI recut ----
    // With client tools in play the RAW agy turn must never reach the client
    // (it is the JSON envelope, not an assistant message): every chunk is
    // buffered, a shape failure is retried (up to TOOL_BRIDGE_MAX_RETRIES
    // extra spawns) BEFORE anything is emitted, and only the recut — text +
    // OpenAI tool_calls + finish_reason — is streamed downstream.
    if (bridgePolicy !== null && bridgePolicy.allowedNames.length > 0) {
      const buffered: StreamChunk[] = []
      const rawSpan = this.driveSpan(rec, 0, true)
      let spanEnd: SpanEnd = { cutOnTool: false }
      for (;;) {
        const next = await rawSpan.next()
        if (next.done === true) {
          // driveSpan's return value (SpanEnd) — the ONLY signal that tells a
          // tool-cut span from a finished one — used to be dropped here
          // (`void buffered`), so a span that cut on the first completed
          // native-tool-step WHILE THE RUN WAS STILL LIVE was handed straight
          // to the recut: `lastResultResponse` found no envelope yet and the
          // client got a spurious 'no usable answer (no result envelope)' 502
          // for a run that then settled OK (t_03a5ca20).
          spanEnd = next.value
          break
        }
        buffered.push(next.value)
      }
      void buffered
      // A tool-cut span means the run may still be producing its result
      // envelope: WAIT (bounded by the run timeout) for the envelope/settle
      // before recutting, exactly like the continuation path waits on the
      // recording. Without the wait the recut reads an empty recording and
      // answers a live run with a spurious 'no result envelope' 502
      // (t_03a5ca20). A wait that expires is surfaced as a TIMEOUT-class
      // failure — never as the structureless-answer text, and never an
      // unbounded park of the single agy slot.
      if (spanEnd.cutOnTool && !rec.isSettled && !(await this.awaitRecordingSettle(rec, cfg.timeoutMs))) {
        yield { type: 'usage', usage: { inputTokens: 0, outputTokens: 0 } }
        yield { type: 'finish', reason: { kind: 'error', failure: { message: 'tool bridge: agy produced no result envelope within ' + cfg.timeoutMs + 'ms (run still live after tool cut)', code: Err.TIMEOUT } } }
        return
      }
      const raw = await respawnForToolBridge(this, rec, bridgePolicy, model, family, lastResultResponse(rec, 0))
      for (const ch of recutToolBridgeSpan(rec, raw, bridgePolicy, model)) {
        // Bridge turns never continue through the agy mirror cursor: the
        // client's continuation comes back as tool messages, not as an
        // agytc- callId, so the recording must not be retained.
        if (ch.type === 'finish') rec.keepForContinuation = false
        yield ch
      }
      if (rec.isSettled) this.deps.runs.forget(rec.runId)
      return
    }

    // First span of the run: stream recorded events until the first
    // completed tool step cuts it (or the result finishes it). The dispatch
    // loop above keeps the recording unsettled until the final attempt, so
    // this span simply never sees a failure frame until retries are done.
    const spanEnd = yield* this.driveSpan(rec, 0, true)
    // A-M4 lifecycle: mirror of the continuation-path rule. A span that did
    // not cut on a tool step ends the client's interest in this recording;
    // if the run already settled (the common case — the result beat the span
    // consumer to it), forget immediately. A tool-cut span flips the
    // keep flag so the settle-time check above retains it for continuation.
    rec.keepForContinuation = spanEnd.cutOnTool
    if (!rec.keepForContinuation && rec.isSettled) this.deps.runs.forget(rec.runId)
  }

  /**
   * Phase 3 tool-bridge retry: one EXTRA spawn of the same agy call for a
   * bridge turn whose answer had no usable structure, executed BEFORE the
   * client sees anything. The fresh process appends into the SAME recording
   * (one recording per logical run), so unmapped events just extend the log
   * — the recut reads the last result envelope, which is the fresh answer.
   * Returns that answer, or undefined when the retry could not run/failed
   * (no pool account, spawn error, steer/abort): the caller then surfaces the
   * original 502-class failure, never a silent success.
   */
  /**
   * Wait (bounded) until the recording settles — the result envelope arrived
   * or the run ended. Returns true when settled, false when the bound expired.
   * The Phase 3 tool-bridge branch needs it: a span that cut on a completed
   * native-tool-step leaves the run LIVE (the envelope is still to come), so
   * the recut must not read the recording until settlement (t_03a5ca20).
   */
  private async awaitRecordingSettle(rec: RunRecording, timeoutMs: number): Promise<boolean> {
    const deadline = Date.now() + Math.max(1_000, timeoutMs)
    for (;;) {
      if (rec.isSettled) return true
      const left = deadline - Date.now()
      if (left <= 0) return rec.isSettled
      await new Promise((r) => setTimeout(r, Math.min(50, left)))
    }
  }

  /** Wait (bounded) until no engine-spawned agy child is live. */
  private async waitForIdleAgy(timeoutMs = 30_000): Promise<void> {
    const deadline = Date.now() + timeoutMs
    while (this.liveAgyChildren > 0 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 25))
    }
  }

  async respawnToolBridgeAttempt(rec: RunRecording, family: ModelFamily): Promise<string | Record<string, unknown> | undefined> {
    if (rec.isSettled && rec.failureInfo !== null) {
      // The run itself failed (quota/auth/process) — a retry would misreport
      // the real cause as a tool-bridge shape problem.
      return undefined
    }
    const cfg = this.deps.getConfig()
    // One agy process at a time (charter §6 / task constraint): the busy MARK
    // belongs to this very request, so it is not the guard — the live child
    // counter is.
    // The previous spawn's async completion may still be in flight (the
    // recording settles as soon as the result envelope arrives, a beat before
    // the child handler resumes) — WAIT for it to leave instead of skipping
    // the retry, otherwise the shape failure reaches the client unretried.
    await this.waitForIdleAgy()
    const bridge = this.bridgeRestart
    if (bridge === undefined) return undefined
    const bin = this.deps.bin()
    if (bin === null) return undefined
    const account = this.deps.pool && this.deps.pool.getAccounts().length > 0
      ? this.deps.pool.selectAccount(family, this.busyAccounts)
      : undefined
    try {
      // Per-account serial queue (charter §6): the retry spawn must not
      // interleave with another request on the same account.
      const parser = new StreamJsonParser()
      // The retry spawn owns its OWN answer: the recording may already be
      // settled (append() is a no-op then), so the fresh result is captured
      // here instead of being read back from the run log.
      let freshResponse: string | Record<string, unknown> | undefined
      const capture = (ev: { kind: string; response?: string; structuredOutput?: unknown }): void => {
        if (ev.kind !== 'result') return
        if (hasStructuredContent(ev.structuredOutput)) {
          freshResponse = typeof ev.structuredOutput === 'string'
            ? ev.structuredOutput
            : (ev.structuredOutput as Record<string, unknown>)
        } else if (ev.response !== undefined && ev.response.trim() !== '') {
          freshResponse = ev.response
        }
      }
      const spawn = async (): Promise<boolean> => {
        this.liveAgyChildren++
        try {
          const proc = startAgyProcess({
            bin,
            // binArgs is the harness seam (node + fake-agy script); production
            // leaves it empty. Dropping it here made the retry spawn a binary
            // that cannot exist, silently killing the retry.
            args: [...(this.deps.binArgs ?? []), ...bridge.args],
            cwd: bridge.cwd,
            timeoutMs: cfg.timeoutMs,
            signal: bridge.signal,
            env: bridge.env,
            ...(bridge.stdinData !== undefined ? { stdinData: bridge.stdinData } : {}),
            onChunk: (chunk) => {
              for (const ev of parser.feed(chunk)) {
                capture(ev)
                rec.append(ev)
              }
            },
          })
          const outcome = await proc.outcome
          for (const ev of parser.flush()) {
            capture(ev)
            rec.append(ev)
          }
          return outcome.code === 0
        } finally {
          this.liveAgyChildren--
        }
      }
      const q = account != null ? this.accountQueues.get(account.id) : undefined
      const ok = q !== undefined ? await q.add(spawn) : await spawn()
      if (!ok || freshResponse === undefined) return undefined
      return freshResponse
    } catch {
      return undefined
    }
  }

  /**
   * Stream one span of a recording: map events from `from` until the mapper
   * finishes (tool-calls cut or result stop), then let the queue drain. When
   * the recording settles without a consumable result, surface its failure
   * as this span's terminal chunk — the turn ends exactly like a native
   * provider error.
   */
  private async *driveSpan(
    rec: RunRecording,
    from: number,
    cutOnTool: boolean,
  ): AsyncGenerator<StreamChunk, SpanEnd> {
    const queue = new ChunkQueue()
    let cutOnToolEnd = false
    // Audit F4: a tool-call block handed across the yield boundary means the
    // client holds a mirror-call id (agytc-<runId>-<idx>) and will come back
    // for a continuation span — the recording must survive settlement.
    let clientSawToolCall = false
    let completed = false
    void (async () => {
      // S1: the pump guard is escape-proof. The mapper construction sits
      // inside the guarded region — a constructor throw would otherwise leave
      // the queue open forever and park the SSE leg on this span. The catch's
      // emitFailure is guarded too (it is the terminal-failure funnel: if IT
      // throws, a fixed internal-string fallback chunk pair closes the span
      // without crossing any token material), and queue.close() is
      // unconditional — every consumer of drain() must see the span end.
      let mapper: EventMapper | undefined
      try {
        mapper = new EventMapper({
          runId: rec.runId,
          cutOnTool,
          initialSawText: rec.sawTextBefore(from),
          usage: rec,
        })
        let i = from
        for await (const ev of rec.eventsFrom(from)) {
          for (const ch of mapper.map(ev, i)) {
            if (ch.type === 'finish' && ch.reason.kind === 'tool-calls') cutOnToolEnd = true
            queue.push(ch)
          }
          i++
          if (mapper.isFinished) break
        }
        if (!mapper.isFinished) {
          const f = rec.failureInfo
          if (f !== null) {
            for (const ch of mapper.emitFailure(f.kind, f.code, f.message)) queue.push(ch)
          } else {
            for (const ch of mapper.emitFailure('error', Err.INVALID_OUTPUT, 'agy stream ended without a result event')) queue.push(ch)
          }
        }
      } catch (err) {
        try {
          const fallback = mapper ?? new EventMapper({ runId: rec.runId, cutOnTool, initialSawText: rec.sawTextBefore(from), usage: rec })
          for (const ch of fallback.emitFailure('error', Err.PROCESS_EXIT, 'internal error: ' + brief(String(err)))) queue.push(ch)
        } catch {
          // Last resort: fixed internal strings only, zero scrubbing risk.
          queue.push({ type: 'usage', usage: { inputTokens: 0, outputTokens: 0 } })
          queue.push({ type: 'finish', reason: { kind: 'error', failure: { message: 'internal error', code: Err.PROCESS_EXIT } } })
        }
      } finally {
        queue.close()
      }
    })()
    try {
      for await (const ch of queue.drain()) {
        // Flag BEFORE the yield: a consumer that breaks on this chunk (budget
        // cap, disconnect) has already received it — for-await delivers the
        // value before .return() finalizes us at this paused yield point.
        if (ch.type === 'block-end' && ch.block.type === 'tool-call') clientSawToolCall = true
        yield ch
      }
      completed = true
    } finally {
      // Audit F4: when the consumer finalizes the generator early (budget cut
      // or disconnect breaks out of the for-await), the keepForContinuation
      // tails after the two `yield*` call sites never run. Mirror the keep
      // decision here: a delivered tool-call block must keep the recording
      // for the coming mirror continuation. OR-merge, never overwrite — this
      // span may BE a continuation whose predecessor already set the keep
      // flag at its tail, and an early break leaves the replay cursor alive,
      // so a false local flag must not undo that decision. (The tails
      // overwrite because a completed span carries the final word.) Only a
      // merged-false settled recording is forgotten, exactly as the tails
      // would forget it.
      if (!completed) {
        const keep = clientSawToolCall || rec.keepForContinuation
        rec.keepForContinuation = keep
        if (!keep && rec.isSettled) this.deps.runs.forget(rec.runId)
      }
    }
    return { cutOnTool: cutOnToolEnd }
  }
}

/**
 * Roll a workspace directory into existence lazily; a failure to create it
 * (read-only volume) is logged instead of failing the spawn —
 * startAgyProcess will surface the real spawn error anyway.
 */
function ensureWorkspaceDir(log: ((msg: string) => void) | undefined, dir: string): void {
  try {
    mkdirSync(dir, { recursive: true })
  } catch (err) {
    log?.('WARNING: workspace dir could not be created (' + dir + '): ' + brief(String(err)))
  }
}

/**
 * Detect a continuation span: the request's LAST message is the tool result
 * of one of our mirrored agy tool calls. Its callId encodes the recording
 * run and the event index to resume after.
 */
export function detectContinuation(messages: readonly EngineMessage[]): { runId: string; eventIndex: number } | null {
  const last = messages[messages.length - 1]
  if (last === undefined || last.role !== 'tool') return null
  const callId = last.toolCallId
  if (typeof callId !== 'string' || callId === '') return null
  return parseMirrorCallId(callId)
}

// TokenUsage is re-exported so protocol layers depend on a single engine
// surface rather than importing stream-types separately.
export type { StreamChunk, TokenUsage } from './stream-types.ts'
