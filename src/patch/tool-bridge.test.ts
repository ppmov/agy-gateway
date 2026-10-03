// Phase 3 tool-bridge tests: OFFLINE, against test/fake-agy.mjs — no network,
// no quota burn. Two layers:
//   1. Pure unit coverage of the bridge (schema/prompt/parse/retry).
//   2. End-to-end through the REAL engine + fake agy + HTTP server, so the
//      /v1/chat/completions contract (message.tool_calls, finish_reason,
//      streaming frames, 502 on a structureless answer) is asserted on the
//      actual wire shape.
//
// Cases (charter Phase 3):
//   - text answer            (envelope with empty tool_calls → content, stop)
//   - one tool_call          (→ finish_reason='tool_calls', valid JSON args)
//   - two tool_calls         (→ both calls, ids unique)
//   - streaming tool_call    (SSE delta.tool_calls + finish_reason tool_calls)
//   - broken JSON → retry    (attempt 1 broken, attempt 2 good → single spawn
//                             retry, client sees only the good turn)
//   - empty answer → error   (structureless text → 502, never a silent 200)
import { describe, it, expect, afterEach, beforeAll } from 'vitest'
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { defaultConfig, type GatewayConfig } from '../common/types.ts'
import { AgyEngine, type EngineDeps } from '../host/engine.ts'
import { ModelCatalog } from '../host/models.ts'
import { SessionStore } from '../host/sessions.ts'
import { RunRegistry } from '../host/recording.ts'
import { buildServer } from '../server/app.ts'
import { buildLogger } from '../server/logger.ts'
import { GatewaySemaphore } from '../server/semaphore.ts'
import {
  agyOverflowCause,
  agyPromptOverflowError,
  buildToolBridgePrompt,
  buildToolBridgeResponseSchema,
  extractJsonObject,
  isAgyPromptOverflow,
  isRetryableToolBridgeError,
  maxToolBridgeAttempts,
  newToolCallId,
  parseToolBridgeAnswer,
  parseToolBridgeEnvelope,
  resolveToolBridgePolicy,
  toOpenAiToolCalls,
  TOOL_BRIDGE_MAX_RETRIES,
  ToolBridgeError,
  withToolBridgeRetries,
  type OpenAiFunctionTool,
} from './tool-bridge.ts'

const fakeBin = process.execPath
// fake-agy.mjs lives in test/ — resolve from the repo root (src/patch/ → ../../test)
const fakeScript = join(import.meta.dirname, '..', '..', 'test', 'fake-agy.mjs')

// ---- fixture helpers -------------------------------------------------------

/** The model's structured answer, in the shape agy returns it: a result
 *  envelope whose `response` carries the JSON document. extraEvents lets a
 *  case add streamed text before the result (the mapper then emits it). */
function bridgeEvents(answer: string, opts: { extraEvents?: string[]; conversationId?: string } = {}): string {
  const conv = opts.conversationId ?? 'conv-bridge-1'
  const lines = [
    JSON.stringify({ event: 'init', conversation_id: conv, model: 'gemini-3.7-flash' }),
    ...(opts.extraEvents ?? []),
    JSON.stringify({
      event: 'result',
      result: {
        conversation_id: conv,
        status: 'DONE',
        response: answer,
        usage: { input_tokens: 120, output_tokens: 24, total_tokens: 144 },
      },
    }),
  ]
  return lines.join('\n') + '\n'
}

/** agy >=1.6 structured-output fixture: the envelope arrives as an OBJECT in
 *  `result.structured_output` while `response` stays EMPTY (the live shape
 *  with `--input-format stream-json` + inline `--json-schema`). */
function bridgeStructuredEvents(envelope: unknown, opts: { conversationId?: string } = {}): string {
  const conv = opts.conversationId ?? 'conv-bridge-structured'
  return [
    JSON.stringify({ event: 'init', conversation_id: conv, model: 'gemini-3.7-flash' }),
    JSON.stringify({
      event: 'result',
      result: {
        conversation_id: conv,
        status: 'DONE',
        response: '',
        structured_output: envelope,
        usage: { input_tokens: 120, output_tokens: 24, total_tokens: 144 },
      },
    }),
  ].join('\n') + '\n'
}

let eventsPath = ''
function useEvents(content: string): void {
  writeFileSync(eventsPath, content, 'utf8')
  process.env.FAKE_AGY_EVENTS_FILE = eventsPath
  delete process.env.FAKE_AGY_EXIT_CODE
}

function makeServer(cfgOverrides: Partial<GatewayConfig> = {}, deps: Partial<EngineDeps> = {}) {
  const cfg: GatewayConfig = { ...defaultConfig(), permissionMode: 'plan', timeoutMs: 20_000, ...cfgOverrides }
  const workDir = mkdtempSync(join(tmpdir(), 'agy-bridge-'))
  process.env.AGY_PROXY_CONVERSATIONS_DIR = join(workDir, 'convs')
  const catalog = new ModelCatalog(async () => { throw new Error('no discovery in tests') }, cfg.fallbackModels, 300_000)
  const sem = new GatewaySemaphore(() => cfg.maxConcurrent, () => cfg.maxQueueDepth)
  const runs = new RunRegistry()
  const engine = new AgyEngine({
    getConfig: () => cfg,
    catalog,
    store: new SessionStore(join(workDir, 'sessions.json')),
    bin: () => fakeBin,
    binArgs: [fakeScript],
    acquire: () => sem.acquire(),
    runs,
    retryDelay: async () => {},
    ...deps,
  })
  const built = buildServer({ getConfig: () => cfg, engine, catalog, log: buildLogger({ AGY_PROXY_LOG_LEVEL: 'warn' }) })
  return { built, workDir, engine, runs }
}

const WEATHER_TOOL: OpenAiFunctionTool = {
  type: 'function',
  function: {
    name: 'get_weather',
    description: 'Get the current weather for a city.',
    parameters: {
      type: 'object',
      properties: { city: { type: 'string', description: 'City name' } },
      required: ['city'],
    },
  },
}

const TOOLS_BODY = [WEATHER_TOOL]

function chatPayload(extra: Record<string, unknown> = {}) {
  return {
    model: 'gemini-3.7-flash',
    messages: [{ role: 'user', content: 'What is the weather in Moscow? Call the tool.' }],
    tools: TOOLS_BODY,
    ...extra,
  }
}

let servers: Array<ReturnType<typeof makeServer>> = []
function track(s: ReturnType<typeof makeServer>) {
  servers.push(s)
  return s
}

beforeAll(() => {
  const dir = mkdtempSync(join(tmpdir(), 'agy-bridge-events-'))
  eventsPath = join(dir, 'events.ndjson')
})

afterEach(() => {
  delete process.env.FAKE_AGY_EVENTS_FILE
  delete process.env.FAKE_AGY_EVENTS_FILES
  delete process.env.FAKE_AGY_EVENTS_SEQ_FILE
  delete process.env.FAKE_AGY_MODE
  delete process.env.FAKE_AGY_MODE_FILE
  delete process.env.FAKE_AGY_ARGS_FILE
  delete process.env.FAKE_AGY_DELAY_MS
  for (const s of servers) {
    void s.built.app.close()
    try { rmSync(s.workDir, { recursive: true, force: true }) } catch { /* best effort */ }
  }
  servers = []
})

// ---------------------------------------------------------------------------
// Layer 1: pure unit coverage
// ---------------------------------------------------------------------------

describe('TB-UNIT: policy', () => {
  it('defaults to auto with the declared tool names allowed', () => {
    const p = resolveToolBridgePolicy(TOOLS_BODY, undefined, [])
    expect(p.allowedNames).toEqual(['get_weather'])
    expect(p.requireToolCall).toBe(false)
    expect(p.tools).toHaveLength(1)
  })

  it("narrows to the forced function for tool_choice {function}", () => {
    const p = resolveToolBridgePolicy(TOOLS_BODY, { type: 'function', function: { name: 'get_weather' } }, [])
    expect(p.allowedNames).toEqual(['get_weather'])
    expect(p.requireToolCall).toBe(true)
  })

  it('requires a call for tool_choice "required" and none for "none"', () => {
    expect(resolveToolBridgePolicy(TOOLS_BODY, 'required', []).requireToolCall).toBe(true)
    const none = resolveToolBridgePolicy(TOOLS_BODY, 'none', [])
    expect(none.requireToolCall).toBe(false)
    expect(none.allowedNames).toEqual([])
  })

  it('rejects a malformed tools[] with a 400-class code', () => {
    expect(() => resolveToolBridgePolicy([{ type: 'function' }], undefined, [])).toThrow(ToolBridgeError)
    try {
      resolveToolBridgePolicy([{ type: 'function' }], undefined, [])
    } catch (e) {
      expect((e as ToolBridgeError).code).toBe('invalid_tools')
      expect((e as ToolBridgeError).status).toBe(400)
    }
  })

  it('flags a continuation when the conversation ends on a tool result', () => {
    const p = resolveToolBridgePolicy(TOOLS_BODY, undefined, [
      { role: 'user', text: 'hi' },
      { role: 'assistant', text: '', toolCalls: [{ id: 'call_x', type: 'function', function: { name: 'get_weather', arguments: '{}' } }] },
      { role: 'tool', text: '{"temp": 5}', toolCallId: 'call_x' },
    ])
    expect(p.continuation).toBe(true)
  })
})

describe('TB-UNIT: envelope schema + prompt', () => {
  it('requires both keys and enumerates the allowed names in tool_calls', () => {
    const schema = buildToolBridgeResponseSchema(resolveToolBridgePolicy(TOOLS_BODY, undefined, []))
    expect(schema.required).toEqual(['content', 'tool_calls'])
    const items = (schema.properties as Record<string, { items: { properties: { name: { enum: string[] } } } }>).tool_calls!.items
    expect(items.properties.name.enum).toEqual(['get_weather'])
  })

  it('drops the name enum when no tools are offered (continuation turn)', () => {
    const schema = buildToolBridgeResponseSchema(resolveToolBridgePolicy([], undefined, []))
    const items = (schema.properties as Record<string, { items: { properties: { name: Record<string, unknown> } } }>).tool_calls!.items
    expect(items.properties.name.enum).toBeUndefined()
  })

  it('packs the definitions, the conversation and the protocol into one prompt', () => {
    const policy = resolveToolBridgePolicy(TOOLS_BODY, undefined, [])
    const prompt = buildToolBridgePrompt(policy, [{ role: 'user', text: 'weather in Moscow?' }])
    expect(prompt).toContain('<client_function_definitions_json>')
    expect(prompt).toContain('"name":"get_weather"')
    expect(prompt).toContain('<conversation>')
    expect(prompt).toContain('weather in Moscow?')
    expect(prompt).toContain('tool_calls')
  })

  it('tells the model to finish rather than chain on a continuation turn', () => {
    const policy = resolveToolBridgePolicy(TOOLS_BODY, undefined, [
      { role: 'assistant', text: '', toolCalls: [{ id: 'call_x', type: 'function', function: { name: 'get_weather', arguments: '{"city":"Moscow"}' } }] },
      { role: 'tool', text: '{"temp": 5}', toolCallId: 'call_x' },
    ])
    expect(buildToolBridgePrompt(policy, [{ role: 'tool', text: '{"temp": 5}' }])).toContain('finish the user')
  })
})

describe('TB-UNIT: answer parsing', () => {
  const policy = resolveToolBridgePolicy(TOOLS_BODY, undefined, [])

  it('accepts the envelope as an ALREADY-PARSED object (result.structured_output)', () => {
    // agy >=1.6 with --json-schema returns result.structured_output as an
    // OBJECT; the parser must not require a JSON string in that case.
    const r = parseToolBridgeAnswer({ content: '', tool_calls: [{ name: 'get_weather', args: '{"city":"Moscow"}' }] }, policy)
    expect(r.finishReason).toBe('tool_calls')
    expect(r.toolCalls).toHaveLength(1)
    expect(r.toolCalls[0]!.function.name).toBe('get_weather')
    expect(JSON.parse(r.toolCalls[0]!.function.arguments)).toEqual({ city: 'Moscow' })
  })

  it('accepts an object envelope whose args are an OBJECT (not a JSON string)', () => {
    const r = parseToolBridgeAnswer({ content: 'checking', tool_calls: [{ name: 'get_weather', args: { city: 'Moscow' } }] }, policy)
    expect(r.content).toBe('checking')
    expect(r.finishReason).toBe('tool_calls')
    expect(JSON.parse(r.toolCalls[0]!.function.arguments)).toEqual({ city: 'Moscow' })
  })

  it('accepts a single-element array structured_output (unwrapped)', () => {
    const r = parseToolBridgeAnswer([{ content: '', tool_calls: [{ name: 'get_weather', args: '{"city":"Moscow"}' }] }] as unknown as Record<string, unknown>, policy)
    expect(r.finishReason).toBe('tool_calls')
    expect(r.toolCalls).toHaveLength(1)
  })

  it('rejects an object with no envelope keys (plain prose object is not structure)', () => {
    expect(() => parseToolBridgeAnswer({ city: 'Moscow', temperature: 5 }, policy)).toThrow(ToolBridgeError)
  })

  it('parses a text answer (empty tool_calls) as a stop turn', () => {
    const r = parseToolBridgeAnswer(JSON.stringify({ content: 'Moscow is cold.', tool_calls: [] }), policy)
    expect(r.content).toBe('Moscow is cold.')
    expect(r.toolCalls).toEqual([])
    expect(r.finishReason).toBe('stop')
  })

  it('parses one tool call into OpenAI shape with VALID JSON arguments', () => {
    const r = parseToolBridgeAnswer(
      JSON.stringify({ content: '', tool_calls: [{ name: 'get_weather', args: '{"city":"Moscow"}' }] }),
      policy,
    )
    expect(r.finishReason).toBe('tool_calls')
    expect(r.toolCalls).toHaveLength(1)
    const call = r.toolCalls[0]!
    expect(call.type).toBe('function')
    expect(call.function.name).toBe('get_weather')
    expect(call.id).toMatch(/^call_[0-9a-f]{24}$/)
    expect(JSON.parse(call.function.arguments)).toEqual({ city: 'Moscow' })
  })

  it('parses two tool calls with distinct ids', () => {
    const r = parseToolBridgeAnswer(
      JSON.stringify({
        content: 'checking both',
        tool_calls: [
          { name: 'get_weather', args: '{"city":"Moscow"}' },
          { name: 'get_weather', args: '{"city":"Sochi"}' },
        ],
      }),
      policy,
    )
    expect(r.toolCalls).toHaveLength(2)
    expect(r.toolCalls[0]!.id).not.toBe(r.toolCalls[1]!.id)
    expect(r.content).toBe('checking both')
    expect(r.finishReason).toBe('tool_calls')
  })

  it('accepts a structured args object and canonicalizes it to a JSON string', () => {
    const r = parseToolBridgeAnswer(JSON.stringify({ content: '', tool_calls: [{ name: 'get_weather', args: { city: 'Kazan' } }] }), policy)
    expect(r.toolCalls[0]!.function.arguments).toBe('{"city":"Kazan"}')
  })

  it('tolerates the response_type/arguments_json alias vocabulary', () => {
    const env = parseToolBridgeEnvelope(
      '```json\n{"response_type":"tool_calls","content":"","tool_calls":[{"name":"get_weather","arguments_json":"{\\"city\\":\\"Omsk\\"}"}]}\n```',
      policy,
    )
    expect(env?.tool_calls).toHaveLength(1)
    expect(JSON.parse(env!.tool_calls[0]!.args)).toEqual({ city: 'Omsk' })
  })

  it('extracts the envelope from prose-wrapped JSON', () => {
    const raw = 'Sure, here it is:\n{"content":"hi","tool_calls":[]}\nThanks!'
    expect(extractJsonObject(raw)).toEqual({ content: 'hi', tool_calls: [] })
    expect(parseToolBridgeAnswer(raw, policy).content).toBe('hi')
  })

  it('throws a 502-class error for structureless text (NOT a silent 200)', () => {
    try {
      parseToolBridgeAnswer('The weather in Moscow is 5 degrees, no tool needed.', policy)
      throw new Error('should have thrown')
    } catch (e) {
      expect(e).toBeInstanceOf(ToolBridgeError)
      expect((e as ToolBridgeError).code).toBe('invalid_output')
      expect((e as ToolBridgeError).status).toBe(502)
    }
  })

  it('throws for an empty answer', () => {
    expect(() => parseToolBridgeAnswer('   ', policy)).toThrow(/empty model answer/)
  })

  it('throws when arguments are not JSON at all (broken JSON path)', () => {
    expect(() =>
      parseToolBridgeAnswer(JSON.stringify({ content: '', tool_calls: [{ name: 'get_weather', args: '{city: Moscow' }] }), policy),
    ).toThrow(/not valid JSON/)
  })

  it('throws when a required tool call is answered with content only', () => {
    const required = resolveToolBridgePolicy(TOOLS_BODY, 'required', [])
    expect(() => parseToolBridgeAnswer(JSON.stringify({ content: 'no idea', tool_calls: [] }), required)).toThrow(/required a tool call/)
  })

  it('mints unique ids and JSON-string arguments via toOpenAiToolCalls', () => {
    const calls = toOpenAiToolCalls({ content: '', tool_calls: [{ name: 'get_weather', args: '{}' }] })
    expect(calls[0]!.id).toMatch(/^call_/)
    expect(newToolCallId()).not.toBe(newToolCallId())
    expect(JSON.parse(calls[0]!.function.arguments)).toEqual({})
  })
})

describe('TB-UNIT: retry loop', () => {
  const policy = resolveToolBridgePolicy(TOOLS_BODY, undefined, [])

  it('retries up to 2 times and returns the good answer on the second attempt', async () => {
    let asks = 0
    const retries: number[] = []
    const { result, attempts } = await withToolBridgeRetries('not json at all', policy, async () => {
      asks++
      return JSON.stringify({ content: '', tool_calls: [{ name: 'get_weather', args: '{"city":"Moscow"}' }] })
    }, { onRetry: (n: number) => retries.push(n) })
    expect(asks).toBe(1)
    expect(retries).toEqual([1])
    expect(attempts).toBe(2)
    expect(result.toolCalls).toHaveLength(1)
  })

  it('gives up after TOOL_BRIDGE_MAX_RETRIES and rethrows the last error', async () => {
    let asks = 0
    await expect(
      withToolBridgeRetries('broken', policy, async () => {
        asks++
        return 'still broken'
      }),
    ).rejects.toBeInstanceOf(ToolBridgeError)
    expect(asks).toBe(TOOL_BRIDGE_MAX_RETRIES)
    expect(maxToolBridgeAttempts()).toBe(3)
  })

  it('does not retry a client-side tools[] error', async () => {
    let asks = 0
    // The policy constructor throws synchronously — that is the point: a
    // malformed tools[] must fail BEFORE any model call.
    expect(() => resolveToolBridgePolicy([{ type: 'function' }], undefined, [])).toThrow(
      expect.objectContaining({ code: 'invalid_tools' }),
    )
    expect(asks).toBe(0)
  })

  it('classifies retryable errors', () => {
    expect(isRetryableToolBridgeError(new ToolBridgeError('invalid_output', 'x'))).toBe(true)
    expect(isRetryableToolBridgeError(new ToolBridgeError('invalid_tools', 'x'))).toBe(false)
  })

  it('never retries an overflow (same prompt can never fit)', () => {
    expect(isRetryableToolBridgeError(new ToolBridgeError('prompt_overflow', 'prompt too long'))).toBe(false)
    // Overflow-shaped raw text (engine-level rethrow without a code).
    expect(isRetryableToolBridgeError(new Error('Input is too long: 2500000 tokens > 1048576 maximum'))).toBe(false)
  })
})

describe('TB-UNIT: prompt overflow classification (Phase 4 fix A-1)', () => {
  const policy = resolveToolBridgePolicy(TOOLS_BODY, undefined, [])

  it('isAgyPromptOverflow matches the documented agy overflow signatures', () => {
    expect(isAgyPromptOverflow('PROMPT_OVERFLOW: conversation too large')).toBe(true)
    expect(isAgyPromptOverflow('Error: prompt is too long for this model')).toBe(true)
    expect(isAgyPromptOverflow('request exceeds the maximum context length')).toBe(true)
    expect(isAgyPromptOverflow('Input is too long: 2500000 tokens > 1048576 maximum')).toBe(true)
    expect(isAgyPromptOverflow('Individual quota reached')).toBe(false)
    expect(isAgyPromptOverflow('model overloaded')).toBe(false)
  })

  it('reclassifies a no-envelope overflow answer as a 413-class error (was a raw 502)', () => {
    // The real shape: agy reports the failure INSTEAD of the JSON envelope.
    try {
      parseToolBridgeAnswer('Error: Input is too long: 2500000 tokens > 1048576 maximum', policy)
      throw new Error('should have thrown')
    } catch (e) {
      expect(e).toBeInstanceOf(ToolBridgeError)
      expect((e as ToolBridgeError).code).toBe('prompt_overflow')
      expect((e as ToolBridgeError).status).toBe(413)
      expect((e as ToolBridgeError).message).toContain('prompt too long for model context')
      expect((e as ToolBridgeError).message).toContain('Input is too long')
    }
  })

  it('reclassifies an {error} structured_output overflow object (no envelope keys)', () => {
    expect(() =>
      parseToolBridgeAnswer({ error: 'PROMPT_OVERFLOW: the request does not fit the model window' }, policy),
    ).toThrow(expect.objectContaining({ code: 'prompt_overflow', status: 413 }))
  })

  it('keeps generic structureless answers at the 502 class', () => {
    try {
      parseToolBridgeAnswer('The weather in Moscow is 5 degrees, no tool needed.', policy)
      throw new Error('should have thrown')
    } catch (e) {
      expect(e).toBeInstanceOf(ToolBridgeError)
      expect((e as ToolBridgeError).code).toBe('invalid_output')
      expect((e as ToolBridgeError).status).toBe(502)
    }
  })

  it('prefers the envelope when the answer parses (overflow only on the no-structure path)', () => {
    // A legit envelope that merely MENTIONS overflow in prose must not flip to 413.
    const res = parseToolBridgeAnswer(
      JSON.stringify({ content: 'The log said: prompt is too long', tool_calls: [] }),
      policy,
    )
    expect(res.content).toBe('The log said: prompt is too long')
    expect(res.finishReason).toBe('stop')
  })

  it('does not retry an overflow answer inside the retry loop', async () => {
    let asks = 0
    await expect(
      withToolBridgeRetries('Error: prompt is too long for this model', policy, async () => {
        asks++
        return JSON.stringify({ content: 'ok', tool_calls: [] })
      }),
    ).rejects.toMatchObject({ code: 'prompt_overflow', status: 413 })
    expect(asks).toBe(0) // terminal on the FIRST sight — no retry spawn
  })

  it('agyOverflowCause finds the cause in failure-channel texts, not in prose', () => {
    expect(agyOverflowCause('Error: 429 RESOURCE_EXHAUSTED: Individual quota reached')).toBeUndefined()
    expect(agyOverflowCause('agy exited with code 1: model overloaded')).toBeUndefined()
    expect(agyOverflowCause('')).toBeUndefined()
    expect(agyOverflowCause('   \n  ')).toBeUndefined()
    const cause = agyOverflowCause('Error: Input is too long: 2500000 tokens > 1048576 maximum\n')
    expect(cause).toBe('Error: Input is too long: 2500000 tokens > 1048576 maximum')
  })

  it('agyPromptOverflowError carries the verbatim agy text after the prefix', () => {
    const err = agyPromptOverflowError('PROMPT_OVERFLOW: 1.2M tokens')
    expect(err.code).toBe('prompt_overflow')
    expect(err.status).toBe(413)
    expect(err.message).toBe('prompt too long for model context: PROMPT_OVERFLOW: 1.2M tokens')
  })
})

// ---------------------------------------------------------------------------
// Layer 2: end-to-end through engine + fake agy + HTTP
// ---------------------------------------------------------------------------

async function postChat(s: ReturnType<typeof makeServer>, body: unknown) {
  return s.built.app.inject({ method: 'POST', url: '/v1/chat/completions', payload: body as object, headers: { 'content-type': 'application/json' } })
}

describe('TB-E2E: non-streaming', () => {
  it('text answer: content only, finish_reason stop, no tool_calls', async () => {
    useEvents(bridgeEvents(JSON.stringify({ content: 'Moscow: -3 °C, light snow.', tool_calls: [] })))
    const s = track(makeServer())
    const res = await postChat(s, chatPayload())
    expect(res.statusCode).toBe(200)
    const body = res.json() as { choices: Array<{ message: { content: string | null; tool_calls?: unknown[] }; finish_reason: string }> }
    expect(body.choices[0]!.finish_reason).toBe('stop')
    expect(body.choices[0]!.message.content).toBe('Moscow: -3 °C, light snow.')
    expect(body.choices[0]!.message.tool_calls).toBeUndefined()
  })

  it('one tool_call: id/type/name + valid JSON arguments, finish_reason tool_calls', async () => {
    useEvents(bridgeEvents(JSON.stringify({ content: '', tool_calls: [{ name: 'get_weather', args: '{"city":"Moscow"}' }] })))
    const s = track(makeServer())
    const res = await postChat(s, chatPayload())
    expect(res.statusCode).toBe(200)
    const body = res.json() as {
      choices: Array<{ message: { content: string | null; tool_calls?: Array<{ id: string; type: string; function: { name: string; arguments: string } }> }; finish_reason: string }>
    }
    const choice = body.choices[0]!
    expect(choice.finish_reason).toBe('tool_calls')
    expect(choice.message.content).toBeNull()
    expect(choice.message.tool_calls).toHaveLength(1)
    const call = choice.message.tool_calls![0]!
    expect(call.type).toBe('function')
    expect(call.function.name).toBe('get_weather')
    expect(call.id).toMatch(/^call_[0-9a-f]{24}$/)
    expect(JSON.parse(call.function.arguments)).toEqual({ city: 'Moscow' })
  })

  it('two tool_calls: both surfaced in order with distinct ids', async () => {
    useEvents(
      bridgeEvents(
        JSON.stringify({
          content: 'Checking two cities.',
          tool_calls: [
            { name: 'get_weather', args: '{"city":"Moscow"}' },
            { name: 'get_weather', args: '{"city":"Sochi"}' },
          ],
        }),
      ),
    )
    const s = track(makeServer())
    const res = await postChat(s, chatPayload())
    expect(res.statusCode).toBe(200)
    const body = res.json() as {
      choices: Array<{ message: { content: string | null; tool_calls?: Array<{ id: string; function: { arguments: string } }> }; finish_reason: string }>
    }
    const calls = body.choices[0]!.message.tool_calls!
    expect(calls).toHaveLength(2)
    expect(calls.map((c) => JSON.parse(c.function.arguments).city)).toEqual(['Moscow', 'Sochi'])
    expect(calls[0]!.id).not.toBe(calls[1]!.id)
    expect(body.choices[0]!.message.content).toBe('Checking two cities.')
    expect(body.choices[0]!.finish_reason).toBe('tool_calls')
  })

  it('raw streamed agy text never leaks: the client sees ONLY the recut turn', async () => {
    // The fake streams thinking + text BEFORE the result envelope; without the
    // recut those raw chunks (including the thinking annotation) would reach
    // the client and corrupt the assistant message.
    useEvents(
      bridgeEvents(JSON.stringify({ content: 'answered', tool_calls: [] }), {
        extraEvents: [
          JSON.stringify({ event: 'step_update', step_update: { conversation_id: 'conv-bridge-1', step_index: 1, state: 'DONE', step_type: 'thinking', text: 'RAW_THINKING_LEAK' } }),
          JSON.stringify({ event: 'step_update', step_update: { conversation_id: 'conv-bridge-1', step_index: 2, state: 'DONE', step_type: 'text', text: 'RAW_STREAM_LEAK' } }),
        ],
      }),
    )
    const s = track(makeServer())
    const res = await postChat(s, chatPayload())
    const raw = res.body
    expect(res.statusCode).toBe(200)
    expect(raw).not.toContain('RAW_THINKING_LEAK')
    expect(raw).not.toContain('RAW_STREAM_LEAK')
    const body = JSON.parse(raw) as { choices: Array<{ message: { content: string | null } }> }
    expect(body.choices[0]!.message.content).toBe('answered')
  })

  it('broken JSON on attempt 1 → retried, client sees the good second attempt only', async () => {
    // FAKE_AGY_EVENTS_FILES replays one event file per spawn: attempt 1
    // answers with broken arguments JSON, the retry answers correctly. The
    // client must see ONLY the good turn, and exactly 2 spawns must happen.
    const dir = mkdtempSync(join(tmpdir(), 'agy-bridge-retry-'))
    const badFile = join(dir, 'bad.ndjson')
    const goodFile = join(dir, 'good.ndjson')
    writeFileSync(badFile, bridgeEvents(JSON.stringify({ content: '', tool_calls: [{ name: 'get_weather', args: '{city: Moscow' }] }), { conversationId: 'conv-bad' }), 'utf8')
    writeFileSync(goodFile, bridgeEvents(JSON.stringify({ content: '', tool_calls: [{ name: 'get_weather', args: '{"city":"Moscow"}' }] }), { conversationId: 'conv-good' }), 'utf8')
    const argsFile = join(dir, 'args.jsonl')
    const seqFile = join(dir, 'seq')
    writeFileSync(seqFile, '0', 'utf8')
    delete process.env.FAKE_AGY_EVENTS_FILE
    process.env.FAKE_AGY_EVENTS_FILES = `${badFile},${goodFile}`
    process.env.FAKE_AGY_EVENTS_SEQ_FILE = seqFile
    process.env.FAKE_AGY_ARGS_FILE = argsFile
    const s = track(makeServer())
    const res = await postChat(s, chatPayload())
    expect(res.statusCode).toBe(200)
    const body = res.json() as { choices: Array<{ message: { tool_calls?: Array<{ function: { arguments: string } }> }; finish_reason: string }> }
    expect(body.choices[0]!.finish_reason).toBe('tool_calls')
    expect(JSON.parse(body.choices[0]!.message.tool_calls![0]!.function.arguments)).toEqual({ city: 'Moscow' })
    // Two spawns happened (attempt 1 + exactly one retry), never more.
    const spawns = readFileSync(argsFile, 'utf8').split('\n').filter((l) => l.trim() !== '')
    expect(spawns.length).toBe(2)
    delete process.env.FAKE_AGY_EVENTS_FILES
    delete process.env.FAKE_AGY_EVENTS_SEQ_FILE
  })

  it('structureless answer after the retries → error body, never a silent 200', async () => {
    // Every attempt answers with plain prose (no envelope): the bridge must
    // fail with the 502-class error after its retries — never 200 with an
    // empty assistant message.
    useEvents(bridgeEvents('The weather in Moscow is 5 degrees today.'))
    const s = track(makeServer())
    const res = await postChat(s, chatPayload())
    expect(res.statusCode).toBe(502)
    expect(res.body).toContain('no structured answer')
  })

  it('plain prose (no envelope) → HTTP 502 with the bridge error text in the body', async () => {
    useEvents(bridgeEvents('The weather in Moscow is 5 degrees today.'))
    const s = track(makeServer())
    const res = await postChat(s, chatPayload())
    expect(res.statusCode).toBe(502)
    expect(res.body).toContain('no structured answer')
  })

  it('overflow answer instead of the envelope → explicit "prompt too long" error, no retries (fix A-1)', async () => {
    // agy reports "prompt too long" through the SAME no-envelope channel. The
    // classified ToolBridgeError carries code='prompt_overflow'/status=413 and
    // its message reaches the client verbatim (the HTTP status wiring lives in
    // the engine recut — REPORT-fixA.md documents the 2-line residual); the
    // regression this test pins: the cause is EXPLICIT (not a bare 502
    // "no structured answer") and NO retry spawn burns on a prompt that can
    // never fit (see the spawns assertion).
    const dir = mkdtempSync(join(tmpdir(), 'agy-bridge-overflow-'))
    const argsFile = join(dir, 'args.jsonl')
    useEvents(bridgeEvents('Error: Input is too long: 2500000 tokens > 1048576 maximum'))
    process.env.FAKE_AGY_ARGS_FILE = argsFile
    const s = track(makeServer())
    const res = await postChat(s, chatPayload())
    delete process.env.FAKE_AGY_ARGS_FILE
    // The engine recut wires the ToolBridgeError code through to HTTP:
    // prompt_overflow → 413 (PROMPT_OVERFLOW row in errorStatus, was 502
    // before the residual wiring landed — the same fix this test pins).
    expect(res.statusCode).toBe(413)
    const body = res.json() as { error?: { code?: string; message?: string } }
    expect(body.error?.code).toBe('PROMPT_OVERFLOW')
    expect(body.error?.message).toContain('prompt too long for model context')
    expect(body.error?.message).toContain('Input is too long')
    // Terminal on the FIRST sight: attempt 1 only, zero retry spawns.
    const spawns = readFileSync(argsFile, 'utf8').split('\n').filter((l) => l.trim() !== '')
    expect(spawns.length).toBe(1)
  })

  it('overflow on a STREAMING bridge request → SSE error frame with the prompt-too-long cause', async () => {
    useEvents(bridgeEvents('Error: prompt is too long for this model'))
    const s = track(makeServer())
    const res = await postChat(s, chatPayload({ stream: true }))
    expect(String(res.headers['content-type'])).toContain('text/event-stream')
    expect(res.body).toContain('"error"')
    expect(res.body).toContain('prompt too long for model context')
    expect(res.body).not.toContain('tool_calls')
  })

  it('exit12 crash → not a bridge success (upstream failure surfaces)', async () => {
    delete process.env.FAKE_AGY_EVENTS_FILE
    process.env.FAKE_AGY_MODE = 'exit12'
    const s = track(makeServer())
    const res = await postChat(s, chatPayload())
    // agy exits 12 with no result envelope: the run-level failure must reach
    // the client (an error body or an SSE error frame) — never a fabricated
    // assistant turn, and never an empty 200 success.
    expect(res.statusCode).toBe(502)
    expect(res.body).not.toContain('tool_calls')
    expect(res.body).not.toContain('"finish_reason":"tool_calls"')
    const body = res.json() as { error?: { code?: string; message?: string }; choices?: unknown }
    expect(body.choices).toBeUndefined()
    expect(body.error?.code).toBe('PROCESS_EXIT')
    expect(body.error?.message).toContain('code 12')
  })

  it('exit12 crash on a STREAMING bridge request → SSE error frame, no tool_calls', async () => {
    // The streaming leg cannot answer 502 after headers, so the failure must
    // ride as an SSE `data: {"error":…}` frame with no assistant delta and no
    // fabricated tool call — an empty `[DONE]`-only stream would be a silent
    // success in disguise.
    delete process.env.FAKE_AGY_EVENTS_FILE
    process.env.FAKE_AGY_MODE = 'exit12'
    const s = track(makeServer())
    const res = await postChat(s, chatPayload({ stream: true }))
    expect(String(res.headers['content-type'])).toContain('text/event-stream')
    expect(res.body).not.toContain('tool_calls')
    expect(res.body).toContain('"error"')
    expect(res.body).toContain('PROCESS_EXIT')
  })

  it('native tool step BEFORE the result envelope → waits for settle, serves the real answer (no spurious 502)', async () => {
    // t_03a5ca20: the raw span cuts on the first COMPLETED native tool step
    // while the run is still live. The recut used to read the recording
    // immediately — no envelope yet — and answer a run that then settles OK
    // with a bogus 'no result envelope' 502. The bridge must wait for the
    // result envelope and recut the real answer.
    delete process.env.FAKE_AGY_EVENTS_FILE
    process.env.FAKE_AGY_MODE = 'tool-cut'
    process.env.FAKE_AGY_DELAY_MS = '500'
    const s = track(makeServer())
    const res = await postChat(s, chatPayload())
    expect(res.statusCode).toBe(200)
    const body = res.json() as { choices: Array<{ message: { content: string | null }; finish_reason: string }> }
    expect(body.choices[0]!.message.content).toBe('tool cut answered')
    expect(body.choices[0]!.finish_reason).toBe('stop')
  })

  it('#902 CANCELED-empty run → the CLIENT gets the #902 cause, not "no result envelope"', async () => {
    // The recut must prefer rec.failureInfo (the engine's real classification:
    // 'agy ended with an empty response (CANCELED, google antigravity #902)')
    // over the generic structureless-answer text (postановка п.3c).
    delete process.env.FAKE_AGY_EVENTS_FILE
    process.env.FAKE_AGY_MODE = 'canceled-empty'
    const s = track(makeServer())
    const res = await postChat(s, chatPayload())
    expect(res.statusCode).toBe(502)
    const body = res.json() as { error?: { code?: string; message?: string } }
    expect(body.error?.code).toBe('INVALID_OUTPUT')
    expect(body.error?.message).toContain('902')
    expect(body.error?.message).not.toContain('no result envelope')
  })

  it('noise mode (non-JSON stdout) still parses the envelope', async () => {
    useEvents(bridgeEvents(JSON.stringify({ content: '', tool_calls: [{ name: 'get_weather', args: '{"city":"Moscow"}' }] })))
    process.env.FAKE_AGY_MODE = 'noise'
    const s = track(makeServer())
    const res = await postChat(s, chatPayload())
    expect(res.statusCode).toBe(200)
    const body = res.json() as { choices: Array<{ finish_reason: string; message: { tool_calls?: unknown[] } }> }
    expect(body.choices[0]!.finish_reason).toBe('tool_calls')
    expect(body.choices[0]!.message.tool_calls).toHaveLength(1)
  })

  it('structured_output OBJECT in the result event → tool_calls (live agy 1.6 shape)', async () => {
    // The LIVE agy 1.6.5 shape: `result.response` is EMPTY and the envelope
    // rides `result.structured_output` as an object. The bridge must read that
    // field — reading only `response` returned 502 "no usable answer" on a
    // perfectly good answer.
    useEvents(bridgeStructuredEvents({ content: '', tool_calls: [{ name: 'get_weather', args: '{"city":"Moscow"}' }] }))
    process.env.FAKE_AGY_MODE = 'ok'
    const s = track(makeServer())
    const res = await postChat(s, chatPayload())
    expect(res.statusCode).toBe(200)
    const body = res.json() as { choices: Array<{ finish_reason: string; message: { tool_calls?: Array<{ function: { name: string; arguments: string } }> } }> }
    expect(body.choices[0]!.finish_reason).toBe('tool_calls')
    expect(body.choices[0]!.message.tool_calls![0]!.function.name).toBe('get_weather')
    expect(JSON.parse(body.choices[0]!.message.tool_calls![0]!.function.arguments)).toEqual({ city: 'Moscow' })
  })

  it('structured_output OBJECT with a text answer → finish_reason stop', async () => {
    useEvents(bridgeStructuredEvents({ content: 'Moscow is 5 degrees.', tool_calls: [] }))
    process.env.FAKE_AGY_MODE = 'ok'
    const s = track(makeServer())
    const res = await postChat(s, chatPayload())
    expect(res.statusCode).toBe(200)
    const body = res.json() as { choices: Array<{ finish_reason: string; message: { content: string | null; tool_calls?: unknown[] } }> }
    expect(body.choices[0]!.finish_reason).toBe('stop')
    expect(body.choices[0]!.message.content).toBe('Moscow is 5 degrees.')
    expect(body.choices[0]!.message.tool_calls ?? []).toHaveLength(0)
  })
})

describe('TB-E2E: streaming', () => {
  function parseSse(body: string): Array<Record<string, unknown>> {
    return body
      .split('\n\n')
      .map((b) => b.trim())
      .filter((b) => b !== '')
      .map((b) => b.split('\n').filter((l) => l.startsWith('data:')).map((l) => l.slice(5).trim()).join(''))
      .filter((d) => d !== '[DONE]' && d !== '')
      .flatMap((d) => {
        try {
          return [JSON.parse(d) as Record<string, unknown>]
        } catch {
          return []
        }
      })
  }

  it('streaming tool_call: delta.tool_calls carries name+arguments, finish_reason tool_calls', async () => {
    useEvents(bridgeEvents(JSON.stringify({ content: '', tool_calls: [{ name: 'get_weather', args: '{"city":"Moscow"}' }] })))
    const s = track(makeServer())
    const res = await postChat(s, chatPayload({ stream: true }))
    expect(res.statusCode).toBe(200)
    expect(String(res.headers['content-type'])).toContain('text/event-stream')
    const frames = parseSse(res.body)
    const toolDelta = frames
      .map((f) => (f.choices as Array<{ delta?: { tool_calls?: Array<{ id: string; type: string; function: { name: string; arguments: string } }> } }> | undefined)?.[0]?.delta?.tool_calls)
      .find((c) => c !== undefined)
    expect(toolDelta).toBeDefined()
    expect(toolDelta![0]!.type).toBe('function')
    expect(toolDelta![0]!.function.name).toBe('get_weather')
    expect(toolDelta![0]!.id).toMatch(/^call_/)
    expect(JSON.parse(toolDelta![0]!.function.arguments)).toEqual({ city: 'Moscow' })
    const finish = frames
      .map((f) => (f.choices as Array<{ finish_reason: string | null }> | undefined)?.[0]?.finish_reason)
      .filter((r) => r !== null && r !== undefined)
    expect(finish).toContain('tool_calls')
    expect(res.body).toContain('data: [DONE]')
  })

  it('streaming text answer: content deltas and finish_reason stop', async () => {
    useEvents(bridgeEvents(JSON.stringify({ content: 'Moscow is cold.', tool_calls: [] })))
    const s = track(makeServer())
    const res = await postChat(s, chatPayload({ stream: true }))
    expect(res.statusCode).toBe(200)
    const frames = parseSse(res.body)
    const text = frames
      .map((f) => (f.choices as Array<{ delta?: { content?: string } }> | undefined)?.[0]?.delta?.content)
      .filter((c) => typeof c === 'string' && c !== '')
      .join('')
    expect(text).toBe('Moscow is cold.')
    const finish = frames
      .map((f) => (f.choices as Array<{ finish_reason: string | null }> | undefined)?.[0]?.finish_reason)
      .filter((r) => r !== null && r !== undefined)
    expect(finish).toEqual(['stop'])
  })

  it('streaming broken answer → error, no fabricated success', async () => {
    useEvents(bridgeEvents('The weather in Moscow is 5 degrees today.'))
    const s = track(makeServer())
    const res = await postChat(s, chatPayload({ stream: true }))
    // The bridge failure must be visible: either an immediate error body or an
    // SSE error frame carrying the bridge message. A 200 with empty content is
    // the failure mode this test exists to prevent.
    const bodyIsError = res.statusCode >= 400
    const frames = parseSse(res.body)
    const sawErrorFrame = frames.some((f) => typeof f.error !== 'undefined')
    const sawBridgeText = res.body.includes('no structured answer')
    expect(bodyIsError || sawErrorFrame || sawBridgeText).toBe(true)
    expect(res.body).not.toMatch(/"content":"[^"]/)
  })
})
