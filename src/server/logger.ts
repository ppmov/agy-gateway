// pino logger factory: NDJSON to stdout (charter §8). Redaction is the last
// line of defense against secret leakage — request/headers payloads must
// already be metadata-only at every log site (development.md §8), and any
// free-form engine text is passed through redactLine() by the caller.
// New code, not a port; the redaction paths follow the G4 gate discipline.
import pino from 'pino'

const REDACT_PATHS = [
  'req.headers.authorization',
  'req.headers.cookie',
  'req.headers["x-api-key"]',
  '*.authorization',
  '*.apiKey',
  '*.api_key',
]

export function buildLogger(
  env: NodeJS.ProcessEnv = process.env,
  destination?: pino.DestinationStream,
): pino.Logger {
  const level = env.AGY_PROXY_LOG_LEVEL ?? 'info'
  const options: pino.LoggerOptions = {
    level,
    redact: { paths: REDACT_PATHS, censor: '[redacted]' },
  }
  if (destination !== undefined) {
    return pino(options, destination)
  }
  // Async stdout sink (EPIPE fix): the bare pino() default writes synchronously
  // to process.stdout, and when the log reader goes away (journal rotation,
  // pipe close) the failed write escalates to an uncaughtException — the
  // process-level crashToExit then exits 1 and takes in-flight HTTP streams
  // with it. pino.destination({ dest: 1, sync: false }) moves writes to a
  // background sink whose failures surface as 'error' events instead of raw
  // throws; they are swallowed here because a lost log line must never cost
  // the gateway (and its live client streams) the process.
  const sink = pino.destination({ dest: 1, sync: false })
  sink.on('error', () => {})
  return pino(options, sink)
}
