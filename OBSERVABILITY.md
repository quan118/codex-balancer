# HTTP Responses logs and traces

HTTP POST at `/v1/responses`, `/codex/responses` and `/v1/codex/responses`
emits correlated structured logs without requiring a collector. Optional
OpenTelemetry tracing exports the same request lifecycle via OTLP HTTP.
Observability does not select accounts, replay inference, or change
cache/ownership policy.

## Enable logs and optional tracing

For JSON logs on stderr and the existing rotating log file:

```sh
codex-balancer server -no-tui -json
```

The server logger already enables DEBUG. HTTP records use `msg="http responses"`,
a `stage`, and an independently generated `request_id`. The response header
`X-Codex-Balancer-Request-Id` exposes that ID for a client bug report. INFO covers
start, acceptance, usage, terminal and final summaries; DEBUG includes candidate
selection, setup attempts and each valid upstream/SSE event's framing metadata.
This is deliberately verbose while the adapter stabilizes. Plan log retention
and disk capacity accordingly.

Enable actual traces with `-otel` and an OTLP HTTP collector, for example:

```sh
export OTEL_EXPORTER_OTLP_ENDPOINT="http://127.0.0.1:4318"
export OTEL_SERVICE_NAME="codex-balancer"
codex-balancer server -no-tui -json -otel
```

`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` can instead specify the full traces URL.
The exporter supports standard OTLP HTTP header, TLS/certificate, compression and
protocol environment settings (`http/protobuf` by default, or `http/json`). Keep
collector authentication in `OTEL_EXPORTER_OTLP_TRACES_HEADERS`, not the endpoint
URL. The endpoint must not contain user info, a query, or a fragment.

Tracing is off by default even if OTEL environment variables are present. No
collector or tracing backend is installed by the balancer. This exports traces,
not OTLP logs or metrics: ship the JSON log file/stderr with your existing log
collector. Existing SQLite usage accounting and `/stats` remain authoritative.

Sampling uses the OTel SDK's `OTEL_TRACES_SAMPLER` and
`OTEL_TRACES_SAMPLER_ARG`. For example, after stabilization:

```sh
export OTEL_TRACES_SAMPLER="parentbased_traceidratio"
export OTEL_TRACES_SAMPLER_ARG="0.1"
```

The default samples new traces. Unsampled requests still have trace/span IDs in
logs; logging-only mode always has `request_id`.

## What to follow

Span names and `http.route` identify the actual supported path, including pi's
Codex aliases. Arbitrary query strings are not recorded. A typical request has
these spans:

```text
POST /v1/responses
  http.request.read
  codex.request.translate
  codex.route
    codex.websocket.handshake
    codex.credentials.wait          # if refresh is needed
    codex.pool_recovery             # if capacity recovery is needed
  codex.model_preflight             # only if compatibility changed during setup
    codex.route
      codex.websocket.handshake
  codex.response                   # one generation, not one span per token
```

`codex.credentials.refresh` is an independent operation span, linked to its
waiters. Its `codex.credentials.complete` child includes persistence/publication
and invalidation. It does not borrow the first request's cancellation lifetime.
Logs for shared work use `msg="credential refresh operation"` and `operation_id`;
request `refresh_joined` records point to that operation and its trace/span IDs.
The operation span ends before its lifecycle registration is released, so server
shutdown joins refresh completion before flushing tracing.

Useful request stages:

| Stage | What it establishes |
| --- | --- |
| `admission`, `authorization`, `body_read` | Admission/authentication, content encoding, wire/decoded sizes and read latency. Rejections are logged before upstream setup. |
| `method_not_allowed`, `rejected` | Unsupported methods on the three Responses paths, including requests rejected before admission/inference. |
| `normalized` | Model, requested/effective tier, fast-mode policy, input/tool counts, reasoning/tool replay counts and private prefix fingerprints. |
| `route_selected`, `routing_candidate` | Selected/prior/blocked owner, routing reason, provisional claim/join and candidate quota/priority state. Selection is not response acceptance. |
| `handshake_started`, `handshake_finished`, `upstream_ready` | Setup latency and status; still no `response.created`. |
| `turn_preflight`, `account_switch_ready` | Model/tier portability checks and a pre-transmission account change. |
| `upstream_write_started`, `upstream_write_finished` | The transmission boundary. A failed write may still have partially transmitted the request. |
| `response_accepted` | Ownership acceptance at `response.created`, whether SQLite persisted it, and the deduplicated `accepted_switch` flag. |
| `first_upstream_event`, `first_delta` | First-event/delta latency; the delta type distinguishes text, reasoning and tool progress. |
| `upstream_event`, `downstream_event` | DEBUG event type, size/index/sequence metadata; never the delta or tool arguments. |
| `reconnect_decision`, `failure`, `relay_close` | Quota/transport/policy failure and why the client must reconnect/replay. |
| `usage` | Actual input, cached, cache-write, output and reasoning tokens, plus `cached_input_percent` when input usage exists. |
| `terminal`, `cleanup`, `finished` | Terminal kind, cleanup, admission release, counts, elapsed time and cancellation state. |

`http_status` describes the status chosen by the adapter; a successful write or
flush is not proof the client consumed its output. Missing/incomplete usage is
not invented. Cancellation can prevent delivery while still producing a final
log/trace outcome.

For a client-reported request ID:

```sh
jq 'select(.msg == "http responses" and .request_id == "REQUEST_ID")' server.log
```

## Diagnosing a pi fallback failure

A sequence of `1012 upstream websocket unavailable` followed by repeated 405s
used to mean pi had switched to HTTP on a GET-only alias. Both POST aliases now
use the HTTP adapter and accept bounded zstd bodies, so they produce ordinary
request logs and can continue without clearing pi's fallback state.

Unsupported methods on these three exact paths are explicitly logged, with an
`Allow: GET, HEAD, POST` response and a correlation ID. GET requests missing the
WebSocket upgrade (or rejected during auth/handshake) use
`msg="responses request rejected"`, with method, path, status and a safe reason.
This does not add an access logger for unrelated application routes.

`msg="upstream websocket failure"` now covers legacy GET clients as well as HTTP
executions. It records read/write phase, error class (EOF, timeout, reset, close,
etc.), close status, connection age, time since the last event, a known event
category, pending/accepted turn counts and frame/read-limit byte counts. Session
and thread hashes correlate the failure with later HTTP fallback in the same
process. Raw error text and upstream close reasons are never logged. A close
code or elapsed time is evidence to investigate, not proof of quota exhaustion;
this diagnostic does not initiate replay or change account selection.

## Checking safe account switches and caching

Problem: treating a successful handshake or an uncommitted JSON response as a
safe replay point can duplicate work and move account-bound state. Moving a
healthy conversation to a different account also crosses its cache boundary.

Read the trace as a sequence:

1. `route_selected` with `routing_reason="retained"` should keep a healthy owner,
   even when another account has more quota or manual priority.
2. A first-turn compatibility switch must precede `upstream_write_started` and
   respect `portable_frame` plus the turn-state-header checks.
3. Once `write_attempted=true`, do not infer replay safety merely from
   `response_created=false` or `sse_committed=false`. The server never replays a
   transmitted generation; retry ownership remains with the client.
4. A usage rejection records whether `response.created` already happened, the
   presence of account-bound turn state, and the existing reconnect decision.
   The client supplies full history without an old response ID or turn-state
   token when moving accounts, analogous to resuming after a manual account
   change. Encrypted reasoning remains in that history, not in logs.
5. Count `response_accepted` records with `accepted_switch=true`, not handshakes
   or speculative account choices. This preserves joined-claim deduplication.
6. Compare successive requests' account, model, effective tier,
   `instructions_hash`, `tools_hash` and `prompt_cache_key_hash`. Then check
   actual cached tokens. Stable affinity/fingerprints are diagnostic clues,
   **not proof of a cache hit**. A zero cached count is not a reason to move a
   healthy owner automatically.

A shortened illustrative trace:

```text
request A: route_selected account=pool-a reason=retained
request A: upstream_write_started -> response_accepted accepted_switch=false
request A: usage input_tokens=100 cached_tokens=75 cached_input_percent=75
request B: usage rejection accepted_before_rejection=true retry_owner=client
request B: failure -> cleanup                         # no internal replay
request C: route_selected prior_owner=pool-a account=pool-b reason=owner_spent
request C: turn_preflight portable_frame=true -> upstream_write_started
request C: response_accepted accepted_switch=true     # new owner/cache boundary
```

These diagnostics help investigate title/background-model churn, changing prefix
settings, quota recovery and real cache usage. They do not reconcile client
model limits or promise upstream cache behavior.

## Privacy and operational limits

New HTTP observations use an explicit scalar metadata allowlist: no prompts,
images, tool arguments/results, encrypted reasoning content, raw error bodies,
API keys, OAuth tokens or arbitrary header maps. Known credentials are also
redacted from decoded identifier/code strings before logging or span attachment.
Session/thread/item identifiers and instruction/tool/cache-key fingerprints use
HMAC with a random, process-local key that is never logged. Fingerprints can be
compared within a process, not across restarts. Pool account IDs and model/tier
names remain visible to the operator. Protect logs and the collector accordingly.
Existing WebSocket logs remain available separately.

Incoming W3C trace context is linked to a new server trace rather than controlling
its identity or sampling. Client baggage/tracestate is not retained, and tracing
headers are not forwarded to the Codex inference upstream. Trace/request IDs are
never conversation affinity or authorization.

Tracing uses a nonblocking batch queue (1,024 spans, batches of 128), a one-second
batch interval, three-second export timeout and bounded span attributes/events.
There are no per-token spans or span events beyond the first-delta marker. Queue
pressure drops traces rather than blocking inference. Export retries are disabled;
collector errors use sanitized diagnostics. Final export shutdown has a five-second
budget after refresh completion. Telemetry is best effort, not accounting storage.
