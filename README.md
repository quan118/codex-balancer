# codex-balancer

<img width="3268" height="1438" alt="CleanShot 2026-09-15 at 11 57 29@2x" src="https://github.com/user-attachments/assets/530effd5-a001-485b-bc92-23340aadfd37" />

_I wrote this README by hand, no LLM :)_

Balancing usage across several ChatGPT Codex accounts.

- One Responses endpoint, with HTTP and WebSocket transports
- 1 single SQLite database

## Install

```sh
go install github.com/supabitapp/codex-balancer@latest
```

## Running the proxy

```
codex-balancer server           # serve the proxy with a TUI at
```

The server runs at http://127.0.0.1:8317

- `/v1/responses` — HTTP `POST` (SSE or JSON) and WebSocket `GET`
- `/codex/responses` and `/v1/codex/responses` — equivalent HTTP `POST` and WebSocket `GET` aliases for pi
- `/v1/alpha/search`, `/v1/images/generations` and `/v1/images/edits` — unary `POST` proxies for Codex's standalone web search and image tools, sent with a pool account's credentials
- `/dashboard` — HTML dashboard
- `/stats` — JSON stats of the server
- `/accounts` — add an account. On a real server, send this to your friends so they join the pool without exposing credentials.

The TUI also allows you to put a `pause` or `priority` on some accounts.

## CLI

There is a CLI to manage the accounts

```sh
codex-balancer accounts add                 # sign in through a local browser
codex-balancer accounts list
codex-balancer accounts mode you@example.com priority
codex-balancer accounts mode you@example.com normal
```

Adding an account preserves its existing model training setting.
Self-serve Business Pro Lite
(`self_serve_business_prolite`) accounts route using their per-account quota.
Other Business and Enterprise workspaces are displayed but excluded from routing.

Use the CLI to manage client API keys:

```sh
codex-balancer keys add my-laptop
codex-balancer keys list
codex-balancer keys rm my-laptop
```

`keys list` includes the input, cached, output, and total tokens attributed to
each key.

State lives in `~/.codex-balancer/state.db`.

## Point Codex at it

On each machine that runs Codex, export a key from the server before starting
Codex:

```sh
export CODEX_BALANCER_API_KEY="<server-key>"
```

add that to your `~/.zshrc` or whatever env loading mechanism or shell you use.

Then in `~/.codex/config.toml`:

```toml
model_provider = "balancer"

[model_providers.balancer]
name = "OpenAI" # must be exactly this for server-side compaction to work
base_url = "http://127.0.0.1:8317/v1"
env_key = "CODEX_BALANCER_API_KEY"
requires_openai_auth = true
supports_websockets = true
```

## Point pi at it

```sh
export CODEX_BALANCER_API_KEY="<server-key>"
```

Merge this into `~/.pi/agent/models.json`, keeping any unrelated providers:

```json
{
  "providers": {
    "openai-codex": {
      "baseUrl": "http://127.0.0.1:8317/v1",
      "apiKey": "$CODEX_BALANCER_API_KEY"
    }
  }
}
```

## HTTP Responses contract

HTTP POST requests go to the configured upstream's `/responses` endpoint over
HTTP. WebSocket GET requests use upstream WebSockets. The balancer does not
switch transports or replay inference requests.

The balancer selects a pool account and replaces the authentication headers.
Request fields pass through, including `stream`, `stream_options`, instructions,
and input history. The configured fast-mode policy can override `service_tier`.
The upstream validates generation options and conversation references.

HTTP supports SSE and JSON responses. SSE is forwarded incrementally; when a
non-streaming client receives an upstream SSE response, the balancer collects
its output into JSON. Account usage and session ownership are tracked for both
transports. Request bodies and individual response events are capped at 256 MiB.

To use HTTP from Codex, set `supports_websockets = false` in the provider
configuration and resume the session. HTTP acceptance of a large conversation
depends on the upstream service's limits.

## Observability

HTTP Responses has verbose, request-correlated logs. Use `server -no-tui -json`
for JSON logs; clients can report `X-Codex-Balancer-Request-Id` from response
headers. Add `-otel` to export real traces to a configured OTLP HTTP collector:

```sh
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 codex-balancer server -no-tui -json -otel
```

See [OBSERVABILITY.md](OBSERVABILITY.md) for trace configuration, privacy/buffering
limits, and how to check account-switch boundaries and actual cached-token usage.
Tracing is optional and does not change routing or retry behavior.

## Routing

Routing logic is in ROUTING.md.
