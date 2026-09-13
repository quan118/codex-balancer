// Loaded by TestPiWebSocketToHTTPFallback from an isolated temporary directory.
// The pi symlink points at a read-only checkout with its installed dependencies.
import assert from "node:assert/strict";
import { zstdDecompressSync } from "node:zlib";
import {
  stream,
  closeOpenAICodexWebSocketSessions,
  getOpenAICodexWebSocketDebugStats,
} from "./pi/packages/ai/src/api/openai-codex-responses.ts";

const baseURL = process.env.CODEX_BALANCER_TEST_URL;
const origin = new URL(baseURL).origin;
assert.equal(new URL(baseURL).hostname, "127.0.0.1");
const expectedPath = `${new URL(baseURL).pathname.replace(/\/$/, "")}/codex/responses`;
const nativeFetch = globalThis.fetch;
const NativeWebSocket = globalThis.WebSocket;
const httpRequests = [];
let connections = 0;
globalThis.fetch = (input, init) => {
  const url = new URL(input instanceof Request ? input.url : input);
  assert.equal(url.origin, origin, "external network access forbidden");
  assert([expectedPath, "/_test/release"].includes(url.pathname));
  if (url.pathname === expectedPath) {
    const headers = new Headers(init.headers);
    assert.equal(init.method, "POST");
    assert.equal(headers.get("content-encoding"), "zstd", "pi did not exercise compressed fallback");
    const body = JSON.parse(zstdDecompressSync(init.body).toString("utf8"));
    assert.equal(body.store, false);
    assert.equal(body.previous_response_id, undefined);
    httpRequests.push(body);
  }
  return nativeFetch(input, init);
};
globalThis.WebSocket = class extends NativeWebSocket {
  constructor(input, options) {
    const url = new URL(input);
    assert.equal(url.origin, origin.replace(/^http/, "ws"), "external WebSocket forbidden");
    assert.equal(url.pathname, expectedPath);
    connections++;
    super(input, options);
  }
};

const sessionId = "pi-fallback-session";
const model = {
  id: "gpt-6-astra", name: "Local synthetic Codex", api: "openai-codex-responses", provider: "openai-codex",
  baseUrl: baseURL, reasoning: true, input: ["text"],
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 1000000, maxTokens: 1000,
};
const options = { apiKey: process.env.CODEX_BALANCER_TEST_KEY, transport: "auto", sessionId, maxRetries: 0, timeoutMs: 10000 };
const user = (content) => ({ role: "user", content, timestamp: Date.now() });
const context = { systemPrompt: "Local integration test", messages: [user("FIRST_TURN")] };

try {
  const firstStream = stream(model, context, options);
  for await (const event of firstStream) assert.notEqual(event.type, "error");
  const first = await firstStream.result();
  assert.equal(first.stopReason, "stop");
  assert(first.content.some((part) => part.type === "text" && part.text === "FIRST_ANSWER"));
  assert.equal(httpRequests.length, 0, "initial turn did not use WebSocket");
  context.messages.push(first, user("INTERRUPTED_TURN"));
  const interruptedStream = stream(model, context, options);
  let receivedPartial = false;
  for await (const event of interruptedStream) {
    if (event.type === "text_delta" && !receivedPartial) {
      receivedPartial = true;
      const response = await fetch(`${origin}/_test/release`, { method: "POST" });
      assert(response.ok);
    }
  }
  const interrupted = await interruptedStream.result();
  assert(receivedPartial);
  assert.equal(interrupted.stopReason, "error");
  assert.match(interrupted.errorMessage, /1012.*upstream websocket unavailable/);
  assert(interrupted.diagnostics.some((d) => d.type === "provider_transport_failure" && d.details.eventsEmitted === true));
  assert.equal(httpRequests.length, 0, "in-flight generation was replayed within the failed call");
  assert.equal(getOpenAICodexWebSocketDebugStats(sessionId).websocketFallbackActive, true);

  context.messages.push(user("CONTINUE_TURN"));
  const recoveredStream = stream(model, context, options);
  for await (const event of recoveredStream) assert.notEqual(event.type, "error", "HTTP fallback failed");
  const recovered = await recoveredStream.result();
  assert.equal(recovered.stopReason, "stop");
  assert(recovered.content.some((part) => part.type === "text" && part.text === "RECOVERED"));
  assert.equal(httpRequests.length, 1);
  const history = JSON.stringify(httpRequests[0].input);
  for (const value of ["FIRST_TURN", "FIRST_ANSWER", "opaque-history", "INTERRUPTED_TURN", "CONTINUE_TURN"]) assert(history.includes(value), `fallback lost ${value}`);
  context.messages.push(recovered, user("AFTER_RECOVERY"));
  const laterStream = stream(model, context, options);
  for await (const event of laterStream) assert.notEqual(event.type, "error");
  assert.equal((await laterStream.result()).stopReason, "stop");
  assert.equal(httpRequests.length, 2, "session-sticky SSE continuation was not exercised");
  assert.equal(connections, 1);
  assert.equal(getOpenAICodexWebSocketDebugStats(sessionId).deltaRequests, 1);
  console.log("Real pi adapter: cached WS turn, interrupted 1012, compressed full-history HTTP fallback, repeated HTTP continuation passed");
} finally {
  closeOpenAICodexWebSocketSessions();
}
