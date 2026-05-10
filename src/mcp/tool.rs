//! `Tool` adapter that bridges a single MCP-advertised tool into the
//! `ToolRegistry`.
//!
//! `McpTool` holds an `Arc<McpClient>` shared with sibling tools registered
//! against the same server, plus the `McpToolDescriptor` so its name (and,
//! later, schema) survive the trip into the registry. `Tool::call` forwards
//! to `McpClient::call_tool`, mapping `McpError` into `anyhow::Error` since
//! the `Tool` trait's failure type is `anyhow::Result`.
//!
//! # Retry policy (JAR2-25)
//!
//! Tool calls that fail with a *transient* `McpError` (`Transport`,
//! `Protocol`) are retried up to [`RetryPolicy::max_attempts`] times in
//! total, sleeping `RetryPolicy::backoff` between attempts. Caller bugs
//! (`McpError::Parse` — e.g. non-object arguments) and deliberate
//! server error frames (`McpError::ServerError`) are **not** retried:
//! parse failures will never become correct, and server errors are a
//! contract the peer is asserting on purpose. After retries are
//! exhausted the final error surfaces to the caller, which is the
//! signal the agent run loop uses to feed the shared per-tick health
//! budget (`FailureKind::ToolCall`) — see `src/health.rs` for the
//! state-machine contract this layer feeds without duplicating.
//!
//! "Max retries" inside this module and `RetryBudget::max_tool` in
//! `src/health.rs` are two distinct concepts: the former bounds attempts
//! within one tool call, the latter bounds exhausted tool calls within
//! one tick before the agent transitions to `Unhealthy`.
//!
//! Bulk registration (`ToolRegistry::register_mcp_server`) and process
//! supervision live in JAR2-24 and the parent ticket respectively.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;
use tracing::warn;

use crate::mcp::{McpClient, McpError, McpToolDescriptor};
use crate::tools::Tool;

/// Retry policy for `McpTool::call`. Bounds attempts and the fixed delay
/// between them. "Simple backoff" per the JAR2-25 spec — a fixed sleep
/// keeps semantics auditable and avoids pulling in a backoff crate; a
/// future ticket can swap in jittered exponential if real traffic
/// motivates it.
///
/// Defaults: 3 total attempts, 50 ms between retries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Total attempts including the first. `1` disables retry (one shot
    /// only). `0` is rejected at construction.
    pub max_attempts: u32,
    /// Fixed sleep between retries. Set to `Duration::ZERO` in tests so
    /// they do not pay for wall-clock backoff.
    pub backoff: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            backoff: Duration::from_millis(50),
        }
    }
}

impl RetryPolicy {
    /// Build a policy with `max_attempts` total attempts and `backoff`
    /// between them. `max_attempts` is clamped to at least `1` — a
    /// zero-attempt policy is a wiring bug, not a useful state.
    pub fn new(max_attempts: u32, backoff: Duration) -> Self {
        Self {
            max_attempts: max_attempts.max(1),
            backoff,
        }
    }

    /// Convenience for tests: retry-3 with zero backoff so the retry loop
    /// runs at virtual-time speed under `tokio::test(start_paused = true)`.
    #[cfg(test)]
    pub fn test_immediate(max_attempts: u32) -> Self {
        Self::new(max_attempts, Duration::ZERO)
    }
}

/// `Tool` impl that proxies one named MCP tool to a shared `McpClient`.
///
/// Multiple `McpTool`s for the same server should share one `Arc<McpClient>`;
/// the underlying rmcp service multiplexes concurrent requests.
pub struct McpTool {
    descriptor: McpToolDescriptor,
    client: Arc<McpClient>,
    retry: RetryPolicy,
}

impl McpTool {
    /// Build an `McpTool` for the descriptor `descriptor` against `client`,
    /// using the default `RetryPolicy`. The descriptor is typically one of
    /// the entries `client.list_tools()` returned during registration.
    pub fn new(descriptor: McpToolDescriptor, client: Arc<McpClient>) -> Self {
        Self::with_retry_policy(descriptor, client, RetryPolicy::default())
    }

    /// Build an `McpTool` with an explicit retry policy. Intended for
    /// callers that want a per-tool override (e.g. a high-cost or
    /// non-idempotent tool that should retry zero or one times). The
    /// ticket's "overridable per-mandate" requirement is satisfied here
    /// by per-tool override at construction; full plumbing through
    /// `Mandate` is a follow-up.
    pub fn with_retry_policy(
        descriptor: McpToolDescriptor,
        client: Arc<McpClient>,
        retry: RetryPolicy,
    ) -> Self {
        Self {
            descriptor,
            client,
            retry,
        }
    }

    /// Borrow the underlying descriptor (name + optional description +
    /// input schema). Useful for callers — e.g. `register_mcp_server` in
    /// JAR2-24 — that need the schema after construction.
    pub fn descriptor(&self) -> &McpToolDescriptor {
        &self.descriptor
    }

    /// Borrow the retry policy this tool was constructed with.
    pub fn retry_policy(&self) -> &RetryPolicy {
        &self.retry
    }
}

/// Should the supplied `McpError` be retried?
///
/// - `Transport` / `Protocol`: yes — the peer died mid-call, the framing
///   blipped, or rmcp surfaced a request cancellation. A fresh attempt
///   may succeed.
/// - `Parse`: no — these come from caller-side argument shape bugs (see
///   `McpClient::call_tool`'s non-object-arguments guard). Retrying
///   identical bad arguments is just burning attempts.
/// - `ServerError`: no — the peer returned a deliberate JSON-RPC error
///   frame. The conservative default is to trust the server's "no"; a
///   future ticket may broaden this with rate-limit-aware retry.
fn is_transient(err: &McpError) -> bool {
    matches!(err, McpError::Transport(_) | McpError::Protocol(_))
}

#[async_trait]
impl Tool for McpTool {
    fn name(&self) -> &str {
        &self.descriptor.name
    }

    async fn call(&self, args: Value) -> anyhow::Result<Value> {
        // The retry loop is factored out into `call_with_retry` so it can
        // be unit-tested against an injectable closure that produces
        // `Result<Value, McpError>` deterministically — exercising the
        // "success on second try after one transient error" case without
        // needing a real flaky transport. The instance method captures
        // `self.client` + `self.descriptor.name` into the closure.
        let name = self.descriptor.name.clone();
        let client = self.client.clone();
        let args_cell = args;
        let result = call_with_retry(&name, self.retry, || {
            let client = client.clone();
            let args = args_cell.clone();
            let name = name.clone();
            async move { client.call_tool(&name, args).await }
        })
        .await;
        result.map_err(|e| anyhow::Error::new(e).context(format!("mcp tool {:?}", name)))
    }
}

/// Drive `f` up to `policy.max_attempts` times, retrying only on
/// transient errors and sleeping `policy.backoff` between attempts. The
/// final error (transient or not) surfaces verbatim — wrapping with
/// tool-name context is the caller's responsibility.
async fn call_with_retry<F, Fut>(
    tool_name: &str,
    policy: RetryPolicy,
    mut f: F,
) -> Result<Value, McpError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Value, McpError>>,
{
    let max = policy.max_attempts.max(1);
    let mut last_err: Option<McpError> = None;
    for attempt in 1..=max {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                if !is_transient(&e) || attempt == max {
                    return Err(e);
                }
                warn!(
                    tool = %tool_name,
                    attempt,
                    max,
                    error = %e,
                    "mcp tool call failed; retrying after backoff"
                );
                last_err = Some(e);
                if !policy.backoff.is_zero() {
                    tokio::time::sleep(policy.backoff).await;
                }
            }
        }
    }
    // Unreachable in practice: the `attempt == max` branch above always
    // returns. Kept defensively so the function signature is total.
    Err(last_err.unwrap_or_else(|| McpError::Protocol("retry loop ran zero times".into())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::McpError;
    use rmcp::handler::server::ServerHandler;
    use rmcp::model::{
        CallToolRequestParams, CallToolResult, Content, ErrorCode, ListToolsResult,
        PaginatedRequestParams, ServerInfo, Tool as RmcpTool,
    };
    use rmcp::service::{NotificationContext, RequestContext, RoleServer};
    use rmcp::{ErrorData, ServiceExt};
    use serde_json::json;
    use std::sync::Arc;
    use tokio::io::duplex;

    /// Hand-built fake server. Mirrors the one in `mcp::tests` (kept local
    /// rather than factored, per the ticket's "duplicate the minimum" note —
    /// tests in sibling modules don't share the parent's `#[cfg(test)]`
    /// items without scope expansion).
    #[derive(Clone)]
    struct FakeServer {
        fail_with: Option<(i32, String)>,
    }

    impl FakeServer {
        fn ok() -> Self {
            Self { fail_with: None }
        }
        fn failing(code: i32, msg: impl Into<String>) -> Self {
            Self {
                fail_with: Some((code, msg.into())),
            }
        }
    }

    impl ServerHandler for FakeServer {
        async fn list_tools(
            &self,
            _request: Option<PaginatedRequestParams>,
            _context: RequestContext<RoleServer>,
        ) -> Result<ListToolsResult, ErrorData> {
            let mut schema = serde_json::Map::new();
            schema.insert("type".into(), json!("object"));
            schema.insert("properties".into(), json!({"text": {"type": "string"}}));
            let tool = RmcpTool::new("repeat", "echo the text back", Arc::new(schema));
            Ok(ListToolsResult {
                meta: None,
                next_cursor: None,
                tools: vec![tool],
            })
        }

        async fn call_tool(
            &self,
            request: CallToolRequestParams,
            _context: RequestContext<RoleServer>,
        ) -> Result<CallToolResult, ErrorData> {
            if let Some((code, msg)) = &self.fail_with {
                return Err(ErrorData::new(ErrorCode(*code), msg.clone(), None));
            }
            let text = request
                .arguments
                .as_ref()
                .and_then(|a| a.get("text"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            Ok(CallToolResult::success(vec![Content::text(format!(
                "echo:{text}"
            ))]))
        }

        fn get_info(&self) -> ServerInfo {
            ServerInfo::default()
        }

        async fn on_initialized(&self, _context: NotificationContext<RoleServer>) {}
    }

    async fn paired(server: FakeServer) -> (McpClient, tokio::task::JoinHandle<()>) {
        let (client_io, server_io) = duplex(8 * 1024);
        let (server_read, server_write) = tokio::io::split(server_io);
        let server_task = tokio::spawn(async move {
            let running = server
                .serve((server_read, server_write))
                .await
                .expect("server handshake");
            let _ = running.waiting().await;
        });

        let (client_read, client_write) = tokio::io::split(client_io);
        let client = McpClient::connect_with((client_read, client_write))
            .await
            .expect("client handshake");
        (client, server_task)
    }

    fn descriptor(name: &str) -> McpToolDescriptor {
        McpToolDescriptor {
            name: name.into(),
            description: None,
            input_schema: json!({"type": "object"}),
        }
    }

    #[tokio::test]
    async fn name_returns_descriptor_name() {
        // No client traffic; just confirm the cheap accessor doesn't lie.
        let (client, server) = paired(FakeServer::ok()).await;
        let tool = McpTool::new(descriptor("repeat"), Arc::new(client));
        assert_eq!(tool.name(), "repeat");
        // Drop the tool (and its Arc<McpClient>) so the server task exits.
        drop(tool);
        let _ = server.await;
    }

    #[tokio::test]
    async fn call_returns_server_result_as_json() {
        let (client, server) = paired(FakeServer::ok()).await;
        let tool = McpTool::new(descriptor("repeat"), Arc::new(client));

        let out = tool.call(json!({"text": "hi"})).await.unwrap();
        let content = out
            .get("content")
            .and_then(Value::as_array)
            .expect("content array");
        assert_eq!(content.len(), 1);
        assert_eq!(
            content[0].get("text").and_then(Value::as_str),
            Some("echo:hi")
        );
        drop(tool);
        let _ = server.await;
    }

    #[tokio::test]
    async fn server_error_frame_surfaces_as_tool_error() {
        let (client, server) = paired(FakeServer::failing(-32099, "boom")).await;
        let tool = McpTool::new(descriptor("repeat"), Arc::new(client));

        let err = tool.call(json!({"text": "hi"})).await.unwrap_err();
        // Source chain should preserve the typed server error so callers
        // who downcast can still recover the code.
        let mcp = err
            .chain()
            .find_map(|e| e.downcast_ref::<McpError>())
            .expect("McpError in source chain");
        assert!(matches!(mcp, McpError::ServerError { code: -32099, .. }));
        // `{:#}` renders the full context chain — the outer "mcp tool ..."
        // wrapper plus the inner server message.
        let msg = format!("{err:#}");
        assert!(
            msg.contains("repeat") && msg.contains("boom"),
            "expected tool name and server message in error, got: {msg}"
        );
        drop(tool);
        let _ = server.await;
    }

    #[tokio::test]
    async fn transport_drop_mid_call_surfaces_as_tool_error() {
        let (client_io, server_io) = duplex(8 * 1024);
        let (server_read, server_write) = tokio::io::split(server_io);

        // Server completes the handshake, then drops the running service.
        let server_task = tokio::spawn(async move {
            let running = FakeServer::ok()
                .serve((server_read, server_write))
                .await
                .expect("server handshake");
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            drop(running);
        });

        let (client_read, client_write) = tokio::io::split(client_io);
        let client = McpClient::connect_with((client_read, client_write))
            .await
            .expect("client handshake");
        let tool = McpTool::new(descriptor("repeat"), Arc::new(client));

        let _ = server_task.await;
        let err = tool.call(json!({"text": "hi"})).await.unwrap_err();
        let mcp = err
            .chain()
            .find_map(|e| e.downcast_ref::<McpError>())
            .expect("McpError in source chain");
        assert!(
            matches!(mcp, McpError::Transport(_) | McpError::Protocol(_)),
            "expected Transport/Protocol after server drop, got {mcp:?}"
        );
    }

    #[tokio::test]
    async fn malformed_arguments_surface_as_parse_error() {
        // The client maps non-object/non-null arguments to `McpError::Parse`
        // before they ever hit the wire (see `McpClient::call_tool`). That's
        // the closest deterministic Parse path available without hand-rolling
        // a non-conforming server, and it's the path JSON arg encoding bugs
        // would actually take.
        let (client, server) = paired(FakeServer::ok()).await;
        let tool = McpTool::new(descriptor("repeat"), Arc::new(client));

        let err = tool.call(json!(42)).await.unwrap_err();
        let mcp = err
            .chain()
            .find_map(|e| e.downcast_ref::<McpError>())
            .expect("McpError in source chain");
        assert!(
            matches!(mcp, McpError::Parse(_)),
            "expected Parse, got {mcp:?}"
        );
        drop(tool);
        let _ = server.await;
    }

    // ---- JAR2-25: retry policy ----

    /// "Success on first try" — the happy path under a non-default policy
    /// uses zero retries beyond the original attempt. Pinned here so the
    /// retry plumbing does not regress the no-failure path.
    #[tokio::test]
    async fn call_succeeds_on_first_attempt_under_retry_policy() {
        let (client, server) = paired(FakeServer::ok()).await;
        let tool = McpTool::with_retry_policy(
            descriptor("repeat"),
            Arc::new(client),
            RetryPolicy::test_immediate(3),
        );
        let out = tool.call(json!({"text": "hi"})).await.unwrap();
        assert_eq!(
            out.get("content")
                .and_then(Value::as_array)
                .and_then(|a| a.first())
                .and_then(|c| c.get("text"))
                .and_then(Value::as_str),
            Some("echo:hi")
        );
        drop(tool);
        let _ = server.await;
    }

    /// Classifier sanity: only Transport/Protocol are retried; Parse
    /// (caller bug) and ServerError (deliberate peer "no") are not.
    /// Pinned so a future broadening of `is_transient` has to update this
    /// test deliberately.
    #[test]
    fn transient_classifier_retries_transport_and_protocol_only() {
        assert!(is_transient(&McpError::Transport("eof".into())));
        assert!(is_transient(&McpError::Protocol("framing".into())));
        assert!(!is_transient(&McpError::Parse("bad".into())));
        assert!(!is_transient(&McpError::ServerError {
            code: -32000,
            message: "nope".into(),
        }));
    }

    /// "Success on second try after one transient error". We drive the
    /// retry loop directly via `call_with_retry` so the test is
    /// deterministic: a `Mutex<u32>` counts attempts, the first attempt
    /// returns `McpError::Transport` (transient), the second succeeds.
    /// Going through `call_with_retry` rather than constructing a flaky
    /// `McpClient` keeps the test free of rmcp transport plumbing — the
    /// real `McpTool::call` body delegates here too, so the retry loop
    /// under test is the production path.
    #[tokio::test(start_paused = true)]
    async fn call_with_retry_succeeds_on_second_attempt_after_transient() {
        let attempts = std::sync::Mutex::new(0u32);
        let out = call_with_retry("repeat", RetryPolicy::test_immediate(3), || async {
            let mut a = attempts.lock().unwrap();
            *a += 1;
            let n = *a;
            drop(a);
            if n == 1 {
                Err(McpError::Transport("pipe closed".into()))
            } else {
                Ok(json!({"content": [{"text": "echo:hi"}]}))
            }
        })
        .await
        .expect("second attempt should succeed");
        assert_eq!(
            out.get("content")
                .and_then(Value::as_array)
                .and_then(|a| a.first())
                .and_then(|c| c.get("text"))
                .and_then(Value::as_str),
            Some("echo:hi")
        );
        assert_eq!(*attempts.lock().unwrap(), 2);
    }

    /// All retries exhaust on persistent transient failure: the final
    /// `McpError::Transport` surfaces, and the closure has been called
    /// exactly `max_attempts` times. This is the "tool error after max
    /// retries" half of the ticket's acceptance criteria; the agent-side
    /// "trip Unhealthy" half is exercised in
    /// `tests/loop_smoke.rs::tool_call_exhausts_retry_budget_*`.
    #[tokio::test(start_paused = true)]
    async fn call_with_retry_exhausts_and_surfaces_last_transient_error() {
        let attempts = std::sync::Mutex::new(0u32);
        let err = call_with_retry("repeat", RetryPolicy::test_immediate(3), || async {
            *attempts.lock().unwrap() += 1;
            Err::<Value, _>(McpError::Transport("still broken".into()))
        })
        .await
        .expect_err("transient errors should never succeed here");
        assert!(matches!(err, McpError::Transport(_)));
        assert_eq!(*attempts.lock().unwrap(), 3);
    }

    /// Non-transient errors short-circuit the loop: even with
    /// `max_attempts = 3`, a `ServerError` on the first call must surface
    /// immediately without burning extra attempts on the peer.
    #[tokio::test(start_paused = true)]
    async fn call_with_retry_short_circuits_on_non_transient() {
        let attempts = std::sync::Mutex::new(0u32);
        let err = call_with_retry("repeat", RetryPolicy::test_immediate(3), || async {
            *attempts.lock().unwrap() += 1;
            Err::<Value, _>(McpError::ServerError {
                code: -32099,
                message: "permission denied".into(),
            })
        })
        .await
        .expect_err("server error should surface");
        assert!(matches!(err, McpError::ServerError { code: -32099, .. }));
        assert_eq!(*attempts.lock().unwrap(), 1);
    }

    /// Policy default sanity-check: 3 attempts, 50 ms backoff. Pinned so
    /// the documented defaults in the module doc-comment stay honest.
    #[test]
    fn retry_policy_default_is_3_attempts_50ms() {
        let p = RetryPolicy::default();
        assert_eq!(p.max_attempts, 3);
        assert_eq!(p.backoff, Duration::from_millis(50));
    }

    /// `RetryPolicy::new(0, _)` is clamped to 1 — a zero-attempt policy is
    /// a wiring bug, not a useful state, and silently bumping to 1 keeps
    /// the loop's "ran at least once" assertion valid.
    #[test]
    fn retry_policy_new_clamps_zero_to_one() {
        assert_eq!(RetryPolicy::new(0, Duration::ZERO).max_attempts, 1);
    }
}
