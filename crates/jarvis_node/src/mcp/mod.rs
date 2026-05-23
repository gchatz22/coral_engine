//! MCP client wrapper around the official `rmcp` SDK.
//!
//! `McpClient` connects to an MCP server over stdio (spawning a subprocess
//! via `rmcp::transport::TokioChildProcess`), folds the MCP `initialize` +
//! `initialized` handshake into the connect step, and exposes typed
//! `list_tools` / `call_tool` methods over JSON-RPC.
//!
//! Errors are normalized into `McpError`, distinguishing transport failures,
//! protocol violations, JSON-RPC server errors, and parse problems. Higher
//! layers — `Tool for McpTool` (JAR2-23), `ToolRegistry::register_mcp_server`
//! (JAR2-24), retry/health wiring (JAR2-25) — live in follower tickets.

use std::sync::Arc;

pub mod tool;

use crate::mandate::RetryPolicy;
use crate::mcp::tool::McpTool;
use crate::tools::ToolRegistry;
use rmcp::model::{CallToolRequestParams, JsonObject};
use rmcp::service::{RoleClient, RunningService, ServiceError, ServiceExt};
use rmcp::transport::TokioChildProcess;
use rmcp::{service::ClientInitializeError, ErrorData};
use serde_json::Value;
use thiserror::Error;
use tokio::process::Command;

/// Public, transport-agnostic description of an MCP tool the connected
/// server advertises. Mirrors `rmcp::model::Tool` but owns its strings and
/// a plain `serde_json::Value` schema (rather than `Arc<JsonObject>`), so
/// callers don't take a dep on rmcp's internal types.
#[derive(Debug, Clone)]
pub struct McpToolDescriptor {
    pub name: String,
    pub description: Option<String>,
    pub input_schema: Value,
}

/// Unified error surface for the MCP client.
#[derive(Debug, Error)]
pub enum McpError {
    /// Transport-layer failure: subprocess spawn error, stdio EOF / pipe
    /// closed mid-call, or framing I/O error.
    #[error("MCP transport error: {0}")]
    Transport(String),

    /// JSON-RPC error response from the peer (`id` matched, but the body
    /// is an `Error` frame). `code` is the JSON-RPC error code; `message`
    /// is the server-supplied message.
    #[error("MCP server error {code}: {message}")]
    ServerError { code: i32, message: String },

    /// Protocol-level violation: handshake reply of the wrong shape, an
    /// unexpected response variant, request cancelled, request timeout,
    /// or any other non-error-frame breakage.
    #[error("MCP protocol error: {0}")]
    Protocol(String),

    /// Failure to (de)serialize a value crossing the boundary — a tool's
    /// arguments, a tool's result, or anything else that should have been
    /// well-formed JSON.
    #[error("MCP parse error: {0}")]
    Parse(String),
}

impl From<ClientInitializeError> for McpError {
    fn from(value: ClientInitializeError) -> Self {
        match value {
            ClientInitializeError::JsonRpcError(ErrorData { code, message, .. }) => {
                McpError::ServerError {
                    code: code.0,
                    message: message.into_owned(),
                }
            }
            ClientInitializeError::TransportError { error, context } => {
                McpError::Transport(format!("{context}: {error}"))
            }
            ClientInitializeError::ConnectionClosed(ctx) => {
                McpError::Transport(format!("connection closed: {ctx}"))
            }
            other => McpError::Protocol(other.to_string()),
        }
    }
}

impl From<ServiceError> for McpError {
    fn from(value: ServiceError) -> Self {
        match value {
            ServiceError::McpError(ErrorData { code, message, .. }) => McpError::ServerError {
                code: code.0,
                message: message.into_owned(),
            },
            ServiceError::TransportSend(e) => McpError::Transport(e.to_string()),
            ServiceError::TransportClosed => McpError::Transport("transport closed".to_string()),
            other => McpError::Protocol(other.to_string()),
        }
    }
}

/// Typed MCP client. Holds a live `RunningService<RoleClient, ()>` whose
/// background task drives the framing and response demux. The subprocess
/// (when stdio-spawned) is owned by the underlying `TokioChildProcess`.
pub struct McpClient {
    inner: RunningService<RoleClient, ()>,
}

impl McpClient {
    /// Spawn `command` with `args`, connect over stdio, and complete the
    /// MCP handshake. Returns once the server's `initialize` response has
    /// been received and the `initialized` notification has been sent.
    pub async fn connect_stdio(command: &str, args: &[&str]) -> Result<Self, McpError> {
        let mut cmd = Command::new(command);
        cmd.args(args);
        let transport = TokioChildProcess::new(cmd)
            .map_err(|e| McpError::Transport(format!("spawn {command:?}: {e}")))?;
        let inner = ().serve(transport).await?;
        Ok(Self { inner })
    }

    /// Connect over an arbitrary `IntoTransport` (e.g. an in-memory duplex
    /// pipe). Pulled out so tests can drive a fake server through
    /// `tokio::io::duplex` without spawning a subprocess.
    #[cfg(test)]
    pub(crate) async fn connect_with<T, E, A>(transport: T) -> Result<Self, McpError>
    where
        T: rmcp::transport::IntoTransport<RoleClient, E, A>,
        E: std::error::Error + Send + Sync + 'static,
    {
        let inner = ().serve(transport).await?;
        Ok(Self { inner })
    }

    /// Enumerate every tool the peer advertises. Pages through `tools/list`
    /// until `next_cursor` is empty.
    pub async fn list_tools(&self) -> Result<Vec<McpToolDescriptor>, McpError> {
        let tools = self.inner.peer().list_all_tools().await?;
        Ok(tools.into_iter().map(to_descriptor).collect())
    }

    /// Call `name` with `args` and return the server's `CallToolResult` as
    /// JSON. `args` must be a JSON object (`tools/call` parameters are an
    /// object per the MCP spec); anything else returns `McpError::Parse`.
    pub async fn call_tool(&self, name: &str, args: Value) -> Result<Value, McpError> {
        let arguments = match args {
            Value::Object(obj) => Some(obj),
            Value::Null => None,
            other => {
                return Err(McpError::Parse(format!(
                    "tools/call arguments must be a JSON object or null, got {}",
                    type_name(&other)
                )));
            }
        };
        let mut params = CallToolRequestParams::new(name.to_string());
        if let Some(obj) = arguments {
            params = params.with_arguments(obj);
        }
        let result = self.inner.peer().call_tool(params).await?;
        serde_json::to_value(result).map_err(|e| McpError::Parse(e.to_string()))
    }

    /// Cancel the background service task and close the transport.
    /// Consumes `self`; subsequent calls would fail with `Transport`.
    pub async fn shutdown(self) -> Result<(), McpError> {
        self.inner
            .cancel()
            .await
            .map_err(|e| McpError::Transport(format!("join error during shutdown: {e}")))?;
        Ok(())
    }
}

/// MCP-aware extension methods on `ToolRegistry`. Lives in the `mcp` module
/// so the core `tools` module stays free of any MCP dependency.
impl ToolRegistry {
    /// Introspect `client` via `tools/list` and register every advertised
    /// tool against this registry, returning the registered names in the
    /// order the server announced them.
    ///
    /// Trusts the server's advertised schemas — no in-engine re-validation.
    /// Spec (JAR2-24) suggested `&self`; this implementation takes
    /// `&mut self` to match the existing `ToolRegistry::register` surface
    /// rather than introduce interior mutability for one helper.
    ///
    /// **Duplicate-name policy.** Reuses `ToolRegistry::register`, which
    /// errors on collision. If the server advertises a name already in the
    /// registry (because another MCP server or built-in tool got there
    /// first), the helper returns `Err` on that descriptor — first-wins.
    /// Registration is *not* atomic: descriptors that registered before the
    /// collision remain in the registry. Pre-check with
    /// `ToolRegistry::contains` if atomicity matters.
    pub async fn register_mcp_server(
        &mut self,
        client: Arc<McpClient>,
    ) -> anyhow::Result<Vec<String>> {
        self.register_mcp_server_with_policy(client, None).await
    }

    /// Same as [`Self::register_mcp_server`], but threads an optional
    /// `RetryPolicy` (typically from `Mandate::retry_policy`) through to
    /// each registered `McpTool`. `None` preserves the
    /// `RetryPolicy::default()` semantics JAR2-25 wired at
    /// `McpTool::new`; `Some(p)` calls `McpTool::with_retry_policy(p)`
    /// for every descriptor advertised by the server. JAR2-31 completes
    /// the JAR2-25 punt by giving callers a single seam at which
    /// per-mandate retry overrides land.
    pub async fn register_mcp_server_with_policy(
        &mut self,
        client: Arc<McpClient>,
        retry_policy: Option<RetryPolicy>,
    ) -> anyhow::Result<Vec<String>> {
        let descriptors = client.list_tools().await?;
        let mut names = Vec::with_capacity(descriptors.len());
        for descriptor in descriptors {
            let name = descriptor.name.clone();
            let tool = match retry_policy {
                Some(p) => McpTool::with_retry_policy(descriptor, Arc::clone(&client), p),
                None => McpTool::new(descriptor, Arc::clone(&client)),
            };
            self.register(Arc::new(tool))?;
            names.push(name);
        }
        Ok(names)
    }
}

fn to_descriptor(tool: rmcp::model::Tool) -> McpToolDescriptor {
    let input_schema = json_object_to_value(tool.input_schema);
    McpToolDescriptor {
        name: tool.name.into_owned(),
        description: tool.description.map(|d| d.into_owned()),
        input_schema,
    }
}

fn json_object_to_value(obj: Arc<JsonObject>) -> Value {
    // `Arc<Map>` -> `Value::Object`. Avoid cloning if we hold the only Arc.
    let map = Arc::try_unwrap(obj).unwrap_or_else(|shared| (*shared).clone());
    Value::Object(map)
}

fn type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmcp::handler::server::ServerHandler;
    use rmcp::model::{
        CallToolResult, Content, ErrorCode, ListToolsResult, PaginatedRequestParams, ServerInfo,
        Tool,
    };
    use rmcp::service::{NotificationContext, RequestContext, RoleServer};
    use rmcp::{ErrorData, ServiceExt};
    use serde_json::json;
    use std::sync::Arc;
    use tokio::io::{duplex, AsyncWriteExt};

    /// Hand-built fake MCP server. Advertises one tool, `repeat`, that
    /// echoes its `text` argument back; can be configured to return a
    /// JSON-RPC error frame for `tools/call` to exercise the error path.
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
            let tool = Tool::new("repeat", "echo the text back", Arc::new(schema));
            Ok(ListToolsResult {
                meta: None,
                next_cursor: None,
                tools: vec![tool],
            })
        }

        async fn call_tool(
            &self,
            request: rmcp::model::CallToolRequestParams,
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

    /// Plug a fake server's stdio into a duplex pipe and return the
    /// matching client end as an `(AsyncRead, AsyncWrite)` tuple, plus a
    /// handle to the server task so the test can keep it alive.
    async fn paired(server: FakeServer) -> (McpClient, tokio::task::JoinHandle<()>) {
        let (client_io, server_io) = duplex(8 * 1024);
        let (server_read, server_write) = tokio::io::split(server_io);
        let server_task = tokio::spawn(async move {
            let running = server
                .serve((server_read, server_write))
                .await
                .expect("server handshake");
            // Hold the running service until the client drops the pipe;
            // `waiting()` returns when transport closes.
            let _ = running.waiting().await;
        });

        let (client_read, client_write) = tokio::io::split(client_io);
        let client = McpClient::connect_with((client_read, client_write))
            .await
            .expect("client handshake");
        (client, server_task)
    }

    #[tokio::test]
    async fn list_and_call_round_trip() {
        let (client, server) = paired(FakeServer::ok()).await;

        let tools = client.list_tools().await.unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "repeat");
        assert_eq!(tools[0].description.as_deref(), Some("echo the text back"));
        assert_eq!(
            tools[0].input_schema.get("type").and_then(Value::as_str),
            Some("object")
        );

        let result = client
            .call_tool("repeat", json!({"text": "hi"}))
            .await
            .unwrap();
        // CallToolResult serializes with a `content` array of text parts.
        let content = result
            .get("content")
            .and_then(Value::as_array)
            .expect("content array");
        assert_eq!(content.len(), 1);
        assert_eq!(
            content[0].get("text").and_then(Value::as_str),
            Some("echo:hi")
        );

        client.shutdown().await.unwrap();
        let _ = server.await;
    }

    #[tokio::test]
    async fn server_error_frame_maps_to_server_error() {
        let (client, server) = paired(FakeServer::failing(-32099, "boom")).await;

        let err = client
            .call_tool("repeat", json!({"text": "hi"}))
            .await
            .unwrap_err();
        match err {
            McpError::ServerError { code, message } => {
                assert_eq!(code, -32099);
                assert_eq!(message, "boom");
            }
            other => panic!("expected ServerError, got {other:?}"),
        }

        client.shutdown().await.unwrap();
        let _ = server.await;
    }

    #[tokio::test]
    async fn call_after_server_drop_yields_transport_error() {
        let (client_io, server_io) = duplex(8 * 1024);
        let (server_read, server_write) = tokio::io::split(server_io);

        // Server completes the handshake, then drops the running service
        // (closing both pipe ends) before the client makes any further call.
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

        // Wait for the server task to drop the running service, which
        // closes the transport.
        let _ = server_task.await;
        let err = client.list_tools().await.unwrap_err();
        assert!(
            matches!(err, McpError::Transport(_) | McpError::Protocol(_)),
            "expected transport/protocol error after server drop, got {err:?}"
        );
    }

    #[tokio::test]
    async fn garbage_bytes_during_handshake_yield_protocol_or_transport_error() {
        // Drive raw bytes into the "server" side instead of running a real
        // server; the handshake should fail when it can't decode an
        // initialize response.
        let (client_io, mut server_io) = duplex(8 * 1024);
        let writer_task = tokio::spawn(async move {
            // Wait for the client to send the initialize request, then
            // reply with malformed (not JSON-RPC) bytes followed by EOF.
            let mut buf = [0u8; 1024];
            let _ = tokio::io::AsyncReadExt::read(&mut server_io, &mut buf).await;
            let _ = server_io.write_all(b"not a json rpc message\n").await;
            // Drop closes the pipe.
        });

        let (client_read, client_write) = tokio::io::split(client_io);
        let err = match McpClient::connect_with((client_read, client_write)).await {
            Ok(_) => panic!("handshake should fail on garbage"),
            Err(e) => e,
        };

        assert!(
            matches!(err, McpError::Transport(_) | McpError::Protocol(_)),
            "expected transport/protocol error from garbage, got {err:?}"
        );
        let _ = writer_task.await;
    }

    #[tokio::test]
    async fn call_tool_rejects_non_object_arguments() {
        let (client, server) = paired(FakeServer::ok()).await;
        let err = client.call_tool("repeat", json!(42)).await.unwrap_err();
        assert!(
            matches!(err, McpError::Parse(_)),
            "expected Parse, got {err:?}"
        );
        client.shutdown().await.unwrap();
        let _ = server.await;
    }

    /// Fake MCP server that advertises two tools — `repeat` (echoes its
    /// `text` argument) and `shout` (echoes the same text uppercased) —
    /// for exercising `ToolRegistry::register_mcp_server`'s bulk-register
    /// path. Kept separate from the single-tool `FakeServer` above per the
    /// "duplicate the minimum" convention used in `mcp::tool::tests`.
    #[derive(Clone)]
    struct MultiToolServer;

    impl ServerHandler for MultiToolServer {
        async fn list_tools(
            &self,
            _request: Option<PaginatedRequestParams>,
            _context: RequestContext<RoleServer>,
        ) -> Result<ListToolsResult, ErrorData> {
            let mut schema = serde_json::Map::new();
            schema.insert("type".into(), json!("object"));
            schema.insert("properties".into(), json!({"text": {"type": "string"}}));
            let schema = Arc::new(schema);
            let repeat = Tool::new("repeat", "echo the text back", Arc::clone(&schema));
            let shout = Tool::new("shout", "echo the text back uppercased", schema);
            Ok(ListToolsResult {
                meta: None,
                next_cursor: None,
                tools: vec![repeat, shout],
            })
        }

        async fn call_tool(
            &self,
            request: rmcp::model::CallToolRequestParams,
            _context: RequestContext<RoleServer>,
        ) -> Result<CallToolResult, ErrorData> {
            let text = request
                .arguments
                .as_ref()
                .and_then(|a| a.get("text"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let out = match request.name.as_ref() {
                "repeat" => format!("echo:{text}"),
                "shout" => format!("echo:{}", text.to_uppercase()),
                other => {
                    return Err(ErrorData::new(
                        ErrorCode(-32601),
                        format!("unknown tool {other}"),
                        None,
                    ));
                }
            };
            Ok(CallToolResult::success(vec![Content::text(out)]))
        }

        fn get_info(&self) -> ServerInfo {
            ServerInfo::default()
        }

        async fn on_initialized(&self, _context: NotificationContext<RoleServer>) {}
    }

    async fn paired_multi() -> (McpClient, tokio::task::JoinHandle<()>) {
        let (client_io, server_io) = duplex(8 * 1024);
        let (server_read, server_write) = tokio::io::split(server_io);
        let server_task = tokio::spawn(async move {
            let running = MultiToolServer
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

    #[tokio::test]
    async fn register_mcp_server_registers_all_advertised_tools() {
        let (client, server) = paired_multi().await;
        let client = Arc::new(client);
        let mut registry = ToolRegistry::new();

        let names = registry
            .register_mcp_server(Arc::clone(&client))
            .await
            .expect("register_mcp_server");

        // Server announces them in (repeat, shout) order; the helper
        // preserves that order in its return value.
        assert_eq!(names, vec!["repeat".to_string(), "shout".to_string()]);
        assert!(registry.contains("repeat"));
        assert!(registry.contains("shout"));

        // Drop the registry (and its Arc<McpTool>s) before awaiting the
        // server task so the transport closes.
        drop(registry);
        // Drop our local Arc too so the server's `waiting()` returns.
        drop(client);
        let _ = server.await;
    }

    #[tokio::test]
    async fn registered_tool_is_invocable_through_registry() {
        let (client, server) = paired_multi().await;
        let client = Arc::new(client);
        let mut registry = ToolRegistry::new();
        registry
            .register_mcp_server(Arc::clone(&client))
            .await
            .expect("register_mcp_server");

        let ev = registry
            .call("shout", json!({"text": "hi"}))
            .await
            .expect("registry.call");
        assert_eq!(ev.tool, "shout");
        // `shout` is the uppercased variant, so the fixture echoes "HI".
        let content = ev
            .result
            .get("content")
            .and_then(Value::as_array)
            .expect("content array");
        assert_eq!(
            content[0].get("text").and_then(Value::as_str),
            Some("echo:HI")
        );

        drop(registry);
        drop(client);
        let _ = server.await;
    }

    #[tokio::test]
    async fn register_mcp_server_errors_on_name_conflict_and_keeps_prior_tools() {
        // First-wins policy: when the server advertises a name already in
        // the registry, the helper returns Err on that descriptor.
        // Registration is *not* atomic — descriptors processed before the
        // collision stay registered, and any descriptors after it are not
        // registered.
        let (client, server) = paired_multi().await;
        let client = Arc::new(client);
        let mut registry = ToolRegistry::new();

        // Pre-register a tool named "shout" so the second descriptor
        // the server advertises collides. "repeat" (advertised first)
        // should still land.
        struct PlaceholderShout;
        #[async_trait::async_trait]
        impl crate::tools::Tool for PlaceholderShout {
            fn name(&self) -> &str {
                "shout"
            }
            async fn call(&self, _args: Value) -> anyhow::Result<Value> {
                Ok(json!("placeholder"))
            }
        }
        registry.register(Arc::new(PlaceholderShout)).unwrap();

        let err = registry
            .register_mcp_server(Arc::clone(&client))
            .await
            .expect_err("expected duplicate-name error");
        let msg = format!("{err}");
        assert!(
            msg.contains("shout") && msg.contains("already"),
            "duplicate-name error should mention the colliding name, got: {msg}"
        );

        // First-wins: the descriptor that came before "shout" landed,
        // the placeholder is still in place, and nothing past the
        // collision was registered.
        assert!(registry.contains("repeat"));
        assert!(registry.contains("shout"));
        let placeholder = registry
            .call("shout", json!({"text": "ignored"}))
            .await
            .expect("placeholder still wired");
        assert_eq!(placeholder.result, json!("placeholder"));

        drop(registry);
        drop(client);
        let _ = server.await;
    }

    // ---- JAR2-31: per-mandate retry policy plumbing ----

    /// `register_mcp_server` (no policy override) still registers and
    /// calls tools end-to-end after the JAR2-31 plumbing. The
    /// "default-policy values" assertion lives in
    /// `mandate::tests::retry_policy_default_is_3_attempts_50ms`; this
    /// test pins the back-compat call surface (legacy callers do not
    /// have to pass `None` explicitly to get the historical behavior).
    #[tokio::test]
    async fn register_mcp_server_without_override_still_registers_and_calls_tools() {
        let (client, server) = paired(FakeServer::ok()).await;
        let client = Arc::new(client);
        let mut registry = ToolRegistry::new();
        registry
            .register_mcp_server(Arc::clone(&client))
            .await
            .expect("register_mcp_server");
        // Round-trip a happy call so we exercise the construction path
        // end-to-end (the policy lives inside the trait object; behavior
        // assertions on retry timing live in the override test below).
        let ev = registry
            .call("repeat", json!({"text": "hi"}))
            .await
            .expect("registry.call");
        assert_eq!(ev.tool, "repeat");
        drop(registry);
        drop(client);
        let _ = server.await;
    }

    /// JAR2-31 override behavior: when a mandate supplies a non-default
    /// `RetryPolicy`, the policy reaches the tools constructed for that
    /// agent and shapes their retry behavior.
    ///
    /// Shape: register the server with `max_attempts = 1, backoff = ZERO`,
    /// then drop the server so every subsequent call surfaces a transient
    /// `Transport` error. Under the default policy that would cost 2
    /// backoff sleeps (~100 ms virtual); with the override it surfaces
    /// after exactly one attempt and zero backoffs. We assert the latter
    /// via virtual time (`tokio::time::Instant::now()` delta under
    /// `start_paused`) — a real behavioral check on the retry loop, not
    /// just a getter sanity check.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn register_mcp_server_with_policy_propagates_to_tools() {
        let (client_io, server_io) = duplex(8 * 1024);
        let (server_read, server_write) = tokio::io::split(server_io);
        let server_task = tokio::spawn(async move {
            let running = FakeServer::ok()
                .serve((server_read, server_write))
                .await
                .expect("server handshake");
            // Give the client time to finish its half of the handshake
            // before we close the transport. Virtual time under
            // `start_paused`, so this is free in wall-clock terms.
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            // Drop closes the transport; subsequent client calls become
            // `McpError::Transport` (retryable).
            drop(running);
        });

        let (client_read, client_write) = tokio::io::split(client_io);
        let client = McpClient::connect_with((client_read, client_write))
            .await
            .expect("client handshake");
        let client = Arc::new(client);

        // Per-mandate override: `max_attempts = 1` means "one shot only",
        // so the retry loop never sleeps no matter how long `backoff`
        // says. Plumbing through `register_mcp_server_with_policy` is the
        // production path a `node-run-mcp`-shaped caller takes when it
        // reads `mandate.retry_policy`.
        let mut registry = ToolRegistry::new();
        registry
            .register_mcp_server_with_policy(
                Arc::clone(&client),
                Some(RetryPolicy::new(1, std::time::Duration::from_secs(60))),
            )
            .await
            .expect("register_mcp_server_with_policy");

        let _ = server_task.await;

        let start = tokio::time::Instant::now();
        let err = registry
            .call("repeat", json!({"text": "hi"}))
            .await
            .expect_err("server is gone; call should fail");
        let elapsed = start.elapsed();

        // The error is the same surface JAR2-25 already pinned; what's
        // new here is the *speed* it surfaces at.
        let msg = format!("{err:#}");
        assert!(
            msg.contains("repeat"),
            "expected tool name in error context, got: {msg}"
        );
        // Under default policy this would be ~120 s (2 × 60 s backoff);
        // under the override it should be zero — pick a tight virtual-time
        // bound that the default policy could not possibly satisfy.
        assert!(
            elapsed < std::time::Duration::from_secs(1),
            "override should short-circuit retries (no backoff); \
             virtual-time elapsed was {elapsed:?}"
        );

        drop(registry);
        drop(client);
    }

    /// Symmetric assertion at the policy default: without an override,
    /// the same paired-then-dropped server forces the retry loop to
    /// spend its `backoff`. The check here is "if we override to a
    /// `backoff` we'd notice, we *do* notice"; combined with the test
    /// above, it pins the propagation in both directions (default vs.
    /// override actually behave differently).
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn register_mcp_server_with_policy_observes_override_backoff() {
        let (client_io, server_io) = duplex(8 * 1024);
        let (server_read, server_write) = tokio::io::split(server_io);
        let server_task = tokio::spawn(async move {
            let running = FakeServer::ok()
                .serve((server_read, server_write))
                .await
                .expect("server handshake");
            // Let the client finish its half of the handshake before we
            // close the transport. Virtual time under `start_paused`.
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            drop(running);
        });

        let (client_read, client_write) = tokio::io::split(client_io);
        let client = McpClient::connect_with((client_read, client_write))
            .await
            .expect("client handshake");
        let client = Arc::new(client);

        // 3 attempts × a recognizable per-attempt backoff. Under
        // virtual time we can read the elapsed delta and assert it
        // matches the policy precisely.
        let backoff = std::time::Duration::from_secs(7);
        let mut registry = ToolRegistry::new();
        registry
            .register_mcp_server_with_policy(
                Arc::clone(&client),
                Some(RetryPolicy::new(3, backoff)),
            )
            .await
            .expect("register_mcp_server_with_policy");

        let _ = server_task.await;

        let start = tokio::time::Instant::now();
        let _err = registry
            .call("repeat", json!({"text": "hi"}))
            .await
            .expect_err("server is gone; call should fail");
        let elapsed = start.elapsed();

        // 3 attempts → 2 inter-attempt sleeps × 7 s = 14 s of virtual
        // time. Lower bound is the salient signal — the upper bound is
        // loose because rmcp transport setup is allowed to consume a
        // bounded amount of additional virtual time.
        assert!(
            elapsed >= 2 * backoff,
            "override backoff should accumulate across retries; \
             virtual-time elapsed was {elapsed:?}, expected >= {:?}",
            2 * backoff
        );

        drop(registry);
        drop(client);
    }
}
