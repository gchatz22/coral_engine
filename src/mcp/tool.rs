//! `Tool` adapter that bridges a single MCP-advertised tool into the
//! `ToolRegistry`.
//!
//! `McpTool` holds an `Arc<McpClient>` shared with sibling tools registered
//! against the same server, plus the `McpToolDescriptor` so its name (and,
//! later, schema) survive the trip into the registry. `Tool::call` forwards
//! to `McpClient::call_tool`, mapping `McpError` into `anyhow::Error` since
//! the `Tool` trait's failure type is `anyhow::Result`.
//!
//! Bulk registration (`ToolRegistry::register_mcp_server`), retry / health
//! wiring, and process supervision are intentionally not implemented here —
//! they live in JAR2-24, JAR2-25, and the parent ticket respectively.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use crate::mcp::{McpClient, McpToolDescriptor};
use crate::tools::Tool;

/// `Tool` impl that proxies one named MCP tool to a shared `McpClient`.
///
/// Multiple `McpTool`s for the same server should share one `Arc<McpClient>`;
/// the underlying rmcp service multiplexes concurrent requests.
pub struct McpTool {
    descriptor: McpToolDescriptor,
    client: Arc<McpClient>,
}

impl McpTool {
    /// Build an `McpTool` for the descriptor `descriptor` against `client`.
    /// The descriptor is typically one of the entries `client.list_tools()`
    /// returned during registration.
    pub fn new(descriptor: McpToolDescriptor, client: Arc<McpClient>) -> Self {
        Self { descriptor, client }
    }

    /// Borrow the underlying descriptor (name + optional description +
    /// input schema). Useful for callers — e.g. `register_mcp_server` in
    /// JAR2-24 — that need the schema after construction.
    pub fn descriptor(&self) -> &McpToolDescriptor {
        &self.descriptor
    }
}

#[async_trait]
impl Tool for McpTool {
    fn name(&self) -> &str {
        &self.descriptor.name
    }

    async fn call(&self, args: Value) -> anyhow::Result<Value> {
        // Wrap the typed `McpError` as an `anyhow::Error` source and add the
        // tool name as context so registry call sites mention which MCP
        // tool blew up. Callers downcasting through `Error::chain` recover
        // the original `McpError` (transport / server / parse).
        self.client
            .call_tool(&self.descriptor.name, args)
            .await
            .map_err(|e| {
                anyhow::Error::new(e).context(format!("mcp tool {:?}", self.descriptor.name))
            })
    }
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
}
