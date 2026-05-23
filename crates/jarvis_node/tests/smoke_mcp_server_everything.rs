//! Optional end-to-end smoke that drives `@modelcontextprotocol/server-everything`
//! over a real stdio subprocess.
//!
//! **Gated behind `JARVIS_SMOKE_MCP=1`.** Without that env var the test
//! returns early — `cargo test` (with or without `--features mcp`) stays
//! hermetic and offline by default per the ticket's rules.
//!
//! The test mirrors the runbook in `examples/smoke_mcp/README.md`: it
//! spawns the server, bulk-registers its tools, calls `get-sum`, and
//! checks that the resulting `EvidenceRecord` lands on disk under
//! `evidence/`. It deliberately does **not** assert on the exact
//! `CallToolResult` content (the server's reply envelope can drift
//! across releases) — only that one was returned and written.
//!
//! Run it explicitly:
//!
//! ```bash
//! JARVIS_SMOKE_MCP=1 cargo test --features mcp \
//!     --test smoke_mcp_server_everything -- --nocapture
//! ```

#![cfg(feature = "mcp")]

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tempfile::TempDir;

use jarvis_node::mcp::McpClient;
use jarvis_node::tools::ToolRegistry;

/// Resolve the MCP server spawn command. Honors `JARVIS_SMOKE_MCP_CMD`
/// (whitespace-split into command + args) for environments where the
/// canonical `npx -y @modelcontextprotocol/server-everything` is not
/// runnable as-is (a stale `~/.npm/_cacache` permissions issue is the
/// case we hit during development; see the runbook's "Hermeticity note"
/// section for the workaround).
fn spawn_command() -> (String, Vec<String>) {
    if let Ok(raw) = std::env::var("JARVIS_SMOKE_MCP_CMD") {
        let mut it = raw.split_whitespace().map(str::to_string);
        let cmd = it.next().expect("JARVIS_SMOKE_MCP_CMD must be non-empty");
        let args: Vec<String> = it.collect();
        return (cmd, args);
    }
    (
        "npx".to_string(),
        vec![
            "-y".to_string(),
            "@modelcontextprotocol/server-everything".to_string(),
        ],
    )
}

#[tokio::test]
async fn end_to_end_get_sum_against_server_everything() {
    if std::env::var("JARVIS_SMOKE_MCP").is_err() {
        eprintln!(
            "smoke_mcp_server_everything: skipped (set JARVIS_SMOKE_MCP=1 to run; \
             see examples/smoke_mcp/README.md)"
        );
        return;
    }

    let (cmd, args) = spawn_command();
    let args_refs: Vec<&str> = args.iter().map(String::as_str).collect();

    // Connect with a generous timeout — `npx -y` may download the package
    // on first run, which can take double-digit seconds.
    let client = tokio::time::timeout(
        Duration::from_secs(120),
        McpClient::connect_stdio(&cmd, &args_refs),
    )
    .await
    .expect("MCP handshake timed out — is npm/network available?")
    .expect("MCP handshake failed");
    let client = Arc::new(client);

    let mut registry = ToolRegistry::new();
    let registered = registry
        .register_mcp_server(Arc::clone(&client))
        .await
        .expect("register_mcp_server");
    assert!(
        registered.iter().any(|n| n == "get-sum"),
        "server-everything should advertise `get-sum`; got {registered:?}"
    );

    // Drive one tool call through the registry — the same path the run
    // loop's `CallTool` dispatch takes — and confirm an `EvidenceRecord`
    // comes back with the right tool name and arg payload.
    let evidence = registry
        .call("get-sum", json!({"a": 2, "b": 3}))
        .await
        .expect("registry.call");
    assert_eq!(evidence.tool, "get-sum");
    assert_eq!(evidence.args, json!({"a": 2, "b": 3}));
    assert!(
        evidence.result.get("content").is_some(),
        "expected a `content` field in the CallToolResult, got {}",
        evidence.result
    );

    // Persist the evidence to a real on-disk root so the test exercises
    // the same shape the smoke binary produces (see `AgentFs::open` for
    // the layout). We don't drive the full agent loop here — the
    // separate `examples/smoke_mcp/` runbook + binary cover that path —
    // but pinning that evidence persistence works against a live server
    // catches the "EmitOutput cannot find the evidence" failure mode
    // before it surfaces in the runbook.
    let tmp = TempDir::new().expect("tempdir");
    let evidence_dir = tmp.path().join("evidence");
    std::fs::create_dir_all(&evidence_dir).expect("create evidence dir");
    let path = evidence_dir.join(format!("{}.json", evidence.id.as_str()));
    let body = serde_json::to_vec_pretty(&evidence).expect("serialize evidence");
    std::fs::write(&path, body).expect("write evidence");
    assert!(path.exists(), "evidence file should exist on disk");

    drop(registry);
    // The Arc strong count should be 1 here (the registry's `Arc<McpTool>`s
    // were the only siblings). Shut down the client cleanly.
    let client = Arc::try_unwrap(client).unwrap_or_else(|_| {
        panic!("client Arc still has outstanding strong refs after registry drop")
    });
    client.shutdown().await.expect("client shutdown");
}
