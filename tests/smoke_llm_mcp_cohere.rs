//! Optional end-to-end smoke for the LLM-driven Decide path against
//! `@modelcontextprotocol/server-everything` and a live Cohere key.
//!
//! **Gated behind BOTH `JARVIS_SMOKE_LLM_MCP=1` and `COHERE_API_KEY`.**
//! Without either env var the test returns early — `cargo test` (with or
//! without `--features "mcp llm-cohere"`) stays hermetic and offline by
//! default. Same pattern as `tests/smoke_llm_mcp_anthropic.rs`.
//!
//! The test shells out to the `node-run-llm` binary (cargo injects its
//! path via `CARGO_BIN_EXE_node-run-llm` for any `[[bin]]` target). That
//! exercises the binary's CLI plumbing, MCP spawn, and shutdown path
//! end-to-end — the same surface a human runs from the runbook in
//! `examples/smoke_llm_mcp/README.md`.
//!
//! Assertions on success:
//!
//! * The binary exits zero.
//! * `retirement.json` exists (terminal marker).
//! * `health.json` records the agent as `Healthy`.
//! * `outputs/` contains at least one JSON Output.
//! * Every Output's `evidence` array resolves to an existing file under
//!   `evidence/<id>.json`.
//!
//! Run it explicitly:
//!
//! ```bash
//! JARVIS_SMOKE_LLM_MCP=1 COHERE_API_KEY=... \
//!     cargo test --features "mcp llm-cohere" \
//!         --test smoke_llm_mcp_cohere -- --nocapture
//! ```

#![cfg(all(feature = "mcp", feature = "llm-cohere"))]

use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;
use tempfile::TempDir;

/// Parse the resolved per-invocation FS root out of `node-run-llm`'s
/// stdout. The binary prints `node-run-llm: fs_root=<absolute path>`
/// on a line of its own (load-bearing: see the binary's `run_inner`).
/// We scan by prefix rather than line index so unrelated startup
/// noise on stdout doesn't break the test.
fn parse_resolved_fs_root(stdout: &str) -> PathBuf {
    const PREFIX: &str = "node-run-llm: fs_root=";
    let line = stdout
        .lines()
        .find(|l| l.starts_with(PREFIX))
        .unwrap_or_else(|| panic!("missing `{PREFIX}` line in stdout:\n{stdout}"));
    PathBuf::from(line.trim_start_matches(PREFIX).trim_end())
}

/// Resolve the MCP server spawn command. Honors `JARVIS_SMOKE_MCP_CMD`
/// (whitespace-split into command + args) for environments where the
/// canonical `npx -y @modelcontextprotocol/server-everything` is not
/// runnable as-is. Mirrors `tests/smoke_mcp_server_everything.rs`.
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

/// Read every JSON file under `dir`, returning each parsed `Value`. Used
/// to walk the agent's `outputs/` directory after the binary exits.
fn read_json_dir(dir: &Path) -> Vec<Value> {
    let entries = match std::fs::read_dir(dir) {
        Ok(it) => it,
        Err(_) => return Vec::new(),
    };
    let mut out = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let bytes = std::fs::read(&path).expect("read output file");
        let v: Value = serde_json::from_slice(&bytes).expect("parse output JSON");
        out.push(v);
    }
    out
}

#[test]
fn end_to_end_llm_decide_against_server_everything() {
    if std::env::var("JARVIS_SMOKE_LLM_MCP").is_err() {
        eprintln!(
            "smoke_llm_mcp_cohere: skipped (set JARVIS_SMOKE_LLM_MCP=1 to run; \
             see examples/smoke_llm_mcp/README.md)"
        );
        return;
    }
    if std::env::var("COHERE_API_KEY").is_err() {
        eprintln!(
            "smoke_llm_mcp_cohere: skipped (COHERE_API_KEY not set; \
             see examples/smoke_llm_mcp/README.md)"
        );
        return;
    }

    // A fresh tempdir is the *parent* of the per-invocation FS root —
    // the binary now stamps a timestamped subdirectory inside whatever
    // path we pass on the CLI. We discover the resolved subdir below
    // by parsing stdout, then assert against files under it.
    let parent_dir = TempDir::new().expect("tempdir");
    let (cmd, args) = spawn_command();

    // Cargo injects this for every `[[bin]]` target. The path points at
    // the compiled artifact under target/<profile>/, so no `cargo run`
    // overhead from inside the test.
    let bin = env!("CARGO_BIN_EXE_node-run-llm");

    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let config = format!("{manifest_dir}/examples/smoke_llm_mcp/config.json");
    let triggers = format!("{manifest_dir}/examples/smoke_llm_mcp/triggers.jsonl");

    let mut command = Command::new(bin);
    command
        .arg("--vendor")
        .arg("cohere")
        .arg(&config)
        .arg(&triggers)
        .arg(parent_dir.path())
        .arg("--")
        .arg(&cmd)
        .args(&args);

    let output = command.output().expect("spawn node-run-llm");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    eprintln!("--- node-run-llm stdout ---\n{stdout}");
    eprintln!("--- node-run-llm stderr ---\n{stderr}");
    assert!(
        output.status.success(),
        "node-run-llm exited with status {:?}",
        output.status
    );

    // Discover the per-invocation FS root from stdout. Everything below
    // asserts under this resolved path, not under `parent_dir.path()`.
    let fs_root = parse_resolved_fs_root(&stdout);
    assert!(
        fs_root.exists(),
        "resolved fs_root {} does not exist",
        fs_root.display()
    );

    // Terminal marker must exist on a clean retirement.
    let retirement = fs_root.join("retirement.json");
    assert!(
        retirement.exists(),
        "retirement.json missing at {}",
        retirement.display()
    );

    // The agent should be `Healthy` after a successful run. `state` is
    // either the string `"Healthy"` or an object with `"Healthy"` key
    // depending on the version of the encoder; check for either shape.
    let health_path = fs_root.join("health.json");
    let health_bytes = std::fs::read(&health_path).expect("read health.json");
    let health: Value = serde_json::from_slice(&health_bytes).expect("parse health.json");
    let state = &health["state"];
    let healthy = state.as_str() == Some("Healthy")
        || state.get("Healthy").is_some()
        || state.as_object().is_some_and(|o| {
            o.get("state").and_then(Value::as_str) == Some("Healthy")
                || o.keys().any(|k| k == "Healthy")
        });
    assert!(
        healthy,
        "expected health.json to record Healthy state, got: {health}"
    );

    // At least one output is the parent-acceptance bar.
    let outputs_dir = fs_root.join("outputs");
    let outputs = read_json_dir(&outputs_dir);
    assert!(
        !outputs.is_empty(),
        "expected >=1 output under {}, found none",
        outputs_dir.display()
    );

    // Every evidence id in every Output must resolve to a file on disk
    // under `evidence/<id>.json`. This is the JAR2-12 parent-acceptance
    // assertion in test form.
    let evidence_dir = fs_root.join("evidence");
    for out in &outputs {
        let evidence = out["evidence"]
            .as_array()
            .expect("Output.evidence is an array");
        assert!(
            !evidence.is_empty(),
            "Output.evidence empty in {out}; LLM is expected to cite the get-sum evidence id"
        );
        for ev in evidence {
            let id = ev.as_str().expect("evidence id is a string");
            let path = evidence_dir.join(format!("{id}.json"));
            assert!(
                path.exists(),
                "evidence id {id} from output does not resolve to {}",
                path.display()
            );
        }
    }
}
