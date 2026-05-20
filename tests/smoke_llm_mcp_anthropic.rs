//! Optional end-to-end smoke for the LLM-driven Decide path against
//! `@modelcontextprotocol/server-everything` and a live Anthropic key.
//!
//! **Gated behind BOTH `JARVIS_SMOKE_LLM_MCP=1` and `ANTHROPIC_API_KEY`.**
//! Without either env var the test returns early — `cargo test` (with or
//! without `--features "mcp llm-anthropic"`) stays hermetic and offline by
//! default. Same pattern as `tests/smoke_mcp_server_everything.rs`.
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
//! JARVIS_SMOKE_LLM_MCP=1 ANTHROPIC_API_KEY=sk-ant-... \
//!     cargo test --features "mcp llm-anthropic" \
//!         --test smoke_llm_mcp_anthropic -- --nocapture
//! ```

#![cfg(all(feature = "mcp", feature = "llm-anthropic"))]

use std::path::Path;
use std::process::Command;

use serde_json::Value;
use tempfile::TempDir;

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
            "smoke_llm_mcp_anthropic: skipped (set JARVIS_SMOKE_LLM_MCP=1 to run; \
             see examples/smoke_llm_mcp/README.md)"
        );
        return;
    }
    if std::env::var("ANTHROPIC_API_KEY").is_err() {
        eprintln!(
            "smoke_llm_mcp_anthropic: skipped (ANTHROPIC_API_KEY not set; \
             see examples/smoke_llm_mcp/README.md)"
        );
        return;
    }

    // A fresh tempdir per run keeps repeated invocations independent.
    let fs_root = TempDir::new().expect("tempdir");
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
        .arg("anthropic")
        .arg(&config)
        .arg(&triggers)
        .arg(fs_root.path())
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

    // Terminal marker must exist on a clean retirement.
    let retirement = fs_root.path().join("retirement.json");
    assert!(
        retirement.exists(),
        "retirement.json missing at {}",
        retirement.display()
    );

    // The agent should be `Healthy` after a successful run. `state` is
    // either the string `"Healthy"` or an object with `"Healthy"` key
    // depending on the version of the encoder; check for either shape.
    let health_path = fs_root.path().join("health.json");
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
    let outputs_dir = fs_root.path().join("outputs");
    let outputs = read_json_dir(&outputs_dir);
    assert!(
        !outputs.is_empty(),
        "expected >=1 output under {}, found none",
        outputs_dir.display()
    );

    // Every evidence id in every Output must resolve to a file on disk
    // under `evidence/<id>.json`. This is the JAR2-12 parent-acceptance
    // assertion in test form.
    let evidence_dir = fs_root.path().join("evidence");
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
