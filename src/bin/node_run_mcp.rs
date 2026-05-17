//! `node-run-mcp` — hand-runnable smoke binary that boots an agent against
//! a live MCP server.
//!
//! Sibling of `node-run`. The difference is the tool registry: instead of
//! the in-process `EchoTool`, this binary spawns an MCP server as a
//! subprocess, completes the MCP handshake, and bulk-registers every tool
//! the server advertises via [`ToolRegistry::register_mcp_server`]. The
//! rest of the wiring — `MockDecide` scripted by `decisions.jsonl`, the
//! JSONL trigger feeder, the per-agent FS, the run loop — is identical.
//!
//! Gated behind the `mcp` cargo feature (same as `src/mcp/mod.rs`); the
//! `[[bin]]` entry in `Cargo.toml` declares `required-features = ["mcp"]`.
//!
//! # Usage
//!
//! ```text
//! node-run-mcp <config.json> <triggers.jsonl> <fs_root> -- <cmd> [args...]
//! ```
//!
//! Everything after a literal `--` is the spawn command for the MCP
//! server. The canonical fixture targets the public reference server:
//!
//! ```text
//! cargo run --features mcp --bin node-run-mcp -- \
//!     examples/smoke_mcp/config.json \
//!     examples/smoke_mcp/triggers.jsonl \
//!     /tmp/jarvis-smoke-mcp-fs \
//!     -- npx -y @modelcontextprotocol/server-everything
//! ```
//!
//! See `examples/smoke_mcp/README.md` for the full runbook, expected
//! outputs, and how to capture the evidence id that the fixture's
//! `EmitOutput` declares.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;

use jarvis_node::agent::{Agent, RetireReason};
use jarvis_node::decision::{Decision, MockDecide};
use jarvis_node::fs::AgentFs;
use jarvis_node::health::{HealthTracker, RetryBudget};
use jarvis_node::mandate::Mandate;
use jarvis_node::mcp::McpClient;
use jarvis_node::tools::ToolRegistry;
use jarvis_node::trigger::Trigger;
use jarvis_node::trigger_queue::SignalSink;

const USAGE: &str = "\
node-run-mcp — boot a single jarvis_node Agent against an MCP server.

USAGE:
    node-run-mcp <config.json> <triggers.jsonl> <fs_root> -- <cmd> [args...]

ARGS:
    <config.json>     JSON-serialized Mandate (text, idle_period ms, max_ticks).
                      A sibling `decisions.jsonl` in the same directory scripts
                      the MockDecide (one JSON Decision per line).
    <triggers.jsonl>  One JSON object per line. Either a bare Trigger or an
                      envelope: {\"delay_ms\": <u64>, \"trigger\": <Trigger>}.
                      Blank lines and lines starting with `#` are ignored.
    <fs_root>         Directory for the agent's per-agent FS layout
                      (mandate.json, outputs/, evidence/, notes/, retirement.json).
    --                Separates jarvis args from the MCP server spawn command.
    <cmd> [args...]   Executable + args that speak the MCP stdio protocol on
                      stdin/stdout. The process is spawned by this binary and
                      shut down when the agent retires.

EXAMPLE:
    node-run-mcp examples/smoke_mcp/config.json \\
                 examples/smoke_mcp/triggers.jsonl \\
                 /tmp/jarvis-smoke-mcp-fs \\
                 -- npx -y @modelcontextprotocol/server-everything
";

/// Parsed command line. Mirrors the fields documented in `USAGE`.
#[derive(Debug, PartialEq, Eq)]
struct Args {
    config: PathBuf,
    triggers: PathBuf,
    fs_root: PathBuf,
    spawn: Vec<String>,
}

/// One line of `triggers.jsonl`. Same shape as `node-run`'s loader; the
/// two binaries do not share the type because Rust binaries do not share
/// non-public items and promoting this to the library for two callers is
/// scope creep.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum TriggerLine {
    Envelope {
        #[serde(default)]
        delay_ms: u64,
        trigger: Trigger,
    },
    Bare(Trigger),
}

impl TriggerLine {
    fn into_parts(self) -> (Duration, Trigger) {
        match self {
            TriggerLine::Envelope { delay_ms, trigger } => {
                (Duration::from_millis(delay_ms), trigger)
            }
            TriggerLine::Bare(t) => (Duration::ZERO, t),
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("node-run-mcp: {e:#}");
            ExitCode::from(1)
        }
    }
}

async fn run() -> Result<()> {
    let args = parse_args(std::env::args().skip(1).collect())?;

    let mandate = load_mandate(&args.config)?;
    let decisions = load_decisions(&decisions_path_for(&args.config))?;
    let triggers = load_triggers(&args.triggers)?;

    let agent_fs = AgentFs::open(args.fs_root.clone(), &mandate)
        .with_context(|| format!("opening agent fs at {}", args.fs_root.display()))?;

    // Same convention as `node-run`: per-agent FS root doubles as the
    // health tracker root.
    let health = HealthTracker::open(&args.fs_root, RetryBudget::default(), chrono::Utc::now())
        .with_context(|| format!("opening health tracker at {}", args.fs_root.display()))?;

    let (cmd, cmd_args) = args.spawn.split_first().ok_or_else(|| {
        anyhow!("internal: parse_args returned an empty spawn vec; should be unreachable")
    })?;
    let cmd_args_refs: Vec<&str> = cmd_args.iter().map(String::as_str).collect();
    let client = McpClient::connect_stdio(cmd, &cmd_args_refs)
        .await
        .with_context(|| format!("connecting MCP server {cmd:?} {cmd_args:?}"))?;
    let client = Arc::new(client);

    let mut tools = ToolRegistry::new();
    let registered = tools
        .register_mcp_server(Arc::clone(&client))
        .await
        .context("bulk-registering MCP server tools")?;
    println!(
        "node-run-mcp: registered {} MCP tool(s): {}",
        registered.len(),
        registered.join(", ")
    );

    let agent = Agent::new(mandate, agent_fs, MockDecide::new(decisions), tools, health);
    let sink = agent.signal();

    let feeder = tokio::spawn(feed_triggers(sink, triggers));

    let RetireReason(reason) = agent.run().await.context("agent run loop")?;
    println!("node-run-mcp: agent retired: {reason}");

    match feeder.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => eprintln!("node-run-mcp: trigger feeder error: {e:#}"),
        Err(join_err) => eprintln!("node-run-mcp: trigger feeder panicked: {join_err}"),
    }

    // `agent.run()` consumed the Agent value, which dropped the registry
    // and every `Arc<McpTool>` inside it. Our local `client` is now the
    // only strong ref to the `McpClient` (modulo whatever sibling Arcs
    // rmcp keeps internally), so `try_unwrap` below has a real chance of
    // succeeding.
    //
    // Shut the MCP server down cleanly. Best-effort: the demo has already
    // produced its evidence; a shutdown error is logged but does not flip
    // the exit code, mirroring the trigger-feeder branch above.
    match Arc::try_unwrap(client) {
        Ok(c) => {
            if let Err(e) = c.shutdown().await {
                eprintln!("node-run-mcp: MCP client shutdown error: {e:#}");
            }
        }
        Err(_) => {
            // An outstanding Arc means some background task still holds the
            // client. Not expected after `drop(agent)`; surface as a warning
            // so the demo author notices rather than swallow it.
            eprintln!("node-run-mcp: client Arc outstanding at shutdown; skipping cancel");
        }
    }

    println!("node-run-mcp: fs tree at {}:", args.fs_root.display());
    let mut out = io::stdout().lock();
    print_tree(&mut out, &args.fs_root)?;

    Ok(())
}

/// Split the CLI on the `--` sentinel: everything before is jarvis args,
/// everything after is the MCP server spawn command. Pulled out of `main`
/// so it can be unit-tested against synthetic argv vectors.
fn parse_args(mut argv: Vec<String>) -> Result<Args> {
    if argv.iter().any(|a| a == "-h" || a == "--help") {
        print!("{USAGE}");
        std::process::exit(0);
    }

    let sep = argv.iter().position(|a| a == "--").ok_or_else(|| {
        anyhow!("missing `--` separator before MCP server spawn command\n\n{USAGE}")
    })?;
    let spawn: Vec<String> = argv.split_off(sep + 1);
    // Drop the `--` itself.
    argv.pop();

    if argv.len() != 3 {
        return Err(anyhow!(
            "expected 3 positional args before `--`, got {}\n\n{USAGE}",
            argv.len()
        ));
    }
    if spawn.is_empty() {
        return Err(anyhow!(
            "expected at least one token after `--` (the MCP server command)\n\n{USAGE}"
        ));
    }

    let mut it = argv.into_iter();
    Ok(Args {
        config: PathBuf::from(it.next().unwrap()),
        triggers: PathBuf::from(it.next().unwrap()),
        fs_root: PathBuf::from(it.next().unwrap()),
        spawn,
    })
}

fn decisions_path_for(config_path: &Path) -> PathBuf {
    config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("decisions.jsonl")
}

fn load_mandate(path: &Path) -> Result<Mandate> {
    let bytes =
        fs::read(path).with_context(|| format!("reading mandate from {}", path.display()))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing mandate JSON in {}", path.display()))
}

fn load_decisions(path: &Path) -> Result<Vec<Decision>> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("reading decisions from {}", path.display()))?;
    let mut out = Vec::new();
    for (i, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let dec: Decision = serde_json::from_str(line).with_context(|| {
            format!(
                "parsing decision JSON on line {} of {}",
                i + 1,
                path.display()
            )
        })?;
        out.push(dec);
    }
    if out.is_empty() {
        return Err(anyhow!(
            "no decisions found in {} (empty script would exhaust on tick 1)",
            path.display()
        ));
    }
    Ok(out)
}

fn load_triggers(path: &Path) -> Result<Vec<TriggerLine>> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("reading triggers from {}", path.display()))?;
    let mut out = Vec::new();
    for (i, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parsed: TriggerLine = serde_json::from_str(line).with_context(|| {
            format!(
                "parsing trigger JSON on line {} of {}",
                i + 1,
                path.display()
            )
        })?;
        out.push(parsed);
    }
    Ok(out)
}

async fn feed_triggers(sink: SignalSink, lines: Vec<TriggerLine>) -> Result<()> {
    for line in lines {
        let (delay, trigger) = line.into_parts();
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        if sink.send(trigger).is_err() {
            break;
        }
    }
    Ok(())
}

fn print_tree(out: &mut impl Write, root: &Path) -> Result<()> {
    if !root.exists() {
        writeln!(out, "(missing)")?;
        return Ok(());
    }
    writeln!(out, "{}", root.display())?;
    let mut entries: Vec<_> = fs::read_dir(root)
        .with_context(|| format!("reading {}", root.display()))?
        .collect::<io::Result<Vec<_>>>()
        .with_context(|| format!("listing {}", root.display()))?;
    entries.sort_by_key(|e| e.file_name());
    let n = entries.len();
    for (i, entry) in entries.into_iter().enumerate() {
        let last = i + 1 == n;
        print_tree_entry(out, &entry.path(), "", last)?;
    }
    Ok(())
}

fn print_tree_entry(out: &mut impl Write, path: &Path, prefix: &str, last: bool) -> Result<()> {
    let connector = if last { "└── " } else { "├── " };
    let name = path
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| path.display().to_string());
    writeln!(out, "{prefix}{connector}{name}")?;

    if path.is_dir() {
        let new_prefix = format!("{prefix}{}", if last { "    " } else { "│   " });
        let mut entries: Vec<_> = fs::read_dir(path)
            .with_context(|| format!("reading {}", path.display()))?
            .collect::<io::Result<Vec<_>>>()
            .with_context(|| format!("listing {}", path.display()))?;
        entries.sort_by_key(|e| e.file_name());
        let n = entries.len();
        for (i, entry) in entries.into_iter().enumerate() {
            print_tree_entry(out, &entry.path(), &new_prefix, i + 1 == n)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn parse_args_splits_on_dash_dash() {
        let parsed = parse_args(v(&[
            "config.json",
            "triggers.jsonl",
            "/tmp/fs",
            "--",
            "npx",
            "-y",
            "@modelcontextprotocol/server-everything",
        ]))
        .expect("parse");
        assert_eq!(parsed.config, PathBuf::from("config.json"));
        assert_eq!(parsed.triggers, PathBuf::from("triggers.jsonl"));
        assert_eq!(parsed.fs_root, PathBuf::from("/tmp/fs"));
        assert_eq!(
            parsed.spawn,
            vec![
                "npx".to_string(),
                "-y".to_string(),
                "@modelcontextprotocol/server-everything".to_string(),
            ]
        );
    }

    #[test]
    fn parse_args_errors_without_separator() {
        let err = parse_args(v(&["config.json", "triggers.jsonl", "/tmp/fs"]))
            .expect_err("expected missing-separator error");
        assert!(format!("{err}").contains("--"));
    }

    #[test]
    fn parse_args_errors_on_empty_spawn() {
        // `--` with nothing after it: the user forgot the server command.
        let err = parse_args(v(&["config.json", "triggers.jsonl", "/tmp/fs", "--"]))
            .expect_err("expected empty-spawn error");
        assert!(format!("{err}").contains("after `--`"));
    }

    #[test]
    fn parse_args_errors_on_wrong_positional_count() {
        // Two positionals before `--` — should be three.
        let err = parse_args(v(&["config.json", "triggers.jsonl", "--", "npx"]))
            .expect_err("expected positional-count error");
        assert!(format!("{err}").contains("expected 3 positional"));
    }
}
