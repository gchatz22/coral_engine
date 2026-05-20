//! `node-run-llm` — hand-runnable smoke binary that boots an agent against
//! a live MCP server *and* a real `ModelClient`.
//!
//! Sibling of `node-run-mcp`. The difference is the `Decide` impl: instead
//! of `MockDecide` driven by a `decisions.jsonl` script, this binary
//! constructs `LlmDecide` over a vendor `ModelClient` so the model itself
//! decides each tick — `CallTool`, `EmitOutput`, `Idle`, or `Retire`. The
//! rest of the wiring (per-agent FS, health tracker, MCP client spawn,
//! `register_mcp_server_with_policy`, trigger feeder, `agent.run`) is
//! identical and copied verbatim.
//!
//! Gated behind both the `mcp` and `llm-anthropic` cargo features; the
//! `[[bin]]` entry in `Cargo.toml` declares `required-features = ["mcp",
//! "llm-anthropic"]`. Anthropic is the only vendor wired at the binary
//! layer for v1; `--vendor cohere` errors at `parse_args` time with a
//! "not yet wired" hint.
//!
//! # Usage
//!
//! ```text
//! node-run-llm --vendor anthropic [--model <id>] [--max-tokens N]
//!     [--temperature F] <config.json> <triggers.jsonl> <fs_root>
//!     -- <cmd> [args...]
//! ```
//!
//! Everything after a literal `--` is the spawn command for the MCP
//! server. The canonical fixture targets the public reference server:
//!
//! ```text
//! cargo run --features "mcp llm-anthropic" --bin node-run-llm -- \
//!     --vendor anthropic \
//!     examples/smoke_llm_mcp/config.json \
//!     examples/smoke_llm_mcp/triggers.jsonl \
//!     /tmp/jarvis-smoke-llm-mcp-fs \
//!     -- npx -y @modelcontextprotocol/server-everything
//! ```
//!
//! See `examples/smoke_llm_mcp/README.md` for the full runbook.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;

use jarvis_node::agent::{Agent, RetireReason};
use jarvis_node::decide_llm::LlmDecide;
use jarvis_node::fs::AgentFs;
use jarvis_node::health::{HealthTracker, RetryBudget};
use jarvis_node::mandate::Mandate;
use jarvis_node::mcp::McpClient;
use jarvis_node::model_client::anthropic::AnthropicClient;
use jarvis_node::model_client::{CompleteOptions, ModelClient};
use jarvis_node::tools::ToolRegistry;
use jarvis_node::trigger::Trigger;
use jarvis_node::trigger_queue::SignalSink;

const USAGE: &str = "\
node-run-llm — boot a single jarvis_node Agent against an MCP server with an LLM-driven Decide.

USAGE:
    node-run-llm --vendor anthropic [--model <id>] [--max-tokens N] [--temperature F]
                 <config.json> <triggers.jsonl> <fs_root> -- <cmd> [args...]

ARGS:
    --vendor <name>   Required. `anthropic` (this binary requires --features llm-anthropic).
                      `cohere` is reserved; rebuild with --features llm-cohere and add the
                      arm when the wiring lands.
    --model <id>      Optional override of the adapter's default model id.
    --max-tokens N    Optional sampling cap on the model's reply (default 1024).
    --temperature F   Optional sampling temperature; omitted from CompleteOptions when unset.

    <config.json>     JSON-serialized Mandate (text, idle_period ms, max_ticks).
    <triggers.jsonl>  One JSON object per line. Either a bare Trigger or an
                      envelope: {\"delay_ms\": <u64>, \"trigger\": <Trigger>}.
                      Blank lines and lines starting with `#` are ignored.
    <fs_root>         Directory for the agent's per-agent FS layout
                      (mandate.json, outputs/, evidence/, notes/, retirement.json).
    --                Separates jarvis args from the MCP server spawn command.
    <cmd> [args...]   Executable + args that speak the MCP stdio protocol on
                      stdin/stdout. The process is spawned by this binary and
                      shut down when the agent retires.

ENV:
    ANTHROPIC_API_KEY  Required for --vendor anthropic. Surfaced verbatim as
                       ModelError::Auth if missing.
    ANTHROPIC_MODEL    Optional. Overrides the default model id when --model
                       is not given.

EXAMPLE:
    node-run-llm --vendor anthropic \\
                 examples/smoke_llm_mcp/config.json \\
                 examples/smoke_llm_mcp/triggers.jsonl \\
                 /tmp/jarvis-smoke-llm-mcp-fs \\
                 -- npx -y @modelcontextprotocol/server-everything
";

const DEFAULT_MAX_TOKENS: u32 = 1024;

/// Vendors the CLI surface understands. Intentionally closed: v1 wires
/// Anthropic only. `--vendor cohere` is rejected inside `parse_vendor`
/// (not represented here) so the reject path has access to a specific
/// "rebuild with --features llm-cohere" hint rather than the generic
/// "unknown vendor" message. New vendors get a variant here as their
/// dispatch arm in `run()` lands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Vendor {
    Anthropic,
}

/// Parsed command line. Mirrors the fields documented in `USAGE`.
#[derive(Debug, PartialEq)]
struct Args {
    vendor: Vendor,
    model: Option<String>,
    max_tokens: u32,
    temperature: Option<f32>,
    config: PathBuf,
    triggers: PathBuf,
    fs_root: PathBuf,
    spawn: Vec<String>,
}

/// One line of `triggers.jsonl`. Same shape as `node-run-mcp`'s loader;
/// the two binaries do not share the type because Rust binaries do not
/// share non-public items and promoting this to the library for two
/// callers is scope creep.
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
            eprintln!("node-run-llm: {e:#}");
            ExitCode::from(1)
        }
    }
}

async fn run() -> Result<()> {
    let args = parse_args(std::env::args().skip(1).collect())?;

    let mandate = load_mandate(&args.config)?;
    let triggers = load_triggers(&args.triggers)?;

    let agent_fs = AgentFs::open(args.fs_root.clone(), &mandate)
        .with_context(|| format!("opening agent fs at {}", args.fs_root.display()))?;

    // Same convention as `node-run-mcp`: per-agent FS root doubles as the
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
    // JAR2-31: thread `Mandate::retry_policy` (when set) into every
    // `McpTool` we mint for this agent. `None` keeps JAR2-25 defaults.
    let registered = tools
        .register_mcp_server_with_policy(Arc::clone(&client), mandate.retry_policy)
        .await
        .context("bulk-registering MCP server tools")?;
    println!(
        "node-run-llm: registered {} MCP tool(s): {}",
        registered.len(),
        registered.join(", ")
    );

    // Vendor dispatch. `--vendor cohere` is rejected at parse time, so
    // only the Anthropic arm exists here. Build the client, optionally
    // override the model, and wrap as `Arc<dyn ModelClient>` so
    // `LlmDecide::new` can take it.
    let model_client: Arc<dyn ModelClient> = match args.vendor {
        Vendor::Anthropic => {
            let c = match args.model.as_deref() {
                Some(m) => AnthropicClient::new().with_model(m),
                None => AnthropicClient::new(),
            };
            println!("node-run-llm: vendor=anthropic model={}", c.model());
            Arc::new(c)
        }
    };
    let options = CompleteOptions {
        max_tokens: args.max_tokens,
        temperature: args.temperature,
    };
    let decide = LlmDecide::new(model_client, options);

    let agent = Agent::new(mandate, agent_fs, decide, tools, health);
    let sink = agent.signal();

    let feeder = tokio::spawn(feed_triggers(sink, triggers));

    let RetireReason(reason) = agent.run().await.context("agent run loop")?;
    println!("node-run-llm: agent retired: {reason}");

    match feeder.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => eprintln!("node-run-llm: trigger feeder error: {e:#}"),
        Err(join_err) => eprintln!("node-run-llm: trigger feeder panicked: {join_err}"),
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
                eprintln!("node-run-llm: MCP client shutdown error: {e:#}");
            }
        }
        Err(_) => {
            // An outstanding Arc means some background task still holds the
            // client. Not expected after `drop(agent)`; surface as a warning
            // so the demo author notices rather than swallow it.
            eprintln!("node-run-llm: client Arc outstanding at shutdown; skipping cancel");
        }
    }

    println!("node-run-llm: fs tree at {}:", args.fs_root.display());
    let mut out = io::stdout().lock();
    print_tree(&mut out, &args.fs_root)?;

    Ok(())
}

/// Parse the CLI. Hand-rolled to match the rest of the binary suite (no
/// `clap`). Flag args may appear in any order before the three positional
/// path args; everything after a literal `--` is the MCP server spawn
/// command. Pulled out of `main` so it can be unit-tested against
/// synthetic argv vectors.
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
    if spawn.is_empty() {
        return Err(anyhow!(
            "expected at least one token after `--` (the MCP server command)\n\n{USAGE}"
        ));
    }

    let mut vendor: Option<Vendor> = None;
    let mut model: Option<String> = None;
    let mut max_tokens: Option<u32> = None;
    let mut temperature: Option<f32> = None;
    let mut positional: Vec<String> = Vec::new();

    let mut it = argv.into_iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--vendor" => {
                let v = it
                    .next()
                    .ok_or_else(|| anyhow!("--vendor requires a value"))?;
                if vendor.is_some() {
                    return Err(anyhow!("--vendor specified more than once"));
                }
                vendor = Some(parse_vendor(&v)?);
            }
            "--model" => {
                let v = it
                    .next()
                    .ok_or_else(|| anyhow!("--model requires a value"))?;
                if model.is_some() {
                    return Err(anyhow!("--model specified more than once"));
                }
                model = Some(v);
            }
            "--max-tokens" => {
                let v = it
                    .next()
                    .ok_or_else(|| anyhow!("--max-tokens requires a value"))?;
                if max_tokens.is_some() {
                    return Err(anyhow!("--max-tokens specified more than once"));
                }
                let n: u32 = v
                    .parse()
                    .with_context(|| format!("parsing --max-tokens value `{v}`"))?;
                if n == 0 {
                    return Err(anyhow!("--max-tokens must be > 0"));
                }
                max_tokens = Some(n);
            }
            "--temperature" => {
                let v = it
                    .next()
                    .ok_or_else(|| anyhow!("--temperature requires a value"))?;
                if temperature.is_some() {
                    return Err(anyhow!("--temperature specified more than once"));
                }
                let f: f32 = v
                    .parse()
                    .with_context(|| format!("parsing --temperature value `{v}`"))?;
                temperature = Some(f);
            }
            other if other.starts_with("--") => {
                return Err(anyhow!("unknown argument `{other}`\n\n{USAGE}"));
            }
            _ => {
                positional.push(arg);
            }
        }
    }

    let vendor = vendor.ok_or_else(|| anyhow!("--vendor is required\n\n{USAGE}"))?;

    if positional.len() != 3 {
        return Err(anyhow!(
            "expected 3 positional args before `--`, got {}\n\n{USAGE}",
            positional.len()
        ));
    }
    let mut pit = positional.into_iter();
    Ok(Args {
        vendor,
        model,
        max_tokens: max_tokens.unwrap_or(DEFAULT_MAX_TOKENS),
        temperature,
        config: PathBuf::from(pit.next().unwrap()),
        triggers: PathBuf::from(pit.next().unwrap()),
        fs_root: PathBuf::from(pit.next().unwrap()),
        spawn,
    })
}

/// Parse the `--vendor` value into a typed `Vendor`. `cohere` is rejected
/// here (not in the dispatch arm) so the CLI surface fails fast with a
/// specific hint and the unit tests don't need feature flags to assert
/// the rejection.
fn parse_vendor(s: &str) -> Result<Vendor> {
    match s {
        "anthropic" => Ok(Vendor::Anthropic),
        "cohere" => Err(anyhow!(
            "vendor 'cohere' is not yet wired; rebuild with --features llm-cohere \
             and add the arm in src/bin/node_run_llm.rs"
        )),
        other => Err(anyhow!("unknown vendor `{other}` (expected `anthropic`)")),
    }
}

fn load_mandate(path: &Path) -> Result<Mandate> {
    let bytes =
        fs::read(path).with_context(|| format!("reading mandate from {}", path.display()))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing mandate JSON in {}", path.display()))
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
    fn parse_args_minimal_anthropic_splits_on_dash_dash() {
        let parsed = parse_args(v(&[
            "--vendor",
            "anthropic",
            "config.json",
            "triggers.jsonl",
            "/tmp/fs",
            "--",
            "npx",
            "-y",
            "@modelcontextprotocol/server-everything",
        ]))
        .expect("parse");
        assert_eq!(parsed.vendor, Vendor::Anthropic);
        assert!(parsed.model.is_none());
        assert_eq!(parsed.max_tokens, DEFAULT_MAX_TOKENS);
        assert!(parsed.temperature.is_none());
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
    fn parse_args_model_override_passes_through() {
        let parsed = parse_args(v(&[
            "--vendor",
            "anthropic",
            "--model",
            "claude-sonnet-4-5",
            "config.json",
            "triggers.jsonl",
            "/tmp/fs",
            "--",
            "npx",
        ]))
        .expect("parse");
        assert_eq!(parsed.model.as_deref(), Some("claude-sonnet-4-5"));
    }

    #[test]
    fn parse_args_max_tokens_override_changes_field() {
        let parsed = parse_args(v(&[
            "--vendor",
            "anthropic",
            "--max-tokens",
            "256",
            "config.json",
            "triggers.jsonl",
            "/tmp/fs",
            "--",
            "npx",
        ]))
        .expect("parse");
        assert_eq!(parsed.max_tokens, 256);
    }

    #[test]
    fn parse_args_temperature_override_populates_field() {
        let parsed = parse_args(v(&[
            "--vendor",
            "anthropic",
            "--temperature",
            "0.25",
            "config.json",
            "triggers.jsonl",
            "/tmp/fs",
            "--",
            "npx",
        ]))
        .expect("parse");
        assert!((parsed.temperature.unwrap() - 0.25_f32).abs() < f32::EPSILON);
    }

    #[test]
    fn parse_args_temperature_default_is_none() {
        // Confirms the "omit from CompleteOptions if not supplied" contract:
        // parser returns None and the dispatch in `run()` passes that
        // through verbatim.
        let parsed = parse_args(v(&[
            "--vendor",
            "anthropic",
            "config.json",
            "triggers.jsonl",
            "/tmp/fs",
            "--",
            "npx",
        ]))
        .expect("parse");
        assert!(parsed.temperature.is_none());
    }

    #[test]
    fn parse_args_errors_without_separator() {
        let err = parse_args(v(&[
            "--vendor",
            "anthropic",
            "config.json",
            "triggers.jsonl",
            "/tmp/fs",
        ]))
        .expect_err("expected missing-separator error");
        assert!(format!("{err}").contains("--"));
    }

    #[test]
    fn parse_args_errors_on_empty_spawn() {
        // `--` with nothing after it: the user forgot the server command.
        let err = parse_args(v(&[
            "--vendor",
            "anthropic",
            "config.json",
            "triggers.jsonl",
            "/tmp/fs",
            "--",
        ]))
        .expect_err("expected empty-spawn error");
        assert!(format!("{err}").contains("after `--`"));
    }

    #[test]
    fn parse_args_errors_on_wrong_positional_count() {
        // Two positionals before `--` — should be three.
        let err = parse_args(v(&[
            "--vendor",
            "anthropic",
            "config.json",
            "triggers.jsonl",
            "--",
            "npx",
        ]))
        .expect_err("expected positional-count error");
        assert!(format!("{err}").contains("expected 3 positional"));
    }

    #[test]
    fn parse_args_rejects_missing_vendor() {
        let err = parse_args(v(&[
            "config.json",
            "triggers.jsonl",
            "/tmp/fs",
            "--",
            "npx",
        ]))
        .expect_err("expected missing-vendor error");
        assert!(format!("{err}").contains("--vendor"));
    }

    #[test]
    fn parse_args_rejects_cohere_vendor_with_helpful_hint() {
        let err = parse_args(v(&[
            "--vendor",
            "cohere",
            "config.json",
            "triggers.jsonl",
            "/tmp/fs",
            "--",
            "npx",
        ]))
        .expect_err("expected cohere-rejected error");
        let msg = format!("{err:#}");
        assert!(msg.contains("cohere"), "msg: {msg}");
        assert!(msg.contains("llm-cohere"), "msg: {msg}");
    }

    #[test]
    fn parse_args_rejects_unknown_vendor() {
        let err = parse_args(v(&[
            "--vendor",
            "openai",
            "config.json",
            "triggers.jsonl",
            "/tmp/fs",
            "--",
            "npx",
        ]))
        .expect_err("expected unknown-vendor error");
        assert!(format!("{err:#}").contains("openai"));
    }

    #[test]
    fn parse_args_rejects_zero_max_tokens() {
        let err = parse_args(v(&[
            "--vendor",
            "anthropic",
            "--max-tokens",
            "0",
            "config.json",
            "triggers.jsonl",
            "/tmp/fs",
            "--",
            "npx",
        ]))
        .expect_err("expected zero-max-tokens error");
        assert!(format!("{err:#}").contains("--max-tokens"));
    }

    #[test]
    fn parse_args_rejects_unknown_flag() {
        let err = parse_args(v(&[
            "--vendor",
            "anthropic",
            "--tools",
            "config.json",
            "triggers.jsonl",
            "/tmp/fs",
            "--",
            "npx",
        ]))
        .expect_err("expected unknown-flag error");
        assert!(format!("{err:#}").contains("--tools"));
    }
}
