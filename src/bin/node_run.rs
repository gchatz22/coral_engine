//! `node-run` — hand-runnable smoke binary for the `jarvis_node` crate.
//!
//! Boots a single `Agent` against a config file and a JSONL trigger source,
//! plus a sibling `decisions.jsonl` that scripts the `MockDecide`. Runs the
//! agent until it retires, then prints the resulting `RetireReason` and a
//! tree of the per-agent FS root.
//!
//! This is the user-facing surface for inspecting the bootstrap (per
//! `scratch/minimal_node_backend.md` § 1: "a binary `node-run` that boots
//! one agent against a config file and a JSONL trigger source, for hand-
//! driven smoke tests"). Real `Decide`, real tools, and real durability are
//! follow-up tickets — for now the binary uses `MockDecide` + `EchoTool`.
//!
//! # Usage
//!
//! ```text
//! node-run <config.json> <triggers.jsonl> <fs_root>
//! ```
//!
//! * `<config.json>` — a JSON-serialized [`jarvis_node::mandate::Mandate`].
//!   Set `max_ticks` so the loop retires deterministically.
//! * `<triggers.jsonl>` — one JSON object per line. Each line is either a
//!   bare [`jarvis_node::trigger::Trigger`] (pushed with zero delay) or an
//!   envelope `{"delay_ms": <u64>, "trigger": <Trigger>}`. Blank lines and
//!   `#`-prefixed comments are ignored.
//! * `<fs_root>` — directory under which the per-agent FS layout is created
//!   (or reused, if it already exists). See [`jarvis_node::fs::AgentFs`] for
//!   the schema.
//! * `decisions.jsonl` — sibling of `<config.json>` (same directory). One
//!   JSON-serialized [`jarvis_node::decision::Decision`] per line, in the
//!   order they will be returned to the loop. Same comment/blank rules.
//!
//! # Smoke fixture
//!
//! The repository ships with a minimal fixture under `examples/smoke/`. Run:
//!
//! ```text
//! cargo run --bin node-run -- examples/smoke/config.json \
//!     examples/smoke/triggers.jsonl /tmp/jarvis-smoke-fs
//! ```
//!
//! The agent will execute three scripted decisions (call echo, emit one
//! output backed by the resulting evidence record, idle), then retire on
//! `max_ticks`. The printed tree should contain `mandate.json`,
//! `outputs/<ulid>.json`, `evidence/<sha256>.json`, and `retirement.json`.

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
use jarvis_node::mandate::Mandate;
use jarvis_node::tools::{EchoTool, ToolRegistry};
use jarvis_node::trigger::Trigger;
use jarvis_node::trigger_queue::SignalSink;

const USAGE: &str = "\
node-run — boot a single jarvis_node Agent against a config + JSONL fixture.

USAGE:
    node-run <config.json> <triggers.jsonl> <fs_root>

ARGS:
    <config.json>     JSON-serialized Mandate (text, idle_period ms, max_ticks).
                      A sibling `decisions.jsonl` in the same directory scripts
                      the MockDecide (one JSON Decision per line).
    <triggers.jsonl>  One JSON object per line. Either a bare Trigger or an
                      envelope: {\"delay_ms\": <u64>, \"trigger\": <Trigger>}.
                      Blank lines and lines starting with `#` are ignored.
    <fs_root>         Directory for the agent's per-agent FS layout
                      (mandate.json, outputs/, evidence/, notes/, retirement.json).

EXAMPLE:
    node-run examples/smoke/config.json \\
             examples/smoke/triggers.jsonl \\
             /tmp/jarvis-smoke-fs
";

/// One line of `triggers.jsonl`. The `delay_ms` field is optional; bare
/// `Trigger` objects are accepted via the `untagged` variant for ergonomic
/// fixtures that don't need a delay.
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
            eprintln!("node-run: {e:#}");
            ExitCode::from(1)
        }
    }
}

async fn run() -> Result<()> {
    let (config_path, triggers_path, fs_root) = parse_args()?;

    let mandate = load_mandate(&config_path)?;
    let decisions = load_decisions(&decisions_path_for(&config_path))?;
    let triggers = load_triggers(&triggers_path)?;

    let agent_fs = AgentFs::open(fs_root.clone(), &mandate)
        .with_context(|| format!("opening agent fs at {}", fs_root.display()))?;

    let mut tools = ToolRegistry::new();
    tools.register(Arc::new(EchoTool))?;

    let agent = Agent::new(mandate, agent_fs, MockDecide::new(decisions), tools);
    let sink = agent.signal();

    // Spawn the trigger feeder before starting the loop so triggers with a
    // zero delay are visible to the first tick. The handle is awaited after
    // `agent.run` returns so a feeder error surfaces in the exit path.
    let feeder = tokio::spawn(feed_triggers(sink, triggers));

    let RetireReason(reason) = agent.run().await.context("agent run loop")?;
    println!("node-run: agent retired: {reason}");

    // The feeder normally finishes well before retirement (it just sleeps +
    // sends and exits). Joining here surfaces a panic / send error from it.
    match feeder.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => eprintln!("node-run: trigger feeder error: {e:#}"),
        Err(join_err) => eprintln!("node-run: trigger feeder panicked: {join_err}"),
    }

    println!("node-run: fs tree at {}:", fs_root.display());
    let mut out = io::stdout().lock();
    print_tree(&mut out, &fs_root)?;

    Ok(())
}

fn parse_args() -> Result<(PathBuf, PathBuf, PathBuf)> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "-h" || a == "--help") {
        print!("{USAGE}");
        std::process::exit(0);
    }
    if args.len() != 3 {
        eprint!("{USAGE}");
        std::process::exit(2);
    }
    let mut it = args.into_iter();
    let config = PathBuf::from(it.next().unwrap());
    let triggers = PathBuf::from(it.next().unwrap());
    let fs_root = PathBuf::from(it.next().unwrap());
    Ok((config, triggers, fs_root))
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

/// Drain the parsed trigger list onto the sink, sleeping `delay_ms` before
/// each push. Stops at the first send error (the queue has been dropped,
/// which means the agent already retired — the remaining triggers are
/// irrelevant).
async fn feed_triggers(sink: SignalSink, lines: Vec<TriggerLine>) -> Result<()> {
    for line in lines {
        let (delay, trigger) = line.into_parts();
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        if sink.send(trigger).is_err() {
            // Receiver dropped — agent retired. Nothing more we can do.
            break;
        }
    }
    Ok(())
}

/// Recursively print the directory rooted at `root` to `out`. Entries are
/// sorted by filename for deterministic output. Files larger than 4 KiB are
/// elided to avoid spamming the terminal with unrelated state.
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
