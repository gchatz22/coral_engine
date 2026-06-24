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
//! # Feature gating
//!
//! Cargo's `required-features` does not support OR, so the per-vendor
//! dispatch arms and the MCP-using body are gated with
//! `#[cfg(feature = "...")]`; a build with neither `mcp` nor a vendor
//! feature still compiles the binary but every `--vendor` choice errors
//! at runtime with a "rebuild with --features ..." hint. Mirrors
//! `src/bin/model_call.rs`.
//!
//! # Usage
//!
//! ```text
//! node-run-llm --vendor <anthropic|cohere> [--model <id>] [--max-tokens N]
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
//!     /tmp/coral-smoke-llm-mcp-fs \
//!     -- npx -y @modelcontextprotocol/server-everything
//! ```
//!
//! See `examples/smoke_llm_mcp/README.md` for the full runbook.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
// `Arc` and `ModelClient` are needed by `run_inner` and by the
// `build_*_client` "not built" stubs, which exist whenever `mcp` is on
// (the stubs cover the missing-vendor runtime error). `Duration` is
// only consumed inside `run_inner`, so it rides the tighter gate.
#[cfg(feature = "mcp")]
use std::sync::Arc;
#[cfg(all(
    feature = "mcp",
    any(feature = "llm-anthropic", feature = "llm-cohere")
))]
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use serde::Deserialize;

// The runtime path needs `mcp` *and* at least one vendor: `LlmDecide`
// itself is gated on `any(llm-anthropic, llm-cohere)` in
// `src/decide_llm/mod.rs`, so a `--features mcp` build without any
// vendor cannot compile `run_inner`. All imports that are only consumed
// inside `run_inner` ride the same compound gate; the binary still
// compiles with zero features (and errors at runtime).
#[cfg(all(
    feature = "mcp",
    any(feature = "llm-anthropic", feature = "llm-cohere")
))]
use coral_node::agent::{Agent, RetireReason};
#[cfg(all(
    feature = "mcp",
    any(feature = "llm-anthropic", feature = "llm-cohere")
))]
use coral_node::decide_llm::LlmDecide;
#[cfg(all(
    feature = "mcp",
    any(feature = "llm-anthropic", feature = "llm-cohere")
))]
use coral_node::fs::AgentFs;
#[cfg(all(
    feature = "mcp",
    any(feature = "llm-anthropic", feature = "llm-cohere")
))]
use coral_node::health::{HealthTracker, RetryBudget};
use coral_node::mandate::Mandate;
#[cfg(all(
    feature = "mcp",
    any(feature = "llm-anthropic", feature = "llm-cohere")
))]
use coral_node::mcp::McpClient;
#[cfg(all(feature = "mcp", feature = "llm-anthropic"))]
use coral_node::model_client::anthropic::AnthropicClient;
#[cfg(all(feature = "mcp", feature = "llm-cohere"))]
use coral_node::model_client::cohere::CohereClient;
// `CompleteOptions` is only used in `run_inner`; `ModelClient` is also
// referenced by the `build_*_client` stubs, so it rides the looser gate.
#[cfg(all(
    feature = "mcp",
    any(feature = "llm-anthropic", feature = "llm-cohere")
))]
use coral_node::model_client::CompleteOptions;
#[cfg(feature = "mcp")]
use coral_node::model_client::ModelClient;
#[cfg(all(
    feature = "mcp",
    any(feature = "llm-anthropic", feature = "llm-cohere")
))]
use coral_node::tools::ToolRegistry;
use coral_node::trigger::Trigger;
#[cfg(all(
    feature = "mcp",
    any(feature = "llm-anthropic", feature = "llm-cohere")
))]
use coral_node::trigger_queue::SignalSink;

const USAGE: &str = "\
node-run-llm — boot a single coral_node Agent against an MCP server with an LLM-driven Decide.

USAGE:
    node-run-llm --vendor <anthropic|cohere> [--model <id>] [--max-tokens N]
                 [--temperature F] <config.json> <triggers.jsonl> <fs_root>
                 -- <cmd> [args...]

ARGS:
    --vendor <name>   Required. `anthropic` (build with --features llm-anthropic)
                      or `cohere` (build with --features llm-cohere). The `mcp`
                      feature is also required for the MCP wiring; without it
                      every --vendor choice errors at runtime.
    --model <id>      Optional override of the adapter's default model id.
    --max-tokens N    Optional sampling cap on the model's reply (default 1024).
    --temperature F   Optional sampling temperature; omitted from CompleteOptions when unset.

    <config.json>     JSON-serialized Mandate (text, idle_period ms, step_cap).
    <triggers.jsonl>  One JSON object per line. Either a bare Trigger or an
                      envelope: {\"delay_ms\": <u64>, \"trigger\": <Trigger>}.
                      Blank lines and lines starting with `#` are ignored.
    <fs_root>         *Parent* directory for the per-agent FS. The binary
                      stamps a fresh timestamped subdirectory inside it for
                      this invocation (`<YYYY-MM-DDTHH-MM-SS-sssZ>`) and
                      writes the agent's FS layout there (mandate.md,
                      outputs/, evidence/, notes/, retirement.json). The
                      resolved absolute path is printed on the first line
                      of stdout (`node-run-llm: fs_root=...`). Two
                      successive invocations accumulate; they do not
                      clobber each other.
    --                Separates coral args from the MCP server spawn command.
    <cmd> [args...]   Executable + args that speak the MCP stdio protocol on
                      stdin/stdout. The process is spawned by this binary and
                      shut down when the agent retires.

ENV:
    ANTHROPIC_API_KEY  Required for --vendor anthropic. Surfaced verbatim as
                       ModelError::Auth if missing.
    ANTHROPIC_MODEL    Optional. Overrides the Anthropic default model id when
                       --model is not given.
    COHERE_API_KEY     Required for --vendor cohere. Surfaced verbatim as
                       ModelError::Auth if missing.
    COHERE_MODEL       Optional. Overrides the Cohere default model id when
                       --model is not given.

EXAMPLE:
    node-run-llm --vendor anthropic \\
                 examples/smoke_llm_mcp/config.json \\
                 examples/smoke_llm_mcp/triggers.jsonl \\
                 /tmp/coral-smoke-llm-mcp-fs \\
                 -- npx -y @modelcontextprotocol/server-everything
";

const DEFAULT_MAX_TOKENS: u32 = 1024;

/// Vendors the CLI surface understands. Each variant is accepted at parse
/// time regardless of which vendor features were compiled in; the runtime
/// dispatch in `run_inner` errors with a "rebuild with --features ..."
/// hint when the requested vendor's adapter is not built. Mirrors
/// `src/bin/model_call.rs`'s `Vendor`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Vendor {
    Anthropic,
    Cohere,
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
///
/// `allow(dead_code)` is applied because when the binary is built
/// without the `mcp` feature the fields are unread (the runtime path
/// that consumes them is itself cfg-gated). The CLI parse path still
/// instantiates the enum from JSON.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
#[allow(dead_code)]
enum TriggerLine {
    Envelope {
        #[serde(default)]
        delay_ms: u64,
        trigger: Trigger,
    },
    Bare(Trigger),
}

impl TriggerLine {
    #[cfg(all(
        feature = "mcp",
        any(feature = "llm-anthropic", feature = "llm-cohere")
    ))]
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
    run_inner(args).await
}

/// MCP + vendor-gated body. Requires both the `mcp` feature *and* at
/// least one vendor (`llm-anthropic` or `llm-cohere`) — `LlmDecide`
/// itself is gated on the vendor set, so a `--features mcp` build
/// with no vendor cannot compile this path. The two `cfg(not(...))`
/// stubs below cover the runtime-error case in both missing
/// configurations. CLI parsing still works unconditionally.
#[cfg(all(
    feature = "mcp",
    any(feature = "llm-anthropic", feature = "llm-cohere")
))]
async fn run_inner(args: Args) -> Result<()> {
    let mandate = load_mandate(&args.config)?;
    let triggers = load_triggers(&args.triggers)?;

    // Treat the supplied `<fs_root>` positional as a *parent* directory
    // and stamp a fresh per-invocation subdirectory inside it. Created
    // before anything else so the resolved path can be printed as the
    // very first stdout line — integration tests parse it by prefix
    // (`node-run-llm: fs_root=...`).
    //
    // Two successive invocations against the same parent must produce
    // distinct subdirs even when they share a wall-clock second; the
    // helper uses millisecond precision and falls back to appending a
    // ULID if a same-millisecond collision somehow happens.
    let now = Utc::now();
    let resolved_fs_root = resolve_fs_root(&args.fs_root, now)
        .with_context(|| format!("resolving fs_root under parent {}", args.fs_root.display()))?;
    // `println!` flushes line-by-line on a tty/pipe; this is the
    // load-bearing first line of stdout.
    println!("node-run-llm: fs_root={}", resolved_fs_root.display());

    let agent_fs = AgentFs::open(resolved_fs_root.clone(), &mandate)
        .await
        .with_context(|| format!("opening agent fs at {}", resolved_fs_root.display()))?;

    // Same convention as `node-run-mcp`: per-agent FS root doubles as the
    // health tracker root.
    let health = HealthTracker::open(&resolved_fs_root, RetryBudget::default(), now)
        .with_context(|| format!("opening health tracker at {}", resolved_fs_root.display()))?;

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
        .register_mcp_server_with_policy(Arc::clone(&client), mandate.retry_policy)
        .await
        .context("bulk-registering MCP server tools")?;
    println!(
        "node-run-llm: registered {} MCP tool(s): {}",
        registered.len(),
        registered.join(", ")
    );

    // Vendor dispatch. Each arm is gated on its vendor feature; the
    // "not built" arm runs when the binary was compiled without that
    // vendor and surfaces a "rebuild with --features ..." hint.
    let model_client: Arc<dyn ModelClient> = match args.vendor {
        Vendor::Anthropic => build_anthropic_client(args.model.as_deref())?,
        Vendor::Cohere => build_cohere_client(args.model.as_deref())?,
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

    println!("node-run-llm: fs tree at {}:", resolved_fs_root.display());
    let mut out = io::stdout().lock();
    print_tree(&mut out, &resolved_fs_root)?;

    Ok(())
}

/// `mcp`-missing stub. Binary compiles + parses; runtime errors with a
/// rebuild hint. Mirrors model_call's per-vendor stubs.
#[cfg(not(feature = "mcp"))]
async fn run_inner(_args: Args) -> Result<()> {
    Err(anyhow!(
        "node-run-llm requires the `mcp` feature; rebuild with --features mcp \
         (plus a vendor feature: llm-anthropic or llm-cohere)"
    ))
}

/// `mcp`-present-but-no-vendor stub. The `mcp`-only build is a valid
/// configuration for the rest of the crate (and is exercised by the CI
/// feature matrix), so the binary must compile in that case. Errors at
/// runtime since `LlmDecide` requires a vendor.
#[cfg(all(
    feature = "mcp",
    not(any(feature = "llm-anthropic", feature = "llm-cohere"))
))]
async fn run_inner(_args: Args) -> Result<()> {
    Err(anyhow!(
        "node-run-llm needs a vendor feature; rebuild with \
         --features \"mcp llm-anthropic\" or --features \"mcp llm-cohere\""
    ))
}

#[cfg(all(feature = "mcp", feature = "llm-anthropic"))]
fn build_anthropic_client(model: Option<&str>) -> Result<Arc<dyn ModelClient>> {
    let c = match model {
        Some(m) => AnthropicClient::new().with_model(m),
        None => AnthropicClient::new(),
    };
    println!("node-run-llm: vendor=anthropic model={}", c.model());
    Ok(Arc::new(c))
}

// `#[allow(dead_code)]`: when the binary is built with `--features mcp`
// alone, `run_inner` is the no-vendor stub and never calls these. The
// unit test in the `tests` module still exercises this path, but cargo's
// dead-code lint considers only the non-test build of the bin target.
#[cfg(all(feature = "mcp", not(feature = "llm-anthropic")))]
#[allow(dead_code)]
fn build_anthropic_client(_model: Option<&str>) -> Result<Arc<dyn ModelClient>> {
    Err(anyhow!(
        "vendor 'anthropic' is not built into this binary; \
         rebuild with --features \"mcp llm-anthropic\""
    ))
}

#[cfg(all(feature = "mcp", feature = "llm-cohere"))]
fn build_cohere_client(model: Option<&str>) -> Result<Arc<dyn ModelClient>> {
    let c = match model {
        Some(m) => CohereClient::new().with_model(m),
        None => CohereClient::new(),
    };
    println!("node-run-llm: vendor=cohere model={}", c.model());
    Ok(Arc::new(c))
}

// `#[allow(dead_code)]`: same reason as build_anthropic_client above.
#[cfg(all(feature = "mcp", not(feature = "llm-cohere")))]
#[allow(dead_code)]
fn build_cohere_client(_model: Option<&str>) -> Result<Arc<dyn ModelClient>> {
    Err(anyhow!(
        "vendor 'cohere' is not built into this binary; \
         rebuild with --features \"mcp llm-cohere\""
    ))
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

/// Parse the `--vendor` value into a typed `Vendor`. The set is closed at
/// the CLI surface: unknown vendors are rejected here, but every accepted
/// variant is validated at runtime against the compiled-in features (see
/// `run_inner` / `build_*_client`).
fn parse_vendor(s: &str) -> Result<Vendor> {
    match s {
        "anthropic" => Ok(Vendor::Anthropic),
        "cohere" => Ok(Vendor::Cohere),
        other => Err(anyhow!(
            "unknown vendor `{other}` (expected `anthropic` or `cohere`)"
        )),
    }
}

/// Format a per-invocation subdirectory name from a UTC timestamp.
///
/// Format is `YYYY-MM-DDTHH-MM-SS-sssZ` — ISO-8601-ish with the time
/// component's colons replaced by dashes (filename-safe across shells
/// and most tools), plus three digits of millisecond precision so two
/// invocations within the same wall-clock second still produce
/// distinct names without a separate uniquifier. `chrono` substitutes
/// `%.3f` for fractional seconds; the literal dashes between H/M/S
/// come from the format string itself, not a post-process step.
fn subdir_name(now: DateTime<Utc>) -> String {
    now.format("%Y-%m-%dT%H-%M-%S-%3fZ").to_string()
}

/// Treat the supplied `parent` as a parent directory and create a
/// fresh timestamped subdirectory inside it for this invocation's
/// per-agent FS. Returns the absolute path of the new subdirectory.
///
/// * Creates `parent` (and any intermediates) if it doesn't already
///   exist — the parent's existence is not a precondition. This is
///   what makes `cargo run ... /tmp/coral-smoke-llm-mcp-fs` a
///   one-shot command rather than requiring `mkdir -p` first.
/// * Uses `create_dir` (not `create_dir_all`) on the *subdir itself*
///   so a name collision with an existing directory surfaces as
///   `AlreadyExists`; in that case appends a short ULID and retries.
///   Same-millisecond collisions are astronomically unlikely with the
///   format above but the fallback keeps the contract crisp.
/// * Canonicalizes the result so the path that lands on stdout is
///   absolute regardless of the input's shape.
#[allow(dead_code)]
fn resolve_fs_root(parent: &Path, now: DateTime<Utc>) -> Result<PathBuf> {
    fs::create_dir_all(parent)
        .with_context(|| format!("creating fs_root parent at {}", parent.display()))?;

    let base = subdir_name(now);
    let mut candidate = parent.join(&base);
    match fs::create_dir(&candidate) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
            // Collision: append a short ULID and retry once. ULID is a
            // crate dep already; ulid::Ulid::new() is monotonic-ish per
            // process but always unique. One retry is enough — a second
            // collision would require two ULIDs to match, which is the
            // 128-bit collision case.
            let suffix = ulid::Ulid::new();
            candidate = parent.join(format!("{base}-{suffix}"));
            fs::create_dir(&candidate).with_context(|| {
                format!(
                    "creating fs_root subdir at {} (post-collision)",
                    candidate.display()
                )
            })?;
        }
        Err(e) => {
            return Err(e)
                .with_context(|| format!("creating fs_root subdir at {}", candidate.display()));
        }
    }

    // Canonicalize so the printed path is absolute. `canonicalize` will
    // resolve symlinks but the subdir we just created is a plain dir,
    // so the only thing it normalizes is `.`-relative input.
    fs::canonicalize(&candidate)
        .with_context(|| format!("canonicalizing fs_root subdir {}", candidate.display()))
}

#[allow(dead_code)]
fn load_mandate(path: &Path) -> Result<Mandate> {
    let bytes =
        fs::read(path).with_context(|| format!("reading mandate from {}", path.display()))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing mandate JSON in {}", path.display()))
}

#[allow(dead_code)]
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

#[cfg(all(
    feature = "mcp",
    any(feature = "llm-anthropic", feature = "llm-cohere")
))]
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

#[allow(dead_code)]
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

#[allow(dead_code)]
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
    fn parse_args_accepts_cohere_vendor() {
        // Parse-only: --vendor cohere is now accepted at parse time. The
        // runtime feature check lives in `build_cohere_client`; this test
        // does not exercise that path.
        let parsed = parse_args(v(&[
            "--vendor",
            "cohere",
            "config.json",
            "triggers.jsonl",
            "/tmp/fs",
            "--",
            "npx",
            "-y",
            "@modelcontextprotocol/server-everything",
        ]))
        .expect("parse");
        assert_eq!(parsed.vendor, Vendor::Cohere);
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

    /// When the binary is built without the `mcp` feature, every run
    /// errors with a "rebuild with --features mcp" hint regardless of
    /// vendor. The CLI parse path still works; only `run_inner` rejects.
    #[tokio::test]
    #[cfg(not(feature = "mcp"))]
    async fn run_inner_without_mcp_errors_with_helpful_hint() {
        let args = Args {
            vendor: Vendor::Anthropic,
            model: None,
            max_tokens: DEFAULT_MAX_TOKENS,
            temperature: None,
            config: PathBuf::from("config.json"),
            triggers: PathBuf::from("triggers.jsonl"),
            fs_root: PathBuf::from("/tmp/fs"),
            spawn: vec!["npx".to_string()],
        };
        let err = run_inner(args)
            .await
            .expect_err("expected mcp-missing error");
        let msg = format!("{err:#}");
        assert!(msg.contains("mcp"), "msg: {msg}");
    }

    /// When the binary is built with `mcp` but without any vendor
    /// feature, `run_inner` is the no-vendor stub: every invocation
    /// errors with a "rebuild with --features ..." hint pointing at the
    /// vendor flags. Covers the CI feature-matrix entry `--features mcp`.
    #[tokio::test]
    #[cfg(all(
        feature = "mcp",
        not(any(feature = "llm-anthropic", feature = "llm-cohere"))
    ))]
    async fn run_inner_mcp_without_vendor_errors_with_helpful_hint() {
        let args = Args {
            vendor: Vendor::Anthropic,
            model: None,
            max_tokens: DEFAULT_MAX_TOKENS,
            temperature: None,
            config: PathBuf::from("config.json"),
            triggers: PathBuf::from("triggers.jsonl"),
            fs_root: PathBuf::from("/tmp/fs"),
            spawn: vec!["npx".to_string()],
        };
        let err = run_inner(args)
            .await
            .expect_err("expected vendor-missing error");
        let msg = format!("{err:#}");
        assert!(msg.contains("llm-anthropic"), "msg: {msg}");
        assert!(msg.contains("llm-cohere"), "msg: {msg}");
    }

    /// When the binary is built with `mcp` but without `llm-anthropic`,
    /// `--vendor anthropic` errors with a "rebuild with --features
    /// llm-anthropic" hint at the build-client step. Tests the runtime
    /// feature check path that model-call also uses.
    ///
    /// `Arc<dyn ModelClient>` does not impl `Debug`, so we can't use
    /// `expect_err` — match the result by hand instead.
    #[test]
    #[cfg(all(feature = "mcp", not(feature = "llm-anthropic")))]
    fn build_anthropic_without_feature_errors_with_helpful_hint() {
        let msg = match build_anthropic_client(None) {
            Ok(_) => panic!("expected llm-anthropic-missing error"),
            Err(e) => format!("{e:#}"),
        };
        assert!(msg.contains("llm-anthropic"), "msg: {msg}");
    }

    /// Cohere counterpart of the anthropic feature-missing test above.
    #[test]
    #[cfg(all(feature = "mcp", not(feature = "llm-cohere")))]
    fn build_cohere_without_feature_errors_with_helpful_hint() {
        let msg = match build_cohere_client(None) {
            Ok(_) => panic!("expected llm-cohere-missing error"),
            Err(e) => format!("{e:#}"),
        };
        assert!(msg.contains("llm-cohere"), "msg: {msg}");
    }

    /// Given a fixed `DateTime<Utc>`, `subdir_name` produces the
    /// documented `YYYY-MM-DDTHH-MM-SS-sssZ` shape with dashes (not
    /// colons) in the time component, three digits of millisecond
    /// precision, and a trailing `Z`. The literal dashes come from the
    /// chrono format string itself; no post-process substitution.
    #[test]
    fn subdir_name_formats_with_dashes_and_milliseconds() {
        use chrono::{TimeZone, Timelike};
        let dt = Utc
            .with_ymd_and_hms(2026, 5, 20, 4, 30, 7)
            .unwrap()
            .with_nanosecond(123_000_000)
            .unwrap();
        let s = subdir_name(dt);
        assert_eq!(s, "2026-05-20T04-30-07-123Z");
        assert!(!s.contains(':'), "expected colons replaced by dashes: {s}");
    }

    /// Two timestamps a millisecond apart produce distinct subdir
    /// names — the back-stop against successive invocations clobbering
    /// each other's FS root.
    #[test]
    fn subdir_name_distinguishes_millisecond_neighbors() {
        use chrono::{TimeZone, Timelike};
        let base = Utc.with_ymd_and_hms(2026, 5, 20, 4, 30, 7).unwrap();
        let a = base.with_nanosecond(123_000_000).unwrap();
        let b = base.with_nanosecond(124_000_000).unwrap();
        assert_ne!(subdir_name(a), subdir_name(b));
    }

    /// `resolve_fs_root` must `create_dir_all` the parent on its own —
    /// the binary should be runnable against a path that doesn't exist
    /// yet without the user having to `mkdir -p` first.
    #[test]
    fn resolve_fs_root_creates_missing_parent() {
        let tmp = tempfile::TempDir::new().expect("tempdir");
        // Parent path that does not exist yet (nested under the tempdir
        // root, which is the only thing on disk).
        let parent = tmp.path().join("nested").join("does-not-exist-yet");
        assert!(!parent.exists());
        let now = Utc::now();
        let resolved = resolve_fs_root(&parent, now).expect("resolve_fs_root");
        assert!(parent.exists(), "parent should have been created");
        assert!(resolved.exists(), "subdir should have been created");
        assert!(
            resolved.starts_with(parent.canonicalize().unwrap()),
            "resolved {} should sit under parent {}",
            resolved.display(),
            parent.display()
        );
        // Resolved name should match the subdir_name(now) format.
        let name = resolved
            .file_name()
            .and_then(|s| s.to_str())
            .expect("subdir name");
        // Loose shape check — the strict format is covered by
        // `subdir_name_formats_with_dashes_and_milliseconds`.
        assert!(name.ends_with('Z'), "subdir name should end with Z: {name}");
        assert!(
            !name.contains(':'),
            "subdir name must not contain colons: {name}"
        );
    }

    /// Calling `resolve_fs_root` twice against the same parent — even
    /// with the same `now` — produces distinct subdirs. The first call
    /// gets the bare timestamp; the second collides on `AlreadyExists`
    /// and falls through to the ULID-suffixed retry. This covers the
    /// pathological same-millisecond case the helper guards against.
    #[test]
    fn resolve_fs_root_avoids_collision_on_repeat_now() {
        use chrono::TimeZone;
        let tmp = tempfile::TempDir::new().expect("tempdir");
        let fixed_now = Utc.with_ymd_and_hms(2026, 5, 20, 4, 30, 7).unwrap();
        let a = resolve_fs_root(tmp.path(), fixed_now).expect("first resolve");
        let b = resolve_fs_root(tmp.path(), fixed_now).expect("second resolve");
        assert_ne!(a, b, "same `now` should still yield distinct subdirs");
    }
}
