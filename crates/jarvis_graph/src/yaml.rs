//! `graph.yaml` schema types + parser + validator (Stage 4.1, JAR2-72).
//!
//! Operator-facing surface for "the graph is the program" (VISION.md § 4).
//! Source of truth for the v1 strawman: `scratch/graph_yaml_schema.md` § 2.
//! Scope narrowings for v1 are locked in <JAR2-71>; this module enforces
//! them at parse + validate time so the downstream `jarvis apply` binary
//! (JAR2-73) can rely on the typed value being well-formed.
//!
//! ## Public surface
//!
//! - [`parse_graph_yaml`] — `&str` → [`GraphYaml`], with source-located
//!   errors (`line:col`) via `serde_yaml::Error::location()`.
//! - [`validate`] — enforces the v1 narrowings the type system cannot
//!   reject on its own (single agent, `kind: builtin` only,
//!   `apiVersion` + `kind` exact-match, URL-path-safe names, tool-ref
//!   resolution).
//! - [`GraphYamlError`] — the unified error surface. JAR2-73 reads from
//!   this; today's `Display` impl is also CLI-suitable.
//!
//! ## What this module deliberately does NOT do
//!
//! - DB writes / Temporal workflow instantiation — JAR2-73's binary.
//! - Conversion into `jarvis_node::Mandate` / structural-DB rows —
//!   JAR2-73; intentionally not coupled to `jarvis_node` here (the
//!   workspace dep graph stays clean).
//! - Multi-agent topology, `defaults:`, `policy:`, `scripted_decisions:`,
//!   `mandate.from_file:`, `kind: mcp` — all explicitly rejected in
//!   [`validate`]; deferred to later stages per <JAR2-71>.
//!
//! ## `kind: mcp` rejection — explicit parse + validator branch
//!
//! `Tool` is `#[serde(tag = "kind", rename_all = "snake_case")]` with
//! both `Builtin` and `Mcp` variants present even though `Mcp` is rejected
//! at validation. This lets us emit the targeted error message
//! ("MCP-in-worker is JAR2-63's flagged follow-up") instead of serde's
//! generic "unknown variant `mcp`" — the ticket's parent makes that hint
//! a requirement, not a nice-to-have.
//!
//! ## Unknown fields = hard error
//!
//! Every struct uses `#[serde(deny_unknown_fields)]`. This is the
//! lowest-cost way to reject `children:`, `defaults:`, `policy:`,
//! `scripted_decisions:`, `mandate.from_file:` with serde_yaml-supplied
//! `line:col` and a "unknown field … expected one of …" message —
//! without polluting the structs with placeholder fields just to flag
//! them.
//!
//! ## Names: strict validate, do not normalize
//!
//! `metadata.name` and `agents[0].id` must already match
//! `^[a-z0-9][a-z0-9-]*[a-z0-9]$` (or be a single `[a-z0-9]` char). We
//! reject uppercase rather than lowercasing on read — gives JAR2-73 a
//! single canonical representation to derive the workflow ID
//! (`graphs/<metadata.name>/agents/<agents[0].id>`) from verbatim, and
//! avoids the round-trip ambiguity ("what does `metadata.name: Foo`
//! become?") that normalization would introduce.

use jarvis_node::agent_ref::{AgentId, GraphId};
use jarvis_node::mandate::Mandate as NodeMandate;
use jarvis_node::trigger::Trigger as NodeTrigger;
use jarvis_temporal::workflow::{agent_workflow_id, AgentConfig, AgentInput, FsHandle};
use schemars::JsonSchema;
use serde::Deserialize;
use std::time::Duration;
use uuid::Uuid;

/// The exact `apiVersion` literal v1 accepts. Bump only when the schema
/// breaks.
pub const API_VERSION: &str = "jarvis.engine/v1alpha1";

/// The exact `kind` literal v1 accepts.
pub const KIND: &str = "Graph";

/// Top-level document type for `graph.yaml`. Read
/// `scratch/graph_yaml_schema.md` § 2 for the strawman this mirrors.
///
/// `seed` is optional in the strawman but required at apply time today —
/// the workflow's first tick would otherwise drain an empty trigger queue
/// and send the LLM an empty prompt. Validation enforces non-empty
/// `seed.triggers`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GraphYaml {
    /// Must be `"jarvis.engine/v1alpha1"`. Validated in [`validate`].
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    /// Must be `"Graph"`. Validated in [`validate`].
    pub kind: String,
    pub metadata: Metadata,
    pub tools: Vec<Tool>,
    pub agents: Vec<Agent>,
    pub seed: Seed,
}

/// `metadata:` block — identity + free-form description.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Metadata {
    /// URL-path-safe name (`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`). Strict-
    /// validated in [`validate`]; not lowercased.
    pub name: String,
    /// Free-form text. Tolerates `|` block scalars per the strawman.
    #[serde(default)]
    pub description: Option<String>,
}

/// A tool registration. `kind` is the discriminant; `Builtin` is the
/// only variant accepted at validate-time today. `Mcp` is parseable so
/// we can reject it with a targeted error message.
///
/// Intentionally **no** `#[serde(deny_unknown_fields)]` here — the
/// `#[serde(flatten)]` + `deny_unknown_fields` combo on a parent struct
/// is incompatible (serde flattens via a `MapAccess` adapter that
/// `deny_unknown_fields` cannot see through). The inner `ToolKind`
/// enum carries `deny_unknown_fields` per-variant, which is the actual
/// guard we want; the outer struct only carries `id`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema)]
pub struct Tool {
    /// Reference id used by `agents[].tools`. URL-path-safe (same regex
    /// as `metadata.name` — checked in [`validate`]).
    pub id: String,
    #[serde(flatten)]
    pub kind: ToolKind,
}

/// Discriminated `kind:` for [`Tool`]. `Mcp` is rejected at validate-
/// time per <JAR2-71> ("MCP-in-worker is JAR2-63's flagged follow-up").
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolKind {
    /// Tool registered against the in-process bootstrap `ToolRegistry`
    /// (today: `EchoTool`). The `builtin` field names which one (e.g.
    /// `echo`).
    Builtin {
        /// Identifier of the in-process tool to register (`"echo"` etc).
        builtin: String,
    },
    /// MCP server. Parseable so [`validate`] can emit the targeted
    /// "JAR2-63 follow-up" hint; not allowed in v1.
    Mcp {
        /// Command to spawn for the MCP server (e.g. `"mcp-web-search"`).
        command: String,
        #[serde(default)]
        args: Vec<String>,
    },
}

/// `agents[i]`. v1 requires exactly one entry — enforced in [`validate`].
///
/// `children`, `defaults`, and any other multi-agent / inheritance
/// constructs are rejected by `#[serde(deny_unknown_fields)]` with a
/// `line:col`-bearing serde error.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Agent {
    /// URL-path-safe id (`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`); strict-
    /// validated in [`validate`]. Combined with `metadata.name` to form
    /// the workflow ID JAR2-73 will pass to the Temporal client:
    /// `graphs/<metadata.name>/agents/<id>`.
    pub id: String,
    pub mandate: Mandate,
    /// References by id into the top-level `tools:`. Resolved + checked
    /// for misses in [`validate`].
    pub tools: Vec<String>,
}

/// `agents[].mandate:` — the standing instruction. **Not** the same wire
/// shape as `jarvis_node::mandate::Mandate` (which is ms-integers +
/// `retry_policy` + `context_policy`); kept distinct because the YAML is
/// the authored / human-edited surface and `jarvis_node::Mandate` is the
/// runtime/wire shape. Conversion lives in JAR2-73.
///
/// `from_file:` (`scratch/graph_yaml_schema.md` § 4.5) is rejected by
/// `deny_unknown_fields`; v1 is inline-only.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Mandate {
    /// Free-form mandate text. YAML block scalars (`|`) are fine.
    pub text: String,
    /// Wake cadence when no signal arrives. Accepts the
    /// [`humantime`](https://docs.rs/humantime) duration grammar:
    /// `100ms`, `5m`, `1h30m`, `1h 30m`, etc. Parsed via
    /// `humantime::parse_duration`. Empty / malformed values surface as
    /// [`GraphYamlError::Parse`] (the adapter raises a serde error
    /// mid-deserialize so `serde_yaml::Location` pins the `line:col`).
    #[serde(deserialize_with = "deserialize_duration")]
    #[schemars(with = "String")]
    pub idle_period: Duration,
    /// Optional safety cap on loop iterations. `None` ⇒ run until
    /// `Retire`. Mirrors `jarvis_node::mandate::Mandate::max_ticks`.
    #[serde(default)]
    pub max_ticks: Option<u64>,
}

/// `seed:` — what kicks the graph off. v1 requires `triggers` to be
/// non-empty (see [`GraphYaml`] doc for the empty-prompt guardrail).
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Seed {
    pub triggers: Vec<SeedTrigger>,
}

/// One row under `seed.triggers:`. Mirrors the strawman § 2 shape:
/// addressed to an agent, fired `at: start`, carrying an `external:`
/// envelope. `scripted_decisions:` is rejected by `deny_unknown_fields`
/// on [`Seed`] — vanished with real `Decide` per <JAR2-71>.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SeedTrigger {
    /// Target agent id. Validated against `agents[].id` in [`validate`].
    pub agent: String,
    /// When to fire. Today only `start` is accepted; future stages may
    /// add `every: <duration>`, `at: <iso8601>`, etc. v1 enforces the
    /// literal in [`validate`].
    pub at: String,
    /// External-signal payload. Mirror of `jarvis_node::trigger::Trigger::External`.
    pub external: ExternalEnvelope,
}

/// Payload of `seed.triggers[].external:`. The runtime translates this
/// into `Trigger::External { kind, payload }` at apply time (JAR2-73).
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExternalEnvelope {
    pub kind: String,
    /// Opaque JSON. Mirrors `Trigger::External::payload`'s shape.
    #[serde(default)]
    #[schemars(with = "serde_json::Value")]
    pub payload: serde_json::Value,
}

// --- duration deserializer -----------------------------------------------

/// Serde adapter that defers to `humantime::parse_duration` and surfaces
/// the source string in the error. The struct-level `serde` error this
/// produces still carries a `serde_yaml::Location` so [`parse_graph_yaml`]
/// can pin it to `line:col`.
fn deserialize_duration<'de, D>(de: D) -> Result<Duration, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(de)?;
    humantime::parse_duration(&s)
        .map_err(|e| serde::de::Error::custom(format!("invalid duration {s:?}: {e}")))
}

// --- error type ---------------------------------------------------------

/// 1-indexed `(line, column)` pair into the original YAML source. Used
/// by validation variants that the parser can locate via a small
/// source-text scan (today: tool-reference miss). `serde_yaml::Location`
/// is the equivalent for parse errors and is preserved via
/// `GraphYamlError::Parse`'s wrapping.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Location {
    pub line: u32,
    pub column: u32,
}

impl std::fmt::Display for Location {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.line, self.column)
    }
}

/// Unified error surface for [`parse_graph_yaml`] + [`validate`].
///
/// `Parse` wraps `serde_yaml::Error` directly so JAR2-73 can pretty-
/// print using `.location()` if desired; the `Display` impl below
/// already prepends `line:col` when one is available, which is the
/// common case operators want to see.
///
/// Validation variants do not carry locations by default — [`validate`]
/// is pure over a typed value with no source access. [`parse_and_validate`]
/// is the convenience wrapper that runs both and enriches the
/// `UnknownToolReference` variant (the one the JAR2-72 acceptance bar
/// names) with a source-scanned `Location`. JAR2-73 can call it directly
/// for CLI-grade error messages.
#[derive(Debug, thiserror::Error)]
pub enum GraphYamlError {
    /// `serde_yaml` failed to deserialize the document — structural
    /// mismatch, missing required field, unknown field
    /// (`deny_unknown_fields` rejection), or duration adapter failure.
    /// `serde_yaml::Error::location()` may carry `line:col`.
    #[error("{}", format_parse_error(.0))]
    Parse(#[from] serde_yaml::Error),

    /// `apiVersion` did not match [`API_VERSION`].
    #[error(
        "unsupported apiVersion {actual:?}: expected {expected:?} (this is v1 of the graph YAML schema; bumping requires a coordinated change)",
        expected = API_VERSION,
    )]
    UnsupportedApiVersion { actual: String },

    /// `kind` did not match [`KIND`].
    #[error(
        "unsupported kind {actual:?}: expected {expected:?}",
        expected = KIND,
    )]
    UnsupportedKind { actual: String },

    /// `agents:` did not contain exactly one entry. v1 is single-agent
    /// only (multi-agent topology is Stage 5).
    #[error(
        "expected exactly 1 entry under `agents:`, got {actual} (multi-agent topology is Stage 5; this is JAR2-71's v1 narrowing)"
    )]
    WrongAgentCount { actual: usize },

    /// A `tools[]` entry was `kind: mcp`. v1 builds register only
    /// `EchoTool`; MCP-in-worker is queued.
    #[error(
        "tool {tool_id:?} has `kind: mcp`, which is not supported in v1 (MCP-in-worker is JAR2-63's flagged follow-up; remove the tool or wait for that ticket)"
    )]
    McpToolRejected { tool_id: String },

    /// `metadata.name` or `agents[].id` did not match the URL-path-safe
    /// regex `^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`.
    #[error(
        "name {value:?} at {field} is not URL-path-safe: must match `^[a-z0-9]([a-z0-9-]*[a-z0-9])?$` (lowercase alphanumerics + `-`, must start/end with alphanumeric)"
    )]
    InvalidName { field: &'static str, value: String },

    /// `agents[0].tools` referenced an id missing from the top-level
    /// `tools:` list. `location` is populated by [`parse_and_validate`]
    /// (which has the source text) via a small source scan; pure
    /// [`validate`] leaves it `None`.
    #[error(
        "{loc_prefix}agent {agent_id:?} references tool id {tool_id:?} which is not defined under top-level `tools:` (define it, or remove the reference)",
        loc_prefix = location.map(|l| format!("{l}: ")).unwrap_or_default(),
    )]
    UnknownToolReference {
        agent_id: String,
        tool_id: String,
        /// Filled in by [`parse_and_validate`]; `None` when produced by
        /// pure [`validate`].
        location: Option<Location>,
    },

    /// `seed.triggers[].agent` referenced an id no agent declares.
    #[error(
        "seed trigger targets agent {agent_id:?} which is not declared under `agents:` (no such agent will receive this trigger)"
    )]
    UnknownTriggerAgent { agent_id: String },

    /// `seed.triggers[].at` was not the literal `"start"`. v1 supports
    /// only kickoff-at-apply; future stages may add `every: <duration>`.
    #[error(
        "seed trigger has `at: {actual:?}`; v1 supports only `at: \"start\"` (timed seeds are deferred)"
    )]
    UnsupportedTriggerAt { actual: String },

    /// `seed.triggers:` was empty. The workflow's first tick would drain
    /// an empty queue and send the LLM a zero-length prompt — JAR2-68's
    /// `triggers.jsonl` loader has the same guardrail.
    #[error(
        "seed.triggers is empty; at least one initial trigger is required (the workflow's first tick would otherwise drain an empty queue and send the LLM an empty prompt)"
    )]
    EmptySeedTriggers,

    /// Top-level `tools:` had duplicate `id` entries.
    #[error("duplicate tool id {tool_id:?} in top-level `tools:` (ids must be unique)")]
    DuplicateToolId { tool_id: String },
}

/// Render a `serde_yaml::Error` with the source location prefixed when
/// available. `serde_yaml::Error`'s own `Display` already includes the
/// location in most cases, but the format is `at line N column M`; we
/// surface it as `line:col` up front so CLI output matches the convention
/// `cargo` / `rustc` use.
fn format_parse_error(e: &serde_yaml::Error) -> String {
    match e.location() {
        Some(loc) => format!("{}:{}: {e}", loc.line(), loc.column()),
        None => format!("{e}"),
    }
}

// --- parser -------------------------------------------------------------

/// Parse a `graph.yaml` document into a [`GraphYaml`]. The returned
/// error preserves `serde_yaml::Error::location()` so the `Display`
/// impl can prefix `line:col`. Validation is **separate** — call
/// [`validate`] before consuming the value.
///
/// `apiVersion` / `kind` exact-match lives in [`validate`] rather than
/// here so that a typo in either field still parses to a typed value
/// (giving the validator the chance to emit a descriptive error
/// referring to both fields' actual contents).
pub fn parse_graph_yaml(text: &str) -> Result<GraphYaml, GraphYamlError> {
    serde_yaml::from_str(text).map_err(GraphYamlError::from)
}

// --- validator ----------------------------------------------------------

/// URL-path-safe name regex, expressed as a hand-rolled check so the
/// crate doesn't take a `regex` dep just for one validation. Mirrors
/// `^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`.
fn is_url_path_safe_name(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    let bytes = s.as_bytes();
    let first = bytes[0];
    let last = bytes[bytes.len() - 1];
    let is_alnum = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit();
    if !is_alnum(first) || !is_alnum(last) {
        return false;
    }
    bytes.iter().all(|&b| is_alnum(b) || b == b'-')
}

/// Enforce the v1 narrowings the type system cannot. Run **after**
/// [`parse_graph_yaml`] and before handing the value to anything that
/// expects valid invariants (DB writes, workflow start).
///
/// Order of checks is chosen so the most operator-actionable errors
/// surface first: apiVersion / kind mismatch (the document is the wrong
/// kind entirely) → single-agent → name shape → tool registry shape →
/// references.
pub fn validate(g: &GraphYaml) -> Result<(), GraphYamlError> {
    if g.api_version != API_VERSION {
        return Err(GraphYamlError::UnsupportedApiVersion {
            actual: g.api_version.clone(),
        });
    }
    if g.kind != KIND {
        return Err(GraphYamlError::UnsupportedKind {
            actual: g.kind.clone(),
        });
    }

    if !is_url_path_safe_name(&g.metadata.name) {
        return Err(GraphYamlError::InvalidName {
            field: "metadata.name",
            value: g.metadata.name.clone(),
        });
    }

    if g.agents.len() != 1 {
        return Err(GraphYamlError::WrongAgentCount {
            actual: g.agents.len(),
        });
    }
    let agent = &g.agents[0];
    if !is_url_path_safe_name(&agent.id) {
        return Err(GraphYamlError::InvalidName {
            field: "agents[0].id",
            value: agent.id.clone(),
        });
    }

    // Tool-list shape: reject MCP, dedupe ids, sanity-check builtin id.
    let mut seen = std::collections::HashSet::new();
    for tool in &g.tools {
        if !seen.insert(tool.id.as_str()) {
            return Err(GraphYamlError::DuplicateToolId {
                tool_id: tool.id.clone(),
            });
        }
        match &tool.kind {
            ToolKind::Mcp { .. } => {
                return Err(GraphYamlError::McpToolRejected {
                    tool_id: tool.id.clone(),
                });
            }
            ToolKind::Builtin { .. } => {}
        }
    }

    // Tool-reference resolution: every id under `agents[0].tools` must
    // exist in top-level `tools[]`.
    let registered: std::collections::HashSet<&str> =
        g.tools.iter().map(|t| t.id.as_str()).collect();
    for tool_id in &agent.tools {
        if !registered.contains(tool_id.as_str()) {
            return Err(GraphYamlError::UnknownToolReference {
                agent_id: agent.id.clone(),
                tool_id: tool_id.clone(),
                location: None,
            });
        }
    }

    // Seeds: non-empty, target the declared agent, `at: start` only.
    if g.seed.triggers.is_empty() {
        return Err(GraphYamlError::EmptySeedTriggers);
    }
    let declared_agents: std::collections::HashSet<&str> =
        g.agents.iter().map(|a| a.id.as_str()).collect();
    for trigger in &g.seed.triggers {
        if !declared_agents.contains(trigger.agent.as_str()) {
            return Err(GraphYamlError::UnknownTriggerAgent {
                agent_id: trigger.agent.clone(),
            });
        }
        if trigger.at != "start" {
            return Err(GraphYamlError::UnsupportedTriggerAt {
                actual: trigger.at.clone(),
            });
        }
    }

    Ok(())
}

/// Parse + validate in one shot, with `line:col` enrichment for the
/// errors validation produces from a typed value (today:
/// [`GraphYamlError::UnknownToolReference`]). JAR2-73's `jarvis apply`
/// is the intended caller — operator-facing CLI output gets `line:col`
/// for tool-reference misses without the binary having to thread the
/// source text through itself.
///
/// Pure [`validate`] and pure [`parse_graph_yaml`] remain available for
/// callers that already have the parsed value (e.g. unit tests, or
/// future round-trip serialization paths) and don't want the source-
/// scan overhead.
pub fn parse_and_validate(text: &str) -> Result<GraphYaml, GraphYamlError> {
    let g = parse_graph_yaml(text)?;
    match validate(&g) {
        Ok(()) => Ok(g),
        Err(mut e) => {
            enrich_with_source(&mut e, text);
            Err(e)
        }
    }
}

/// Source-scan enrichment for validator errors. Currently only fills in
/// `UnknownToolReference::location`; other variants are left as-is
/// (their messages already pin the offender by name). Extend per
/// follow-up tickets that surface a need.
fn enrich_with_source(err: &mut GraphYamlError, source: &str) {
    if let GraphYamlError::UnknownToolReference {
        agent_id,
        tool_id,
        location,
    } = err
    {
        *location = locate_agent_tool_reference(source, agent_id, tool_id);
    }
}

/// Find the `line:col` of the missing `tool_id` token within the named
/// agent's `tools:` list. Heuristic: anchor on `id: <agent_id>`, walk
/// forward to the next `tools:` line under that agent, then scan that
/// line and any continuation lines for the bare token. Bails to `None`
/// rather than guessing if the structure doesn't match expectations.
///
/// Sufficient for v1's single-agent shape; multi-agent topology
/// (Stage 5) will need a more disciplined locator (likely operating on
/// a serde_yaml AST). Kept here as a private helper so JAR2-73's binary
/// doesn't have to know the heuristic.
fn locate_agent_tool_reference(source: &str, agent_id: &str, tool_id: &str) -> Option<Location> {
    // Find the line containing the agent's `id: <agent_id>`. We accept
    // any indent (the agent record may live under `agents:` or
    // `agents[]` flow-style; v1 only uses the block style).
    let agent_anchor = format!("id: {agent_id}");
    let mut lines = source.lines().enumerate();
    let mut after_agent = false;
    for (idx, line) in lines.by_ref() {
        if line.contains(&agent_anchor) {
            after_agent = true;
            // Don't break — fall through to the next loop body below
            // by continuing to scan from the next line.
            let _ = idx;
            break;
        }
    }
    if !after_agent {
        return None;
    }

    // Walk forward to find the agent's `tools:` line.
    for (line_no, line) in lines.by_ref() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("tools:") {
            // The agent's `tools:` line itself may be either flow-style
            // (`tools: [a, b, c]`) or block-style with following items.
            // Scan the rest of this line first.
            if let Some(loc) = locate_token_in_line(line, line_no, tool_id) {
                return Some(loc);
            }
            // Block-style continuation: scan subsequent indented lines
            // until indent drops back to the agent's level or a new key
            // sibling appears.
            for (cont_no, cont_line) in lines.by_ref() {
                let ct = cont_line.trim_start();
                if ct.is_empty() {
                    continue;
                }
                // Block-style tool list entries start with `- `. Once we
                // see a line that isn't a list continuation or whose
                // indent matches the agent's `tools:` key, give up.
                if !cont_line.starts_with(' ') && !cont_line.starts_with('\t') {
                    return None;
                }
                if let Some(loc) = locate_token_in_line(cont_line, cont_no, tool_id) {
                    return Some(loc);
                }
                // If this line looks like another agent-level key
                // (`mandate:`, etc.) at the same indent as `tools:`,
                // stop scanning — we've left the tools list.
                if !ct.starts_with('-')
                    && !ct.starts_with(',')
                    && !ct.starts_with('[')
                    && !ct.starts_with(']')
                    && ct.contains(':')
                {
                    return None;
                }
            }
            return None;
        }
    }
    None
}

/// Find the column of a bare `tool_id` token in a single line of YAML.
/// "Bare token" = the id is bounded on each side by characters that are
/// not letters / digits / `-` (the URL-path-safe alphabet). Returns
/// 1-indexed `(line_no_plus_one, col_plus_one)`.
fn locate_token_in_line(
    line: &str,
    line_no_zero_indexed: usize,
    tool_id: &str,
) -> Option<Location> {
    if tool_id.is_empty() {
        return None;
    }
    let bytes = line.as_bytes();
    let needle = tool_id.as_bytes();
    let is_id_char = |b: u8| b.is_ascii_alphanumeric() || b == b'-' || b == b'_';
    let mut i = 0;
    while i + needle.len() <= bytes.len() {
        if &bytes[i..i + needle.len()] == needle {
            let left_ok = i == 0 || !is_id_char(bytes[i - 1]);
            let right_ok = i + needle.len() == bytes.len() || !is_id_char(bytes[i + needle.len()]);
            if left_ok && right_ok {
                return Some(Location {
                    line: (line_no_zero_indexed + 1) as u32,
                    column: (i + 1) as u32,
                });
            }
        }
        i += 1;
    }
    None
}

// --- YAML → workflow input conversion (Stage 4.2, JAR2-73) -------------

/// Build the [`AgentInput`] the `AgentWorkflow` consumes from a validated
/// [`GraphYaml`]. Hermetic — no DB, no Temporal client, no filesystem.
///
/// Caller-side invariants:
///
/// - The graph must already have passed [`validate`] (or
///   [`parse_and_validate`]); this function unwraps the v1 "exactly one
///   agent" guarantee via `agents[0]` and would panic on an empty agents
///   vector otherwise. Treat it as a typed-witness consumer of the
///   validator.
/// - The returned `AgentInput.fs_handle.prefix` is
///   [`agent_workflow_id`]`(metadata.name, agents[0].id)` —
///   `graphs/<metadata.name>/agents/<agents[0].id>`. The worker daemon's
///   [`jarvis_node::storage::LocalStorage`] is rooted at its configured
///   `AGENT_FS_ROOT` and is shared process-wide across every workflow
///   the daemon hosts, so per-workflow namespacing has to come from the
///   prefix; otherwise tick-keyed artifacts (`decisions/<tick>.jsonl`)
///   and single-file ones (`mandate.json`, `retirement.json`) collide
///   across distinct graphs landing on the same daemon. The chosen
///   prefix mirrors the workflow ID so operators inspecting on-disk
///   artifacts can map back to `temporal workflow describe …` by eye.
///
/// ## Field mapping
///
/// | YAML | `AgentInput` |
/// |---|---|
/// | `agents[0].mandate.{text, idle_period, max_ticks}` | `mandate` via [`NodeMandate::new`] |
/// | `seed.triggers[*].external.{kind, payload}` | **not** in `AgentInput` — the binary signals them post-`start_workflow` via [`yaml_seed_triggers`] |
/// | `metadata.name`, `agents[0].id` | **not** in `AgentInput` — they form the workflow ID (`agent_workflow_id`), passed separately by the binary |
///
/// `cfg` defaults to [`AgentConfig::default`] (the JAR2-58 placeholder),
/// `parent_handle` defaults to `None` (no parent — single-agent v1),
/// `carryover` defaults to `None` (a fresh apply is a first run, not a
/// post-CAN hydrate).
///
/// `NodeMandate::new` synthesizes the `retry_policy: None` +
/// `context_policy: ContextPolicy::default()` defaults; v1 YAML does not
/// surface either knob (out of scope per parent JAR2-71).
pub fn into_agent_input(graph: &GraphYaml) -> AgentInput {
    debug_assert_eq!(
        graph.agents.len(),
        1,
        "into_agent_input requires a validated single-agent graph; \
         call parse_and_validate or validate before this conversion",
    );
    let agent = &graph.agents[0];
    let mandate = NodeMandate::new(
        agent.mandate.text.clone(),
        agent.mandate.idle_period,
        agent.mandate.max_ticks,
    );
    // JAR2-80 (Stage 5 Project decision 8): `AgentInput` now requires
    // structural identity fields (`graph_id` + `agent_id`). v1's YAML
    // is single-agent and pre-DB; the structural-DB resolver that
    // pulls real UUIDs lives on the `jarvis-apply` binary side, not
    // here. Mint synthetic v4 UUIDs as a stand-in so the workflow
    // body has something to thread through `Decision::SpawnChild`'s
    // activity input. Note: these are NOT the same as the
    // `GraphStore::create_from_yaml` UUIDs (the binary doesn't yet
    // wire those back into `AgentInput`); a future cleanup ticket
    // (deferred — see PR coordination note) should align them so
    // `Decision::SpawnChild` writes edges keyed off the same root
    // agent the structural-DB row identifies.
    let graph_id = GraphId::new(Uuid::new_v4());
    let agent_id = AgentId::new(Uuid::new_v4());
    AgentInput {
        cfg: AgentConfig::default(),
        fs_handle: FsHandle {
            prefix: agent_workflow_id(&graph.metadata.name, &agent.id),
        },
        parent_handle: None,
        carryover: None,
        mandate,
        graph_id,
        agent_id,
        agent_name: agent.id.clone(),
    }
}

/// Translate the YAML's `seed.triggers` into the
/// [`jarvis_node::trigger::Trigger::External`] values the `jarvis-apply`
/// binary will `handle.signal(AgentWorkflow::external_signal, ...)` to
/// the workflow.
///
/// In v1 every seed trigger targets `agents[0].id` (the validator
/// rejects others), so this function does not return the destination
/// agent — the binary signals against the single workflow handle it
/// started. Stage 5+ multi-agent will need the destination back; the
/// shape is reserved by `SeedTrigger.agent` already.
///
/// Order is preserved from the YAML — the binary signals them in the
/// same order the operator wrote them.
pub fn yaml_seed_triggers(graph: &GraphYaml) -> Vec<NodeTrigger> {
    graph
        .seed
        .triggers
        .iter()
        .map(|seed| NodeTrigger::External {
            kind: seed.external.kind.clone(),
            payload: seed.external.payload.clone(),
        })
        .collect()
}

// --- tests --------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// The canonical v1 happy-path fixture. Mirrors today's
    /// `examples/smoke_llm_temporal/config.json` + `triggers.jsonl`
    /// content into one document; JAR2-74 lands the actual on-disk
    /// fixture under `examples/`. Kept inline here so this ticket's
    /// scope stays "schema + parser + validator" without a fixture
    /// file edit.
    const HAPPY_YAML: &str = r#"
apiVersion: jarvis.engine/v1alpha1
kind: Graph
metadata:
  name: smoke
  description: |
    Smoke fixture for the v1 graph.yaml — workflow-driven echo run.
tools:
  - id: echo
    kind: builtin
    builtin: echo
agents:
  - id: root
    mandate:
      text: |
        Your task: call the `echo` tool exactly once with arguments {"msg": "hello from temporal"},
        then on the next tick emit an Output via the `emit_output` decision whose `content` is a short
        summary citing the resulting evidence id, then retire. Do not call any other tool; do not loop;
        do not idle except as a last resort.
      idle_period: 1s
      max_ticks: 8
    tools: [echo]
seed:
  triggers:
    - agent: root
      at: start
      external:
        kind: kickoff
        payload: {}
"#;

    fn parse_err(text: &str) -> GraphYamlError {
        parse_graph_yaml(text).expect_err("parse expected to fail")
    }

    fn validate_err(text: &str) -> GraphYamlError {
        let g = parse_graph_yaml(text).expect("parse should succeed for validator-rejection tests");
        validate(&g).expect_err("validate expected to fail")
    }

    // --- Happy path ----------------------------------------------------

    #[test]
    fn parses_and_validates_canonical_v1_fixture() {
        let g = parse_and_validate(HAPPY_YAML).expect("happy path");
        assert_eq!(g.api_version, API_VERSION);
        assert_eq!(g.kind, KIND);
        assert_eq!(g.metadata.name, "smoke");
        assert_eq!(g.tools.len(), 1);
        assert_eq!(g.tools[0].id, "echo");
        assert!(matches!(g.tools[0].kind, ToolKind::Builtin { ref builtin } if builtin == "echo"));
        assert_eq!(g.agents.len(), 1);
        let agent = &g.agents[0];
        assert_eq!(agent.id, "root");
        assert_eq!(agent.tools, vec!["echo".to_string()]);
        assert_eq!(agent.mandate.idle_period, Duration::from_secs(1));
        assert_eq!(agent.mandate.max_ticks, Some(8));
        assert_eq!(g.seed.triggers.len(), 1);
        let trigger = &g.seed.triggers[0];
        assert_eq!(trigger.agent, "root");
        assert_eq!(trigger.at, "start");
        assert_eq!(trigger.external.kind, "kickoff");
        assert_eq!(trigger.external.payload, serde_json::json!({}));
    }

    // --- `deny_unknown_fields` rejections (parse-time) -----------------

    #[test]
    fn rejects_children_field_on_agent() {
        // `children:` is the multi-agent topology field; v1 is single-
        // agent only. `deny_unknown_fields` on Agent surfaces it.
        let yaml = HAPPY_YAML.replace(
            "    tools: [echo]\n",
            "    tools: [echo]\n    children:\n      - id: child\n",
        );
        let err = parse_err(&yaml);
        let msg = format!("{err}");
        assert!(msg.contains("unknown field `children`"), "{msg}");
        // Should carry a line:col prefix.
        assert!(msg.contains(":"), "expected line:col prefix, got {msg}");
    }

    #[test]
    fn rejects_top_level_defaults_block() {
        let yaml = HAPPY_YAML.replace(
            "tools:\n  - id: echo\n",
            "defaults:\n  idle_period: 1h\ntools:\n  - id: echo\n",
        );
        let err = parse_err(&yaml);
        let msg = format!("{err}");
        assert!(msg.contains("unknown field `defaults`"), "{msg}");
    }

    #[test]
    fn rejects_top_level_policy_block() {
        let yaml = HAPPY_YAML.to_string() + "\npolicy:\n  cost_budget:\n    daily_usd: 50\n";
        let err = parse_err(&yaml);
        let msg = format!("{err}");
        assert!(msg.contains("unknown field `policy`"), "{msg}");
    }

    #[test]
    fn rejects_scripted_decisions_under_seed() {
        let yaml = HAPPY_YAML.replace(
            "  triggers:\n",
            "  scripted_decisions:\n    root: []\n  triggers:\n",
        );
        let err = parse_err(&yaml);
        let msg = format!("{err}");
        assert!(msg.contains("unknown field `scripted_decisions`"), "{msg}");
    }

    #[test]
    fn rejects_mandate_from_file() {
        let yaml = HAPPY_YAML.replace(
            "    mandate:\n      text: |\n        Your task: call the `echo` tool exactly once with arguments {\"msg\": \"hello from temporal\"},\n        then on the next tick emit an Output via the `emit_output` decision whose `content` is a short\n        summary citing the resulting evidence id, then retire. Do not call any other tool; do not loop;\n        do not idle except as a last resort.\n      idle_period: 1s\n      max_ticks: 8\n",
            "    mandate:\n      text: stub\n      from_file: ./mandates/root.md\n      idle_period: 1s\n      max_ticks: 8\n",
        );
        let err = parse_err(&yaml);
        let msg = format!("{err}");
        assert!(msg.contains("unknown field `from_file`"), "{msg}");
    }

    // --- Validator rejections ------------------------------------------

    #[test]
    fn rejects_mcp_tool_with_jar2_63_hint() {
        let yaml = HAPPY_YAML
            .replace(
                "  - id: echo\n    kind: builtin\n    builtin: echo\n",
                "  - id: web\n    kind: mcp\n    command: mcp-web-search\n",
            )
            // Also drop the agent's tool reference so the validator gets to
            // the MCP check before the unknown-tool-ref check.
            .replace("    tools: [echo]\n", "    tools: []\n");
        let err = validate_err(&yaml);
        let msg = format!("{err}");
        assert!(
            matches!(err, GraphYamlError::McpToolRejected { ref tool_id } if tool_id == "web"),
            "got {err:?}",
        );
        assert!(msg.contains("JAR2-63"), "expected JAR2-63 hint, got: {msg}");
    }

    #[test]
    fn rejects_zero_agents() {
        let yaml = HAPPY_YAML.replace(
            "agents:\n  - id: root\n    mandate:\n      text: |\n        Your task: call the `echo` tool exactly once with arguments {\"msg\": \"hello from temporal\"},\n        then on the next tick emit an Output via the `emit_output` decision whose `content` is a short\n        summary citing the resulting evidence id, then retire. Do not call any other tool; do not loop;\n        do not idle except as a last resort.\n      idle_period: 1s\n      max_ticks: 8\n    tools: [echo]\n",
            "agents: []\n",
        );
        let err = validate_err(&yaml);
        assert!(
            matches!(err, GraphYamlError::WrongAgentCount { actual: 0 }),
            "got {err:?}",
        );
    }

    #[test]
    fn rejects_more_than_one_agent() {
        let yaml = HAPPY_YAML.to_string().replace(
            "    tools: [echo]\n",
            "    tools: [echo]\n  - id: second\n    mandate:\n      text: x\n      idle_period: 1s\n    tools: []\n",
        );
        let err = validate_err(&yaml);
        assert!(
            matches!(err, GraphYamlError::WrongAgentCount { actual: 2 }),
            "got {err:?}",
        );
    }

    #[test]
    fn rejects_non_path_safe_metadata_name() {
        let yaml = HAPPY_YAML.replace("  name: smoke\n", "  name: Foo Bar\n");
        let err = validate_err(&yaml);
        assert!(
            matches!(
                err,
                GraphYamlError::InvalidName {
                    field: "metadata.name",
                    ref value,
                } if value == "Foo Bar",
            ),
            "got {err:?}",
        );
    }

    #[test]
    fn rejects_non_path_safe_agent_id() {
        let yaml = HAPPY_YAML.replace("  - id: root\n", "  - id: Root!\n");
        // Also fix the seed trigger so it doesn't unknown-agent before
        // the id-shape check; we want this test to bite on the id.
        let yaml = yaml.replace("    - agent: root\n", "    - agent: Root!\n");
        let err = validate_err(&yaml);
        assert!(
            matches!(
                err,
                GraphYamlError::InvalidName {
                    field: "agents[0].id",
                    ref value,
                } if value == "Root!",
            ),
            "got {err:?}",
        );
    }

    #[test]
    fn rejects_unknown_tool_reference() {
        let yaml = HAPPY_YAML.replace("    tools: [echo]\n", "    tools: [echo, missing]\n");
        let err = validate_err(&yaml);
        assert!(
            matches!(
                err,
                GraphYamlError::UnknownToolReference {
                    ref agent_id,
                    ref tool_id,
                    location: None,
                } if agent_id == "root" && tool_id == "missing",
            ),
            "got {err:?}",
        );
        // The error should name the missing id verbatim so operators can
        // grep for it in their YAML.
        let msg = format!("{err}");
        assert!(msg.contains("\"missing\""), "{msg}");
    }

    #[test]
    fn parse_and_validate_enriches_tool_reference_miss_with_line_col() {
        // `parse_and_validate` is the convenience that combines parser
        // + validator and runs the source-scan enrichment for the
        // tool-ref-miss case. Verifies the JAR2-72 acceptance bar:
        // "Errors point at line:col for at least: ... tool-reference
        // miss."
        let yaml = HAPPY_YAML.replace("    tools: [echo]\n", "    tools: [echo, missing]\n");
        let err = parse_and_validate(&yaml).expect_err("missing tool ref must fail");
        match err {
            GraphYamlError::UnknownToolReference {
                agent_id,
                tool_id,
                location,
            } => {
                assert_eq!(agent_id, "root");
                assert_eq!(tool_id, "missing");
                let loc = location.expect("parse_and_validate should populate location");
                // The replaced line is the agent's `tools:` row. Find
                // the exact line number from the source to keep the
                // assertion robust to future fixture edits.
                let expected_line = yaml
                    .lines()
                    .position(|l| l.contains("tools: [echo, missing]"))
                    .map(|i| i + 1)
                    .expect("the modified tools line must be present");
                assert_eq!(loc.line as usize, expected_line, "got {loc}");
                // The column should point at the `missing` token, not
                // the start of the line. Sanity-check it's > 1.
                assert!(loc.column > 1, "expected non-trivial column, got {loc}");
                // Display should embed the location prefix.
                let msg = format!(
                    "{}",
                    GraphYamlError::UnknownToolReference {
                        agent_id: "root".into(),
                        tool_id: "missing".into(),
                        location: Some(loc),
                    }
                );
                assert!(msg.starts_with(&format!("{loc}: ")), "{msg}");
            }
            other => panic!("expected UnknownToolReference, got {other:?}"),
        }
    }

    #[test]
    fn rejects_unknown_tool_kind_with_line_col() {
        // Serde's "unknown variant" path is one of the four error
        // categories the JAR2-72 acceptance bar names. Confirm it
        // surfaces `line:col` via the parser.
        let yaml = HAPPY_YAML.replace(
            "  - id: echo\n    kind: builtin\n    builtin: echo\n",
            "  - id: echo\n    kind: bogus\n",
        );
        let err = parse_err(&yaml);
        let msg = format!("{err}");
        assert!(
            msg.contains("unknown variant") || msg.contains("bogus"),
            "expected unknown-variant error, got: {msg}",
        );
        // Prefix should carry `line:col`.
        let prefix: &str = msg.split_once(' ').map(|(p, _)| p).unwrap_or(msg.as_str());
        assert!(prefix.contains(':'), "expected line:col prefix, got: {msg}",);
    }

    #[test]
    fn locate_agent_tool_reference_finds_token_in_flow_style_list() {
        let src = "agents:\n  - id: root\n    tools: [echo, missing]\n";
        let loc = super::locate_agent_tool_reference(src, "root", "missing")
            .expect("locator should find the token");
        assert_eq!(loc.line, 3);
        assert!(loc.column > 1);
    }

    #[test]
    fn rejects_bogus_extra_field_on_tool_via_inner_deny_unknown_fields() {
        // The `Tool` outer struct deliberately *doesn't* carry
        // `deny_unknown_fields` (incompatible with `#[serde(flatten)]`).
        // The inner `ToolKind` enum's variant-level guard is what
        // actually rejects extra fields. Confirm an extra field on the
        // builtin variant is rejected with a `line:col`.
        let yaml = HAPPY_YAML.replace(
            "  - id: echo\n    kind: builtin\n    builtin: echo\n",
            "  - id: echo\n    kind: builtin\n    builtin: echo\n    surprise: extra\n",
        );
        let err = parse_err(&yaml);
        let msg = format!("{err}");
        assert!(
            msg.contains("unknown field `surprise`") || msg.contains("surprise"),
            "expected unknown-field error, got: {msg}",
        );
        // Prefix should carry `line:col`.
        let prefix: &str = msg.split_once(' ').map(|(p, _)| p).unwrap_or(msg.as_str());
        assert!(prefix.contains(':'), "expected line:col prefix, got: {msg}",);
    }

    #[test]
    fn rejects_wrong_api_version() {
        let yaml = HAPPY_YAML.replace(
            "apiVersion: jarvis.engine/v1alpha1\n",
            "apiVersion: jarvis.engine/v9\n",
        );
        let err = validate_err(&yaml);
        assert!(
            matches!(
                err,
                GraphYamlError::UnsupportedApiVersion { ref actual } if actual == "jarvis.engine/v9",
            ),
            "got {err:?}",
        );
    }

    #[test]
    fn rejects_wrong_kind() {
        let yaml = HAPPY_YAML.replace("kind: Graph\n", "kind: ToolBundle\n");
        let err = validate_err(&yaml);
        assert!(
            matches!(
                err,
                GraphYamlError::UnsupportedKind { ref actual } if actual == "ToolBundle",
            ),
            "got {err:?}",
        );
    }

    #[test]
    fn rejects_empty_seed_triggers() {
        let yaml = HAPPY_YAML.replace(
            "  triggers:\n    - agent: root\n      at: start\n      external:\n        kind: kickoff\n        payload: {}\n",
            "  triggers: []\n",
        );
        let err = validate_err(&yaml);
        assert!(
            matches!(err, GraphYamlError::EmptySeedTriggers),
            "got {err:?}",
        );
    }

    #[test]
    fn rejects_unsupported_trigger_at() {
        let yaml = HAPPY_YAML.replace("      at: start\n", "      at: every-5m\n");
        let err = validate_err(&yaml);
        assert!(
            matches!(err, GraphYamlError::UnsupportedTriggerAt { ref actual } if actual == "every-5m"),
            "got {err:?}",
        );
    }

    #[test]
    fn rejects_unknown_trigger_agent() {
        let yaml = HAPPY_YAML.replace("    - agent: root\n", "    - agent: ghost\n");
        let err = validate_err(&yaml);
        assert!(
            matches!(err, GraphYamlError::UnknownTriggerAgent { ref agent_id } if agent_id == "ghost"),
            "got {err:?}",
        );
    }

    #[test]
    fn rejects_duplicate_tool_ids() {
        let yaml = HAPPY_YAML.replace(
            "tools:\n  - id: echo\n    kind: builtin\n    builtin: echo\n",
            "tools:\n  - id: echo\n    kind: builtin\n    builtin: echo\n  - id: echo\n    kind: builtin\n    builtin: echo\n",
        );
        let err = validate_err(&yaml);
        assert!(
            matches!(err, GraphYamlError::DuplicateToolId { ref tool_id } if tool_id == "echo"),
            "got {err:?}",
        );
    }

    // --- Duration parser ----------------------------------------------

    #[test]
    fn duration_accepts_100ms() {
        let yaml = HAPPY_YAML.replace("      idle_period: 1s\n", "      idle_period: 100ms\n");
        let g = parse_graph_yaml(&yaml).unwrap();
        assert_eq!(g.agents[0].mandate.idle_period, Duration::from_millis(100));
    }

    #[test]
    fn duration_accepts_5m() {
        let yaml = HAPPY_YAML.replace("      idle_period: 1s\n", "      idle_period: 5m\n");
        let g = parse_graph_yaml(&yaml).unwrap();
        assert_eq!(g.agents[0].mandate.idle_period, Duration::from_secs(5 * 60));
    }

    #[test]
    fn duration_accepts_1h() {
        let yaml = HAPPY_YAML.replace("      idle_period: 1s\n", "      idle_period: 1h\n");
        let g = parse_graph_yaml(&yaml).unwrap();
        assert_eq!(g.agents[0].mandate.idle_period, Duration::from_secs(3600));
    }

    #[test]
    fn duration_rejects_garbage() {
        let yaml = HAPPY_YAML.replace("      idle_period: 1s\n", "      idle_period: garbage\n");
        let err = parse_err(&yaml);
        let msg = format!("{err}");
        assert!(msg.contains("invalid duration"), "{msg}");
        assert!(msg.contains("garbage"), "{msg}");
    }

    // --- Error rendering ----------------------------------------------

    #[test]
    fn parse_error_display_includes_line_col() {
        // Force a structural mismatch (`tools` should be a list, not a
        // scalar) so serde_yaml reports a `Location`.
        let yaml = HAPPY_YAML.replace(
            "tools:\n  - id: echo\n    kind: builtin\n    builtin: echo\n",
            "tools: \"this is not a list\"\n",
        );
        let err = parse_err(&yaml);
        let msg = format!("{err}");
        // `format_parse_error` prefixes `<line>:<col>: ` when the
        // underlying `serde_yaml::Error::location()` returns Some. Look
        // for that shape at the start of the message.
        let prefix: &str = msg.split_once(' ').map(|(p, _)| p).unwrap_or(msg.as_str());
        let (line_str, col_part) = prefix
            .split_once(':')
            .unwrap_or_else(|| panic!("expected `line:col:` prefix, got: {msg}"));
        assert!(
            line_str.parse::<usize>().is_ok(),
            "expected numeric line in prefix, got: {msg}",
        );
        // `col_part` is `"<col>:"` (with trailing colon from the `:` we
        // print after the column). Strip it and parse the column.
        let col_str = col_part.trim_end_matches(':');
        assert!(
            col_str.parse::<usize>().is_ok(),
            "expected numeric column in prefix, got: {msg}",
        );
    }

    // --- URL-path-safe predicate --------------------------------------

    #[test]
    fn url_path_safe_predicate_accepts_canonical_examples() {
        assert!(is_url_path_safe_name("a"));
        assert!(is_url_path_safe_name("smoke"));
        assert!(is_url_path_safe_name("fda-monitor"));
        assert!(is_url_path_safe_name("drug-alpha"));
        assert!(is_url_path_safe_name("a1"));
        assert!(is_url_path_safe_name("a-b-c"));
    }

    #[test]
    fn url_path_safe_predicate_rejects_invalid_examples() {
        assert!(!is_url_path_safe_name(""));
        assert!(!is_url_path_safe_name("-foo"));
        assert!(!is_url_path_safe_name("foo-"));
        assert!(!is_url_path_safe_name("Foo"));
        assert!(!is_url_path_safe_name("foo bar"));
        assert!(!is_url_path_safe_name("foo_bar"));
        assert!(!is_url_path_safe_name("foo/bar"));
        assert!(!is_url_path_safe_name("foo.bar"));
    }

    // --- YAML → AgentInput conversion (Stage 4.2, JAR2-73) -----------

    #[test]
    fn into_agent_input_maps_canonical_fixture_to_expected_values() {
        let g = parse_and_validate(HAPPY_YAML).expect("happy path");
        let input = super::into_agent_input(&g);

        // `mandate.text`, `idle_period`, `max_ticks` round-trip from the
        // YAML's inline mandate.
        assert!(
            input
                .mandate
                .text
                .contains("call the `echo` tool exactly once"),
            "mandate text propagated: {}",
            input.mandate.text,
        );
        assert_eq!(input.mandate.idle_period, Duration::from_secs(1));
        assert_eq!(input.mandate.max_ticks, Some(8));

        // Defaults that v1 YAML does not surface.
        assert!(input.mandate.retry_policy.is_none());
        assert_eq!(
            input.mandate.context_policy,
            jarvis_node::mandate::ContextPolicy::default(),
        );

        // FS handle prefix mirrors the workflow ID so the daemon's
        // shared `LocalStorage` namespaces artifacts per agent (otherwise
        // tick-keyed files like `decisions/<tick>.jsonl` would collide
        // across distinct graphs on the same daemon).
        assert_eq!(input.fs_handle.prefix, "graphs/smoke/agents/root");

        // Parent + carryover are None on a fresh apply (first run).
        assert!(input.parent_handle.is_none());
        assert!(input.carryover.is_none());
    }

    #[test]
    fn into_agent_input_propagates_humanized_idle_period_units() {
        // Cover the duration adapter end-to-end through the conversion
        // — `100ms` survives as `Duration::from_millis(100)`, etc.
        let yaml = HAPPY_YAML.replace("      idle_period: 1s\n", "      idle_period: 100ms\n");
        let g = parse_and_validate(&yaml).expect("happy path");
        let input = super::into_agent_input(&g);
        assert_eq!(input.mandate.idle_period, Duration::from_millis(100));
    }

    #[test]
    fn into_agent_input_propagates_max_ticks_none_when_absent() {
        // `max_ticks` is optional in the YAML; missing → `None` on the
        // way through (matches `Mandate::new`'s contract).
        let yaml = HAPPY_YAML.replace("      max_ticks: 8\n", "");
        let g = parse_and_validate(&yaml).expect("happy path");
        let input = super::into_agent_input(&g);
        assert!(input.mandate.max_ticks.is_none());
    }

    #[test]
    fn yaml_seed_triggers_translates_external_envelopes_in_order() {
        // Two ordered seeds, both targeting the same agent (v1's only
        // legal shape per the validator). The binary will signal them
        // in the YAML's declared order; assert the translated vector
        // preserves that order verbatim.
        let yaml = HAPPY_YAML.replace(
            "  triggers:\n    - agent: root\n      at: start\n      external:\n        kind: kickoff\n        payload: {}\n",
            "  triggers:\n    - agent: root\n      at: start\n      external:\n        kind: kickoff\n        payload: {}\n    - agent: root\n      at: start\n      external:\n        kind: heartbeat\n        payload:\n          beat: 1\n",
        );
        let g = parse_and_validate(&yaml).expect("happy path");
        let triggers = super::yaml_seed_triggers(&g);
        assert_eq!(triggers.len(), 2);
        match &triggers[0] {
            jarvis_node::trigger::Trigger::External { kind, payload } => {
                assert_eq!(kind, "kickoff");
                assert_eq!(*payload, serde_json::json!({}));
            }
            other => panic!("expected External, got {other:?}"),
        }
        match &triggers[1] {
            jarvis_node::trigger::Trigger::External { kind, payload } => {
                assert_eq!(kind, "heartbeat");
                assert_eq!(*payload, serde_json::json!({"beat": 1}));
            }
            other => panic!("expected External, got {other:?}"),
        }
    }

    #[test]
    fn yaml_seed_triggers_passes_arbitrary_json_payloads_through() {
        // Payload is `serde_json::Value`; non-trivial shapes (nested
        // objects, arrays, scalars) must survive the YAML → JSON
        // translation unmangled.
        let yaml = HAPPY_YAML.replace(
            "        payload: {}\n",
            "        payload:\n          nested:\n            list: [1, 2, 3]\n            flag: true\n            text: hello\n",
        );
        let g = parse_and_validate(&yaml).expect("happy path");
        let triggers = super::yaml_seed_triggers(&g);
        assert_eq!(triggers.len(), 1);
        match &triggers[0] {
            jarvis_node::trigger::Trigger::External { kind, payload } => {
                assert_eq!(kind, "kickoff");
                assert_eq!(
                    *payload,
                    serde_json::json!({
                        "nested": {
                            "list": [1, 2, 3],
                            "flag": true,
                            "text": "hello",
                        }
                    }),
                );
            }
            other => panic!("expected External, got {other:?}"),
        }
    }

    // --- Schema-drift regression --------------------------------------

    /// Re-derive the JSON schema from the `JsonSchema` derives and assert
    /// byte-equality against the checked-in `examples/graph.schema.json`.
    ///
    /// Regeneration path: run `cargo test -p jarvis_graph yaml::tests::regenerate_graph_schema -- --ignored --include-ignored`
    /// with `JARVIS_REGENERATE_SCHEMA=1` to overwrite the file. The
    /// drift-check below then passes again.
    #[test]
    fn graph_schema_json_matches_schemars_derive() {
        let actual = render_schema();
        let path = examples_graph_schema_path();
        let expected = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            panic!(
                "could not read {} (did you forget to check it in? regenerate via `JARVIS_REGENERATE_SCHEMA=1 cargo test -p jarvis_graph regenerate_graph_schema -- --ignored`): {e}",
                path.display(),
            )
        });
        assert_eq!(
            actual.trim_end(),
            expected.trim_end(),
            "examples/graph.schema.json is out of date; regenerate via \
             `JARVIS_REGENERATE_SCHEMA=1 cargo test -p jarvis_graph regenerate_graph_schema -- --ignored`",
        );
    }

    /// Regenerator: writes `examples/graph.schema.json` from the current
    /// `JsonSchema` derives. Gated by `JARVIS_REGENERATE_SCHEMA=1` to
    /// avoid accidentally rewriting the file during a routine test run.
    /// Skipped from the default test sweep via `#[ignore]`.
    #[test]
    #[ignore = "regenerator: run via `JARVIS_REGENERATE_SCHEMA=1 cargo test ... -- --ignored`"]
    fn regenerate_graph_schema() {
        if std::env::var("JARVIS_REGENERATE_SCHEMA").as_deref() != Ok("1") {
            panic!(
                "set JARVIS_REGENERATE_SCHEMA=1 to confirm overwrite of examples/graph.schema.json",
            );
        }
        let rendered = render_schema();
        let path = examples_graph_schema_path();
        std::fs::write(&path, rendered).unwrap_or_else(|e| {
            panic!("writing {}: {e}", path.display());
        });
        eprintln!("regenerated {}", path.display());
    }

    fn render_schema() -> String {
        let schema = schemars::schema_for!(GraphYaml);
        // Pretty-print so the checked-in file is human-reviewable in
        // diff. Trailing newline matched by the byte-equality test
        // (we `trim_end` both sides above to be tolerant of editors
        // that strip / add a trailing newline).
        let mut out = serde_json::to_string_pretty(&schema)
            .expect("schemars schemas serialize without error");
        out.push('\n');
        out
    }

    fn examples_graph_schema_path() -> std::path::PathBuf {
        // `CARGO_MANIFEST_DIR` is `crates/jarvis_graph` at test time;
        // the schema lives at `<workspace>/examples/graph.schema.json`.
        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        manifest_dir
            .parent()
            .and_then(|p| p.parent())
            .expect("workspace root above crates/jarvis_graph")
            .join("examples")
            .join("graph.schema.json")
    }
}
