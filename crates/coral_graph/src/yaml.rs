//! `graph.yaml` schema types, parser, and validator. Converts the
//! operator-authored YAML into the `AgentInput`s the runtime starts
//! workflows from. Names are strict-validated (URL-path-safe) and not
//! normalized; UUIDs come from `GraphStore`, not synthesized here.

use coral_node::agent_ref::{AgentId, GraphId};
use coral_node::mandate::Mandate as NodeMandate;
use coral_node::trigger::Trigger as NodeTrigger;
use coral_temporal::workflow::{build_child_input, build_root_input, AgentConfig, AgentInput};
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;

/// The exact `apiVersion` literal v1 accepts.
pub const API_VERSION: &str = "coral.engine/v1alpha1";

/// The exact `kind` literal v1 accepts.
pub const KIND: &str = "Graph";

/// Top-level document type for `graph.yaml`. `agents:` is a forest;
/// each `Agent` may nest `children:`. Validation enforces a non-empty
/// `seed.triggers` so the workflow's first tick is not handed an empty
/// queue.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GraphYaml {
    /// Must be `"coral.engine/v1alpha1"`. Validated in [`validate`].
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    /// Must be `"Graph"`. Validated in [`validate`].
    pub kind: String,
    pub metadata: Metadata,
    /// Top-level defaults applied to every agent unless overridden
    /// inline.
    #[serde(default)]
    pub defaults: Option<AgentDefaults>,
    pub tools: Vec<Tool>,
    pub agents: Vec<Agent>,
    pub seed: Seed,
    /// Operator-level policy block, stored verbatim into
    /// `graphs.metadata` jsonb. Pass-through only; no knobs are enforced
    /// yet. Schema is opaque JSON so adding knobs does not force a wire
    /// bump.
    #[serde(default)]
    pub policy: Option<PolicyYaml>,
}

/// `metadata:` block — identity + free-form description.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Metadata {
    /// URL-path-safe name (`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`).
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
}

/// A tool registration. `kind` is the discriminant selecting one of the
/// [`ToolKind`] variants.
//
// No `deny_unknown_fields` on this outer struct: it is incompatible
// with `#[serde(flatten)]`. The inner `ToolKind` enum carries the
// per-variant guard.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema)]
pub struct Tool {
    /// Reference id used by `agents[].tools`. URL-path-safe.
    pub id: String,
    #[serde(flatten)]
    pub kind: ToolKind,
}

/// Discriminated `kind:` for [`Tool`].
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ToolKind {
    /// Tool registered against the in-process bootstrap `ToolRegistry`.
    Builtin {
        /// Identifier of the in-process tool to register (`"echo"` etc).
        builtin: String,
    },
    /// MCP server spawned as a stdio subprocess.
    Mcp {
        /// Command to spawn for the MCP server. Must be non-empty.
        command: String,
        #[serde(default)]
        args: Vec<String>,
        /// Environment passed to the spawned server, as literal
        /// `NAME: value` pairs. Values are stored verbatim; do not put
        /// secrets here that should not live in the graph definition.
        #[serde(default)]
        env: Option<HashMap<String, String>>,
    },
}

/// One node in the agent forest. `children:` makes the schema recursive;
/// the validator DFS-walks the tree.
///
/// `mandate.idle_period` is `Option<Duration>` because the top-level
/// `defaults:` block may provide it; the validator reports a typed
/// error if neither inline nor default supplies a value.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Agent {
    /// URL-path-safe operator-authored id, distinct from the structural-
    /// DB UUID `GraphStore::add_agent` allocates. The apply walker maps
    /// this id to the allocated `AgentId` via [`AppliedGraph::id_map`].
    pub id: String,
    pub mandate: Mandate,
    /// References by id into the top-level `tools:`. Resolved in
    /// [`validate`].
    #[serde(default)]
    pub tools: Vec<String>,
    /// Nested child agents. Empty for leaves. The validator enforces
    /// unique `id`s across the whole tree.
    #[serde(default)]
    pub children: Vec<Agent>,
}

/// `agents[].mandate:` — the standing instruction. Distinct from
/// `coral_node::mandate::Mandate` (the runtime/wire shape); conversion
/// happens in [`into_agent_input`] / [`build_workflow_starts`].
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Mandate {
    /// Free-form mandate text. YAML block scalars (`|`) are fine.
    pub text: String,
    /// Wake cadence when no signal arrives. Accepts the
    /// [`humantime`](https://docs.rs/humantime) duration grammar
    /// (`100ms`, `5m`, `1h30m`, ...). Malformed values surface as
    /// [`GraphYamlError::Parse`] with a `line:col`.
    ///
    /// Optional so [`AgentDefaults::idle_period`] can supply the value;
    /// the validator emits [`GraphYamlError::MissingMandateIdlePeriod`]
    /// if neither inline nor default is present.
    #[serde(default, deserialize_with = "deserialize_duration_opt")]
    #[schemars(with = "Option<String>")]
    pub idle_period: Option<Duration>,
    /// Optional safety cap on loop iterations. `None` ⇒ run until
    /// `Retire`. May be supplied via [`AgentDefaults::max_ticks`].
    #[serde(default)]
    pub max_ticks: Option<u64>,
    /// Whether this agent must persist and refresh rather than terminate
    /// itself. Absent ⇒ `false` (today's one-shot behavior). Carries no
    /// behavior on its own; the runtime's stop contract and wake/refresh
    /// paths consume it.
    #[serde(default)]
    pub persistent: bool,
    /// Optional per-agent model override (e.g. `claude-opus-4-8` for a
    /// reconciling parent). Absent ⇒ the worker's configured default model.
    /// Interpreted within the worker's configured vendor; a model id that
    /// vendor doesn't recognize is an operator misconfig surfacing as a
    /// runtime error.
    #[serde(default)]
    pub model: Option<String>,
}

/// Top-level `defaults:` block — knobs applied to every agent unless
/// overridden inline.
//
// Separate from `Mandate` because `mandate.text` is per-agent (each
// child has its own narrow mandate); a default text would not be
// meaningful.
#[derive(Clone, Debug, PartialEq, Eq, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentDefaults {
    /// Default `idle_period` when an agent's inline value is absent.
    /// Same `humantime` grammar as the inline field.
    #[serde(default, deserialize_with = "deserialize_duration_opt")]
    #[schemars(with = "Option<String>")]
    pub idle_period: Option<Duration>,
    /// Default `max_ticks` when an agent's inline value is absent.
    /// `None` ⇒ no cap by default.
    #[serde(default)]
    pub max_ticks: Option<u64>,
}

/// `policy:` block — operator-level constraints. Pass-through only:
/// stored verbatim into `graphs.metadata` jsonb under a `"policy"` key.
/// No knob is enforced yet. Opaque JSON so the schema does not pin
/// field names that will reshape when enforcement lands.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct PolicyYaml(#[schemars(with = "serde_json::Value")] pub serde_json::Value);

/// `seed:` — what kicks the graph off. Validation requires `triggers`
/// to be non-empty so the workflow's first tick is not handed an empty
/// queue.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Seed {
    pub triggers: Vec<SeedTrigger>,
}

/// One row under `seed.triggers:` — an external signal addressed to an
/// agent and fired `at: start`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SeedTrigger {
    /// Target agent id. Validated against `agents[].id` in [`validate`].
    pub agent: String,
    /// When to fire. Only `"start"` is accepted in v1.
    pub at: String,
    /// External-signal payload. Mirror of
    /// `coral_node::trigger::Trigger::External`.
    pub external: ExternalEnvelope,
}

/// Payload of `seed.triggers[].external:`. The runtime translates this
/// into `Trigger::External { kind, payload }` at apply time.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExternalEnvelope {
    pub kind: String,
    #[serde(default)]
    #[schemars(with = "serde_json::Value")]
    pub payload: serde_json::Value,
}

// --- duration deserializer -----------------------------------------------

/// Serde adapter for `Option<Duration>` using `humantime::parse_duration`.
fn deserialize_duration_opt<'de, D>(de: D) -> Result<Option<Duration>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt = Option::<String>::deserialize(de)?;
    match opt {
        Some(s) => humantime::parse_duration(&s)
            .map(Some)
            .map_err(|e| serde::de::Error::custom(format!("invalid duration {s:?}: {e}"))),
        None => Ok(None),
    }
}

// --- error type ---------------------------------------------------------

/// 1-indexed `(line, column)` pair into the original YAML source.
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

/// Unified error surface for [`parse_graph_yaml`] + [`validate`]. The
/// `Display` impl prepends `line:col` whenever the source location is
/// available. Validation variants do not carry locations by default;
/// [`parse_and_validate`] enriches `UnknownToolReference` via a small
/// source-text scan.
#[derive(Debug, thiserror::Error)]
pub enum GraphYamlError {
    /// `serde_yaml` failed to deserialize the document — structural
    /// mismatch, missing required field, unknown field, or duration
    /// adapter failure. `serde_yaml::Error::location()` may carry
    /// `line:col`.
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

    /// `agents:` was empty. The forest must contain at least one root.
    #[error("`agents:` is empty; at least one root agent is required")]
    NoAgents,

    /// Two agents in the tree share the same operator-authored `id`.
    /// The id is the lookup key for `seed.triggers[].agent` and for the
    /// apply walker's `id_map`; duplicates would silently fire triggers
    /// at the wrong workflow.
    #[error(
        "duplicate agent id {agent_id:?} in the agent tree (every `agents[].id`, including nested `children:` ids, must be unique)"
    )]
    DuplicateAgentId { agent_id: String },

    /// Defensive guard against cyclic `children:` references. The
    /// recursive `Vec<Agent>` shape cannot structurally express a cycle,
    /// but the validator's promise that the tree is a tree should
    /// survive a future schema change that adds a `parent:` ref form.
    #[error("cyclic `children:` detected via agent {agent_id:?}")]
    CyclicChildren { agent_id: String },

    /// An agent's mandate has no `idle_period` and no
    /// `defaults.idle_period` is provided. The runtime requires a
    /// concrete wake cadence rather than defaulting silently to zero.
    #[error(
        "agent {agent_id:?} has no `mandate.idle_period` and `defaults.idle_period` is not set (declare one or the other)"
    )]
    MissingMandateIdlePeriod { agent_id: String },

    /// A `kind: mcp` tool had an empty `command`; there is nothing to
    /// spawn for the server.
    #[error("tool {tool_id:?} has `kind: mcp` with an empty `command` (set the executable to spawn for the MCP server)")]
    EmptyMcpCommand { tool_id: String },

    /// `metadata.name` or `agents[].id` did not match the URL-path-safe
    /// regex `^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`.
    #[error(
        "name {value:?} at {field} is not URL-path-safe: must match `^[a-z0-9]([a-z0-9-]*[a-z0-9])?$` (lowercase alphanumerics + `-`, must start/end with alphanumeric)"
    )]
    InvalidName { field: &'static str, value: String },

    /// An agent's `tools` referenced an id missing from the top-level
    /// `tools:` list. `location` is populated by [`parse_and_validate`]
    /// via a source scan; pure [`validate`] leaves it `None`.
    #[error(
        "{loc_prefix}agent {agent_id:?} references tool id {tool_id:?} which is not defined under top-level `tools:` (define it, or remove the reference)",
        loc_prefix = location.map(|l| format!("{l}: ")).unwrap_or_default(),
    )]
    UnknownToolReference {
        agent_id: String,
        tool_id: String,
        location: Option<Location>,
    },

    /// `seed.triggers[].agent` referenced an id no agent declares.
    #[error(
        "seed trigger targets agent {agent_id:?} which is not declared under `agents:` (no such agent will receive this trigger)"
    )]
    UnknownTriggerAgent { agent_id: String },

    /// `seed.triggers[].at` was not the literal `"start"`. v1 supports
    /// only kickoff-at-apply.
    #[error(
        "seed trigger has `at: {actual:?}`; v1 supports only `at: \"start\"` (timed seeds are deferred)"
    )]
    UnsupportedTriggerAt { actual: String },

    /// `seed.triggers:` was empty; the workflow's first tick would
    /// otherwise drain an empty queue and send the LLM an empty prompt.
    #[error(
        "seed.triggers is empty; at least one initial trigger is required (the workflow's first tick would otherwise drain an empty queue and send the LLM an empty prompt)"
    )]
    EmptySeedTriggers,

    /// Top-level `tools:` had duplicate `id` entries.
    #[error("duplicate tool id {tool_id:?} in top-level `tools:` (ids must be unique)")]
    DuplicateToolId { tool_id: String },
}

/// Render a `serde_yaml::Error` with the source location prefixed as
/// `line:col` so CLI output matches the `cargo` / `rustc` convention.
fn format_parse_error(e: &serde_yaml::Error) -> String {
    match e.location() {
        Some(loc) => format!("{}:{}: {e}", loc.line(), loc.column()),
        None => format!("{e}"),
    }
}

// --- parser -------------------------------------------------------------

/// Parse a `graph.yaml` document into a [`GraphYaml`]. Validation is
/// separate — call [`validate`] (or [`parse_and_validate`]) before
/// consuming the value. `apiVersion` / `kind` exact-match lives in the
/// validator so a typo in either field still parses to a typed value
/// the validator can describe.
pub fn parse_graph_yaml(text: &str) -> Result<GraphYaml, GraphYamlError> {
    serde_yaml::from_str(text).map_err(GraphYamlError::from)
}

// --- validator ----------------------------------------------------------

/// Hand-rolled `^[a-z0-9]([a-z0-9-]*[a-z0-9])?$` check; avoids pulling
/// in `regex` for a single validation.
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

/// Enforce invariants the type system cannot. Run after
/// [`parse_graph_yaml`] and before handing the value to anything that
/// depends on those invariants (DB writes, workflow start). Checks are
/// ordered so the most operator-actionable errors surface first:
/// apiVersion / kind → non-empty agents → name shape → tools → tree
/// walk → seeds.
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

    if g.agents.is_empty() {
        return Err(GraphYamlError::NoAgents);
    }

    let mut seen = std::collections::HashSet::new();
    for tool in &g.tools {
        if !seen.insert(tool.id.as_str()) {
            return Err(GraphYamlError::DuplicateToolId {
                tool_id: tool.id.clone(),
            });
        }
        match &tool.kind {
            ToolKind::Mcp { command, .. } if command.trim().is_empty() => {
                return Err(GraphYamlError::EmptyMcpCommand {
                    tool_id: tool.id.clone(),
                });
            }
            ToolKind::Mcp { .. } | ToolKind::Builtin { .. } => {}
        }
    }

    let registered: std::collections::HashSet<&str> =
        g.tools.iter().map(|t| t.id.as_str()).collect();

    let mut seen_agent_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    for root in &g.agents {
        validate_agent_tree(root, &registered, &g.defaults, &mut seen_agent_ids)?;
    }

    if g.seed.triggers.is_empty() {
        return Err(GraphYamlError::EmptySeedTriggers);
    }
    for trigger in &g.seed.triggers {
        if !seen_agent_ids.contains(trigger.agent.as_str()) {
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

/// DFS the agent subtree rooted at `agent`, enforcing per-node
/// invariants and accumulating ids into `seen_agent_ids` (raising
/// [`GraphYamlError::DuplicateAgentId`] on collision).
fn validate_agent_tree(
    agent: &Agent,
    registered_tools: &std::collections::HashSet<&str>,
    defaults: &Option<AgentDefaults>,
    seen_agent_ids: &mut std::collections::HashSet<String>,
) -> Result<(), GraphYamlError> {
    if !is_url_path_safe_name(&agent.id) {
        return Err(GraphYamlError::InvalidName {
            field: "agents[].id",
            value: agent.id.clone(),
        });
    }
    if !seen_agent_ids.insert(agent.id.clone()) {
        return Err(GraphYamlError::DuplicateAgentId {
            agent_id: agent.id.clone(),
        });
    }
    if agent.mandate.idle_period.is_none()
        && defaults.as_ref().and_then(|d| d.idle_period).is_none()
    {
        return Err(GraphYamlError::MissingMandateIdlePeriod {
            agent_id: agent.id.clone(),
        });
    }
    for tool_id in &agent.tools {
        if !registered_tools.contains(tool_id.as_str()) {
            return Err(GraphYamlError::UnknownToolReference {
                agent_id: agent.id.clone(),
                tool_id: tool_id.clone(),
                location: None,
            });
        }
    }
    for child in &agent.children {
        // Surface a child id matching its parent's id as
        // `CyclicChildren` rather than the generic duplicate-id error.
        if child.id == agent.id {
            return Err(GraphYamlError::CyclicChildren {
                agent_id: child.id.clone(),
            });
        }
        validate_agent_tree(child, registered_tools, defaults, seen_agent_ids)?;
    }
    Ok(())
}

/// `true` if this YAML uses any multi-agent feature: more than one root
/// agent, or any agent has nested `children:`.
pub fn is_multi_agent(g: &GraphYaml) -> bool {
    g.agents.len() > 1 || g.agents.iter().any(|a| !a.children.is_empty())
}

/// Resolve an agent's mandate against the top-level defaults, returning
/// the concrete [`NodeMandate`] the runtime consumes. Panics if no
/// `idle_period` is resolvable; callers must have already run
/// [`validate`].
pub(crate) fn resolve_mandate(agent: &Agent, defaults: &Option<AgentDefaults>) -> NodeMandate {
    let idle_period = agent
        .mandate
        .idle_period
        .or_else(|| defaults.as_ref().and_then(|d| d.idle_period))
        .expect("validate() should have rejected agents without idle_period");
    let max_ticks = agent
        .mandate
        .max_ticks
        .or_else(|| defaults.as_ref().and_then(|d| d.max_ticks));
    let mut mandate = NodeMandate::new(agent.mandate.text.clone(), idle_period, max_ticks);
    mandate.persistent = agent.mandate.persistent;
    mandate.model = agent.mandate.model.clone();
    mandate
}

/// Parse and validate in one shot, with `line:col` enrichment for the
/// validator errors that can be located in the source text (currently
/// [`GraphYamlError::UnknownToolReference`]). Pure [`validate`] and
/// pure [`parse_graph_yaml`] remain available for callers that already
/// have the parsed value and don't want the source-scan overhead.
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

/// Source-scan enrichment for validator errors. Currently only fills
/// in `UnknownToolReference::location`; other variants pin the offender
/// by name in the message itself.
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
/// forward to the next `tools:` line, then scan that line and any
/// block-style continuation lines for the bare token. Returns `None`
/// rather than guessing if the structure doesn't match expectations.
fn locate_agent_tool_reference(source: &str, agent_id: &str, tool_id: &str) -> Option<Location> {
    let agent_anchor = format!("id: {agent_id}");
    let mut lines = source.lines().enumerate();
    let mut after_agent = false;
    for (idx, line) in lines.by_ref() {
        if line.contains(&agent_anchor) {
            after_agent = true;
            let _ = idx;
            break;
        }
    }
    if !after_agent {
        return None;
    }

    for (line_no, line) in lines.by_ref() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("tools:") {
            if let Some(loc) = locate_token_in_line(line, line_no, tool_id) {
                return Some(loc);
            }
            for (cont_no, cont_line) in lines.by_ref() {
                let ct = cont_line.trim_start();
                if ct.is_empty() {
                    continue;
                }
                if !cont_line.starts_with(' ') && !cont_line.starts_with('\t') {
                    return None;
                }
                if let Some(loc) = locate_token_in_line(cont_line, cont_no, tool_id) {
                    return Some(loc);
                }
                // Sibling key at the same indent as `tools:` — left the
                // tools list, give up.
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

/// Find the column of a bare `tool_id` token in a single line of YAML;
/// "bare" meaning bounded on each side by non-id characters. Returns
/// 1-indexed `(line, column)`.
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

// --- YAML → workflow input conversion ----------------------------------

/// One resolved agent in the apply walk: the operator-authored id paired
/// with the structural-DB UUIDs `GraphStore::create_from_yaml`
/// allocated. The walker returns these in DFS parents-first order so
/// each child's `parent_handle` can reference an already-allocated
/// parent workflow id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedAgent {
    /// Operator-authored `agents[].id` from the YAML.
    pub operator_id: String,
    /// Freshly-allocated structural-DB UUID from `GraphStore::add_agent`.
    pub db_agent_id: AgentId,
    /// The parent's `db_agent_id`, or `None` for forest roots.
    pub parent_db_agent_id: Option<AgentId>,
}

/// Handoff from the structural-DB phase to the workflow-start phase.
/// Carries the allocated `graph_id`, the resolved agent list in DFS
/// parents-first order, and the lookup map from operator-authored agent
/// id to allocated UUID + workflow id.
#[derive(Clone, Debug)]
pub struct AppliedGraph {
    pub graph_id: GraphId,
    /// Operator-authored graph name, used in CLI / log output. Not
    /// used in workflow-id derivation; UUIDs go there.
    pub graph_name: String,
    /// Every agent the YAML declared, in DFS parents-first order.
    pub agents: Vec<ResolvedAgent>,
    /// Map from operator-authored agent id → `(db_agent_id,
    /// workflow_id)`. Workflow id is the canonical UUID-shaped form
    /// `graphs/<graph_uuid>/agents/<agent_uuid>`.
    pub id_map: HashMap<String, ResolvedAgentWorkflow>,
}

/// Bundled `(db_agent_id, workflow_id)` value for
/// [`AppliedGraph::id_map`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedAgentWorkflow {
    /// Structural-DB UUID, matching `agents.id`.
    pub db_agent_id: AgentId,
    /// Temporal workflow id used at `start_workflow` time.
    pub workflow_id: String,
}

/// One workflow to start, with its `AgentInput`. The apply binary
/// iterates this vector in DFS parents-first order so each child's
/// `parent_handle.workflow_id` references a workflow id already issued
/// earlier in the list.
#[derive(Clone, Debug)]
pub struct WorkflowStart {
    /// `graphs/<graph_uuid>/agents/<agent_uuid>`.
    pub workflow_id: String,
    pub input: AgentInput,
}

/// One seed trigger paired with the workflow id its target agent
/// resolved to.
#[derive(Clone, Debug)]
pub struct ResolvedSeedTrigger {
    pub workflow_id: String,
    pub trigger: NodeTrigger,
}

/// Build the [`WorkflowStart`] list the apply binary feeds to the
/// Temporal client, against the `GraphStore`-allocated UUIDs in
/// [`AppliedGraph`]. Hermetic — no DB, no Temporal client, no
/// filesystem.
///
/// The output preserves DFS parents-first order so each child's
/// `parent_handle.workflow_id` references a workflow id earlier in the
/// list.
///
/// Caller invariants:
///
/// - `graph` must have passed [`validate`].
/// - `applied.agents` must be in DFS parents-first order, matching the
///   order [`crate::store::GraphStore::create_from_yaml`] allocates
///   UUIDs. The `id_map` must contain every entry's `operator_id`.
pub fn build_workflow_starts(graph: &GraphYaml, applied: &AppliedGraph) -> Vec<WorkflowStart> {
    let mut yaml_by_id: HashMap<&str, &Agent> = HashMap::new();
    for root in &graph.agents {
        index_agents(root, &mut yaml_by_id);
    }

    let mut starts = Vec::with_capacity(applied.agents.len());
    for resolved in &applied.agents {
        let yaml_agent = yaml_by_id
            .get(resolved.operator_id.as_str())
            .copied()
            .expect("AppliedGraph operator_id must reference a YAML agent");
        let mandate = resolve_mandate(yaml_agent, &graph.defaults);
        let workflow_id = applied
            .id_map
            .get(&resolved.operator_id)
            .expect("AppliedGraph id_map missing entry")
            .workflow_id
            .clone();
        let input = match resolved.parent_db_agent_id {
            None => build_root_input(
                applied.graph_id,
                resolved.db_agent_id,
                resolved.operator_id.clone(),
                mandate,
                AgentConfig::default(),
            ),
            Some(parent_db_id) => {
                let parent_operator_id = applied
                    .agents
                    .iter()
                    .find(|a| a.db_agent_id == parent_db_id)
                    .map(|a| a.operator_id.as_str())
                    .expect("parent_db_agent_id must reference some agent in AppliedGraph");
                let parent_workflow_id = applied
                    .id_map
                    .get(parent_operator_id)
                    .expect("id_map missing parent operator id")
                    .workflow_id
                    .clone();
                build_child_input(
                    &parent_workflow_id,
                    parent_db_id,
                    applied.graph_id,
                    resolved.db_agent_id,
                    resolved.operator_id.clone(),
                    mandate,
                    AgentConfig::default(),
                )
            }
        };
        starts.push(WorkflowStart { workflow_id, input });
    }
    starts
}

/// Index the agent tree under `root` into `out`, keyed by operator id.
/// Validator pre-condition: ids are unique tree-wide.
fn index_agents<'a>(root: &'a Agent, out: &mut HashMap<&'a str, &'a Agent>) {
    out.insert(root.id.as_str(), root);
    for child in &root.children {
        index_agents(child, out);
    }
}

/// Single-agent shim that builds an `AgentInput` from a validated YAML
/// using `GraphStore`-allocated UUIDs.
///
/// Caller invariants:
///
/// - The YAML must have passed [`validate`].
/// - `graph` must be single-agent (`agents.len() == 1`, no children).
///   Use [`build_workflow_starts`] for the multi-agent case.
pub fn into_agent_input(graph: &GraphYaml, graph_id: GraphId, agent_id: AgentId) -> AgentInput {
    debug_assert_eq!(
        graph.agents.len(),
        1,
        "into_agent_input requires a single-agent graph; use \
         build_workflow_starts for multi-agent",
    );
    debug_assert!(
        graph.agents[0].children.is_empty(),
        "into_agent_input requires a flat single-agent graph; use \
         build_workflow_starts for hierarchical",
    );
    let agent = &graph.agents[0];
    let mandate = resolve_mandate(agent, &graph.defaults);
    build_root_input(
        graph_id,
        agent_id,
        agent.id.clone(),
        mandate,
        AgentConfig::default(),
    )
}

/// Translate the YAML's `seed.triggers` into resolved
/// `(workflow_id, Trigger::External)` pairs the binary signals against.
/// Order is preserved. Returns [`GraphYamlError::UnknownTriggerAgent`]
/// as a belt-and-braces guard; [`validate`] should have caught this
/// upstream.
pub fn yaml_seed_triggers(
    graph: &GraphYaml,
    applied: &AppliedGraph,
) -> Result<Vec<ResolvedSeedTrigger>, GraphYamlError> {
    let mut out = Vec::with_capacity(graph.seed.triggers.len());
    for seed in &graph.seed.triggers {
        let workflow_id = applied
            .id_map
            .get(&seed.agent)
            .ok_or_else(|| GraphYamlError::UnknownTriggerAgent {
                agent_id: seed.agent.clone(),
            })?
            .workflow_id
            .clone();
        out.push(ResolvedSeedTrigger {
            workflow_id,
            trigger: NodeTrigger::External {
                kind: seed.external.kind.clone(),
                payload: seed.external.payload.clone(),
            },
        });
    }
    Ok(out)
}

// --- tests --------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// The canonical v1 happy-path fixture.
    const HAPPY_YAML: &str = r#"
apiVersion: coral.engine/v1alpha1
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
        assert_eq!(agent.mandate.idle_period, Some(Duration::from_secs(1)));
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
    fn accepts_mcp_tool() {
        let yaml = HAPPY_YAML
            .replace(
                "  - id: echo\n    kind: builtin\n    builtin: echo\n",
                "  - id: web\n    kind: mcp\n    command: mcp-web-search\n    args: [--verbose]\n",
            )
            .replace("    tools: [echo]\n", "    tools: [web]\n");
        let g = parse_and_validate(&yaml).expect("valid mcp graph");
        let web = g.tools.iter().find(|t| t.id == "web").expect("web tool");
        assert!(matches!(
            &web.kind,
            ToolKind::Mcp { command, args, env }
                if command == "mcp-web-search"
                    && args == &vec!["--verbose".to_string()]
                    && env.is_none()
        ));
    }

    #[test]
    fn mcp_env_round_trips() {
        let yaml = HAPPY_YAML
            .replace(
                "  - id: echo\n    kind: builtin\n    builtin: echo\n",
                "  - id: web\n    kind: mcp\n    command: mcp-web-search\n    env:\n      API_KEY: secret\n      LOG: debug\n",
            )
            .replace("    tools: [echo]\n", "    tools: [web]\n");
        let g = parse_and_validate(&yaml).expect("valid mcp graph with env");
        let web = g.tools.iter().find(|t| t.id == "web").expect("web tool");
        match &web.kind {
            ToolKind::Mcp { env, .. } => {
                let env = env.as_ref().expect("env present");
                assert_eq!(env.get("API_KEY").map(String::as_str), Some("secret"));
                assert_eq!(env.get("LOG").map(String::as_str), Some("debug"));
            }
            other => panic!("expected mcp tool, got {other:?}"),
        }
    }

    #[test]
    fn rejects_mcp_tool_with_empty_command() {
        let yaml = HAPPY_YAML
            .replace(
                "  - id: echo\n    kind: builtin\n    builtin: echo\n",
                "  - id: web\n    kind: mcp\n    command: \"\"\n",
            )
            .replace("    tools: [echo]\n", "    tools: []\n");
        let err = validate_err(&yaml);
        assert!(
            matches!(err, GraphYamlError::EmptyMcpCommand { ref tool_id } if tool_id == "web"),
            "got {err:?}",
        );
    }

    #[test]
    fn rejects_zero_agents() {
        let yaml = HAPPY_YAML.replace(
            "agents:\n  - id: root\n    mandate:\n      text: |\n        Your task: call the `echo` tool exactly once with arguments {\"msg\": \"hello from temporal\"},\n        then on the next tick emit an Output via the `emit_output` decision whose `content` is a short\n        summary citing the resulting evidence id, then retire. Do not call any other tool; do not loop;\n        do not idle except as a last resort.\n      idle_period: 1s\n      max_ticks: 8\n    tools: [echo]\n",
            "agents: []\n",
        );
        let err = validate_err(&yaml);
        assert!(matches!(err, GraphYamlError::NoAgents), "got {err:?}",);
    }

    #[test]
    fn accepts_multiple_top_level_agents() {
        let yaml = HAPPY_YAML.to_string().replace(
            "    tools: [echo]\n",
            "    tools: [echo]\n  - id: second\n    mandate:\n      text: x\n      idle_period: 1s\n    tools: []\n",
        );
        let g = parse_and_validate(&yaml).expect("multi-agent v1 happy path");
        assert_eq!(g.agents.len(), 2);
        assert_eq!(g.agents[0].id, "root");
        assert_eq!(g.agents[1].id, "second");
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
        // Adjust the seed trigger so the unknown-agent check doesn't
        // fire before the id-shape check.
        let yaml = yaml.replace("    - agent: root\n", "    - agent: Root!\n");
        let err = validate_err(&yaml);
        assert!(
            matches!(
                err,
                GraphYamlError::InvalidName {
                    field: "agents[].id",
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
        let msg = format!("{err}");
        assert!(msg.contains("\"missing\""), "{msg}");
    }

    #[test]
    fn parse_and_validate_enriches_tool_reference_miss_with_line_col() {
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
                let expected_line = yaml
                    .lines()
                    .position(|l| l.contains("tools: [echo, missing]"))
                    .map(|i| i + 1)
                    .expect("the modified tools line must be present");
                assert_eq!(loc.line as usize, expected_line, "got {loc}");
                assert!(loc.column > 1, "expected non-trivial column, got {loc}");
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
        // The `Tool` outer struct can't carry `deny_unknown_fields`
        // (incompatible with `#[serde(flatten)]`); the inner `ToolKind`
        // enum's variant-level guard does the rejection.
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
        let prefix: &str = msg.split_once(' ').map(|(p, _)| p).unwrap_or(msg.as_str());
        assert!(prefix.contains(':'), "expected line:col prefix, got: {msg}",);
    }

    #[test]
    fn rejects_wrong_api_version() {
        let yaml = HAPPY_YAML.replace(
            "apiVersion: coral.engine/v1alpha1\n",
            "apiVersion: coral.engine/v9\n",
        );
        let err = validate_err(&yaml);
        assert!(
            matches!(
                err,
                GraphYamlError::UnsupportedApiVersion { ref actual } if actual == "coral.engine/v9",
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
        assert_eq!(
            g.agents[0].mandate.idle_period,
            Some(Duration::from_millis(100))
        );
    }

    #[test]
    fn duration_accepts_5m() {
        let yaml = HAPPY_YAML.replace("      idle_period: 1s\n", "      idle_period: 5m\n");
        let g = parse_graph_yaml(&yaml).unwrap();
        assert_eq!(
            g.agents[0].mandate.idle_period,
            Some(Duration::from_secs(5 * 60))
        );
    }

    #[test]
    fn duration_accepts_1h() {
        let yaml = HAPPY_YAML.replace("      idle_period: 1s\n", "      idle_period: 1h\n");
        let g = parse_graph_yaml(&yaml).unwrap();
        assert_eq!(
            g.agents[0].mandate.idle_period,
            Some(Duration::from_secs(3600))
        );
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
        // Force a structural mismatch so serde_yaml reports a Location.
        let yaml = HAPPY_YAML.replace(
            "tools:\n  - id: echo\n    kind: builtin\n    builtin: echo\n",
            "tools: \"this is not a list\"\n",
        );
        let err = parse_err(&yaml);
        let msg = format!("{err}");
        let prefix: &str = msg.split_once(' ').map(|(p, _)| p).unwrap_or(msg.as_str());
        let (line_str, col_part) = prefix
            .split_once(':')
            .unwrap_or_else(|| panic!("expected `line:col:` prefix, got: {msg}"));
        assert!(
            line_str.parse::<usize>().is_ok(),
            "expected numeric line in prefix, got: {msg}",
        );
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

    // --- YAML -> AgentInput conversion --------------------------------

    /// Build a synthetic [`AppliedGraph`] for the single-agent fixture
    /// so seed-trigger tests don't need a live DB.
    fn synthetic_applied(graph: &GraphYaml, root_id: &str) -> super::AppliedGraph {
        let graph_uuid = uuid::Uuid::new_v4();
        let agent_uuid = uuid::Uuid::new_v4();
        let graph_id = coral_node::agent_ref::GraphId::new(graph_uuid);
        let agent_id = coral_node::agent_ref::AgentId::new(agent_uuid);
        let workflow_id = format!("graphs/{graph_uuid}/agents/{agent_uuid}");
        let mut id_map = std::collections::HashMap::new();
        id_map.insert(
            root_id.to_string(),
            super::ResolvedAgentWorkflow {
                db_agent_id: agent_id,
                workflow_id,
            },
        );
        super::AppliedGraph {
            graph_id,
            graph_name: graph.metadata.name.clone(),
            agents: vec![super::ResolvedAgent {
                operator_id: root_id.to_string(),
                db_agent_id: agent_id,
                parent_db_agent_id: None,
            }],
            id_map,
        }
    }

    #[test]
    fn into_agent_input_maps_canonical_fixture_to_expected_values() {
        let g = parse_and_validate(HAPPY_YAML).expect("happy path");
        let graph_uuid = uuid::Uuid::new_v4();
        let agent_uuid = uuid::Uuid::new_v4();
        let graph_id = coral_node::agent_ref::GraphId::new(graph_uuid);
        let agent_id = coral_node::agent_ref::AgentId::new(agent_uuid);
        let input = super::into_agent_input(&g, graph_id, agent_id);

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
        // Absent `persistent:` ⇒ false (today's one-shot default).
        assert!(!input.mandate.persistent);
        // Absent `model:` ⇒ None (worker's configured default model).
        assert!(input.mandate.model.is_none());

        assert!(input.mandate.retry_policy.is_none());
        assert_eq!(
            input.mandate.context_policy,
            coral_node::mandate::ContextPolicy::default(),
        );

        // FS handle prefix is derived from the GraphStore-allocated
        // UUIDs (not the operator-authored name) so cross-agent FS
        // reads keyed off `agent_id` resolve correctly.
        assert_eq!(
            input.fs_handle.prefix,
            format!("graphs/{graph_uuid}/agents/{agent_uuid}"),
        );
        assert_eq!(input.graph_id, graph_id);
        assert_eq!(input.agent_id, agent_id);
        assert_eq!(input.agent_name, "root");

        assert!(input.parent_handle.is_none());
        assert!(input.carryover.is_none());
    }

    #[test]
    fn into_agent_input_propagates_humanized_idle_period_units() {
        let yaml = HAPPY_YAML.replace("      idle_period: 1s\n", "      idle_period: 100ms\n");
        let g = parse_and_validate(&yaml).expect("happy path");
        let graph_id = coral_node::agent_ref::GraphId::new(uuid::Uuid::new_v4());
        let agent_id = coral_node::agent_ref::AgentId::new(uuid::Uuid::new_v4());
        let input = super::into_agent_input(&g, graph_id, agent_id);
        assert_eq!(input.mandate.idle_period, Duration::from_millis(100));
    }

    #[test]
    fn into_agent_input_propagates_max_ticks_none_when_absent() {
        let yaml = HAPPY_YAML.replace("      max_ticks: 8\n", "");
        let g = parse_and_validate(&yaml).expect("happy path");
        let graph_id = coral_node::agent_ref::GraphId::new(uuid::Uuid::new_v4());
        let agent_id = coral_node::agent_ref::AgentId::new(uuid::Uuid::new_v4());
        let input = super::into_agent_input(&g, graph_id, agent_id);
        assert!(input.mandate.max_ticks.is_none());
    }

    #[test]
    fn persistent_true_in_mandate_reaches_agent_input() {
        let yaml = HAPPY_YAML.replace(
            "      idle_period: 1s\n",
            "      idle_period: 1s\n      persistent: true\n",
        );
        let g = parse_and_validate(&yaml).expect("persistent graph validates");
        assert!(g.agents[0].mandate.persistent);
        let graph_id = coral_node::agent_ref::GraphId::new(uuid::Uuid::new_v4());
        let agent_id = coral_node::agent_ref::AgentId::new(uuid::Uuid::new_v4());
        let input = super::into_agent_input(&g, graph_id, agent_id);
        assert!(
            input.mandate.persistent,
            "persistent: true must reach the workflow input mandate"
        );
    }

    #[test]
    fn persistent_absent_defaults_to_false_at_parse() {
        let g = parse_and_validate(HAPPY_YAML).expect("happy path");
        assert!(!g.agents[0].mandate.persistent);
    }

    #[test]
    fn model_override_in_mandate_reaches_agent_input() {
        let yaml = HAPPY_YAML.replace(
            "      idle_period: 1s\n",
            "      idle_period: 1s\n      model: claude-opus-4-8\n",
        );
        let g = parse_and_validate(&yaml).expect("model graph validates");
        assert_eq!(
            g.agents[0].mandate.model.as_deref(),
            Some("claude-opus-4-8")
        );
        let graph_id = coral_node::agent_ref::GraphId::new(uuid::Uuid::new_v4());
        let agent_id = coral_node::agent_ref::AgentId::new(uuid::Uuid::new_v4());
        let input = super::into_agent_input(&g, graph_id, agent_id);
        assert_eq!(
            input.mandate.model.as_deref(),
            Some("claude-opus-4-8"),
            "model: must reach the workflow input mandate"
        );
    }

    #[test]
    fn model_absent_defaults_to_none_at_parse() {
        let g = parse_and_validate(HAPPY_YAML).expect("happy path");
        assert!(g.agents[0].mandate.model.is_none());
    }

    #[test]
    fn yaml_seed_triggers_translates_external_envelopes_in_order() {
        // Two ordered seeds, both targeting the same agent. The
        // translated vector must preserve declared order and resolve
        // each to the right workflow id via the id_map.
        let yaml = HAPPY_YAML.replace(
            "  triggers:\n    - agent: root\n      at: start\n      external:\n        kind: kickoff\n        payload: {}\n",
            "  triggers:\n    - agent: root\n      at: start\n      external:\n        kind: kickoff\n        payload: {}\n    - agent: root\n      at: start\n      external:\n        kind: heartbeat\n        payload:\n          beat: 1\n",
        );
        let g = parse_and_validate(&yaml).expect("happy path");
        let applied = synthetic_applied(&g, "root");
        let triggers = super::yaml_seed_triggers(&g, &applied).expect("seed triggers resolve");
        assert_eq!(triggers.len(), 2);
        let expected_workflow_id = applied.id_map.get("root").unwrap().workflow_id.clone();
        assert_eq!(triggers[0].workflow_id, expected_workflow_id);
        match &triggers[0].trigger {
            coral_node::trigger::Trigger::External { kind, payload } => {
                assert_eq!(kind, "kickoff");
                assert_eq!(*payload, serde_json::json!({}));
            }
            other => panic!("expected External, got {other:?}"),
        }
        assert_eq!(triggers[1].workflow_id, expected_workflow_id);
        match &triggers[1].trigger {
            coral_node::trigger::Trigger::External { kind, payload } => {
                assert_eq!(kind, "heartbeat");
                assert_eq!(*payload, serde_json::json!({"beat": 1}));
            }
            other => panic!("expected External, got {other:?}"),
        }
    }

    #[test]
    fn yaml_seed_triggers_passes_arbitrary_json_payloads_through() {
        let yaml = HAPPY_YAML.replace(
            "        payload: {}\n",
            "        payload:\n          nested:\n            list: [1, 2, 3]\n            flag: true\n            text: hello\n",
        );
        let g = parse_and_validate(&yaml).expect("happy path");
        let applied = synthetic_applied(&g, "root");
        let triggers = super::yaml_seed_triggers(&g, &applied).expect("seed triggers resolve");
        assert_eq!(triggers.len(), 1);
        match &triggers[0].trigger {
            coral_node::trigger::Trigger::External { kind, payload } => {
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

    // --- Multi-agent tests -------------------------------------------

    /// Hierarchical fixture: `children:`, top-level `defaults:`,
    /// pass-through `policy:`, and a `seed.triggers[].agent` targeting
    /// a non-root agent.
    const HIERARCHICAL_YAML: &str = r#"
apiVersion: coral.engine/v1alpha1
kind: Graph
metadata:
  name: fda-monitor
  description: "Continuous watch on FDA decisions for biotech X"
defaults:
  idle_period: 1h
tools:
  - id: web-search
    kind: builtin
    builtin: echo
  - id: fda-feed
    kind: builtin
    builtin: echo
agents:
  - id: root
    mandate:
      text: "Monitor the FDA"
      idle_period: 4h
    tools: [web-search]
    children:
      - id: drug-alpha
        mandate:
          text: "Watch Drug Alpha"
        tools: [fda-feed, web-search]
      - id: drug-beta
        mandate:
          text: "Watch Drug Beta"
        tools: [fda-feed, web-search]
      - id: competitive-landscape
        mandate:
          text: "Track competitors"
          idle_period: 12h
        tools: [web-search]
        children:
          - id: competitor-a
            mandate:
              text: "Watch Competitor A"
            tools: [fda-feed]
          - id: competitor-b
            mandate:
              text: "Watch Competitor B"
            tools: [fda-feed]
seed:
  triggers:
    - agent: root
      at: start
      external:
        kind: kickoff
        payload: {}
policy:
  cost_budget:
    daily_usd: 50
  on_budget_exhausted: pause
"#;

    #[test]
    fn parses_and_validates_hierarchical_fixture() {
        let g = parse_and_validate(HIERARCHICAL_YAML).expect("hierarchical happy path");
        assert_eq!(g.metadata.name, "fda-monitor");
        assert_eq!(g.agents.len(), 1, "single root forest");
        let root = &g.agents[0];
        assert_eq!(root.id, "root");
        assert_eq!(root.children.len(), 3);
        let comp = root
            .children
            .iter()
            .find(|a| a.id == "competitive-landscape")
            .unwrap();
        assert_eq!(comp.children.len(), 2);
        assert!(g.defaults.is_some());
        assert_eq!(
            g.defaults.as_ref().unwrap().idle_period,
            Some(Duration::from_secs(3600))
        );
        // drug-alpha has no inline idle_period; the validator allows
        // that only because defaults provides one.
        let alpha = root.children.iter().find(|a| a.id == "drug-alpha").unwrap();
        assert!(alpha.mandate.idle_period.is_none());
        assert!(g.policy.is_some());
    }

    #[test]
    fn is_multi_agent_detects_hierarchical_and_forest_shapes() {
        let g = parse_and_validate(HIERARCHICAL_YAML).expect("happy");
        assert!(
            super::is_multi_agent(&g),
            "nested children: triggers multi-agent"
        );

        let single = parse_and_validate(HAPPY_YAML).expect("single-agent happy");
        assert!(!super::is_multi_agent(&single));

        let two_roots_yaml = HAPPY_YAML.to_string().replace(
            "    tools: [echo]\n",
            "    tools: [echo]\n  - id: second\n    mandate:\n      text: x\n      idle_period: 1s\n    tools: []\n",
        );
        let two_roots = parse_and_validate(&two_roots_yaml).expect("two-roots happy");
        assert!(super::is_multi_agent(&two_roots));
    }

    #[test]
    fn resolve_mandate_uses_defaults_when_inline_idle_period_absent() {
        let g = parse_and_validate(HIERARCHICAL_YAML).expect("happy");
        let root = &g.agents[0];
        let alpha = root.children.iter().find(|a| a.id == "drug-alpha").unwrap();
        let mandate = super::resolve_mandate(alpha, &g.defaults);
        assert_eq!(mandate.idle_period, Duration::from_secs(3600));
        assert_eq!(mandate.text, "Watch Drug Alpha");
    }

    #[test]
    fn resolve_mandate_inline_overrides_defaults() {
        let g = parse_and_validate(HIERARCHICAL_YAML).expect("happy");
        let root = &g.agents[0];
        let mandate = super::resolve_mandate(root, &g.defaults);
        assert_eq!(mandate.idle_period, Duration::from_secs(4 * 3600));
    }

    #[test]
    fn rejects_duplicate_agent_id_across_tree() {
        let yaml = r#"
apiVersion: coral.engine/v1alpha1
kind: Graph
metadata:
  name: dup
tools:
  - id: echo
    kind: builtin
    builtin: echo
agents:
  - id: root
    mandate:
      text: r
      idle_period: 1s
    tools: []
    children:
      - id: dupe
        mandate:
          text: a
          idle_period: 1s
        tools: []
      - id: dupe
        mandate:
          text: b
          idle_period: 1s
        tools: []
seed:
  triggers:
    - agent: root
      at: start
      external:
        kind: k
        payload: {}
"#;
        let err = validate_err(yaml);
        assert!(
            matches!(err, GraphYamlError::DuplicateAgentId { ref agent_id } if agent_id == "dupe"),
            "got {err:?}",
        );
    }

    #[test]
    fn rejects_cyclic_children_via_direct_self_reference() {
        // A child whose `id` matches its parent's id should surface as
        // `CyclicChildren` rather than the generic duplicate-id error.
        let yaml = r#"
apiVersion: coral.engine/v1alpha1
kind: Graph
metadata:
  name: cyc
tools:
  - id: echo
    kind: builtin
    builtin: echo
agents:
  - id: same
    mandate:
      text: parent
      idle_period: 1s
    tools: []
    children:
      - id: same
        mandate:
          text: child
          idle_period: 1s
        tools: []
seed:
  triggers:
    - agent: same
      at: start
      external:
        kind: k
        payload: {}
"#;
        let err = validate_err(yaml);
        assert!(
            matches!(err, GraphYamlError::CyclicChildren { ref agent_id } if agent_id == "same"),
            "got {err:?}",
        );
    }

    #[test]
    fn rejects_unresolved_seed_trigger_target_across_tree() {
        let yaml = HIERARCHICAL_YAML.replace("- agent: root", "- agent: ghost");
        let err = validate_err(&yaml);
        assert!(
            matches!(err, GraphYamlError::UnknownTriggerAgent { ref agent_id } if agent_id == "ghost"),
            "got {err:?}",
        );
    }

    #[test]
    fn rejects_missing_mandate_idle_period_when_no_defaults() {
        let yaml = r#"
apiVersion: coral.engine/v1alpha1
kind: Graph
metadata:
  name: no-idle
tools:
  - id: echo
    kind: builtin
    builtin: echo
agents:
  - id: root
    mandate:
      text: x
    tools: []
seed:
  triggers:
    - agent: root
      at: start
      external:
        kind: k
        payload: {}
"#;
        let err = validate_err(yaml);
        assert!(
            matches!(err, GraphYamlError::MissingMandateIdlePeriod { ref agent_id } if agent_id == "root"),
            "got {err:?}",
        );
    }

    #[test]
    fn seed_trigger_can_target_a_leaf_agent() {
        let yaml = HIERARCHICAL_YAML.replace("- agent: root", "- agent: competitor-a");
        let g = parse_and_validate(&yaml).expect("leaf-targeted seed happy");
        assert_eq!(g.seed.triggers[0].agent, "competitor-a");
    }

    #[test]
    fn unresolved_tool_reference_in_deep_child_is_caught() {
        let yaml = HIERARCHICAL_YAML.replace("tools: [fda-feed]\n", "tools: [fda-feed, missing]\n");
        let err = validate_err(&yaml);
        assert!(
            matches!(
                err,
                GraphYamlError::UnknownToolReference {
                    ref tool_id,
                    ..
                } if tool_id == "missing",
            ),
            "got {err:?}",
        );
    }

    #[test]
    fn build_workflow_starts_produces_dfs_parents_first_with_real_uuids() {
        // Hermetic synthetic AppliedGraph mirroring what
        // GraphStore::create_from_yaml would return; asserts DFS
        // parents-first order with `parent_handle.workflow_id` pointing
        // earlier in the list.
        let g = parse_and_validate(HIERARCHICAL_YAML).expect("happy");
        let graph_uuid = uuid::Uuid::new_v4();
        let graph_id = coral_node::agent_ref::GraphId::new(graph_uuid);
        let mut resolved = Vec::new();
        let mut id_map = std::collections::HashMap::new();
        fn walk(
            agent: &super::Agent,
            parent: Option<coral_node::agent_ref::AgentId>,
            graph_uuid: uuid::Uuid,
            resolved: &mut Vec<super::ResolvedAgent>,
            id_map: &mut std::collections::HashMap<String, super::ResolvedAgentWorkflow>,
        ) {
            let agent_uuid = uuid::Uuid::new_v4();
            let agent_id = coral_node::agent_ref::AgentId::new(agent_uuid);
            id_map.insert(
                agent.id.clone(),
                super::ResolvedAgentWorkflow {
                    db_agent_id: agent_id,
                    workflow_id: format!("graphs/{graph_uuid}/agents/{agent_uuid}"),
                },
            );
            resolved.push(super::ResolvedAgent {
                operator_id: agent.id.clone(),
                db_agent_id: agent_id,
                parent_db_agent_id: parent,
            });
            for child in &agent.children {
                walk(child, Some(agent_id), graph_uuid, resolved, id_map);
            }
        }
        for root in &g.agents {
            walk(root, None, graph_uuid, &mut resolved, &mut id_map);
        }
        let applied = super::AppliedGraph {
            graph_id,
            graph_name: g.metadata.name.clone(),
            agents: resolved,
            id_map,
        };
        let starts = super::build_workflow_starts(&g, &applied);
        // 6 agents: root + 3 children + 2 grandchildren.
        assert_eq!(starts.len(), 6);
        assert!(
            starts[0].input.parent_handle.is_none(),
            "root has no parent_handle"
        );
        assert_eq!(starts[0].input.agent_name, "root");
        let mut seen_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
        for start in &starts {
            if let Some(parent) = &start.input.parent_handle {
                assert!(
                    seen_ids.contains(&parent.workflow_id),
                    "child's parent_handle.workflow_id {} not in earlier entries: {:?}",
                    parent.workflow_id,
                    seen_ids
                );
            }
            seen_ids.insert(start.workflow_id.clone());
        }
        for start in &starts {
            assert_eq!(start.input.fs_handle.prefix, start.workflow_id);
        }
        let alpha_start = starts
            .iter()
            .find(|s| s.input.agent_name == "drug-alpha")
            .unwrap();
        assert_eq!(
            alpha_start.input.mandate.idle_period,
            Duration::from_secs(3600)
        );
    }

    // --- Schema-drift regression --------------------------------------

    /// Re-derive the JSON schema from the `JsonSchema` derives and
    /// assert byte-equality against the checked-in
    /// `examples/graph.schema.json`. Regenerate via
    /// `CORAL_REGENERATE_SCHEMA=1 cargo test -p coral_graph regenerate_graph_schema -- --ignored`.
    #[test]
    fn graph_schema_json_matches_schemars_derive() {
        let actual = render_schema();
        let path = examples_graph_schema_path();
        let expected = std::fs::read_to_string(&path).unwrap_or_else(|e| {
            panic!(
                "could not read {} (did you forget to check it in? regenerate via `CORAL_REGENERATE_SCHEMA=1 cargo test -p coral_graph regenerate_graph_schema -- --ignored`): {e}",
                path.display(),
            )
        });
        assert_eq!(
            actual.trim_end(),
            expected.trim_end(),
            "examples/graph.schema.json is out of date; regenerate via \
             `CORAL_REGENERATE_SCHEMA=1 cargo test -p coral_graph regenerate_graph_schema -- --ignored`",
        );
    }

    /// Writes `examples/graph.schema.json` from the current
    /// `JsonSchema` derives. Gated by `CORAL_REGENERATE_SCHEMA=1` and
    /// `#[ignore]` to avoid rewriting the file during a routine test
    /// run.
    #[test]
    #[ignore = "regenerator: run via `CORAL_REGENERATE_SCHEMA=1 cargo test ... -- --ignored`"]
    fn regenerate_graph_schema() {
        if std::env::var("CORAL_REGENERATE_SCHEMA").as_deref() != Ok("1") {
            panic!(
                "set CORAL_REGENERATE_SCHEMA=1 to confirm overwrite of examples/graph.schema.json",
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
        let mut out = serde_json::to_string_pretty(&schema)
            .expect("schemars schemas serialize without error");
        out.push('\n');
        out
    }

    fn examples_graph_schema_path() -> std::path::PathBuf {
        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        manifest_dir
            .parent()
            .and_then(|p| p.parent())
            .expect("workspace root above crates/coral_graph")
            .join("examples")
            .join("graph.schema.json")
    }
}
