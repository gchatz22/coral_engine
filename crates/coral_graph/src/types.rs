//! Rust types mirroring the structural-DB schema. Every struct
//! corresponds 1:1 with a table from `migrations/0001_initial.sql`.
//! `sqlx::FromRow` lets the `GraphStore` CRUD API decode query rows
//! directly into them. `coral_node::Trigger` is execution state and
//! lives in Temporal, not here — there is deliberately no `Trigger`
//! table.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;
use uuid::Uuid;

/// A graph — the top-level container for a set of agents and their
/// edges. Mirrors the `graphs` table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromRow)]
pub struct Graph {
    pub id: Uuid,
    pub name: String,
    /// Free-form metadata blob (`{}` by default). The structural DB
    /// does not interpret the contents; applications can use it for
    /// e.g. provenance about who authored the graph.
    pub metadata: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

/// An agent within a graph. Mirrors the `agents` table.
///
/// `mandate_ref` is an opaque text handle to the authored mandate
/// (see the schema decision in `migrations/0001_initial.sql`).
/// Authored mandates live outside this DB (git-versioned
/// `graph.yaml`), so there's no FK target — applications choose the
/// convention.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromRow)]
pub struct AgentRecord {
    pub id: Uuid,
    pub graph_id: Uuid,
    pub name: String,
    pub mandate_ref: Option<String>,
    /// Whether this agent must persist and refresh rather than terminate
    /// itself. Defaults to `false` at the schema level; see
    /// `migrations/0003_agents_persistent.sql`.
    pub persistent: bool,
    /// Optional per-agent model override. `None` (the schema default) means
    /// the worker's configured default model; see
    /// `migrations/0004_agents_model.sql`.
    pub model: Option<String>,
    pub created_at: DateTime<Utc>,
}

/// A parent->child edge between two agents. Mirrors the `edges` table.
///
/// The schema enforces `UNIQUE (parent_agent_id, child_agent_id)`. No
/// `graph_id` column today — see the schema-decision note on cross-graph
/// edges in `migrations/0001_initial.sql`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromRow)]
pub struct Edge {
    pub id: Uuid,
    pub parent_agent_id: Uuid,
    pub child_agent_id: Uuid,
    pub created_at: DateTime<Utc>,
}

/// A tool registration. Mirrors the `tools` table.
///
/// `kind` is a free-form string (e.g. `"echo"`, `"mcp"`). `args` and
/// `env_refs` default to empty JSON arrays in the schema; in Rust we
/// model them as `serde_json::Value` so the column shape is exactly
/// what `coral apply` and the worker handle today.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromRow)]
pub struct ToolRecord {
    pub id: Uuid,
    pub kind: String,
    pub command: Option<String>,
    pub args: serde_json::Value,
    pub env_refs: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

/// One row of the `file_index` table — the current `filepath → blob_sha`
/// binding for a file. `blob_sha` is the 40-hex git blob sha as stored
/// (`String` rather than [`coral_node::storage::BlobSha`] so `sqlx::FromRow`
/// decodes the `TEXT` column directly; the store API takes/returns `BlobSha`
/// at its boundary). Per-file history lives in git, not here.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromRow)]
pub struct FileIndexEntry {
    pub agent_id: Uuid,
    pub filepath: String,
    pub blob_sha: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// One row of the `citations` table — a single version-pinned reference
/// edge. Both ends carry a pinned blob sha so provenance is time-scrubbable
/// (an old citing version stays bound to the exact cited version). `*_sha`
/// columns are `String` for the same `FromRow` reason as [`FileIndexEntry`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, FromRow)]
pub struct Citation {
    pub id: Uuid,
    pub citing_agent_id: Uuid,
    pub citing_filepath: String,
    pub citing_blob_sha: String,
    pub cited_agent_id: Uuid,
    pub cited_filepath: String,
    pub cited_blob_sha: String,
    pub created_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-05-23T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn graph_serde_round_trip() {
        let g = Graph {
            id: Uuid::new_v4(),
            name: "demo".into(),
            metadata: serde_json::json!({"author": "tests"}),
            created_at: ts(),
        };
        let s = serde_json::to_string(&g).unwrap();
        let back: Graph = serde_json::from_str(&s).unwrap();
        assert_eq!(g, back);
    }

    #[test]
    fn agent_record_serde_round_trip_with_mandate_ref() {
        let a = AgentRecord {
            id: Uuid::new_v4(),
            graph_id: Uuid::new_v4(),
            name: "worker".into(),
            mandate_ref: Some("v1".into()),
            persistent: true,
            model: Some("claude-opus-4-8".into()),
            created_at: ts(),
        };
        let s = serde_json::to_string(&a).unwrap();
        let back: AgentRecord = serde_json::from_str(&s).unwrap();
        assert_eq!(a, back);
        assert!(back.persistent);
        assert_eq!(back.model.as_deref(), Some("claude-opus-4-8"));
    }

    #[test]
    fn agent_record_serde_round_trip_without_mandate_ref() {
        let a = AgentRecord {
            id: Uuid::new_v4(),
            graph_id: Uuid::new_v4(),
            name: "leaf".into(),
            mandate_ref: None,
            persistent: false,
            model: None,
            created_at: ts(),
        };
        let s = serde_json::to_string(&a).unwrap();
        let back: AgentRecord = serde_json::from_str(&s).unwrap();
        assert_eq!(a, back);
        assert!(back.mandate_ref.is_none());
    }

    #[test]
    fn edge_serde_round_trip() {
        let e = Edge {
            id: Uuid::new_v4(),
            parent_agent_id: Uuid::new_v4(),
            child_agent_id: Uuid::new_v4(),
            created_at: ts(),
        };
        let s = serde_json::to_string(&e).unwrap();
        let back: Edge = serde_json::from_str(&s).unwrap();
        assert_eq!(e, back);
    }

    #[test]
    fn tool_record_serde_round_trip() {
        let t = ToolRecord {
            id: Uuid::new_v4(),
            kind: "mcp".into(),
            command: Some("npx".into()),
            args: serde_json::json!(["-y", "@modelcontextprotocol/server-everything"]),
            env_refs: serde_json::json!([]),
            created_at: ts(),
        };
        let s = serde_json::to_string(&t).unwrap();
        let back: ToolRecord = serde_json::from_str(&s).unwrap();
        assert_eq!(t, back);
    }

    #[test]
    fn file_index_entry_serde_round_trip() {
        let f = FileIndexEntry {
            agent_id: Uuid::new_v4(),
            filepath: "outputs/tsmc-cowos-capacity.md".into(),
            blob_sha: "b49d959381fbab4b3324a4c36e07829509a68880".into(),
            created_at: ts(),
            updated_at: ts(),
        };
        let s = serde_json::to_string(&f).unwrap();
        let back: FileIndexEntry = serde_json::from_str(&s).unwrap();
        assert_eq!(f, back);
    }

    #[test]
    fn citation_serde_round_trip() {
        let c = Citation {
            id: Uuid::new_v4(),
            citing_agent_id: Uuid::new_v4(),
            citing_filepath: "outputs/summary.md".into(),
            citing_blob_sha: "1111111111111111111111111111111111111111".into(),
            cited_agent_id: Uuid::new_v4(),
            cited_filepath: "evidence/tsmc-q3.md".into(),
            cited_blob_sha: "2222222222222222222222222222222222222222".into(),
            created_at: ts(),
        };
        let s = serde_json::to_string(&c).unwrap();
        let back: Citation = serde_json::from_str(&s).unwrap();
        assert_eq!(c, back);
    }
}
