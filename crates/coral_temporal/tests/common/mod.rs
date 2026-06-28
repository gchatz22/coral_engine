//! Shared test support for the live (`TEMPORAL_LIVE_TEST`) integration
//! tests in this crate. Included per-binary via `mod common;`.

#![allow(dead_code)]

use async_trait::async_trait;
use coral_node::agent_ref::{AgentId, GraphId};
use coral_node::storage::BlobSha;
use coral_temporal::worker::StructuralDbStore;
use uuid::Uuid;

/// A do-nothing [`StructuralDbStore`] for live tests that drive the agent
/// loop but don't assert on the reference graph. `persist_output` now
/// requires a structural DB to be installed (it writes `file_index` +
/// citations); this satisfies that contract without a Postgres backend. The
/// reference-graph behaviour itself is covered hermetically in
/// `activities.rs`; tests needing the real wiring install a `GraphStore`.
#[derive(Debug, Default)]
pub struct NoopStructuralDb {
    next_id: std::sync::Mutex<u128>,
}

impl NoopStructuralDb {
    pub fn new() -> Self {
        Self {
            next_id: std::sync::Mutex::new(1),
        }
    }
}

#[async_trait]
impl StructuralDbStore for NoopStructuralDb {
    async fn add_agent(&self, _graph_id: GraphId, _name: &str) -> anyhow::Result<AgentId> {
        let mut next = self.next_id.lock().unwrap();
        let id = AgentId::new(Uuid::from_u128(*next));
        *next += 1;
        Ok(id)
    }

    async fn add_edge(
        &self,
        _parent_agent_id: AgentId,
        _child_agent_id: AgentId,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn list_tool_def_ids_for_graph(&self, _graph_id: GraphId) -> anyhow::Result<Vec<String>> {
        Ok(Vec::new())
    }

    async fn set_file_version(
        &self,
        _agent_id: AgentId,
        _filepath: &str,
        _blob_sha: &BlobSha,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn add_citation(
        &self,
        _citing_agent_id: AgentId,
        _citing_filepath: &str,
        _citing_blob_sha: &BlobSha,
        _cited_agent_id: AgentId,
        _cited_filepath: &str,
        _cited_blob_sha: &BlobSha,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}
