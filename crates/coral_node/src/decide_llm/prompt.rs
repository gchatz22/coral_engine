//! Render a `ContextBundle` into a `Vec<Message>` the model can consume.
//!
//! Layout: one `system` message (mandate text + standing invariants); an
//! optional `user` correction message when the runtime rejected the
//! previous tick's `Decision`; one `user` message per non-empty content
//! window (triggers, recent outputs, recent evidence, open claims). Empty
//! windows are dropped to keep the prompt budget tight.
//!
//! Output is deterministic across runs: time-varying fields (`OutputId`,
//! `created_at`) are dropped; JSON values round-trip through
//! `serde_json::to_string` with the default `BTreeMap`-backed
//! `serde_json::Map`, emitting keys in sorted order; window ordering is
//! the caller's responsibility (`assemble_context` reads via `AgentFs`'s
//! lex-sorted listings). That stability is what makes the snapshot tests
//! below viable.

use crate::decision::{ContextBundle, CorrectionContext};
use crate::evidence::EvidenceRecord;
use crate::fs::Claim;
use crate::mandate::{Mandate, Output};
use crate::model_client::Message;
use crate::trigger::Trigger;

/// Shared head of the standing invariants (clauses 1–4): provenance,
/// one-decision-per-turn, parallel `call_tool`, evidence-from-tools. These
/// hold for every agent regardless of lifecycle. [`render_system`] joins
/// this head with a lifecycle-specific tail
/// ([`ONESHOT_INVARIANTS_TAIL`] or [`PERSISTENT_INVARIANTS_TAIL`]) to form
/// the full block; the snapshot tests pin the joined result.
///
/// The list is split into short, single-purpose clauses rather than a few
/// dense paragraphs: empirically, models would re-emit Outputs across
/// consecutive turns when the lifecycle rule was buried as the last
/// sentence of a long paragraph and worded conditionally — so each rule
/// gets its own numbered, unconditional clause.
///
/// Invariant 3 permits K parallel `call_tool` blocks in a single
/// response — the runtime folds them into a single
/// `Decision::CallTools` and dispatches them in the same tick, then
/// stages K paired `tool_result` blocks on the next prompt bundle.
/// Terminal decision tools (`emit_output`, `rewrite_fs`, `idle`,
/// `retire`) remain singular: mixing a terminal with `call_tool` or
/// issuing two terminals in one response is rejected.
const INVARIANTS_HEAD: &str = "\
Invariants:
1. Provenance. Every `emit_output` decision must cite `evidence` ids that resolve in this agent's evidence store. The runtime will reject outputs whose evidence does not resolve.
2. One decision per turn. Reply by calling exactly one terminal decision tool (`emit_output`, `rewrite_fs`, `idle`, `retire`) OR one or more `call_tool` blocks dispatched together as a single parallel batch.
3. Parallel call_tool is supported. K `call_tool` `tool_use` blocks in one response run together this tick; the next prompt carries the matching `tool_result` blocks. Do not mix `call_tool` with a terminal decision tool in the same response.
4. Evidence comes from tool calls. Each `call_tool` result becomes a fresh evidence record that later `emit_output` decisions can cite.";

/// Lifecycle tail for a one-shot agent (the default): emit once, then
/// retire. Invariant 5's unconditional phrasing and the explicit tagging of
/// the recent-outputs window as *yours* are load-bearing — without them the
/// model treats prior Outputs as ambient context and re-emits paraphrased
/// copies across turns.
const ONESHOT_INVARIANTS_TAIL: &str = "\
5. Do not re-emit. Once you have emitted any Output on this run, your next decision must be `retire`. Do not emit a revised, paraphrased, or improved version of a prior Output. Outputs shown in the \"Recent outputs by you on this run\" window were emitted by you and count toward this rule.
6. Retire is final. After the mandate's required Output has been emitted, `retire` is the only correct decision.";

/// Lifecycle tail for a `persistent` agent — a continuous monitor that
/// refreshes its work on a cadence instead of retiring after one Output.
/// Replaces the one-shot "retire after Output / never re-emit" rules with
/// the refresh contract. The runtime enforces the non-termination (a
/// model-emitted `Retire` is demoted to `Idle`); this tail aligns the
/// model's intent so it does useful refresh work between wakes.
const PERSISTENT_INVARIANTS_TAIL: &str = "\
5. Refresh, don't stop. You are a persistent monitor. After you `emit_output`, choose `idle` to wait for your cadence; on the next scheduled wake, re-research and emit an updated Output that reflects what changed since the last one. The \"Recent outputs by you on this run\" window is your durable memory — build on it rather than re-emitting an unchanged copy.
6. Do not retire yourself. `retire` is not a valid self-decision for a persistent agent: the runtime stops you only via a retirement signal or your tick budget, and a `retire` decision is treated as `idle`. Keep cycling: research -> emit_output -> idle -> refresh.";

/// Render a `ContextBundle` into the message list a `ModelClient::complete`
/// call should send.
///
/// The returned `Vec<Message>` is intended to be passed verbatim as
/// `CompleteRequest::messages`. The caller is responsible for filling
/// `CompleteRequest::tools` with `decide_llm::schema::decision_tools()`.
pub fn render(bundle: &ContextBundle) -> Vec<Message> {
    // Capacity is at most 6: system + correction + triggers + outputs +
    // evidence + open_claims. Pre-allocating saves a couple of small
    // reallocs in the common case without committing to a fixed shape.
    let mut out = Vec::with_capacity(6);
    out.push(Message::system(render_system(&bundle.mandate)));
    if let Some(c) = &bundle.correction {
        out.push(Message::user(render_correction(c)));
    }
    if !bundle.triggers.is_empty() {
        out.push(Message::user(render_triggers(&bundle.triggers)));
    }
    if !bundle.recent_outputs.is_empty() {
        out.push(Message::user(render_outputs(&bundle.recent_outputs)));
    }
    if !bundle.recent_evidence.is_empty() {
        out.push(Message::user(render_evidence(&bundle.recent_evidence)));
    }
    if !bundle.open_claims.is_empty() {
        out.push(Message::user(render_open_claims(&bundle.open_claims)));
    }
    out
}

/// Build the system-message body: mandate text followed by the invariants.
///
/// The mandate text is interpolated verbatim. Sanitization (length caps,
/// HTML stripping, etc.) is the maintainer's concern at mandate-creation
/// time, not the renderer's — the kernel treats the mandate string as
/// already-trusted input.
fn render_system(m: &Mandate) -> String {
    let tail = if m.persistent {
        PERSISTENT_INVARIANTS_TAIL
    } else {
        ONESHOT_INVARIANTS_TAIL
    };
    format!(
        "You are an agent operating under the following mandate:\n\n{}\n\n{INVARIANTS_HEAD}\n{tail}",
        m.text
    )
}

/// Render the correction window: a single user message describing the
/// failure that prompted this continuation tick.
///
/// Phrasing is plain English rather than serialized JSON because the model
/// is expected to act on it directly ("here's what to fix"), not summarize
/// it as background context. The text is kept short and ends with the
/// concrete next-step instruction so the model has no excuse to hallucinate
/// a non-decision turn.
fn render_correction(c: &CorrectionContext) -> String {
    format!(
        "# Previous-attempt failure\n\
         \n\
         The runtime could not satisfy your previous decision: {failure}.\n\
         \n\
         Reply by calling exactly one decision tool that addresses the failure.",
        failure = c.failure,
    )
}

/// Render the trigger window as a bulleted list.
///
/// Most variants are serialized via their existing serde shape — the same
/// shape the kernel uses on the wire — so the prompt cannot drift from
/// the typed enum without a serde test failure elsewhere.
///
/// Cross-agent variants (`ChildOutput`, `ChildRetired`) render as
/// human-readable prose instead: the model needs the child's name as a
/// first-class signal ("which child should I reconcile?"), and an opaque
/// `External`-shaped JSON blob buries that name behind a nested struct.
fn render_triggers(triggers: &[Trigger]) -> String {
    // Header has no trailing newline; each bullet is `\n\n- BODY`, giving
    // a blank line between header-and-first-bullet and between every pair
    // of bullets. Read better than tight bullets when these strings are
    // dumped into a debug log.
    let mut s = format!("# Triggers ({})", triggers.len());
    for t in triggers {
        s.push_str("\n\n- ");
        match t {
            Trigger::ChildOutput {
                agent_name,
                output_id,
                ..
            } => {
                // The agent name is the load-bearing piece; output_id is
                // the citation handle the model needs if it later emits
                // a `ReconcileChildren` decision pointing at this output.
                s.push_str(&format!("Child output: {agent_name} emitted {output_id}"));
            }
            Trigger::ChildRetired {
                agent_name, reason, ..
            } => {
                s.push_str(&format!("Child retired: {agent_name} ({reason})"));
            }
            _ => {
                s.push_str(&serde_json::to_string(t).expect("Trigger serializes"));
            }
        }
    }
    s
}

/// Render the recent-outputs window.
///
/// Each entry shows `content` (the public claim) and the list of evidence
/// ids that justify it. `OutputId` and `created_at` are deliberately
/// dropped — see module docs.
fn render_outputs(outputs: &[Output]) -> String {
    let mut s = format!("# Recent outputs by you on this run ({})", outputs.len());
    for o in outputs {
        s.push_str("\n\n- content: ");
        s.push_str(&serde_json::to_string(&o.content).expect("string serializes"));
        s.push_str("\n  evidence: ");
        s.push_str(&serde_json::to_string(&o.evidence).expect("evidence list serializes"));
    }
    s
}

/// Render the recent-evidence window.
///
/// Each entry shows the content-addressed `id` and the `(tool, args, result)`
/// triple that produced it. `created_at` is deliberately dropped — see
/// module docs.
fn render_evidence(evidence: &[EvidenceRecord]) -> String {
    let mut s = format!("# Recent evidence ({})", evidence.len());
    for r in evidence {
        s.push_str("\n\n- id: ");
        s.push_str(r.id.as_str());
        s.push_str("\n  tool: ");
        s.push_str(&r.tool);
        s.push_str("\n  args: ");
        s.push_str(&serde_json::to_string(&r.args).expect("Value serializes"));
        s.push_str("\n  result: ");
        s.push_str(&serde_json::to_string(&r.result).expect("Value serializes"));
    }
    s
}

/// Render the open-claims window.
///
/// Each entry shows the `seed` (the canonical claim id the agent attaches
/// to `Decision::CallTool { claim_seed }`) and the human-readable
/// `description` the agent stored when minting the seed. `status` is
/// elided because the runtime pre-filters to `Open`; `created_at` is
/// elided for the same reason recent-output / recent-evidence timestamps
/// are. The window exists so the model consults this list before minting
/// a fresh `ClaimSeed` for conceptual work it has already opened. See
/// `scratch/claim_seed_persistence.md` for the convention and
/// `scratch/context_assembly_v2.md` § 3 for the warm-cache rationale.
fn render_open_claims(claims: &[Claim]) -> String {
    let mut s = format!("# Open claims ({})", claims.len());
    for c in claims {
        s.push_str("\n\n- seed: ");
        s.push_str(&serde_json::to_string(&c.seed).expect("string serializes"));
        s.push_str("\n  description: ");
        s.push_str(&serde_json::to_string(&c.description).expect("string serializes"));
    }
    s
}

#[cfg(test)]
mod tests {
    //! Snapshot tests for `render`.
    //!
    //! These lock the prompt wording verbatim. A diff to the rendered
    //! string must be a *deliberate* edit to one of the constants or the
    //! per-window helpers, never a side effect of an unrelated change.
    //!
    //! Fixtures avoid time-varying inputs:
    //!
    //! - `OutputId`s are randomly generated by `Output::new` but never
    //!   rendered, so they do not affect snapshots.
    //! - `created_at` is fixed via `ts()` and never rendered, but pinning it
    //!   keeps the constructor calls reproducible.
    //! - `EvidenceId` is sha256 of `(tool, args, result)` (see
    //!   `crate::evidence`); identical inputs always produce identical ids.

    use super::*;
    use crate::agent_ref::{AgentId, AgentRef};
    use crate::decision::{ContextBundle, CorrectionContext};
    use crate::evidence::{EvidenceId, EvidenceRecord};
    use crate::mandate::OutputId;
    use crate::mandate::{Mandate, Output};
    use crate::model_client::{ContentBlock, Role};
    use crate::trigger::{HumanOp, Trigger};
    use chrono::{DateTime, Utc};
    use serde_json::json;
    use std::time::Duration;
    use uuid::Uuid;

    fn ts() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-05-06T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    fn mandate() -> Mandate {
        Mandate::new(
            "Watch the FDA holds list and report drug-program risk.",
            Duration::from_secs(60),
            Some(100),
        )
    }

    /// Tiny helper: extract the single text block from a message we just
    /// constructed via `Message::system` / `Message::user`. The renderer
    /// only ever produces single-text-block messages, so anything else is a
    /// shape regression.
    fn text(m: &Message) -> &str {
        match m.content.as_slice() {
            [ContentBlock::Text { text }] => text,
            other => panic!("expected single text block, got {other:?}"),
        }
    }

    fn empty_bundle() -> ContextBundle {
        ContextBundle {
            mandate: mandate(),
            triggers: vec![],
            recent_outputs: vec![],
            recent_evidence: vec![],
            open_claims: vec![],
            correction: None,
        }
    }

    // ---- shape invariants ------------------------------------------------

    #[test]
    fn render_always_starts_with_system_message() {
        for bundle in [
            empty_bundle(),
            // single-trigger
            ContextBundle {
                triggers: vec![Trigger::ScheduledWake],
                ..empty_bundle()
            },
        ] {
            let msgs = render(&bundle);
            assert_eq!(msgs.first().unwrap().role, Role::System);
            // The system message body always begins with the mandate
            // preamble; pin that prefix so a refactor to the system
            // template surfaces here, not just in the per-shape snapshots.
            assert!(text(msgs.first().unwrap())
                .starts_with("You are an agent operating under the following mandate:"));
        }
    }

    #[test]
    fn render_is_deterministic_across_calls() {
        // Build a bundle with all three windows populated and call `render`
        // twice; outputs must match byte-for-byte. This is the property the
        // module docs claim.
        let ev = EvidenceRecord::new("echo", json!({"msg": "hi"}), json!({"echoed": "hi"}), ts());
        let bundle = ContextBundle {
            mandate: mandate(),
            triggers: vec![Trigger::ScheduledWake],
            recent_outputs: vec![Output::new("draft", vec![ev.id.clone()], ts())],
            recent_evidence: vec![ev],
            open_claims: vec![],
            correction: None,
        };
        let a = render(&bundle);
        let b = render(&bundle);
        assert_eq!(a.len(), b.len());
        for (x, y) in a.iter().zip(b.iter()) {
            assert_eq!(text(x), text(y));
        }
    }

    #[test]
    fn render_drops_time_varying_output_fields() {
        // Two bundles identical in everything we render but differing in
        // OutputId and created_at must produce the same prompt.
        let evs = vec![EvidenceId::new("t", &json!({}), &json!({}))];
        let bundle_a = ContextBundle {
            recent_outputs: vec![Output::new("same", evs.clone(), ts())],
            ..empty_bundle()
        };
        let later = DateTime::parse_from_rfc3339("2027-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let bundle_b = ContextBundle {
            recent_outputs: vec![Output::new("same", evs, later)],
            ..empty_bundle()
        };
        let a = render(&bundle_a);
        let b = render(&bundle_b);
        for (x, y) in a.iter().zip(b.iter()) {
            assert_eq!(text(x), text(y));
        }
    }

    #[test]
    fn render_drops_time_varying_evidence_field() {
        // Same evidence triple, different created_at — id is hashed from
        // the triple and is stable; created_at is not rendered, so prompts
        // match.
        let early = EvidenceRecord::new("t", json!({"a": 1}), json!({"r": 1}), ts());
        let later_ts = DateTime::parse_from_rfc3339("2099-12-31T23:59:59Z")
            .unwrap()
            .with_timezone(&Utc);
        let later = EvidenceRecord::new("t", json!({"a": 1}), json!({"r": 1}), later_ts);
        assert_eq!(
            early.id, later.id,
            "evidence id should be content-addressed"
        );

        let a = render(&ContextBundle {
            recent_evidence: vec![early],
            ..empty_bundle()
        });
        let b = render(&ContextBundle {
            recent_evidence: vec![later],
            ..empty_bundle()
        });
        for (x, y) in a.iter().zip(b.iter()) {
            assert_eq!(text(x), text(y));
        }
    }

    #[test]
    fn render_emits_canonical_json_for_evidence_args_with_unsorted_keys() {
        // `EvidenceRecord` carries args/result as `serde_json::Value`. The
        // default (no `preserve_order`) `Map` is BTreeMap-backed, so two
        // logically equal but textually-different inputs produce identical
        // prompt text. This is what makes the snapshot tests stable across
        // input typed in different orders.
        let mut a = serde_json::Map::new();
        a.insert("b".into(), json!(2));
        a.insert("a".into(), json!(1));
        let mut b = serde_json::Map::new();
        b.insert("a".into(), json!(1));
        b.insert("b".into(), json!(2));

        let ra = EvidenceRecord::new("t", serde_json::Value::Object(a), json!({}), ts());
        let rb = EvidenceRecord::new("t", serde_json::Value::Object(b), json!({}), ts());

        let pa = render(&ContextBundle {
            recent_evidence: vec![ra],
            ..empty_bundle()
        });
        let pb = render(&ContextBundle {
            recent_evidence: vec![rb],
            ..empty_bundle()
        });
        assert_eq!(text(&pa[1]), text(&pb[1]));
    }

    // ---- snapshot tests --------------------------------------------------

    /// Snapshot 1 of 5: empty bundle. Expected message count = 1 (system
    /// only). No content windows means no per-window user messages.
    #[test]
    fn snapshot_empty_bundle() {
        let msgs = render(&empty_bundle());
        assert_eq!(msgs.len(), 1, "expected system message only");

        assert_eq!(msgs[0].role, Role::System);
        assert_eq!(
            text(&msgs[0]),
            "You are an agent operating under the following mandate:\n\
             \n\
             Watch the FDA holds list and report drug-program risk.\n\
             \n\
             Invariants:\n\
             1. Provenance. Every `emit_output` decision must cite `evidence` ids that resolve in this agent's evidence store. The runtime will reject outputs whose evidence does not resolve.\n\
             2. One decision per turn. Reply by calling exactly one terminal decision tool (`emit_output`, `rewrite_fs`, `idle`, `retire`) OR one or more `call_tool` blocks dispatched together as a single parallel batch.\n\
             3. Parallel call_tool is supported. K `call_tool` `tool_use` blocks in one response run together this tick; the next prompt carries the matching `tool_result` blocks. Do not mix `call_tool` with a terminal decision tool in the same response.\n\
             4. Evidence comes from tool calls. Each `call_tool` result becomes a fresh evidence record that later `emit_output` decisions can cite.\n\
             5. Do not re-emit. Once you have emitted any Output on this run, your next decision must be `retire`. Do not emit a revised, paraphrased, or improved version of a prior Output. Outputs shown in the \"Recent outputs by you on this run\" window were emitted by you and count toward this rule.\n\
             6. Retire is final. After the mandate's required Output has been emitted, `retire` is the only correct decision."
        );
    }

    /// A `persistent` mandate renders the refresh invariant set: clauses 1–4
    /// match the default verbatim, clauses 5–6 swap the one-shot
    /// retire-after-Output rules for the refresh contract, and the
    /// "Do not re-emit" / "Retire is final" wording is gone. `snapshot_empty_bundle`
    /// guards the non-persistent string byte-for-byte (no regression).
    #[test]
    fn snapshot_persistent_mandate_renders_refresh_invariants() {
        let mut m = mandate();
        m.persistent = true;
        let bundle = ContextBundle {
            mandate: m,
            ..empty_bundle()
        };
        let msgs = render(&bundle);
        assert_eq!(msgs.len(), 1, "expected system message only");
        assert_eq!(
            text(&msgs[0]),
            "You are an agent operating under the following mandate:\n\
             \n\
             Watch the FDA holds list and report drug-program risk.\n\
             \n\
             Invariants:\n\
             1. Provenance. Every `emit_output` decision must cite `evidence` ids that resolve in this agent's evidence store. The runtime will reject outputs whose evidence does not resolve.\n\
             2. One decision per turn. Reply by calling exactly one terminal decision tool (`emit_output`, `rewrite_fs`, `idle`, `retire`) OR one or more `call_tool` blocks dispatched together as a single parallel batch.\n\
             3. Parallel call_tool is supported. K `call_tool` `tool_use` blocks in one response run together this tick; the next prompt carries the matching `tool_result` blocks. Do not mix `call_tool` with a terminal decision tool in the same response.\n\
             4. Evidence comes from tool calls. Each `call_tool` result becomes a fresh evidence record that later `emit_output` decisions can cite.\n\
             5. Refresh, don't stop. You are a persistent monitor. After you `emit_output`, choose `idle` to wait for your cadence; on the next scheduled wake, re-research and emit an updated Output that reflects what changed since the last one. The \"Recent outputs by you on this run\" window is your durable memory — build on it rather than re-emitting an unchanged copy.\n\
             6. Do not retire yourself. `retire` is not a valid self-decision for a persistent agent: the runtime stops you only via a retirement signal or your tick budget, and a `retire` decision is treated as `idle`. Keep cycling: research -> emit_output -> idle -> refresh."
        );

        let body = text(&msgs[0]);
        assert!(
            !body.contains("Do not re-emit."),
            "persistent invariants must drop the one-shot re-emit rule"
        );
        assert!(
            !body.contains("Retire is final."),
            "persistent invariants must drop the retire-is-final rule"
        );
    }

    /// Snapshot 2 of 5: single trigger, no outputs, no evidence. Expected
    /// message count = 2 (system + triggers).
    #[test]
    fn snapshot_single_trigger() {
        let bundle = ContextBundle {
            triggers: vec![Trigger::ScheduledWake],
            ..empty_bundle()
        };
        let msgs = render(&bundle);
        assert_eq!(msgs.len(), 2);

        assert_eq!(msgs[1].role, Role::User);
        assert_eq!(
            text(&msgs[1]),
            "# Triggers (1)\n\
             \n\
             - {\"type\":\"scheduled_wake\"}"
        );
    }

    /// Snapshot 3 of 5: mixed triggers exercising every variant in the
    /// `Trigger` enum. Locks the JSON shape the renderer relies on.
    #[test]
    fn snapshot_mixed_triggers() {
        let bundle = ContextBundle {
            triggers: vec![
                Trigger::ScheduledWake,
                Trigger::External {
                    kind: "webhook".into(),
                    payload: json!({"x": 1}),
                },
                Trigger::HumanOverride {
                    op: HumanOp::new(json!({"action": "pause"})),
                },
            ],
            ..empty_bundle()
        };
        let msgs = render(&bundle);
        assert_eq!(msgs.len(), 2);

        assert_eq!(
            text(&msgs[1]),
            "# Triggers (3)\n\
             \n\
             - {\"type\":\"scheduled_wake\"}\n\
             \n\
             - {\"type\":\"external\",\"kind\":\"webhook\",\"payload\":{\"x\":1}}\n\
             \n\
             - {\"type\":\"human_override\",\"op\":{\"action\":\"pause\"}}"
        );
    }

    // ---- cross-agent trigger rendering ---------------------------------

    fn child_ref() -> AgentRef {
        AgentRef::new(
            "graphs/g/agents/agent-7",
            AgentId::new(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()),
        )
    }

    /// Snapshot: a `ChildOutput` trigger renders as a human-readable
    /// bullet that names the child and cites the `OutputId`. Distinct from
    /// the opaque `External` JSON shape.
    #[test]
    fn snapshot_child_output_trigger() {
        let output_id = OutputId::from_hex("ab".repeat(32));
        let bundle = ContextBundle {
            triggers: vec![Trigger::ChildOutput {
                child_ref: child_ref(),
                agent_name: "fda_scraper".into(),
                output_id: output_id.clone(),
            }],
            ..empty_bundle()
        };
        let msgs = render(&bundle);
        assert_eq!(msgs.len(), 2);
        assert_eq!(
            text(&msgs[1]),
            format!(
                "# Triggers (1)\n\
                 \n\
                 - Child output: fda_scraper emitted {output_id}"
            )
        );
    }

    /// Snapshot: a `ChildRetired` trigger renders as a human-readable
    /// bullet that names the child and surfaces the retirement reason.
    #[test]
    fn snapshot_child_retired_trigger() {
        let bundle = ContextBundle {
            triggers: vec![Trigger::ChildRetired {
                child_ref: child_ref(),
                agent_name: "fda_scraper".into(),
                reason: "mandate satisfied".into(),
            }],
            ..empty_bundle()
        };
        let msgs = render(&bundle);
        assert_eq!(msgs.len(), 2);
        assert_eq!(
            text(&msgs[1]),
            "# Triggers (1)\n\
             \n\
             - Child retired: fda_scraper (mandate satisfied)"
        );
    }

    /// The cross-agent variants must render structurally distinct from
    /// `External` — the model needs the child's name as a first-class
    /// signal, not buried behind an opaque JSON payload. This test pins
    /// the distinction by asserting the rendered bullet does NOT look like
    /// the serde JSON form of either variant.
    #[test]
    fn child_output_trigger_is_distinct_from_external() {
        let output_id = OutputId::from_hex("ab".repeat(32));
        let child_bundle = ContextBundle {
            triggers: vec![Trigger::ChildOutput {
                child_ref: child_ref(),
                agent_name: "fda_scraper".into(),
                output_id,
            }],
            ..empty_bundle()
        };
        let external_bundle = ContextBundle {
            triggers: vec![Trigger::External {
                kind: "child_output".into(),
                payload: serde_json::json!({"agent_name": "fda_scraper"}),
            }],
            ..empty_bundle()
        };
        let child = text(&render(&child_bundle)[1]).to_string();
        let external = text(&render(&external_bundle)[1]).to_string();

        // `External` is rendered via the verbatim serde shape — leading
        // `{"type":"external"`. `ChildOutput` must NOT use that shape.
        assert!(
            !child.contains("\"type\":\"child_output\""),
            "ChildOutput rendered using the opaque serde shape: {child}"
        );
        assert!(
            external.contains("\"type\":\"external\""),
            "External lost its serde shape: {external}"
        );
        // The child name is surfaced in plain prose.
        assert!(
            child.contains("Child output: fda_scraper"),
            "agent_name not surfaced: {child}"
        );
        // And the two prompts must not be byte-equal.
        assert_ne!(child, external);
    }

    /// Mixed-trigger snapshot covering every variant in priority order
    /// (human > external > child_output/child_retired > scheduled). Locks
    /// both the per-variant rendering and the fact that the renderer
    /// preserves whatever order the bundle hands it (the queue is
    /// responsible for the priority sort upstream).
    #[test]
    fn snapshot_mixed_triggers_with_cross_agent_variants() {
        let output_id = OutputId::from_hex("cd".repeat(32));
        let bundle = ContextBundle {
            triggers: vec![
                Trigger::HumanOverride {
                    op: HumanOp::new(json!({"action": "pause"})),
                },
                Trigger::External {
                    kind: "webhook".into(),
                    payload: json!({"x": 1}),
                },
                Trigger::ChildOutput {
                    child_ref: child_ref(),
                    agent_name: "fda_scraper".into(),
                    output_id: output_id.clone(),
                },
                Trigger::ChildRetired {
                    child_ref: child_ref(),
                    agent_name: "fda_scraper".into(),
                    reason: "mandate satisfied".into(),
                },
                Trigger::ScheduledWake,
            ],
            ..empty_bundle()
        };
        let msgs = render(&bundle);
        assert_eq!(msgs.len(), 2);
        assert_eq!(
            text(&msgs[1]),
            format!(
                "# Triggers (5)\n\
                 \n\
                 - {{\"type\":\"human_override\",\"op\":{{\"action\":\"pause\"}}}}\n\
                 \n\
                 - {{\"type\":\"external\",\"kind\":\"webhook\",\"payload\":{{\"x\":1}}}}\n\
                 \n\
                 - Child output: fda_scraper emitted {output_id}\n\
                 \n\
                 - Child retired: fda_scraper (mandate satisfied)\n\
                 \n\
                 - {{\"type\":\"scheduled_wake\"}}"
            )
        );
    }

    /// Snapshot 4 of 5: multiple outputs, no triggers, no evidence.
    /// Exercises the outputs window in isolation.
    #[test]
    fn snapshot_multiple_outputs() {
        // Build deterministic evidence ids by hashing a fixed triple.
        let ev1 = EvidenceId::new("echo", &json!({"a": 1}), &json!({"r": 1}));
        let ev2 = EvidenceId::new("echo", &json!({"a": 2}), &json!({"r": 2}));

        let bundle = ContextBundle {
            recent_outputs: vec![
                Output::new("first claim", vec![ev1.clone()], ts()),
                Output::new("second claim", vec![ev1.clone(), ev2.clone()], ts()),
            ],
            ..empty_bundle()
        };
        let msgs = render(&bundle);
        assert_eq!(msgs.len(), 2);

        let expected = format!(
            "# Recent outputs by you on this run (2)\n\
             \n\
             - content: \"first claim\"\n  evidence: [\"{}\"]\n\
             \n\
             - content: \"second claim\"\n  evidence: [\"{}\",\"{}\"]",
            ev1.as_str(),
            ev1.as_str(),
            ev2.as_str(),
        );
        assert_eq!(text(&msgs[1]), expected);
    }

    /// Snapshot 5 of 5: evidence chain — multiple evidence records
    /// representing a sequence of tool calls. Exercises the evidence
    /// window in isolation.
    #[test]
    fn snapshot_evidence_chain() {
        let r1 = EvidenceRecord::new(
            "list_holds",
            json!({"date": "2026-05-06"}),
            json!({"holds": ["X", "Y"]}),
            ts(),
        );
        let r2 = EvidenceRecord::new(
            "fetch_drug",
            json!({"id": "X"}),
            json!({"sponsor": "Acme"}),
            ts(),
        );

        let bundle = ContextBundle {
            recent_evidence: vec![r1.clone(), r2.clone()],
            ..empty_bundle()
        };
        let msgs = render(&bundle);
        assert_eq!(msgs.len(), 2);

        let expected = format!(
            "# Recent evidence (2)\n\
             \n\
             - id: {}\n  tool: list_holds\n  args: {{\"date\":\"2026-05-06\"}}\n  result: {{\"holds\":[\"X\",\"Y\"]}}\n\
             \n\
             - id: {}\n  tool: fetch_drug\n  args: {{\"id\":\"X\"}}\n  result: {{\"sponsor\":\"Acme\"}}",
            r1.id.as_str(),
            r2.id.as_str(),
        );
        assert_eq!(text(&msgs[1]), expected);
    }

    /// Bonus shape: every window populated. Verifies the message order
    /// (system → triggers → outputs → evidence) and the count.
    #[test]
    fn snapshot_all_windows_populated_message_order() {
        let ev = EvidenceRecord::new("echo", json!({"k": 1}), json!({"v": 2}), ts());
        let bundle = ContextBundle {
            mandate: mandate(),
            triggers: vec![Trigger::ScheduledWake],
            recent_outputs: vec![Output::new("draft", vec![ev.id.clone()], ts())],
            recent_evidence: vec![ev],
            open_claims: vec![],
            correction: None,
        };
        let msgs = render(&bundle);

        // 1 system + 3 windows = 4
        assert_eq!(msgs.len(), 4);
        assert_eq!(msgs[0].role, Role::System);
        assert_eq!(msgs[1].role, Role::User);
        assert!(text(&msgs[1]).starts_with("# Triggers"));
        assert_eq!(msgs[2].role, Role::User);
        assert!(text(&msgs[2]).starts_with("# Recent outputs"));
        assert_eq!(msgs[3].role, Role::User);
        assert!(text(&msgs[3]).starts_with("# Recent evidence"));
    }

    /// Correction snapshot: a bundle with `correction = Some(...)` produces
    /// a dedicated user message describing the failure, placed between the
    /// system message and any other windows.
    #[test]
    fn snapshot_correction_only() {
        let bundle = ContextBundle {
            correction: Some(CorrectionContext::new(
                "call_tool: no tool registered under name \"send_email\"",
            )),
            ..empty_bundle()
        };
        let msgs = render(&bundle);
        assert_eq!(msgs.len(), 2, "expected system + correction");

        assert_eq!(msgs[1].role, Role::User);
        assert_eq!(
            text(&msgs[1]),
            "# Previous-attempt failure\n\
             \n\
             The runtime could not satisfy your previous decision: call_tool: no tool registered under name \"send_email\".\n\
             \n\
             Reply by calling exactly one decision tool that addresses the failure."
        );
    }

    /// Position invariant: when both correction and triggers are present,
    /// the correction message appears immediately after system and before
    /// triggers. The model's most actionable signal this tick is "your last
    /// move failed because X" — putting it ahead of fresh triggers keeps
    /// the framing right.
    #[test]
    fn correction_renders_before_triggers_and_other_windows() {
        let ev = EvidenceRecord::new("echo", json!({"k": 1}), json!({"v": 2}), ts());
        let bundle = ContextBundle {
            mandate: mandate(),
            triggers: vec![Trigger::ScheduledWake],
            recent_outputs: vec![Output::new("draft", vec![ev.id.clone()], ts())],
            recent_evidence: vec![ev],
            open_claims: vec![],
            correction: Some(CorrectionContext::new(
                "emit_output: evidence list is empty (provenance contract)",
            )),
        };
        let msgs = render(&bundle);

        // 1 system + 1 correction + 3 windows = 5
        assert_eq!(msgs.len(), 5);
        assert_eq!(msgs[0].role, Role::System);
        assert_eq!(msgs[1].role, Role::User);
        assert!(text(&msgs[1]).starts_with("# Previous-attempt failure"));
        assert_eq!(msgs[2].role, Role::User);
        assert!(text(&msgs[2]).starts_with("# Triggers"));
        assert_eq!(msgs[3].role, Role::User);
        assert!(text(&msgs[3]).starts_with("# Recent outputs"));
        assert_eq!(msgs[4].role, Role::User);
        assert!(text(&msgs[4]).starts_with("# Recent evidence"));
    }

    // ---- open_claims rendering -----------------------------------------

    use crate::fs::{Claim, ClaimStatus};

    fn open_claim(seed: &str, description: &str) -> Claim {
        Claim {
            seed: seed.into(),
            description: description.into(),
            status: ClaimStatus::Open,
            created_at: ts(),
        }
    }

    /// Snapshot: open_claims renders as a `# Open claims` user message
    /// with one bullet per claim, showing `seed` and `description`.
    #[test]
    fn snapshot_open_claims_window() {
        let bundle = ContextBundle {
            open_claims: vec![
                open_claim("phase-2-clearance", "Did drug X pass phase 2?"),
                open_claim("acme-revenue-q3", "What did Acme book in Q3?"),
            ],
            ..empty_bundle()
        };
        let msgs = render(&bundle);
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[1].role, Role::User);
        assert_eq!(
            text(&msgs[1]),
            "# Open claims (2)\n\
             \n\
             - seed: \"phase-2-clearance\"\n  description: \"Did drug X pass phase 2?\"\n\
             \n\
             - seed: \"acme-revenue-q3\"\n  description: \"What did Acme book in Q3?\""
        );
    }

    /// Position: open_claims renders after recent_evidence when both are
    /// present. The model treats recent_outputs/recent_evidence as the
    /// "what just happened" pair; open_claims is separate lifetime state.
    #[test]
    fn open_claims_renders_after_evidence_when_both_present() {
        let ev = EvidenceRecord::new("echo", json!({"k": 1}), json!({"v": 2}), ts());
        let bundle = ContextBundle {
            mandate: mandate(),
            triggers: vec![Trigger::ScheduledWake],
            recent_outputs: vec![Output::new("draft", vec![ev.id.clone()], ts())],
            recent_evidence: vec![ev],
            open_claims: vec![open_claim("c", "d")],
            correction: None,
        };
        let msgs = render(&bundle);
        // 1 system + 4 windows = 5
        assert_eq!(msgs.len(), 5);
        assert!(text(&msgs[1]).starts_with("# Triggers"));
        assert!(text(&msgs[2]).starts_with("# Recent outputs"));
        assert!(text(&msgs[3]).starts_with("# Recent evidence"));
        assert!(text(&msgs[4]).starts_with("# Open claims"));
    }

    /// Empty open_claims must not emit a `# Open claims (0)` placeholder —
    /// the prompt budget is precious and "(none)" surfaces no signal.
    #[test]
    fn empty_open_claims_emits_no_window() {
        let bundle = empty_bundle();
        let msgs = render(&bundle);
        assert_eq!(msgs.len(), 1, "expected system message only");
        assert!(!text(&msgs[0]).contains("Open claims"));
    }

    /// Strings with characters that need JSON escaping (quotes, newlines)
    /// must round-trip safely through `serde_json::to_string` so the
    /// rendered prompt remains parseable as JSON inside the bullet list.
    #[test]
    fn render_escapes_control_chars_in_output_content() {
        let ev = EvidenceId::new("t", &json!({}), &json!({}));
        let bundle = ContextBundle {
            recent_outputs: vec![Output::new(
                "she said \"hi\"\nthen left",
                vec![ev.clone()],
                ts(),
            )],
            ..empty_bundle()
        };
        let msgs = render(&bundle);
        // \" inside the JSON string and \n is the escape sequence — the
        // newline in the literal source must not appear raw.
        assert_eq!(
            text(&msgs[1]),
            format!(
                "# Recent outputs by you on this run (1)\n\n- content: \"she said \\\"hi\\\"\\nthen left\"\n  evidence: [\"{}\"]",
                ev.as_str()
            )
        );
    }
}
