//! Render a `Session` into a `Vec<Message>` the model can consume.
//!
//! Layout: one `system` message (the standing prompt — identity, principles,
//! and rules — with the mandate and tool catalog interpolated in); one `user`
//! message per non-empty seed window (triggers, the FS index); and, once the
//! cycle has taken steps, one `user` message summarizing the
//! `(action, observation)` history so far. Empty windows are dropped to keep
//! the prompt budget tight.
//!
//! This is the *pull* surface: the seed carries an index of filenames, not
//! file bodies. The model fetches contents with `read`/`list`/`search`, and
//! those observations accumulate in the session — which `render` replays
//! each step so the model reasons over its own history.
//!
//! Output is deterministic across runs: time-varying fields are dropped or
//! never rendered; JSON values round-trip through `serde_json::to_string`
//! with the default `BTreeMap`-backed `serde_json::Map`, emitting keys in
//! sorted order. That stability is what makes the snapshot tests viable.

use crate::decision::{Decision, FsIndex, ReconcileSource, Remainder, Seed, Session, Step};
use crate::mandate::Mandate;
use crate::model_client::Message;
use crate::trigger::Trigger;

/// The standing system prompt — agent identity, operating principles, and the
/// hard rules — authored as markdown in `system_prompt.md`. `{{MANDATE}}` and
/// `{{TOOLS}}` are interpolation slots filled per agent by [`render_system`].
const SYSTEM_TEMPLATE: &str = include_str!("system_prompt.md");

/// Render a `Session` into the message list a `ModelClient::complete` call
/// should send.
///
/// The returned `Vec<Message>` is intended to be passed verbatim as
/// `CompleteRequest::messages`. The caller fills `CompleteRequest::tools`
/// with `decide_llm::schema::decision_tools()`.
pub fn render(session: &Session) -> Vec<Message> {
    let Seed {
        mandate,
        triggers,
        index,
    } = &session.seed;
    // At most 4: system + triggers + index + steps.
    let mut out = Vec::with_capacity(4);
    out.push(Message::system(render_system(mandate)));
    if !triggers.is_empty() {
        out.push(Message::user(render_triggers(triggers)));
    }
    out.push(Message::user(render_index(index)));
    if !session.steps.is_empty() {
        out.push(Message::user(render_steps(&session.steps)));
    }
    out
}

/// Build the system message: the standing template with the per-agent mandate
/// and tool catalog interpolated into it.
///
/// The mandate text is interpolated verbatim — the kernel treats it as
/// already-trusted input, sanitized at mandate-creation time. `{{TOOLS}}` is
/// filled before `{{MANDATE}}` so a sentinel appearing in the mandate text is
/// left untouched.
fn render_system(m: &Mandate) -> String {
    SYSTEM_TEMPLATE
        .replace("{{TOOLS}}", &render_tool_catalog(&m.tools))
        .replace("{{MANDATE}}", &m.text)
}

/// Render the per-agent tool catalog: the tool *definitions* the agent is
/// assigned. Assignment is enforced at dispatch — a call to a tool outside
/// this set is rejected — so the catalog states the boundary. The FS-nav
/// steps (`read`/`list`/`search`) are always available and are not listed
/// here.
fn render_tool_catalog(tools: &[String]) -> String {
    if tools.is_empty() {
        return "You have no tools assigned; you cannot call any tool (but `read`, `list`, and `search` over your own files are always available).".to_string();
    }
    format!(
        "You may call only these assigned tools: {}. Each may expose one or more named operations.",
        tools.join(", ")
    )
}

/// Render the trigger window as a bulleted list.
///
/// Most variants are serialized via their existing serde shape — the same
/// shape the kernel uses on the wire — so the prompt cannot drift from the
/// typed enum without a serde test failure elsewhere.
///
/// Cross-agent variants (`ChildOutput`, `ChildRetired`) render as
/// human-readable prose instead: the model needs the child's name as a
/// first-class signal, and an opaque `External`-shaped JSON blob buries that
/// name behind a nested struct.
fn render_triggers(triggers: &[Trigger]) -> String {
    let mut s = format!("# Triggers ({})", triggers.len());
    for t in triggers {
        s.push_str("\n\n- ");
        match t {
            Trigger::ChildOutput {
                child_ref,
                agent_name,
                output_id,
            } => {
                let source = serde_json::to_string(&ReconcileSource {
                    child_ref: child_ref.clone(),
                    output_id: output_id.clone(),
                })
                .expect("ReconcileSource serializes");
                s.push_str(&format!(
                    "Child output: {agent_name} emitted {output_id}. To fold it, pass this exact object in the `reconcile_children` `sources` array: {source}"
                ));
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

/// Render the FS index: filenames only (pointers), never bodies. This is the
/// orienting surface the model navigates from — it `read`s what it needs.
fn render_index(index: &FsIndex) -> String {
    let notes = render_index_bucket(&index.notes, index.notes_more, "notes/");
    let outputs = render_index_bucket(&index.outputs, index.outputs_more, "outputs/");
    format!(
        "# Your files (index)\n\
         \n\
         notes/: {notes}\n\
         outputs/: {outputs}\n\
         \n\
         This index lists only your most recent files by name, not their contents, and not necessarily all of them. Use `read` to fetch a file, `list` a directory to see everything in it, or `search` to find a string across files. When something you need isn't listed here, explore for it — it has not been deleted, just not surfaced."
    )
}

/// Render one index bucket: comma-joined filenames (or `(none)`), with a count
/// of the files beyond the window when there are any. `+N` is an exact count,
/// `N+` a lower bound (the recency index was at capacity).
fn render_index_bucket(files: &[String], more: Remainder, dir: &str) -> String {
    if files.is_empty() {
        return "(none)".to_string();
    }
    let joined = files.join(", ");
    match more {
        Remainder::None => joined,
        Remainder::Exactly(k) => format!("{joined} (+{k} more — `list {dir}` for the full set)"),
        Remainder::AtLeast(k) => format!("{joined} ({k}+ more — `list {dir}` for the full set)"),
    }
}

/// Render the steps taken so far this cycle as a numbered history of
/// `(action, observation)` pairs, so the model reasons over what it has
/// already done and seen rather than repeating work.
fn render_steps(steps: &[Step]) -> String {
    let mut s = format!("# Steps so far this cycle ({})", steps.len());
    for (i, step) in steps.iter().enumerate() {
        s.push_str(&format!(
            "\n\n{}. {}\n   -> ",
            i + 1,
            action_label(&step.action)
        ));
        if !step.observation.ok {
            s.push_str("FAILED: ");
        }
        s.push_str(&step.observation.content);
    }
    s
}

/// One-line label for a repertoire action, used in the step history. Compact
/// and deterministic — the observation carries the detail.
fn action_label(action: &Decision) -> String {
    match action {
        Decision::CallTools { calls } => {
            let names: Vec<&str> = calls.iter().map(|c| c.name.as_str()).collect();
            format!("call_tool: {}", names.join(", "))
        }
        Decision::WriteOutput { .. } => "write_output".to_string(),
        Decision::RewriteFs { .. } => "rewrite_fs".to_string(),
        Decision::Read { path } => format!("read {path}"),
        Decision::List { path } => format!("list {path}"),
        Decision::Search { query, path } => match path {
            Some(p) => format!("search {query:?} in {p}"),
            None => format!("search {query:?}"),
        },
        Decision::Idle { .. } => "idle".to_string(),
        Decision::SpawnChild { agent_name, .. } => format!("spawn_child {agent_name}"),
        Decision::ReconcileChildren { .. } => "reconcile_children".to_string(),
        Decision::RetireChild { .. } => "retire_child".to_string(),
        Decision::ReplaceChild { .. } => "replace_child".to_string(),
    }
}

#[cfg(test)]
mod tests {
    //! Snapshot tests for `render`.
    //!
    //! The per-window helpers (triggers, index, steps) are locked verbatim
    //! here. The system prose lives in `system_prompt.md` — that file is the
    //! reviewable artifact and nothing but `render_system` touches it, so the
    //! system-message test asserts structure and the interpolation seams
    //! rather than re-pasting the whole prompt.

    use super::*;
    use crate::agent_ref::{AgentId, AgentRef};
    use crate::decision::{ClaimSeed, Decision, FsIndex, Observation, Seed, Session, ToolCall};
    use crate::mandate::{Mandate, OutputId};
    use crate::model_client::{ContentBlock, Role};
    use crate::trigger::{HumanOp, Trigger};
    use serde_json::json;
    use std::time::Duration;
    use uuid::Uuid;

    fn mandate() -> Mandate {
        Mandate::new(
            "Watch the FDA holds list and report drug-program risk.",
            Duration::from_secs(60),
            Some(100),
        )
    }

    /// Extract the single text block from a renderer-produced message.
    fn text(m: &Message) -> &str {
        match m.content.as_slice() {
            [ContentBlock::Text { text }] => text,
            other => panic!("expected single text block, got {other:?}"),
        }
    }

    fn seed_with(triggers: Vec<Trigger>, index: FsIndex) -> Seed {
        Seed::new(mandate(), triggers, index)
    }

    /// A bare seed: no triggers, empty index.
    fn bare_session() -> Session {
        Session::new(seed_with(vec![], FsIndex::default()))
    }

    // ---- tool catalog ---------------------------------------------------

    #[test]
    fn system_message_lists_assigned_tools() {
        let mut m = mandate();
        m.tools = vec!["echo".into(), "web-search".into()];
        let session = Session::new(Seed::new(m, vec![], FsIndex::default()));
        let msgs = render(&session);
        let sys = text(&msgs[0]);
        assert!(
            sys.contains("You may call only these assigned tools: echo, web-search"),
            "system message must list the assigned tools, got: {sys}"
        );
    }

    #[test]
    fn system_message_notes_no_tools_when_unassigned() {
        let sys = render_system(&mandate());
        assert!(
            sys.contains("no tools assigned"),
            "system message must state when no tools are assigned, got: {sys}"
        );
        // FS-nav is always available even with no assigned tools.
        assert!(sys.contains("`read`, `list`, and `search`"));
    }

    // ---- shape invariants -----------------------------------------------

    #[test]
    fn render_always_starts_with_system_then_index() {
        let msgs = render(&bare_session());
        // system + index (no triggers, no steps).
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[0].role, Role::System);
        assert!(text(&msgs[0]).starts_with("# You are a Coral agent"));
        assert_eq!(msgs[1].role, Role::User);
        assert!(text(&msgs[1]).starts_with("# Your files (index)"));
    }

    #[test]
    fn render_is_deterministic_across_calls() {
        let mut session = Session::new(seed_with(
            vec![Trigger::ScheduledWake],
            FsIndex {
                notes: vec!["plan.md".into()],
                outputs: vec!["abc.json".into()],
                ..Default::default()
            },
        ));
        session.push(
            Decision::Read {
                path: "notes/plan.md".into(),
            },
            Observation::ok("the plan body"),
        );
        let a = render(&session);
        let b = render(&session);
        assert_eq!(a.len(), b.len());
        for (x, y) in a.iter().zip(b.iter()) {
            assert_eq!(text(x), text(y));
        }
    }

    // ---- system message snapshot ----------------------------------------

    #[test]
    fn snapshot_system_message() {
        let sys = render_system(&mandate());
        assert!(
            sys.starts_with("# You are a Coral agent"),
            "system message must open with the identity header, got: {sys}"
        );
        assert!(
            sys.contains(
                "## Your mandate\n\nWatch the FDA holds list and report drug-program risk.\n\n## Your tools\n\n"
            ),
            "the mandate must be interpolated between its header and the tools section, got: {sys}"
        );
        assert!(
            sys.contains(
                "You have no tools assigned; you cannot call any tool (but `read`, `list`, and `search` over your own files are always available).\n\n## What a good Output is"
            ),
            "the tool catalog must be interpolated ahead of the Output section, got: {sys}"
        );
        assert!(
            !sys.contains("{{"),
            "no interpolation slot may be left unfilled, got: {sys}"
        );
        for rule in [
            "One step per turn.",
            "Pull what you need.",
            "Cite your evidence.",
            "Refresh, don't stop.",
            "Idle ends the cycle.",
            "Fold child reports",
            "Keep your status note current.",
        ] {
            assert!(
                sys.contains(rule),
                "missing operating rule {rule:?}, got: {sys}"
            );
        }
    }

    #[test]
    fn system_prompt_names_the_pinned_status_note_path() {
        assert!(
            SYSTEM_TEMPLATE.contains(crate::agent_core::STATUS_NOTE_PATH),
            "the status-note rule must reference the pinned path so the prompt and the seed pin cannot drift"
        );
    }

    // ---- index snapshot -------------------------------------------------

    #[test]
    fn snapshot_empty_index() {
        let msgs = render(&bare_session());
        assert_eq!(
            text(&msgs[1]),
            "# Your files (index)\n\
             \n\
             notes/: (none)\n\
             outputs/: (none)\n\
             \n\
             This index lists only your most recent files by name, not their contents, and not necessarily all of them. Use `read` to fetch a file, `list` a directory to see everything in it, or `search` to find a string across files. When something you need isn't listed here, explore for it — it has not been deleted, just not surfaced."
        );
    }

    #[test]
    fn snapshot_populated_index() {
        let session = Session::new(seed_with(
            vec![],
            FsIndex {
                notes: vec!["plan.md".into(), "scratch.md".into()],
                outputs: vec!["deadbeef.json".into()],
                ..Default::default()
            },
        ));
        let msgs = render(&session);
        assert_eq!(
            text(&msgs[1]),
            "# Your files (index)\n\
             \n\
             notes/: plan.md, scratch.md\n\
             outputs/: deadbeef.json\n\
             \n\
             This index lists only your most recent files by name, not their contents, and not necessarily all of them. Use `read` to fetch a file, `list` a directory to see everything in it, or `search` to find a string across files. When something you need isn't listed here, explore for it — it has not been deleted, just not surfaced."
        );
    }

    #[test]
    fn snapshot_index_signposts_count_when_a_bucket_is_truncated() {
        let session = Session::new(seed_with(
            vec![],
            FsIndex {
                notes: vec!["STATUS.md".into(), "recent.md".into()],
                outputs: vec!["deadbeef.json".into()],
                notes_more: Remainder::Exactly(3),
                outputs_more: Remainder::None,
            },
        ));
        let msgs = render(&session);
        let body = text(&msgs[1]);
        assert!(
            body.contains(
                "notes/: STATUS.md, recent.md (+3 more — `list notes/` for the full set)"
            ),
            "an exact overflow renders as `+N more`; got:\n{body}"
        );
        assert!(
            body.contains("outputs/: deadbeef.json\n"),
            "a complete bucket must not signpost more; got:\n{body}"
        );
    }

    #[test]
    fn render_index_uses_plus_notation_for_a_lower_bound() {
        let session = Session::new(seed_with(
            vec![],
            FsIndex {
                notes: vec!["a.md".into()],
                outputs: vec![],
                notes_more: Remainder::AtLeast(56),
                outputs_more: Remainder::None,
            },
        ));
        let msgs = render(&session);
        let body = text(&msgs[1]);
        assert!(
            body.contains("notes/: a.md (56+ more — `list notes/` for the full set)"),
            "a lower bound renders as `N+ more`; got:\n{body}"
        );
    }

    // ---- trigger snapshots ----------------------------------------------

    #[test]
    fn snapshot_single_trigger() {
        let session = Session::new(seed_with(vec![Trigger::ScheduledWake], FsIndex::default()));
        let msgs = render(&session);
        // system + triggers + index.
        assert_eq!(msgs.len(), 3);
        assert_eq!(msgs[1].role, Role::User);
        assert_eq!(
            text(&msgs[1]),
            "# Triggers (1)\n\
             \n\
             - {\"type\":\"scheduled_wake\"}"
        );
    }

    #[test]
    fn snapshot_mixed_triggers() {
        let session = Session::new(seed_with(
            vec![
                Trigger::ScheduledWake,
                Trigger::External {
                    kind: "webhook".into(),
                    payload: json!({"x": 1}),
                },
                Trigger::HumanOverride {
                    op: HumanOp::new(json!({"action": "pause"})),
                },
            ],
            FsIndex::default(),
        ));
        let msgs = render(&session);
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

    fn child_ref() -> AgentRef {
        AgentRef::new(
            "graphs/g/agents/agent-7",
            AgentId::new(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()),
        )
    }

    #[test]
    fn snapshot_child_output_trigger() {
        let output_id = OutputId::from_hex("ab".repeat(32));
        let session = Session::new(seed_with(
            vec![Trigger::ChildOutput {
                child_ref: child_ref(),
                agent_name: "fda_scraper".into(),
                output_id: output_id.clone(),
            }],
            FsIndex::default(),
        ));
        let msgs = render(&session);
        let source = serde_json::to_string(&ReconcileSource {
            child_ref: child_ref(),
            output_id: output_id.clone(),
        })
        .expect("ReconcileSource serializes");
        assert_eq!(
            text(&msgs[1]),
            format!(
                "# Triggers (1)\n\n- Child output: fda_scraper emitted {output_id}. \
                 To fold it, pass this exact object in the `reconcile_children` `sources` array: {source}"
            )
        );
    }

    #[test]
    fn snapshot_child_retired_trigger() {
        let session = Session::new(seed_with(
            vec![Trigger::ChildRetired {
                child_ref: child_ref(),
                agent_name: "fda_scraper".into(),
                reason: "mandate satisfied".into(),
            }],
            FsIndex::default(),
        ));
        let msgs = render(&session);
        assert_eq!(
            text(&msgs[1]),
            "# Triggers (1)\n\
             \n\
             - Child retired: fda_scraper (mandate satisfied)"
        );
    }

    #[test]
    fn child_output_trigger_is_distinct_from_external() {
        let output_id = OutputId::from_hex("ab".repeat(32));
        let child = Session::new(seed_with(
            vec![Trigger::ChildOutput {
                child_ref: child_ref(),
                agent_name: "fda_scraper".into(),
                output_id,
            }],
            FsIndex::default(),
        ));
        let external = Session::new(seed_with(
            vec![Trigger::External {
                kind: "child_output".into(),
                payload: json!({"agent_name": "fda_scraper"}),
            }],
            FsIndex::default(),
        ));
        let child_txt = text(&render(&child)[1]).to_string();
        let external_txt = text(&render(&external)[1]).to_string();
        assert!(!child_txt.contains("\"type\":\"child_output\""));
        assert!(external_txt.contains("\"type\":\"external\""));
        assert!(child_txt.contains("Child output: fda_scraper"));
        assert_ne!(child_txt, external_txt);
    }

    // ---- step-history snapshots -----------------------------------------

    #[test]
    fn snapshot_step_history() {
        let mut session = Session::new(seed_with(vec![], FsIndex::default()));
        session.push(
            Decision::Read {
                path: "notes/plan.md".into(),
            },
            Observation::ok("the standing plan"),
        );
        session.push(
            Decision::CallTools {
                calls: vec![ToolCall::new(
                    "echo",
                    json!({"msg": "hi"}),
                    ClaimSeed::new("s"),
                )],
            },
            Observation::err("call_tool \"echo\" failed: boom"),
        );
        let msgs = render(&session);
        // system + index + steps (no triggers).
        assert_eq!(msgs.len(), 3);
        assert!(text(&msgs[1]).starts_with("# Your files (index)"));
        assert_eq!(
            text(&msgs[2]),
            "# Steps so far this cycle (2)\n\
             \n\
             1. read notes/plan.md\n   -> the standing plan\n\
             \n\
             2. call_tool: echo\n   -> FAILED: call_tool \"echo\" failed: boom"
        );
    }

    #[test]
    fn action_label_covers_every_variant() {
        assert_eq!(
            action_label(&Decision::Read {
                path: "notes/a.md".into()
            }),
            "read notes/a.md"
        );
        assert_eq!(
            action_label(&Decision::List {
                path: "notes/".into()
            }),
            "list notes/"
        );
        assert_eq!(
            action_label(&Decision::Search {
                query: "q".into(),
                path: Some("notes/".into())
            }),
            "search \"q\" in notes/"
        );
        assert_eq!(
            action_label(&Decision::Search {
                query: "q".into(),
                path: None
            }),
            "search \"q\""
        );
        assert_eq!(
            action_label(&Decision::WriteOutput {
                body: "x".into(),
                citations: vec![]
            }),
            "write_output"
        );
        assert_eq!(
            action_label(&Decision::RewriteFs { ops: vec![] }),
            "rewrite_fs"
        );
    }

    // ---- full shape: all windows ----------------------------------------

    #[test]
    fn render_message_order_is_system_triggers_index_steps() {
        let mut session = Session::new(seed_with(
            vec![Trigger::ScheduledWake],
            FsIndex {
                notes: vec!["a.md".into()],
                outputs: vec![],
                ..Default::default()
            },
        ));
        session.push(
            Decision::List {
                path: "notes/".into(),
            },
            Observation::ok("a.md"),
        );
        let msgs = render(&session);
        assert_eq!(msgs.len(), 4);
        assert_eq!(msgs[0].role, Role::System);
        assert!(text(&msgs[1]).starts_with("# Triggers"));
        assert!(text(&msgs[2]).starts_with("# Your files (index)"));
        assert!(text(&msgs[3]).starts_with("# Steps so far this cycle"));
    }
}
