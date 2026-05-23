//! Tool-use schema + parser for `Decision`.
//!
//! Design **#1** (one tool per `Decision` variant): the model sees five
//! tools — `call_tool`, `emit_output`, `rewrite_fs`, `idle`, `retire` — one
//! per `Decision` variant tag. The variant is fixed by the tool name, so
//! the model can't confuse the discriminator with a payload field. This
//! reads as more drift-resistant than a single `emit_decision` tool with a
//! tagged-union argument because tool selection is the part of the
//! tool-use contract providers fine-tune on hardest, and per-tool
//! `input_schema`s give us provider-side validation per variant.
//!
//! The parser leans on `Decision`'s existing
//! `#[serde(tag = "type", rename_all = "snake_case")]` instead of
//! reimplementing five field validators by hand: it injects the variant tag
//! and feeds the resulting JSON object through `serde_json::from_value`.
//! This also picks up the `duration_ms` / `transparent` helpers already on
//! the relevant fields, so the schema can never silently drift from
//! `decision.rs` without a compile or test failure.

use crate::decision::{ClaimSeed, Decision, ToolCall as DecisionToolCall};
use crate::model_client::{ToolCall, ToolSpec};
use serde_json::{json, Value};
use thiserror::Error;

/// Tool name → matching `Decision` variant tag (`#[serde(tag = "type")]`
/// value). Listed in the same order as the `Decision` enum so a reviewer
/// can scan the two side by side.
const TOOL_NAMES: &[&str] = &["call_tool", "emit_output", "rewrite_fs", "idle", "retire"];

/// Build the `ToolSpec` list to publish to the model via
/// `CompleteRequest::tools`.
///
/// Schemas are deliberately loose where `Decision`'s serde derive already
/// validates the shape (e.g. `FsOp`'s internally-tagged `op` field): we
/// describe the field as `object` and let `parse_decision` surface a
/// structured error if the model emits the wrong inner shape. Tighter JSON
/// Schema would duplicate `decision.rs` and drift from it under change.
pub fn decision_tools() -> Vec<ToolSpec> {
    vec![
        ToolSpec {
            name: "call_tool".into(),
            description: "Express the decision to invoke a runtime tool. The runtime — \
                 not this call — performs the dispatch; this tool only \
                 records the agent's intent for the next tick."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Name of the runtime tool to invoke."
                    },
                    "args": {
                        "description": "JSON arguments for the tool. Shape \
                                        is tool-specific."
                    },
                    "claim_seed": {
                        "type": "string",
                        "description": "Opaque seed the agent picks so the \
                                        resulting evidence can be linked \
                                        back to the claim it supports."
                    }
                },
                "required": ["name", "args", "claim_seed"]
            }),
        },
        ToolSpec {
            name: "emit_output".into(),
            description: "Express the decision to emit a finished output. Every id in \
                 `evidence` must resolve in the agent's evidence store; the \
                 runtime will refuse to persist the output otherwise."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "content": { "type": "string" },
                    "evidence": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Hex-encoded evidence ids."
                    }
                },
                "required": ["content", "evidence"]
            }),
        },
        ToolSpec {
            name: "rewrite_fs".into(),
            description: "Express the decision to mutate the per-agent filesystem. \
                 Each op is `{op: \"write_file\", path, content}` or \
                 `{op: \"delete_file\", path}`."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "ops": {
                        "type": "array",
                        "items": { "type": "object" }
                    }
                },
                "required": ["ops"]
            }),
        },
        ToolSpec {
            name: "idle".into(),
            description: "Express the decision to idle. The scheduler waits at least \
                 `next_after` milliseconds before the next idle wake."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "next_after": {
                        "type": "integer",
                        "minimum": 0,
                        "description": "Milliseconds to wait."
                    }
                },
                "required": ["next_after"]
            }),
        },
        ToolSpec {
            name: "retire".into(),
            description: "Express the decision to stop running. The reason is \
                 persisted so retirement is auditable."
                .into(),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "reason": { "type": "string" }
                },
                "required": ["reason"]
            }),
        },
    ]
}

/// Structured failure mode from `parse_decision`. Each variant carries
/// enough context that a future correction loop (JAR2-19) can quote the
/// failure back to the model in a corrective system message.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DecisionParseError {
    /// The model returned no tool call where at least one was expected.
    #[error("expected at least one tool call, got none")]
    NoCalls,
    /// The model returned multiple tool calls that mix `call_tool` (the
    /// parallel-tool path) with one of the terminal decision tools
    /// (`emit_output`, `rewrite_fs`, `idle`, `retire`). Terminal
    /// decisions cannot batch with other calls in the same tick.
    #[error("mixed decision tools in one response: {names:?}")]
    MixedDecisionTools { names: Vec<String> },
    /// The model returned multiple instances of a terminal decision tool
    /// (e.g. two `retire` blocks in the same response). Terminal
    /// decisions are singular by construction.
    #[error("expected exactly one `{tool}` call, got {count}")]
    DuplicateTerminalTool { tool: String, count: usize },
    /// The tool name does not correspond to any `Decision` variant.
    #[error("unknown tool: {0}")]
    UnknownTool(String),
    /// `arguments` was not a JSON object.
    #[error("tool {tool}: arguments must be a JSON object")]
    ArgumentsNotObject { tool: String },
    /// A required field on the tool's input schema was absent.
    #[error("tool {tool}: missing required field `{field}`")]
    MissingField { tool: String, field: String },
    /// A required field was present but failed to deserialize into the
    /// expected `Decision` variant shape.
    #[error("tool {tool}: invalid value for `{field}`: {reason}")]
    InvalidValue {
        tool: String,
        field: String,
        reason: String,
    },
}

/// Convert a vendor-normalized list of tool calls into a `Decision`.
///
/// The slice shape is the contract the parser cares about — `ToolCall` is
/// already vendor-neutral (Anthropic and Cohere both reduce to
/// `{ id, name, arguments }`). Vendor-specific normalization (e.g.
/// Cohere's `parameters` field) is the responsibility of the relevant
/// `ModelClient` impl, not this layer.
///
/// Multi-call shape: when every entry names the `call_tool` decision
/// tool, the parser folds them into a single `Decision::CallTools(vec![...])`.
/// The vendor `ToolCall.id` (the `tool_use.id` on the wire) propagates
/// into each `decision::ToolCall.tool_use_id` so the run loop can stage
/// the paired `tool_result` blocks for the next prompt bundle. Terminal
/// decision tools (`emit_output`, `rewrite_fs`, `idle`, `retire`)
/// remain singular: a response that includes any terminal tool alongside
/// another call (terminal or `call_tool`) fails as `MixedDecisionTools`
/// rather than silently discarding the extras.
pub fn parse_decision(calls: &[ToolCall]) -> Result<Decision, DecisionParseError> {
    if calls.is_empty() {
        return Err(DecisionParseError::NoCalls);
    }

    // Validate every name up front so an unknown tool name surfaces as
    // `UnknownTool` regardless of where it sits in the batch (the
    // single-call path used to do this implicitly).
    for c in calls {
        if !TOOL_NAMES.contains(&c.name.as_str()) {
            return Err(DecisionParseError::UnknownTool(c.name.clone()));
        }
    }

    // Mixed-shape detection. If any call is `call_tool` and at least one
    // other call is a terminal tool — or vice versa — the batch is
    // malformed: terminals are singular, and mixing them with parallel
    // calls is never a valid single-tick decision.
    let any_call_tool = calls.iter().any(|c| c.name == "call_tool");
    let any_terminal = calls.iter().any(|c| c.name != "call_tool");
    if any_call_tool && any_terminal {
        let names: Vec<String> = calls.iter().map(|c| c.name.clone()).collect();
        return Err(DecisionParseError::MixedDecisionTools { names });
    }
    if any_terminal && calls.len() > 1 {
        // Two terminals (e.g. two `retire` blocks) — also invalid.
        return Err(DecisionParseError::DuplicateTerminalTool {
            tool: calls[0].name.clone(),
            count: calls.len(),
        });
    }

    if any_call_tool {
        // Parallel-call path: fold every `call_tool` into a single
        // `Decision::CallTools(vec![...])`. Vendor `ToolCall.id` becomes
        // `decision::ToolCall.tool_use_id` so the dispatch layer can
        // emit paired `tool_result` blocks next tick.
        let mut tool_calls = Vec::with_capacity(calls.len());
        for c in calls {
            let args = parse_call_tool_args(c)?;
            tool_calls.push(DecisionToolCall::with_tool_use_id(
                args.name,
                args.args,
                args.claim_seed,
                c.id.clone(),
            ));
        }
        return Ok(Decision::CallTools { calls: tool_calls });
    }

    // Single terminal-tool path.
    let call = &calls[0];
    let Value::Object(args) = &call.arguments else {
        return Err(DecisionParseError::ArgumentsNotObject {
            tool: call.name.clone(),
        });
    };

    // Required-field check is up-front so the structured "missing field"
    // error fires cleanly before serde gets a chance to complain about
    // shape. Lists below mirror the variant fields in `decision.rs`.
    let required: &[&str] = match call.name.as_str() {
        "emit_output" => &["content", "evidence"],
        "rewrite_fs" => &["ops"],
        "idle" => &["next_after"],
        "retire" => &["reason"],
        _ => unreachable!("guarded by mixed/any_call_tool checks above"),
    };
    for field in required {
        if !args.contains_key(*field) {
            return Err(DecisionParseError::MissingField {
                tool: call.name.clone(),
                field: (*field).into(),
            });
        }
    }

    // Re-tag the arguments as a `Decision` and let serde do the actual
    // shape validation. The `type` injection mirrors the `#[serde(tag =
    // "type")]` on `Decision`; tool name == variant tag by construction
    // for the terminal tools.
    let mut tagged = args.clone();
    tagged.insert("type".into(), Value::String(call.name.clone()));

    serde_json::from_value::<Decision>(Value::Object(tagged)).map_err(|e| {
        DecisionParseError::InvalidValue {
            tool: call.name.clone(),
            field: serde_path_or(&e),
            reason: e.to_string(),
        }
    })
}

/// Inner shape of one `call_tool` decision-tool invocation, extracted from
/// the model's tool-use payload. The fields mirror
/// `decision::ToolCall` minus the vendor `tool_use_id` (which the caller
/// pulls from `ToolCall.id`).
struct ParsedCallToolArgs {
    name: String,
    args: serde_json::Value,
    claim_seed: ClaimSeed,
}

fn parse_call_tool_args(call: &ToolCall) -> Result<ParsedCallToolArgs, DecisionParseError> {
    let Value::Object(args) = &call.arguments else {
        return Err(DecisionParseError::ArgumentsNotObject {
            tool: call.name.clone(),
        });
    };
    for field in ["name", "args", "claim_seed"] {
        if !args.contains_key(field) {
            return Err(DecisionParseError::MissingField {
                tool: call.name.clone(),
                field: field.into(),
            });
        }
    }
    let name = args
        .get("name")
        .and_then(Value::as_str)
        .ok_or_else(|| DecisionParseError::InvalidValue {
            tool: call.name.clone(),
            field: "name".into(),
            reason: "expected string".into(),
        })?
        .to_string();
    let inner_args = args.get("args").cloned().unwrap_or(Value::Null);
    let claim_seed_str = args
        .get("claim_seed")
        .and_then(Value::as_str)
        .ok_or_else(|| DecisionParseError::InvalidValue {
            tool: call.name.clone(),
            field: "claim_seed".into(),
            reason: "expected string".into(),
        })?
        .to_string();
    Ok(ParsedCallToolArgs {
        name,
        args: inner_args,
        claim_seed: ClaimSeed::new(claim_seed_str),
    })
}

/// Best-effort field name to attach to an `InvalidValue` error.
///
/// `serde_json::Error` does not surface a structured path; the message
/// usually reads `... at line L column C` or names the offending field via
/// `invalid type: ... expected ...`. Without re-parsing the message we
/// can't reliably extract the field, so we fall back to `<unknown>` and
/// let `reason` carry the detail. The future correction loop only needs
/// `reason` to be human-parseable.
fn serde_path_or(_e: &serde_json::Error) -> String {
    "<unknown>".into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decision::{ClaimSeed, FsOp, ToolCall as DecisionToolCall};
    use crate::evidence::EvidenceId;
    use serde_json::json;
    use std::time::Duration;

    fn call(name: &str, arguments: Value) -> ToolCall {
        ToolCall {
            id: format!("toolu_{name}"),
            name: name.into(),
            arguments,
        }
    }

    // ---- decision_tools() metadata --------------------------------------

    #[test]
    fn decision_tools_has_one_entry_per_variant() {
        let tools = decision_tools();
        assert_eq!(tools.len(), TOOL_NAMES.len());
        let names: Vec<&str> = tools.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, TOOL_NAMES);
    }

    #[test]
    fn decision_tools_have_nonempty_descriptions_and_object_schemas() {
        for t in decision_tools() {
            assert!(
                !t.description.is_empty(),
                "{} has empty description",
                t.name
            );
            assert_eq!(
                t.input_schema.get("type"),
                Some(&json!("object")),
                "{} input_schema is not an object",
                t.name
            );
        }
    }

    // ---- happy-path round trips: one per Decision variant ---------------

    #[test]
    fn parse_single_call_tool_round_trips_as_one_element_call_tools() {
        let tc = call(
            "call_tool",
            json!({
                "name": "echo",
                "args": {"msg": "hi"},
                "claim_seed": "seed-1"
            }),
        );
        // The vendor `ToolCall.id` is "toolu_call_tool" (set by `call`); the
        // parser must carry it through as the decision-side `tool_use_id`.
        let d = parse_decision(&[tc]).unwrap();
        assert_eq!(
            d,
            Decision::CallTools {
                calls: vec![DecisionToolCall::with_tool_use_id(
                    "echo",
                    json!({"msg": "hi"}),
                    ClaimSeed::new("seed-1"),
                    "toolu_call_tool",
                )]
            }
        );
    }

    #[test]
    fn parse_k_call_tool_blocks_folds_into_one_call_tools_decision() {
        // K=3 parallel call_tool blocks → single Decision::CallTools. The
        // wire-side `id` on each becomes the per-call `tool_use_id`,
        // load-bearing for the next prompt's `tool_result` pairing.
        let calls = vec![
            ToolCall {
                id: "toolu_a".into(),
                name: "call_tool".into(),
                arguments: json!({
                    "name": "read",
                    "args": {"path": "a.md"},
                    "claim_seed": "seed-a",
                }),
            },
            ToolCall {
                id: "toolu_b".into(),
                name: "call_tool".into(),
                arguments: json!({
                    "name": "read",
                    "args": {"path": "b.md"},
                    "claim_seed": "seed-b",
                }),
            },
            ToolCall {
                id: "toolu_c".into(),
                name: "call_tool".into(),
                arguments: json!({
                    "name": "read",
                    "args": {"path": "c.md"},
                    "claim_seed": "seed-c",
                }),
            },
        ];
        let d = parse_decision(&calls).unwrap();
        match d {
            Decision::CallTools { calls: tc } => {
                assert_eq!(tc.len(), 3);
                assert_eq!(tc[0].name, "read");
                assert_eq!(tc[0].args, json!({"path": "a.md"}));
                assert_eq!(tc[0].claim_seed, ClaimSeed::new("seed-a"));
                assert_eq!(tc[0].tool_use_id.as_deref(), Some("toolu_a"));
                assert_eq!(tc[2].tool_use_id.as_deref(), Some("toolu_c"));
            }
            other => panic!("expected CallTools, got {other:?}"),
        }
    }

    #[test]
    fn parse_emit_output_round_trips() {
        let ev = EvidenceId::new("echo", &json!({"a": 1}), &json!({"r": 1}));
        let tc = call(
            "emit_output",
            json!({
                "content": "hello",
                "evidence": [ev.as_str()]
            }),
        );
        let d = parse_decision(&[tc]).unwrap();
        assert_eq!(
            d,
            Decision::EmitOutput {
                content: "hello".into(),
                evidence: vec![ev],
            }
        );
    }

    #[test]
    fn parse_emit_output_accepts_empty_evidence() {
        let tc = call("emit_output", json!({"content": "draft", "evidence": []}));
        let d = parse_decision(&[tc]).unwrap();
        assert_eq!(
            d,
            Decision::EmitOutput {
                content: "draft".into(),
                evidence: vec![],
            }
        );
    }

    #[test]
    fn parse_rewrite_fs_round_trips() {
        let tc = call(
            "rewrite_fs",
            json!({
                "ops": [
                    {"op": "write_file", "path": "notes/a.md", "content": "hi"},
                    {"op": "delete_file", "path": "notes/old.md"}
                ]
            }),
        );
        let d = parse_decision(&[tc]).unwrap();
        assert_eq!(
            d,
            Decision::RewriteFs {
                ops: vec![
                    FsOp::WriteFile {
                        path: "notes/a.md".into(),
                        content: "hi".into(),
                    },
                    FsOp::DeleteFile {
                        path: "notes/old.md".into(),
                    },
                ],
            }
        );
    }

    #[test]
    fn parse_idle_round_trips_ms() {
        let tc = call("idle", json!({"next_after": 2500}));
        let d = parse_decision(&[tc]).unwrap();
        assert_eq!(
            d,
            Decision::Idle {
                next_after: Duration::from_millis(2500),
            }
        );
    }

    #[test]
    fn parse_retire_round_trips() {
        let tc = call("retire", json!({"reason": "done"}));
        let d = parse_decision(&[tc]).unwrap();
        assert_eq!(
            d,
            Decision::Retire {
                reason: "done".into(),
            }
        );
    }

    #[test]
    fn every_decision_tools_name_parses() {
        // Round-trip the metadata: every name `decision_tools()` advertises
        // is one `parse_decision` accepts. Catches drift between the schema
        // list and the parser's allowed-names table. `call_tool` folds into
        // the `call_tools` variant tag (the parallel-tool shape); the other
        // four map one-to-one with their own variant tags.
        for spec in decision_tools() {
            let minimal: Value = match spec.name.as_str() {
                "call_tool" => json!({
                    "name": "noop",
                    "args": {},
                    "claim_seed": "s"
                }),
                "emit_output" => json!({"content": "", "evidence": []}),
                "rewrite_fs" => json!({"ops": []}),
                "idle" => json!({"next_after": 0}),
                "retire" => json!({"reason": "ok"}),
                other => panic!("unhandled tool {other}"),
            };
            let tc = call(&spec.name, minimal);
            let d = parse_decision(&[tc]).expect(&spec.name);
            let back = serde_json::to_value(&d).unwrap();
            let expected_variant = if spec.name == "call_tool" {
                "call_tools"
            } else {
                spec.name.as_str()
            };
            assert_eq!(
                back.get("type").and_then(Value::as_str),
                Some(expected_variant),
                "tool {} should parse to variant {expected_variant}",
                spec.name,
            );
        }
    }

    // ---- vendor-shape fixture (Cohere) ----------------------------------

    #[test]
    fn parse_accepts_cohere_shaped_tool_call() {
        // Cohere's tool-call wire shape reduces to the same vendor-neutral
        // `{id, name, arguments}` the trait surface (JAR2-14) uses; vendor
        // adapters do any field-name normalization (e.g. Cohere's
        // `parameters` → `arguments`) before this layer ever sees the
        // call. We exercise that property here by constructing a fixture
        // shaped the way a Cohere `ModelClient` impl would emit it; the
        // parser stays vendor-agnostic because its input is already
        // normalized.
        let cohere_style = ToolCall {
            id: "tool_call_abc".into(),
            name: "idle".into(),
            arguments: json!({"next_after": 1000}),
        };
        let d = parse_decision(&[cohere_style]).unwrap();
        assert_eq!(
            d,
            Decision::Idle {
                next_after: Duration::from_millis(1000),
            }
        );
    }

    // ---- error variants -------------------------------------------------

    #[test]
    fn parse_no_calls_errors() {
        let err = parse_decision(&[]).unwrap_err();
        assert_eq!(err, DecisionParseError::NoCalls);
    }

    #[test]
    fn parse_mixed_call_tool_and_terminal_errors() {
        // `call_tool` is the parallel-call shape; mixing it with any
        // terminal decision (`emit_output`, `rewrite_fs`, `idle`,
        // `retire`) is never a valid single-tick decision.
        let err = parse_decision(&[
            call(
                "call_tool",
                json!({"name": "echo", "args": {}, "claim_seed": "s"}),
            ),
            call("retire", json!({"reason": "stop"})),
        ])
        .unwrap_err();
        assert!(
            matches!(err, DecisionParseError::MixedDecisionTools { ref names }
                if names == &["call_tool".to_string(), "retire".to_string()]),
            "got {err:?}"
        );
    }

    #[test]
    fn parse_duplicate_terminal_tools_errors() {
        // Two `retire` blocks in one response — terminals are singular.
        let err = parse_decision(&[
            call("retire", json!({"reason": "a"})),
            call("retire", json!({"reason": "b"})),
        ])
        .unwrap_err();
        assert!(
            matches!(err, DecisionParseError::DuplicateTerminalTool { ref tool, count: 2 }
                if tool == "retire"),
            "got {err:?}"
        );
    }

    #[test]
    fn parse_unknown_tool_errors() {
        let err = parse_decision(&[call("send_email", json!({}))]).unwrap_err();
        assert_eq!(err, DecisionParseError::UnknownTool("send_email".into()));
    }

    #[test]
    fn parse_missing_field_errors() {
        let err = parse_decision(&[call("retire", json!({}))]).unwrap_err();
        assert_eq!(
            err,
            DecisionParseError::MissingField {
                tool: "retire".into(),
                field: "reason".into(),
            }
        );
    }

    #[test]
    fn parse_invalid_value_errors() {
        // `next_after` must deserialize as an integer; a string is a shape
        // error that surfaces as `InvalidValue`.
        let err = parse_decision(&[call("idle", json!({"next_after": "soon"}))]).unwrap_err();
        match err {
            DecisionParseError::InvalidValue { tool, reason, .. } => {
                assert_eq!(tool, "idle");
                assert!(!reason.is_empty(), "reason should carry serde detail");
            }
            other => panic!("expected InvalidValue, got {other:?}"),
        }
    }

    #[test]
    fn parse_arguments_not_object_errors() {
        let bogus = ToolCall {
            id: "x".into(),
            name: "idle".into(),
            arguments: json!([1, 2, 3]),
        };
        let err = parse_decision(&[bogus]).unwrap_err();
        assert_eq!(
            err,
            DecisionParseError::ArgumentsNotObject {
                tool: "idle".into(),
            }
        );
    }

    #[test]
    fn parse_call_tool_missing_inner_field_errors() {
        // The inner `{name, args, claim_seed}` block carries its own
        // required-field check inside `parse_call_tool_args`. A missing
        // `claim_seed` must surface as `MissingField` with the inner
        // field name, not as a serde shape error against the outer
        // CallTools variant.
        let err = parse_decision(&[call(
            "call_tool",
            json!({"name": "echo", "args": {"msg": "hi"}}),
        )])
        .unwrap_err();
        assert!(
            matches!(err, DecisionParseError::MissingField { ref tool, ref field }
                if tool == "call_tool" && field == "claim_seed"),
            "got {err:?}"
        );
    }

    #[test]
    fn parse_invalid_inner_fs_op_errors() {
        // `ops[0].op` is required by `FsOp`'s internally-tagged enum.
        // Missing it is a shape error that surfaces via serde, not via
        // our up-front field check (which only inspects the top level).
        let tc = call("rewrite_fs", json!({"ops": [{"path": "x"}]}));
        let err = parse_decision(&[tc]).unwrap_err();
        assert!(
            matches!(err, DecisionParseError::InvalidValue { ref tool, .. } if tool == "rewrite_fs"),
            "got {err:?}"
        );
    }
}
