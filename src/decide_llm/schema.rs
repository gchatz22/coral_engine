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

use crate::decision::Decision;
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
    /// The model returned no tool call where exactly one was expected.
    #[error("expected exactly one tool call, got none")]
    NoCalls,
    /// The model returned more than one tool call. Design #1 forbids this:
    /// every `Decision` is one tool call.
    #[error("expected exactly one tool call, got {count}")]
    MultipleCalls { count: usize },
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
pub fn parse_decision(calls: &[ToolCall]) -> Result<Decision, DecisionParseError> {
    let call = match calls {
        [] => return Err(DecisionParseError::NoCalls),
        [c] => c,
        more => {
            return Err(DecisionParseError::MultipleCalls { count: more.len() });
        }
    };

    if !TOOL_NAMES.contains(&call.name.as_str()) {
        return Err(DecisionParseError::UnknownTool(call.name.clone()));
    }

    let Value::Object(args) = &call.arguments else {
        return Err(DecisionParseError::ArgumentsNotObject {
            tool: call.name.clone(),
        });
    };

    // Required-field check is up-front so the structured "missing field"
    // error fires cleanly before serde gets a chance to complain about
    // shape. Lists below mirror the variant fields in `decision.rs`.
    let required: &[&str] = match call.name.as_str() {
        "call_tool" => &["name", "args", "claim_seed"],
        "emit_output" => &["content", "evidence"],
        "rewrite_fs" => &["ops"],
        "idle" => &["next_after"],
        "retire" => &["reason"],
        _ => unreachable!("guarded by TOOL_NAMES check above"),
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
    // (see `TOOL_NAMES`).
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
    use crate::decision::{ClaimSeed, FsOp};
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
    fn parse_call_tool_round_trips() {
        let tc = call(
            "call_tool",
            json!({
                "name": "echo",
                "args": {"msg": "hi"},
                "claim_seed": "seed-1"
            }),
        );
        let d = parse_decision(&[tc]).unwrap();
        assert_eq!(
            d,
            Decision::CallTool {
                name: "echo".into(),
                args: json!({"msg": "hi"}),
                claim_seed: ClaimSeed::new("seed-1"),
            }
        );
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
        // list and the parser's allowed-names table.
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
            // Tool name should map to the same variant tag the parser
            // injected — round-trip the variant out via serde to verify.
            let back = serde_json::to_value(&d).unwrap();
            assert_eq!(
                back.get("type").and_then(Value::as_str),
                Some(spec.name.as_str())
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
    fn parse_multiple_calls_errors() {
        let err = parse_decision(&[
            call("idle", json!({"next_after": 1})),
            call("retire", json!({"reason": "x"})),
        ])
        .unwrap_err();
        assert_eq!(err, DecisionParseError::MultipleCalls { count: 2 });
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
