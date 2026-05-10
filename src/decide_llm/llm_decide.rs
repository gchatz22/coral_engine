//! `LlmDecide` — `Decide` impl backed by a `ModelClient`.
//!
//! This is the runtime adapter that turns a typed `ContextBundle` into a
//! `Decision` by asking a model. The flow per `decide` call:
//!
//! 1. Render the bundle to messages via [`crate::decide_llm::prompt::render`].
//! 2. Call `ModelClient::complete` with those messages and the
//!    decision-tool list from
//!    [`crate::decide_llm::schema::decision_tools`].
//! 3. Parse the model's tool-use response with
//!    [`crate::decide_llm::schema::parse_decision`].
//! 4. **On parse failure**: append the model's bad turn plus a corrective
//!    `system` message naming the failure, and call `complete` again — up
//!    to [`MAX_DECISION_RETRIES`] additional times. If every attempt fails
//!    to parse, return `Err`. The agent run loop treats this `Err` as
//!    inference-retry exhaustion (per JAR2-19's spec) and goes straight to
//!    the health-policy `Unhealthy` transition.
//! 5. **On vendor error** (transport / rate-limit / auth / other): bubble
//!    immediately, no retry — vendor-side backoff is out of scope per the
//!    parent JAR2-12's "Decided" notes.
//!
//! The internal retry loop (capped at [`MAX_DECISION_RETRIES`]) exists
//! because tool-use payload errors are frequently soft: a malformed
//! `arguments` blob, a hallucinated tool name, a missing required field. A
//! handful of corrective turns fix most of these without the runtime having
//! to escalate to an `Unhealthy` transition. Anything past the cap is
//! signal that the model is genuinely confused, and that is what the
//! per-tick budget is for.
//!
//! # Why the corrective message is `system`
//!
//! The ticket asks for a "corrective system message", which is also the
//! more reliable surface for the model: vendor adapters concatenate all
//! `system` turns into the top-level `system` field (see
//! `crate::model_client::anthropic`'s role-mapping notes), so the
//! correction lands as part of the prompt's standing instructions rather
//! than as an in-conversation user message that the model might
//! misinterpret as additional context to summarize.

use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;

use crate::decide_llm::prompt;
use crate::decide_llm::schema::{decision_tools, parse_decision, DecisionParseError};
use crate::decision::{ContextBundle, Decide, Decision};
use crate::model_client::{
    CompleteOptions, CompleteRequest, ContentBlock, Message, ModelClient, ModelError, Role,
};

/// Number of corrective re-asks performed after the first attempt fails to
/// parse. Total upstream calls per `decide` is therefore at most
/// `1 + MAX_DECISION_RETRIES`. The default of `1` matches the original
/// JAR2-19 behavior; raise it if soft tool-use mistakes need more rope
/// before falling through to the per-tick `RetryBudget`.
pub const MAX_DECISION_RETRIES: usize = 1;

/// `Decide` impl that asks a `ModelClient` what to do next.
///
/// Holds the client behind `Arc` so callers can share one HTTP-backed
/// instance across many agents and so `LlmDecide` itself stays cheap to
/// clone if a future ticket needs to.
pub struct LlmDecide {
    client: Arc<dyn ModelClient>,
    options: CompleteOptions,
}

impl LlmDecide {
    /// Wire an `LlmDecide` against the supplied client and sampling
    /// options. The options are reused verbatim for both the initial
    /// attempt and the corrective retry.
    pub fn new(client: Arc<dyn ModelClient>, options: CompleteOptions) -> Self {
        Self { client, options }
    }
}

#[async_trait]
impl Decide for LlmDecide {
    async fn decide(&self, ctx: ContextBundle) -> Result<Decision> {
        let tools = decision_tools();
        // Conversation grows across attempts: original prompt, then for
        // each parse failure an assistant-echo of the bad turn followed by
        // a system-role corrective. The model thus sees its full failure
        // history, not just the most recent miss.
        let mut messages = prompt::render(&ctx);
        let mut errors: Vec<DecisionParseError> = Vec::new();
        let total_attempts = MAX_DECISION_RETRIES + 1;

        for attempt in 0..total_attempts {
            let resp = self
                .client
                .complete(CompleteRequest {
                    messages: messages.clone(),
                    tools: tools.clone(),
                    options: self.options.clone(),
                })
                .await
                .map_err(model_err_to_anyhow)?;

            match parse_decision(&resp.tool_calls) {
                Ok(d) => return Ok(d),
                Err(e) => {
                    let is_last = attempt + 1 == total_attempts;
                    if !is_last {
                        // Stage the next attempt's prompt before recording
                        // the error so `e` is still owned by us.
                        messages.push(assistant_echo(&resp.content));
                        messages.push(Message {
                            role: Role::System,
                            content: vec![ContentBlock::Text {
                                text: corrective_system_text(&e),
                            }],
                        });
                    }
                    errors.push(e);
                }
            }
        }

        Err(anyhow!(
            "LlmDecide: parse failed on all {} attempt(s). {}",
            total_attempts,
            format_attempt_errors(&errors)
        ))
    }
}

/// Render a list of per-attempt parse errors into a single string for the
/// final `anyhow!` payload. Each entry is prefixed `attempt N` so a reader
/// can correlate it with the upstream call count.
fn format_attempt_errors(errors: &[DecisionParseError]) -> String {
    errors
        .iter()
        .enumerate()
        .map(|(i, e)| {
            let n = i + 1;
            if i == 0 {
                format!("attempt {n}: {e}")
            } else {
                format!("attempt {n} (after corrective system message): {e}")
            }
        })
        .collect::<Vec<_>>()
        .join("; ")
}

/// Build the `assistant` turn we replay back to the model on retry, so the
/// corrective message has the model's own bad output to refer to. We echo
/// every content block verbatim — the parser already failed, so trimming
/// or pretty-printing is the wrong layer.
fn assistant_echo(content: &[ContentBlock]) -> Message {
    Message {
        role: Role::Assistant,
        content: content.to_vec(),
    }
}

/// Phrasing of the corrective system message. Promoted to a function so
/// tests can reference the same source of truth as the renderer.
fn corrective_system_text(err: &DecisionParseError) -> String {
    format!(
        "Your previous tool-use response could not be parsed into a Decision: {err}. \
         Reply by calling exactly one of the five decision tools \
         (`call_tool`, `emit_output`, `rewrite_fs`, `idle`, `retire`) \
         with the schema-correct arguments."
    )
}

/// Wrap a typed `ModelError` into the `anyhow::Error` the `Decide` trait
/// returns. Preserves the `ModelError` source so callers that care about
/// the category can downcast.
fn model_err_to_anyhow(err: ModelError) -> anyhow::Error {
    anyhow::Error::new(err)
}

#[cfg(test)]
mod tests {
    //! Unit tests for `LlmDecide`. A test-only `MockModelClient` returns
    //! scripted `CompleteResponse`s; no live HTTP traffic.

    use super::*;
    use crate::decision::{ClaimSeed, ContextBundle};
    use crate::evidence::EvidenceId;
    use crate::mandate::Mandate;
    use crate::model_client::{CompleteResponse, ToolCall, Usage};
    use serde_json::json;
    use std::sync::Mutex;
    use std::time::Duration;

    /// Test-only `ModelClient`. Each call to `complete` pops the next
    /// scripted outcome and returns it. Captures the requests it saw so
    /// tests can assert on the messages the adapter sent.
    struct MockModelClient {
        script: Mutex<Vec<MockOutcome>>,
        seen: Mutex<Vec<CompleteRequest>>,
    }

    /// One scripted outcome: either a successful response or a typed
    /// vendor error to surface verbatim.
    enum MockOutcome {
        Resp(CompleteResponse),
        Err(ModelError),
    }

    impl MockModelClient {
        fn new(script: Vec<MockOutcome>) -> Arc<Self> {
            Arc::new(Self {
                script: Mutex::new(script),
                seen: Mutex::new(Vec::new()),
            })
        }

        fn seen(&self) -> Vec<CompleteRequest> {
            self.seen.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl ModelClient for MockModelClient {
        async fn complete(&self, req: CompleteRequest) -> Result<CompleteResponse, ModelError> {
            self.seen.lock().unwrap().push(req);
            let next = self
                .script
                .lock()
                .unwrap()
                .drain(..1)
                .next()
                .expect("MockModelClient: script exhausted");
            match next {
                MockOutcome::Resp(r) => Ok(r),
                MockOutcome::Err(e) => Err(e),
            }
        }
    }

    /// Build a `CompleteResponse` whose `tool_calls` is the supplied list,
    /// with empty content/usage. The parser only inspects `tool_calls`.
    fn resp_with_tool_calls(calls: Vec<ToolCall>) -> CompleteResponse {
        CompleteResponse {
            content: calls
                .iter()
                .map(|c| ContentBlock::ToolUse {
                    id: c.id.clone(),
                    name: c.name.clone(),
                    input: c.arguments.clone(),
                })
                .collect(),
            tool_calls: calls,
            usage: Usage::default(),
        }
    }

    fn good_idle_call() -> ToolCall {
        ToolCall {
            id: "toolu_idle".into(),
            name: "idle".into(),
            arguments: json!({"next_after": 1000}),
        }
    }

    fn good_call_tool() -> ToolCall {
        ToolCall {
            id: "toolu_ct".into(),
            name: "call_tool".into(),
            arguments: json!({
                "name": "echo",
                "args": {"msg": "hi"},
                "claim_seed": "seed-1"
            }),
        }
    }

    fn malformed_unknown_tool() -> ToolCall {
        ToolCall {
            id: "toolu_bad".into(),
            name: "send_email".into(),
            arguments: json!({"to": "ops@example.com"}),
        }
    }

    fn empty_bundle() -> ContextBundle {
        ContextBundle {
            mandate: Mandate::new("test", Duration::from_secs(1), Some(1)),
            triggers: vec![],
            recent_outputs: vec![],
            recent_evidence: vec![],
            correction: None,
        }
    }

    #[tokio::test]
    async fn first_attempt_success_returns_decision_without_retry() {
        let mock = MockModelClient::new(vec![MockOutcome::Resp(resp_with_tool_calls(vec![
            good_idle_call(),
        ]))]);
        let decide = LlmDecide::new(mock.clone(), CompleteOptions::default());

        let dec = decide.decide(empty_bundle()).await.unwrap();
        assert_eq!(
            dec,
            Decision::Idle {
                next_after: Duration::from_millis(1000),
            }
        );
        // No retry → exactly one upstream call.
        assert_eq!(mock.seen().len(), 1);
    }

    #[tokio::test]
    async fn parse_failure_then_recovery_succeeds_with_corrective_system_message() {
        let mock = MockModelClient::new(vec![
            // 1st attempt: model picks an unknown tool name.
            MockOutcome::Resp(resp_with_tool_calls(vec![malformed_unknown_tool()])),
            // 2nd attempt (after corrective): valid call_tool decision.
            MockOutcome::Resp(resp_with_tool_calls(vec![good_call_tool()])),
        ]);
        let decide = LlmDecide::new(mock.clone(), CompleteOptions::default());

        let dec = decide.decide(empty_bundle()).await.unwrap();
        assert_eq!(
            dec,
            Decision::CallTool {
                name: "echo".into(),
                args: json!({"msg": "hi"}),
                claim_seed: ClaimSeed::new("seed-1"),
            }
        );

        let seen = mock.seen();
        assert_eq!(seen.len(), 2, "expected exactly two upstream calls");

        // The retry must replay the assistant's bad turn and append a
        // corrective system message — that's the contract A1.6 promises.
        let retry = &seen[1];
        let last = retry.messages.last().expect("retry has messages");
        assert_eq!(last.role, Role::System);
        let last_text = match last.content.as_slice() {
            [ContentBlock::Text { text }] => text.as_str(),
            _ => panic!("corrective should be a single text block"),
        };
        assert!(
            last_text.contains("could not be parsed"),
            "corrective text should describe the parse failure, got: {last_text}"
        );

        let echoed = &retry.messages[retry.messages.len() - 2];
        assert_eq!(echoed.role, Role::Assistant);
        assert!(matches!(
            echoed.content[0],
            ContentBlock::ToolUse { ref name, .. } if name == "send_email"
        ));
    }

    #[tokio::test]
    async fn parse_failure_on_both_attempts_returns_err() {
        let mock = MockModelClient::new(vec![
            MockOutcome::Resp(resp_with_tool_calls(vec![malformed_unknown_tool()])),
            // 2nd attempt: still bad — different malformed payload.
            MockOutcome::Resp(resp_with_tool_calls(vec![ToolCall {
                id: "toolu_bad2".into(),
                name: "retire".into(),
                arguments: json!({}), // missing required `reason` field
            }])),
        ]);
        let decide = LlmDecide::new(mock.clone(), CompleteOptions::default());

        let err = decide.decide(empty_bundle()).await.unwrap_err();
        let s = err.to_string();
        // The message must enumerate every attempt so an operator reading
        // a log can see how the model failed at each step. We pin the
        // exact attempt count so the loop refactor stays honest.
        let total_attempts = MAX_DECISION_RETRIES + 1;
        assert!(
            s.contains(&format!("all {total_attempts} attempt")),
            "error should report total attempt count, got: {s}"
        );
        for n in 1..=total_attempts {
            assert!(
                s.contains(&format!("attempt {n}")),
                "error should reference attempt {n}, got: {s}"
            );
        }
        assert_eq!(mock.seen().len(), total_attempts);
    }

    #[tokio::test]
    async fn parse_failure_when_model_returns_no_tool_calls_triggers_retry() {
        // Defensive: a model could reply text-only (no tool_calls). The
        // schema parser surfaces that as `NoCalls`, which is a parse-style
        // error and must enter the same retry path.
        let mock = MockModelClient::new(vec![
            MockOutcome::Resp(CompleteResponse {
                content: vec![ContentBlock::Text {
                    text: "I think we should call echo.".into(),
                }],
                tool_calls: vec![],
                usage: Usage::default(),
            }),
            MockOutcome::Resp(resp_with_tool_calls(vec![good_idle_call()])),
        ]);
        let decide = LlmDecide::new(mock.clone(), CompleteOptions::default());

        let dec = decide.decide(empty_bundle()).await.unwrap();
        assert_eq!(
            dec,
            Decision::Idle {
                next_after: Duration::from_millis(1000),
            }
        );
        assert_eq!(mock.seen().len(), 2);
    }

    #[tokio::test]
    async fn vendor_transport_error_bubbles_immediately_without_retry() {
        let mock = MockModelClient::new(vec![MockOutcome::Err(ModelError::Transport(
            "DNS failure".into(),
        ))]);
        let decide = LlmDecide::new(mock.clone(), CompleteOptions::default());

        let err = decide.decide(empty_bundle()).await.unwrap_err();
        // Source chain must preserve the `ModelError` for callers that
        // want to discriminate by category.
        let model_err = err
            .downcast_ref::<ModelError>()
            .expect("ModelError preserved in source chain");
        assert!(matches!(model_err, ModelError::Transport(_)));
        // No retry attempted on vendor error.
        assert_eq!(mock.seen().len(), 1);
    }

    #[tokio::test]
    async fn vendor_rate_limit_on_retry_bubbles() {
        // 1st: parse fails. 2nd: rate-limited. We must surface the
        // rate-limit error rather than swallowing it.
        let mock = MockModelClient::new(vec![
            MockOutcome::Resp(resp_with_tool_calls(vec![malformed_unknown_tool()])),
            MockOutcome::Err(ModelError::RateLimit("slow down".into())),
        ]);
        let decide = LlmDecide::new(mock.clone(), CompleteOptions::default());

        let err = decide.decide(empty_bundle()).await.unwrap_err();
        let model_err = err
            .downcast_ref::<ModelError>()
            .expect("ModelError preserved");
        assert!(matches!(model_err, ModelError::RateLimit(_)));
        assert_eq!(mock.seen().len(), 2);
    }

    #[tokio::test]
    async fn first_attempt_request_carries_decision_tools() {
        let mock = MockModelClient::new(vec![MockOutcome::Resp(resp_with_tool_calls(vec![
            good_idle_call(),
        ]))]);
        let decide = LlmDecide::new(mock.clone(), CompleteOptions::default());
        let _ = decide.decide(empty_bundle()).await.unwrap();

        let seen = mock.seen();
        let req = &seen[0];
        let names: Vec<&str> = req.tools.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"call_tool"));
        assert!(names.contains(&"emit_output"));
        assert!(names.contains(&"rewrite_fs"));
        assert!(names.contains(&"idle"));
        assert!(names.contains(&"retire"));
    }

    #[tokio::test]
    async fn first_attempt_request_messages_match_prompt_render() {
        let bundle = empty_bundle();
        let expected_messages = prompt::render(&bundle);

        let mock = MockModelClient::new(vec![MockOutcome::Resp(resp_with_tool_calls(vec![
            good_idle_call(),
        ]))]);
        let decide = LlmDecide::new(mock.clone(), CompleteOptions::default());
        let _ = decide.decide(bundle).await.unwrap();

        let seen = mock.seen();
        assert_eq!(seen[0].messages, expected_messages);
    }

    #[tokio::test]
    async fn retry_does_not_blow_away_emit_output_evidence_correlation() {
        // Sanity: when the corrective fixes the issue and the second
        // attempt is `emit_output`, the parsed `Decision` carries the
        // evidence id verbatim. This pins one of the more error-prone
        // variants through the retry path.
        let ev = EvidenceId::from_hex(
            "1d6a153a000000000000000000000000000000000000000000000000abcdef00",
        );
        let mock = MockModelClient::new(vec![
            MockOutcome::Resp(resp_with_tool_calls(vec![malformed_unknown_tool()])),
            MockOutcome::Resp(resp_with_tool_calls(vec![ToolCall {
                id: "toolu_emit".into(),
                name: "emit_output".into(),
                arguments: json!({
                    "content": "the answer",
                    "evidence": [ev.as_str()],
                }),
            }])),
        ]);
        let decide = LlmDecide::new(mock, CompleteOptions::default());

        let dec = decide.decide(empty_bundle()).await.unwrap();
        match dec {
            Decision::EmitOutput { content, evidence } => {
                assert_eq!(content, "the answer");
                assert_eq!(evidence, vec![ev]);
            }
            other => panic!("expected EmitOutput, got {other:?}"),
        }
    }
}
