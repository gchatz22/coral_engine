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

use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use tracing::debug;

use crate::decide_llm::prompt;
use crate::decide_llm::schema::{decision_tools, parse_decision, DecisionParseError};
use crate::decision::{ContextBundle, Decide, Decision};
use crate::model_client::{
    CallStats, CompleteOptions, CompleteRequest, ContentBlock, Message, ModelClient, ModelError,
    Role,
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
///
/// `tick_stats` is the JAR2-20 per-tick cost/latency accumulator. One
/// `decide()` call == one tick (per the agent loop), and that may issue
/// multiple `ModelClient::complete` calls because of JAR2-19's
/// parse-retry / corrective re-ask. The accumulator captures *every*
/// call within that tick and resets at the start of the next `decide`.
/// `Decide::decide` takes `&self`, so the storage uses interior
/// mutability via `Mutex` — the lock is held only for `push`/`clear`
/// (microseconds), never across an `await`.
///
/// JAR2-33: the accumulator lives behind an `Arc<Mutex<...>>` so callers
/// (notably `Agent<LlmDecide>::stats_handle`) can hand out a cheap
/// post-construction handle that survives `Agent::run` consuming the
/// `LlmDecide`. The handle is read-only from the caller's side; only
/// `decide()` mutates the inner vec.
pub struct LlmDecide {
    client: Arc<dyn ModelClient>,
    options: CompleteOptions,
    tick_stats: Arc<Mutex<Vec<CallStats>>>,
}

impl LlmDecide {
    /// Wire an `LlmDecide` against the supplied client and sampling
    /// options. The options are reused verbatim for both the initial
    /// attempt and the corrective retry.
    pub fn new(client: Arc<dyn ModelClient>, options: CompleteOptions) -> Self {
        Self {
            client,
            options,
            tick_stats: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Aggregate stats across every `complete` call issued during the most
    /// recent `decide()` invocation. Reset at the start of every `decide`,
    /// so reading this between ticks returns the previous tick's totals.
    /// Returns `TickTotals::default()` (all zeros, no calls) before the
    /// first `decide()` runs.
    pub fn last_tick_totals(&self) -> TickTotals {
        let stats = self.tick_stats.lock().expect("tick_stats mutex poisoned");
        TickTotals::from_calls(&stats)
    }

    /// Per-call stats for the most recent `decide()` invocation, in call
    /// order. Useful for assertions that need to inspect individual calls
    /// (e.g. that vendor/model fields are populated correctly).
    pub fn last_tick_calls(&self) -> Vec<CallStats> {
        self.tick_stats
            .lock()
            .expect("tick_stats mutex poisoned")
            .clone()
    }

    /// JAR2-33: clone of the per-tick stats accumulator handle. Callers
    /// that need to read stats after `Agent::run` has consumed the
    /// `LlmDecide` should capture this before construction and read it
    /// post-run. The returned `Arc` shares storage with this `LlmDecide`'s
    /// internal accumulator; the inner vec is updated at the end of every
    /// upstream `complete()` call and cleared at the start of every
    /// `decide()`. Lock the inner mutex only briefly — never across
    /// `await` boundaries — and clone the contents out rather than
    /// holding the guard.
    pub fn stats_handle(&self) -> Arc<Mutex<Vec<CallStats>>> {
        self.tick_stats.clone()
    }
}

/// Sum of one tick's worth of `CallStats`. Tokens accumulate, latency
/// sums (wall-clock budget is what callers want, not max or mean), and
/// `calls` is the count of upstream `complete` invocations the tick
/// issued. Vendor/model are deliberately *not* aggregated here — a
/// future ticket may want a per-vendor breakdown, but a single Decide
/// instance only talks to one client so the breakdown isn't needed yet.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TickTotals {
    pub input_tokens: u32,
    pub output_tokens: u32,
    pub latency_ms: u64,
    pub calls: u32,
}

impl TickTotals {
    pub(crate) fn from_calls(calls: &[CallStats]) -> Self {
        let mut totals = TickTotals::default();
        for c in calls {
            totals.input_tokens = totals.input_tokens.saturating_add(c.usage.input_tokens);
            totals.output_tokens = totals.output_tokens.saturating_add(c.usage.output_tokens);
            totals.latency_ms = totals.latency_ms.saturating_add(c.latency_ms);
            totals.calls = totals.calls.saturating_add(1);
        }
        totals
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

        // JAR2-20: reset the per-tick accumulator at the start of every
        // `decide` so reading `last_tick_totals` between ticks reflects
        // the previous tick only.
        self.tick_stats
            .lock()
            .expect("tick_stats mutex poisoned")
            .clear();

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

            // JAR2-20: record per-call stats and emit a tracing event.
            // Matches the `debug!` level used for per-tick decision
            // events in `agent.rs`; `llm_decide.rs` has no prior
            // tracing of its own to pattern-match against.
            debug!(
                vendor = resp.stats.vendor.as_str(),
                model = %resp.stats.model,
                input_tokens = resp.stats.usage.input_tokens,
                output_tokens = resp.stats.usage.output_tokens,
                latency_ms = resp.stats.latency_ms,
                attempt,
                "llm_decide: call stats",
            );
            self.tick_stats
                .lock()
                .expect("tick_stats mutex poisoned")
                .push(resp.stats.clone());

            match parse_decision(&resp.tool_calls) {
                Ok(d) => {
                    self.emit_tick_summary();
                    return Ok(d);
                }
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

        // Emit a tick summary even on the failure path so operators see
        // the cost the tick burned before going `Unhealthy`.
        self.emit_tick_summary();
        Err(anyhow!(
            "LlmDecide: parse failed on all {} attempt(s). {}",
            total_attempts,
            format_attempt_errors(&errors)
        ))
    }
}

impl LlmDecide {
    /// Emit a single tracing event with the aggregate totals for the tick
    /// that just finished. Matches the `debug!` level used elsewhere for
    /// per-tick events.
    fn emit_tick_summary(&self) {
        let totals = self.last_tick_totals();
        debug!(
            calls = totals.calls,
            input_tokens = totals.input_tokens,
            output_tokens = totals.output_tokens,
            latency_ms = totals.latency_ms,
            "llm_decide: tick totals",
        );
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
    use crate::model_client::{CompleteResponse, ToolCall, Usage, Vendor};
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
    /// with empty content/usage and default stats. The parser only inspects
    /// `tool_calls`; stats-aware tests use `resp_with_stats` instead.
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
            stats: CallStats::default(),
        }
    }

    /// Like `resp_with_tool_calls`, but with a specific `CallStats` block.
    /// Used by the JAR2-20 accumulator tests so they can assert on the
    /// totals math without depending on a real wall-clock measurement.
    fn resp_with_stats(calls: Vec<ToolCall>, stats: CallStats) -> CompleteResponse {
        let usage = stats.usage;
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
            usage,
            stats,
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
                stats: CallStats::default(),
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

    // ---------- JAR2-20: cost + latency accounting ---------------------

    fn anthropic_stats(input: u32, output: u32, latency_ms: u64) -> CallStats {
        CallStats {
            usage: Usage {
                input_tokens: input,
                output_tokens: output,
            },
            latency_ms,
            vendor: Vendor::Anthropic,
            model: "claude-haiku-4-5".into(),
        }
    }

    fn cohere_stats(input: u32, output: u32, latency_ms: u64) -> CallStats {
        CallStats {
            usage: Usage {
                input_tokens: input,
                output_tokens: output,
            },
            latency_ms,
            vendor: Vendor::Cohere,
            model: "command-a-03-2025".into(),
        }
    }

    #[tokio::test]
    async fn tick_totals_default_zero_before_first_decide() {
        let mock = MockModelClient::new(vec![]);
        let decide = LlmDecide::new(mock, CompleteOptions::default());
        assert_eq!(decide.last_tick_totals(), TickTotals::default());
        assert!(decide.last_tick_calls().is_empty());
    }

    #[tokio::test]
    async fn tick_totals_single_call_equal_call_stats() {
        let stats = anthropic_stats(11, 7, 42);
        let mock = MockModelClient::new(vec![MockOutcome::Resp(resp_with_stats(
            vec![good_idle_call()],
            stats.clone(),
        ))]);
        let decide = LlmDecide::new(mock, CompleteOptions::default());
        decide.decide(empty_bundle()).await.unwrap();

        let totals = decide.last_tick_totals();
        assert_eq!(totals.calls, 1);
        assert_eq!(totals.input_tokens, stats.usage.input_tokens);
        assert_eq!(totals.output_tokens, stats.usage.output_tokens);
        assert_eq!(totals.latency_ms, stats.latency_ms);

        let calls = decide.last_tick_calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0], stats);
    }

    #[tokio::test]
    async fn tick_totals_multi_call_sum_correctly() {
        // Two upstream calls in one tick — first is a parse failure that
        // forces a retry, second succeeds. JAR2-20 totals must cover both.
        let s1 = anthropic_stats(100, 25, 350);
        let s2 = anthropic_stats(180, 12, 410);
        let mock = MockModelClient::new(vec![
            MockOutcome::Resp(resp_with_stats(vec![malformed_unknown_tool()], s1.clone())),
            MockOutcome::Resp(resp_with_stats(vec![good_idle_call()], s2.clone())),
        ]);
        let decide = LlmDecide::new(mock, CompleteOptions::default());
        decide.decide(empty_bundle()).await.unwrap();

        let totals = decide.last_tick_totals();
        assert_eq!(totals.calls, 2);
        assert_eq!(totals.input_tokens, 280);
        assert_eq!(totals.output_tokens, 37);
        assert_eq!(totals.latency_ms, 760);

        let calls = decide.last_tick_calls();
        assert_eq!(calls, vec![s1, s2]);
    }

    #[tokio::test]
    async fn tick_totals_reset_between_ticks() {
        // First decide: one call with stats s1. Second decide on the same
        // LlmDecide: one call with stats s2. After the second tick, totals
        // must reflect *only* s2 — the first tick's totals are gone.
        let s1 = anthropic_stats(50, 5, 100);
        let s2 = anthropic_stats(33, 9, 250);
        let mock = MockModelClient::new(vec![
            MockOutcome::Resp(resp_with_stats(vec![good_idle_call()], s1.clone())),
            MockOutcome::Resp(resp_with_stats(vec![good_idle_call()], s2.clone())),
        ]);
        let decide = LlmDecide::new(mock, CompleteOptions::default());

        decide.decide(empty_bundle()).await.unwrap();
        let after_first = decide.last_tick_totals();
        assert_eq!(after_first.calls, 1);
        assert_eq!(after_first.input_tokens, s1.usage.input_tokens);

        decide.decide(empty_bundle()).await.unwrap();
        let after_second = decide.last_tick_totals();
        assert_eq!(after_second.calls, 1, "totals reset between ticks");
        assert_eq!(after_second.input_tokens, s2.usage.input_tokens);
        assert_eq!(after_second.output_tokens, s2.usage.output_tokens);
        assert_eq!(after_second.latency_ms, s2.latency_ms);
    }

    #[tokio::test]
    async fn tick_totals_capture_stats_even_when_all_attempts_fail_to_parse() {
        // When parsing fails on every attempt the call still happened on
        // the wire; the operator log must see the cost even though decide
        // returns Err. Both attempts contribute to the totals.
        let s1 = anthropic_stats(20, 3, 80);
        let s2 = anthropic_stats(40, 4, 90);
        let mock = MockModelClient::new(vec![
            MockOutcome::Resp(resp_with_stats(vec![malformed_unknown_tool()], s1.clone())),
            MockOutcome::Resp(resp_with_stats(
                vec![ToolCall {
                    id: "toolu_bad2".into(),
                    name: "retire".into(),
                    arguments: json!({}),
                }],
                s2.clone(),
            )),
        ]);
        let decide = LlmDecide::new(mock, CompleteOptions::default());
        let err = decide.decide(empty_bundle()).await.unwrap_err();
        assert!(err.to_string().contains("parse failed"));

        let totals = decide.last_tick_totals();
        assert_eq!(totals.calls, 2);
        assert_eq!(totals.input_tokens, 60);
        assert_eq!(totals.output_tokens, 7);
        assert_eq!(totals.latency_ms, 170);
    }

    #[tokio::test]
    async fn tick_totals_carry_vendor_and_model_for_both_vendors() {
        // The accumulator must preserve per-call vendor and model fields
        // so a future ticket can attribute cost by provider. Cover both
        // sides of the `Vendor` enum.
        for stats in [anthropic_stats(7, 2, 50), cohere_stats(12, 4, 75)] {
            let mock = MockModelClient::new(vec![MockOutcome::Resp(resp_with_stats(
                vec![good_idle_call()],
                stats.clone(),
            ))]);
            let decide = LlmDecide::new(mock, CompleteOptions::default());
            decide.decide(empty_bundle()).await.unwrap();
            let calls = decide.last_tick_calls();
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0].vendor, stats.vendor);
            assert_eq!(calls[0].model, stats.model);
            assert_eq!(calls[0].usage, stats.usage);
            assert_eq!(calls[0].latency_ms, stats.latency_ms);
        }
    }

    #[cfg(feature = "llm-anthropic")]
    #[test]
    fn anthropic_parse_response_carries_tokens_and_zero_stats_defaults() {
        // The pure `parse_response` populates `usage` from the wire body
        // and leaves `stats` at default (latency/vendor/model are filled
        // by `complete`). This pins the contract the vendor adapter
        // depends on. The full `complete` wire path is end-to-end-tested
        // by JAR2-21's recorded-fixture suite (see follow-ups).
        let raw = br#"{
            "content": [{"type": "text", "text": "hi"}],
            "usage": {"input_tokens": 21, "output_tokens": 8}
        }"#;
        let r = crate::model_client::anthropic::parse_response(raw).unwrap();
        assert_eq!(r.usage.input_tokens, 21);
        assert_eq!(r.usage.output_tokens, 8);
        // stats defaults to all-zero — complete() overwrites this.
        assert_eq!(r.stats.latency_ms, 0);
        assert!(r.stats.model.is_empty());
    }

    #[cfg(feature = "llm-cohere")]
    #[test]
    fn cohere_parse_response_carries_tokens_and_zero_stats_defaults() {
        let raw = br#"{
            "message": {"role": "assistant", "content": [{"type": "text", "text": "hi"}]},
            "usage": {"tokens": {"input_tokens": 33, "output_tokens": 14}}
        }"#;
        let r = crate::model_client::cohere::parse_response(raw).unwrap();
        assert_eq!(r.usage.input_tokens, 33);
        assert_eq!(r.usage.output_tokens, 14);
        assert_eq!(r.stats.latency_ms, 0);
        assert!(r.stats.model.is_empty());
    }
}
