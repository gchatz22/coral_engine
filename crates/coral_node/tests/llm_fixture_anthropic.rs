//! Recorded-fixture integration tests for the Anthropic adapter.
//!
//! Drives `LlmDecide` and `Agent::run` against the real `AnthropicClient`
//! HTTP path, with the client pointed at the mock server in
//! `tests/llm_fixture/mod.rs`. Fixtures live under
//! `tests/fixtures/llm/anthropic/`. Plus one `CORAL_LIVE_LLM=1`-gated
//! smoke against the real Anthropic API. Feature-gated:
//! `--features llm-anthropic`.

#![cfg(feature = "llm-anthropic")]

mod llm_fixture;

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::time::timeout;

use coral_node::agent::{Agent, RetireReason};
use coral_node::decision::{ContextBundle, Decide, Decision};
use coral_node::fs::AgentFs;
use coral_node::health::{HealthTracker, RetryBudget};
use coral_node::mandate::Mandate;
use coral_node::model_client::anthropic::AnthropicClient;
use coral_node::model_client::{CompleteOptions, ModelClient, Vendor};
use coral_node::tools::{EchoTool, ToolRegistry};

use coral_node::decide_llm::llm_decide::LlmDecide;

use llm_fixture::{live_llm_enabled, load_fixture, MockServer};

/// `ANTHROPIC_API_KEY` must be set to *some* non-empty value for the
/// adapter's pre-flight env check to pass. The mock server doesn't
/// validate the key — any string works. We set it for the duration of
/// each test rather than relying on the ambient environment so the
/// hermetic suite stays self-contained.
const FIXTURE_API_KEY: &str = "fixture-key-not-real";

/// Ensure the Anthropic adapter's env-read for the API key succeeds
/// (the adapter returns `ModelError::Auth` otherwise). Sets a dummy
/// value if the ambient env is missing. `unsafe` per Rust 2024.
fn ensure_dummy_api_key() {
    if std::env::var("ANTHROPIC_API_KEY")
        .map(|v| v.is_empty())
        .unwrap_or(true)
    {
        // SAFETY: the test runner is single-threaded for env writes;
        // hermetic tests don't read the env concurrently.
        unsafe {
            std::env::set_var("ANTHROPIC_API_KEY", FIXTURE_API_KEY);
        }
    }
}

fn empty_bundle() -> ContextBundle {
    ContextBundle {
        mandate: Mandate::new("anthropic fixture", Duration::from_secs(60), Some(8)),
        triggers: vec![],
        recent_outputs: vec![],
        recent_evidence: vec![],
        open_claims: vec![],
        correction: None,
    }
}

fn build_client(base_url: String) -> AnthropicClient {
    AnthropicClient::new().with_base_url(base_url)
}

fn expected_model() -> String {
    AnthropicClient::new().model().to_string()
}

// ---------- (a) happy-path tick ----------

#[tokio::test]
async fn happy_path_tick_drives_call_tool_then_emit_output() {
    ensure_dummy_api_key();
    let fixtures = load_fixture("anthropic", "happy_path_tick");
    let mock = MockServer::spawn(fixtures).await;

    let client = Arc::new(build_client(mock.base_url()));
    let decide = LlmDecide::new(client.clone(), CompleteOptions::default());

    // Tick 1: model emits CallTools(vec![echo, ...]).
    let dec_1 = decide.decide(empty_bundle()).await.expect("tick 1 decide");
    match &dec_1 {
        Decision::CallTools { calls } => {
            assert_eq!(calls.len(), 1, "single-call happy path");
            let c = &calls[0];
            assert_eq!(c.name, "echo");
            assert_eq!(c.args, json!({"msg": "hello coral"}));
            assert_eq!(c.claim_seed.as_str(), "fixture-seed-1");
            // Vendor `tool_use.id` from the fixture must propagate.
            assert_eq!(c.tool_use_id.as_deref(), Some("toolu_call_tool_1"));
        }
        other => panic!("expected CallTools, got {other:?}"),
    }

    let calls_1 = decide.last_tick_calls();
    assert_eq!(
        calls_1.len(),
        1,
        "tick 1 should issue exactly one upstream call"
    );
    let s1 = &calls_1[0];
    assert_eq!(s1.vendor, Vendor::Anthropic);
    assert_eq!(s1.model, expected_model());
    assert!(
        s1.latency_ms > 0,
        "tick 1 stats.latency_ms must be > 0 (mock sleeps before responding); got {}",
        s1.latency_ms
    );

    // Tick 2: model emits EmitOutput citing the synthesized evidence id.
    let dec_2 = decide.decide(empty_bundle()).await.expect("tick 2 decide");
    match &dec_2 {
        Decision::EmitOutput { content, evidence } => {
            assert_eq!(content, "the echo tool returned hello coral");
            assert_eq!(evidence.len(), 1);
            // The fixture id is hex-encoded; just check the round-trip.
            assert_eq!(
                evidence[0].as_str(),
                "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"
            );
        }
        other => panic!("expected EmitOutput, got {other:?}"),
    }

    let calls_2 = decide.last_tick_calls();
    assert_eq!(
        calls_2.len(),
        1,
        "tick 2 should issue exactly one upstream call"
    );
    let s2 = &calls_2[0];
    assert_eq!(s2.vendor, Vendor::Anthropic);
    assert_eq!(s2.model, expected_model());
    assert!(s2.latency_ms > 0, "tick 2 stats.latency_ms must be > 0");

    // The mock should have served both fixtures.
    assert_eq!(
        mock.remaining(),
        0,
        "all fixtures should have been consumed"
    );

    // The captured wire requests should be JSON the Anthropic adapter
    // produced. Pin one structural property so an accidental schema
    // change in `build_body` doesn't silently pass.
    let captured = mock.captured();
    assert_eq!(captured.len(), 2, "exactly two upstream POSTs expected");
    for req in &captured {
        assert_eq!(req.method, "POST");
        let body: Value = req.json();
        assert_eq!(body["model"], json!(expected_model()));
        // Anthropic wire format puts the prompt's system turn at the
        // top-level `system` field and `messages[]` carries user/assistant/
        // tool turns. An empty-bundle prompt renders to just a system turn
        // so `messages` is correctly empty here; `system` must be present.
        assert!(
            body["system"]
                .as_str()
                .map(|s| !s.is_empty())
                .unwrap_or(false),
            "request body must carry a non-empty `system` field"
        );
        assert!(
            body["tools"]
                .as_array()
                .map(|t| t.iter().any(|s| s["name"] == json!("emit_output")))
                .unwrap_or(false),
            "request body must publish the decision-tool list"
        );
    }
}

// ---------- (b) parse-retry correction (LlmDecide-internal) ----------

#[tokio::test]
async fn parse_retry_recovers_after_malformed_tool_use() {
    ensure_dummy_api_key();
    let fixtures = load_fixture("anthropic", "corrective_tick");
    let mock = MockServer::spawn(fixtures).await;

    let client = Arc::new(build_client(mock.base_url()));
    let decide = LlmDecide::new(client.clone(), CompleteOptions::default());

    let dec = decide
        .decide(empty_bundle())
        .await
        .expect("decide should recover");
    match &dec {
        Decision::Idle { next_after } => {
            assert_eq!(*next_after, Duration::from_millis(1500));
        }
        other => panic!("expected Idle after corrective retry, got {other:?}"),
    }

    // Both upstream calls (the bad one + the corrective successful one)
    // must accumulate into the tick stats.
    let calls = decide.last_tick_calls();
    assert_eq!(
        calls.len(),
        2,
        "corrective tick should record both upstream calls"
    );
    for s in &calls {
        assert_eq!(s.vendor, Vendor::Anthropic);
        assert_eq!(s.model, expected_model());
        assert!(s.latency_ms > 0, "every call's latency_ms must be > 0");
    }
    let totals = decide.last_tick_totals();
    assert_eq!(totals.calls, 2);
    assert_eq!(totals.input_tokens, 121 + 178);
    assert_eq!(totals.output_tokens, 33 + 22);
    assert!(totals.latency_ms > 0);

    // The second request must echo the failing assistant turn and a
    // corrective system message back to the model.
    let captured = mock.captured();
    assert_eq!(captured.len(), 2);
    let retry_body: Value = captured[1].json();
    let msgs = retry_body["messages"]
        .as_array()
        .expect("retry messages array");
    // Anthropic wire format: system messages collapse to the top-level
    // `system` field. The corrective system text the retry sent must
    // therefore appear there.
    let system_text = retry_body["system"]
        .as_str()
        .expect("retry must carry a top-level system field");
    assert!(
        system_text.contains("could not be parsed"),
        "retry's system field must include the corrective phrase, got: {system_text}"
    );
    // The retry interleaves a synthesized tool-result turn after the
    // bad assistant echo so each `tool_use.id` has a matching
    // `tool_result` (vendor API requirement). On Anthropic that
    // rewraps as a `user` message carrying `tool_result` content; the
    // bad-tool_use assistant echo sits one slot before it.
    //   msgs[last]   = user (synthesized tool_result envelope)
    //   msgs[last-1] = assistant (bad tool_use echo)
    let last_msg = msgs.last().expect("retry has messages");
    assert_eq!(
        last_msg["role"],
        json!("user"),
        "synthesized tool-result envelope rewrapped as user on Anthropic"
    );
    let tool_results = last_msg["content"].as_array().expect("content array");
    assert!(
        tool_results
            .iter()
            .any(|b| b["type"] == json!("tool_result")
                && b["tool_use_id"] == json!("toolu_unknown_1")),
        "synthesized tool_result must carry the bad tool_use's id, got: {last_msg}"
    );
    let echoed = &msgs[msgs.len() - 2];
    assert_eq!(echoed["role"], json!("assistant"));
}

// ---------- (c) unhealthy → recovery via Agent::run ----------

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn unhealthy_then_recovery_cycle_via_agent_run() {
    ensure_dummy_api_key();
    let fixtures = load_fixture("anthropic", "unhealthy_recovery");
    let mock = MockServer::spawn(fixtures).await;

    let tmp = TempDir::new().expect("tempdir");
    // The fixture's 4 responses span 3 ticks (one tick parse-fails then
    // recovers on its internal retry); cap at 3 so the loop consumes them
    // all then stops on the safety cap (agents never self-terminate).
    let mandate = Mandate::new(
        "unhealthy recovery cycle",
        Duration::from_millis(50),
        Some(3),
    );
    let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate)
        .await
        .expect("open fs");

    let client = Arc::new(build_client(mock.base_url()));
    let decide = LlmDecide::new(client.clone(), CompleteOptions::default());

    let mut registry = ToolRegistry::new();
    registry
        .register(Arc::new(EchoTool))
        .expect("register echo");

    let health = HealthTracker::open(tmp.path(), RetryBudget::default(), chrono::Utc::now())
        .expect("open health");

    let agent = Agent::new(mandate, fs, decide, registry, health);

    // Capture the stats handle *before* `Agent::run` consumes the
    // agent. The handle survives the run and lets the test read post-run
    // `CallStats` directly instead of inferring from captured HTTP
    // traffic.
    let stats = agent.stats_handle();

    let handle = tokio::spawn(agent.run());
    let RetireReason(reason) = timeout(Duration::from_secs(10), handle)
        .await
        .expect("agent did not retire in time")
        .expect("join")
        .expect("run ok");
    assert_eq!(reason, "max_ticks (3) reached");

    // All four fixtures should have been consumed across the three ticks.
    // We keep this wire-level remaining/captured check because it pins
    // the *number* of upstream calls across the whole run; the new
    // stats accessor only sees the most recent tick.
    assert_eq!(mock.remaining(), 0);
    assert_eq!(mock.captured().len(), 4);

    // The final tick is the recovery `idle` — a single successful upstream
    // call (the `max_ticks` cap retires the agent on the next iteration).
    // The handle therefore reports exactly one call carrying Anthropic
    // vendor + the configured model.
    //
    // Latency is *not* asserted here (unlike tests (a)/(b)): this test
    // uses `start_paused = true`, which freezes tokio's clock; the
    // mock's `tokio::time::sleep` returns instantly and the adapter's
    // `std::time::Instant` delta may round to 0ms.
    let calls = stats.last_tick_calls();
    assert_eq!(
        calls.len(),
        1,
        "last tick was the recovery idle — one upstream call"
    );
    assert_eq!(calls[0].vendor, Vendor::Anthropic);
    assert_eq!(calls[0].model, expected_model());
    let totals = stats.last_tick_totals();
    assert_eq!(totals.calls, 1);
    assert!(totals.input_tokens > 0);
    assert!(totals.output_tokens > 0);

    // The final health state must be Healthy with a prior incident archived.
    let live: Value = serde_json::from_slice(
        &std::fs::read(tmp.path().join("health.json")).expect("read health"),
    )
    .expect("parse health");
    assert_eq!(
        live.get("state").and_then(|x| x.as_str()),
        Some("Healthy"),
        "agent must recover to Healthy after the next successful tick"
    );

    let archive_dir = tmp.path().join("health");
    assert!(
        archive_dir.is_dir(),
        "archive dir should exist after recovery"
    );
    let archived: Vec<_> = std::fs::read_dir(&archive_dir)
        .expect("read archive")
        .map(|e| e.expect("dirent").path())
        .collect();
    assert_eq!(archived.len(), 1, "exactly one archived incident expected");
    let inc: Value = serde_json::from_slice(&std::fs::read(&archived[0]).expect("read archive"))
        .expect("parse archive");
    assert_eq!(
        inc.get("incident")
            .and_then(|i| i.get("failing"))
            .and_then(|f| f.get("type"))
            .and_then(|x| x.as_str()),
        Some("Inference"),
        "Decide-Err from LlmDecide must produce an Inference incident"
    );
}

// ---------- (d) parallel tool calls ----------

/// A single Anthropic response carrying K=3 `tool_use` blocks for
/// `call_tool` parses into one `Decision::CallTools` with three entries.
/// Pins the per-block `tool_use.id` propagation contract.
#[tokio::test]
async fn parallel_tool_calls_k3_folds_into_single_call_tools_decision() {
    ensure_dummy_api_key();
    let fixtures = load_fixture("anthropic", "parallel_tool_calls");
    let mock = MockServer::spawn(fixtures).await;

    let client = Arc::new(build_client(mock.base_url()));
    let decide = LlmDecide::new(client.clone(), CompleteOptions::default());

    let dec = decide
        .decide(empty_bundle())
        .await
        .expect("parallel decide");
    match dec {
        Decision::CallTools { calls } => {
            assert_eq!(calls.len(), 3, "parser must fold K=3 `tool_use` blocks");
            assert_eq!(calls[0].name, "echo");
            assert_eq!(calls[0].args, json!({"path": "a.md"}));
            assert_eq!(calls[0].claim_seed.as_str(), "seed-a");
            assert_eq!(calls[0].tool_use_id.as_deref(), Some("toolu_read_a"));
            assert_eq!(calls[1].tool_use_id.as_deref(), Some("toolu_read_b"));
            assert_eq!(calls[2].tool_use_id.as_deref(), Some("toolu_read_c"));
        }
        other => panic!("expected CallTools, got {other:?}"),
    }

    // Exactly one upstream call — K=3 fits in one response, no retry.
    let calls = decide.last_tick_calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].vendor, Vendor::Anthropic);

    // The request body must not carry a `tool_choice` block — parallel
    // tool calls require that field to be absent.
    let captured = mock.captured();
    assert_eq!(captured.len(), 1);
    let body: Value = captured[0].json();
    assert!(
        body.get("tool_choice").is_none(),
        "tool_choice block must be absent, got: {body}"
    );
}

// ---------- live-gated smoke ----------

/// Hits the real Anthropic API when `CORAL_LIVE_LLM=1` is set. Skipped
/// in CI / default `cargo test` runs. Re-run manually:
///   `CORAL_LIVE_LLM=1 cargo test --features llm-anthropic \
///       happy_path_tick_live_smoke -- --ignored --nocapture`
#[tokio::test]
#[ignore]
async fn happy_path_tick_live_smoke() {
    if !live_llm_enabled() {
        eprintln!("CORAL_LIVE_LLM!=1; skipping live smoke");
        return;
    }
    if std::env::var("ANTHROPIC_API_KEY")
        .map(|v| v.is_empty())
        .unwrap_or(true)
    {
        panic!("ANTHROPIC_API_KEY must be set in the environment for the live smoke");
    }

    // Real client; no mock. We ask the model to pick a decision tool and
    // assert only that *some* parseable Decision came back. Prompt content
    // is intentionally trivial — this is a smoke, not a quality bar.
    let client: Arc<dyn ModelClient> = Arc::new(AnthropicClient::new());
    let decide = LlmDecide::new(client, CompleteOptions::default());

    // Seed a trigger so the prompt has a user turn (Anthropic accepts
    // empty messages with `system` set, but matching the Cohere smoke
    // here keeps the two live paths shaped identically).
    let bundle = ContextBundle {
        mandate: Mandate::new(
            "Reply with idle for at least 1000ms.",
            Duration::from_secs(60),
            Some(1),
        ),
        triggers: vec![coral_node::trigger::Trigger::ScheduledWake],
        recent_outputs: vec![],
        recent_evidence: vec![],
        open_claims: vec![],
        correction: None,
    };
    let dec = decide
        .decide(bundle)
        .await
        .expect("live anthropic decide should succeed");

    let calls = decide.last_tick_calls();
    assert!(!calls.is_empty());
    assert_eq!(calls[0].vendor, Vendor::Anthropic);
    assert!(calls[0].latency_ms > 0);
    eprintln!("live anthropic decision: {dec:?}; stats: {:?}", calls[0]);
}
