//! JAR2-21 recorded-fixture integration tests for the Cohere adapter.
//!
//! Mirror of `llm_fixture_anthropic.rs`. Drives `LlmDecide` and
//! `Agent::run` against the real `CohereClient` HTTP path, pointed at
//! the mock server in `tests/llm_fixture/mod.rs`. Fixtures live under
//! `tests/fixtures/llm/cohere/`.
//!
//! Three hermetic scenarios per the JAR2-21 spec:
//! (a) `happy_path_tick_drives_call_tool_then_emit_output`
//! (b) `parse_retry_recovers_after_malformed_tool_use`
//!     (the JAR2-19 LlmDecide-internal parse-retry path — distinct from
//!     the agent-level apply-time correction path exercised by
//!     `loop_smoke::invalid_call_tool_stages_correction_then_recovers`)
//! (c) `unhealthy_then_recovery_cycle_via_agent_run`
//!
//! Plus one `JARVIS_LIVE_LLM=1`-gated smoke against the real Cohere API.
//!
//! Feature-gated: this whole file is a no-op unless built with
//! `--features llm-cohere`.

#![cfg(feature = "llm-cohere")]

mod llm_fixture;

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tempfile::TempDir;
use tokio::time::timeout;

use jarvis_node::agent::{Agent, RetireReason};
use jarvis_node::decision::{ContextBundle, Decide, Decision};
use jarvis_node::fs::AgentFs;
use jarvis_node::health::{HealthTracker, RetryBudget};
use jarvis_node::mandate::Mandate;
use jarvis_node::model_client::cohere::CohereClient;
use jarvis_node::model_client::{CompleteOptions, ModelClient, Vendor};
use jarvis_node::tools::{EchoTool, ToolRegistry};

use jarvis_node::decide_llm::llm_decide::LlmDecide;

use llm_fixture::{live_llm_enabled, load_fixture, MockServer};

const FIXTURE_API_KEY: &str = "fixture-key-not-real";

fn ensure_dummy_api_key() {
    if std::env::var("COHERE_API_KEY")
        .map(|v| v.is_empty())
        .unwrap_or(true)
    {
        // SAFETY: hermetic tests don't read the env concurrently.
        unsafe {
            std::env::set_var("COHERE_API_KEY", FIXTURE_API_KEY);
        }
    }
}

fn empty_bundle() -> ContextBundle {
    ContextBundle {
        mandate: Mandate::new("jar2-21 fixture", Duration::from_secs(60), Some(8)),
        triggers: vec![],
        recent_outputs: vec![],
        recent_evidence: vec![],
        correction: None,
    }
}

fn build_client(base_url: String) -> CohereClient {
    CohereClient::new().with_base_url(base_url)
}

fn expected_model() -> String {
    CohereClient::new().model().to_string()
}

// ---------- (a) happy-path tick ----------

#[tokio::test]
async fn happy_path_tick_drives_call_tool_then_emit_output() {
    ensure_dummy_api_key();
    let fixtures = load_fixture("cohere", "happy_path_tick");
    let mock = MockServer::spawn(fixtures).await;

    let client = Arc::new(build_client(mock.base_url()));
    let decide = LlmDecide::new(client.clone(), CompleteOptions::default());

    // Tick 1: model emits CallTool(echo, ...).
    let dec_1 = decide.decide(empty_bundle()).await.expect("tick 1 decide");
    match &dec_1 {
        Decision::CallTool {
            name,
            args,
            claim_seed,
        } => {
            assert_eq!(name, "echo");
            assert_eq!(args, &json!({"msg": "hello jarvis"}));
            assert_eq!(claim_seed.as_str(), "fixture-seed-1");
        }
        other => panic!("expected CallTool, got {other:?}"),
    }
    let calls_1 = decide.last_tick_calls();
    assert_eq!(calls_1.len(), 1);
    assert_eq!(calls_1[0].vendor, Vendor::Cohere);
    assert_eq!(calls_1[0].model, expected_model());
    assert!(
        calls_1[0].latency_ms > 0,
        "tick 1 stats.latency_ms must be > 0"
    );

    // Tick 2: model emits EmitOutput.
    let dec_2 = decide.decide(empty_bundle()).await.expect("tick 2 decide");
    match &dec_2 {
        Decision::EmitOutput { content, evidence } => {
            assert_eq!(content, "the echo tool returned hello jarvis");
            assert_eq!(evidence.len(), 1);
            assert_eq!(
                evidence[0].as_str(),
                "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"
            );
        }
        other => panic!("expected EmitOutput, got {other:?}"),
    }
    let calls_2 = decide.last_tick_calls();
    assert_eq!(calls_2.len(), 1);
    assert_eq!(calls_2[0].vendor, Vendor::Cohere);
    assert_eq!(calls_2[0].model, expected_model());
    assert!(
        calls_2[0].latency_ms > 0,
        "tick 2 stats.latency_ms must be > 0"
    );

    assert_eq!(mock.remaining(), 0);
    let captured = mock.captured();
    assert_eq!(captured.len(), 2);
    for req in &captured {
        assert_eq!(req.method, "POST");
        let body: Value = req.json();
        assert_eq!(body["model"], json!(expected_model()));
        // Cohere wire format keeps the system turn inside `messages[]`,
        // so the array is non-empty even on an otherwise-empty bundle.
        let msgs = body["messages"].as_array().expect("messages array");
        assert!(!msgs.is_empty(), "messages must be non-empty");
        assert_eq!(msgs[0]["role"], json!("system"));
        // Tool list wraps each entry in `{type: "function", function: {...}}`.
        let tools = body["tools"].as_array().expect("tools array");
        assert!(
            tools
                .iter()
                .any(|t| t["function"]["name"] == json!("emit_output")),
            "decision-tool list must include emit_output"
        );
    }
}

// ---------- (b) parse-retry correction (LlmDecide-internal) ----------

#[tokio::test]
async fn parse_retry_recovers_after_malformed_tool_use() {
    ensure_dummy_api_key();
    let fixtures = load_fixture("cohere", "corrective_tick");
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

    let calls = decide.last_tick_calls();
    assert_eq!(
        calls.len(),
        2,
        "corrective tick should record both upstream calls"
    );
    for s in &calls {
        assert_eq!(s.vendor, Vendor::Cohere);
        assert_eq!(s.model, expected_model());
        assert!(s.latency_ms > 0, "every call's latency_ms must be > 0");
    }
    let totals = decide.last_tick_totals();
    assert_eq!(totals.calls, 2);
    assert_eq!(totals.input_tokens, 130 + 188);
    assert_eq!(totals.output_tokens, 36 + 25);
    assert!(totals.latency_ms > 0);

    // Cohere keeps `system` turns inside `messages[]` (no top-level
    // `system` field), so the corrective system message we appended on
    // retry shows up as a `{role: "system", content: "..."}` entry near
    // the end of the second request's messages array.
    let captured = mock.captured();
    assert_eq!(captured.len(), 2);
    let retry_body: Value = captured[1].json();
    let msgs = retry_body["messages"]
        .as_array()
        .expect("retry messages array");
    let has_corrective_system = msgs.iter().any(|m| {
        m["role"] == json!("system")
            && m["content"]
                .as_str()
                .map(|s| s.contains("could not be parsed"))
                .unwrap_or(false)
    });
    assert!(
        has_corrective_system,
        "retry must include a system message describing the parse failure, got: {msgs:?}"
    );
}

// ---------- (c) unhealthy → recovery via Agent::run ----------

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn unhealthy_then_recovery_cycle_via_agent_run() {
    ensure_dummy_api_key();
    let fixtures = load_fixture("cohere", "unhealthy_recovery");
    let mock = MockServer::spawn(fixtures).await;

    let tmp = TempDir::new().expect("tempdir");
    let mandate = Mandate::new(
        "jar2-21 unhealthy cycle",
        Duration::from_millis(50),
        Some(8),
    );
    let fs = AgentFs::open(tmp.path().to_path_buf(), &mandate).expect("open fs");

    let client = Arc::new(build_client(mock.base_url()));
    let decide = LlmDecide::new(client.clone(), CompleteOptions::default());

    let mut registry = ToolRegistry::new();
    registry
        .register(Arc::new(EchoTool))
        .expect("register echo");

    let health = HealthTracker::open(tmp.path(), RetryBudget::default(), chrono::Utc::now())
        .expect("open health");

    let agent = Agent::new(mandate, fs, decide, registry, health);

    // JAR2-33: capture stats handle before `Agent::run` consumes the
    // agent. Mirrors the Anthropic fixture's migration.
    let stats = agent.stats_handle();

    let handle = tokio::spawn(agent.run());
    let RetireReason(reason) = timeout(Duration::from_secs(10), handle)
        .await
        .expect("agent did not retire in time")
        .expect("join")
        .expect("run ok");
    assert_eq!(reason, "post-recovery");

    // Wire-level count assertion — kept because the new stats accessor
    // only sees the most recent tick, not the whole run.
    assert_eq!(mock.remaining(), 0);
    assert_eq!(mock.captured().len(), 4);

    // JAR2-33: post-run stats inspection. The recovery tick issues one
    // successful upstream call; its CallStats must carry Cohere vendor +
    // configured model. Latency is not asserted here — `start_paused`
    // freezes tokio's clock and the millisecond-resolution adapter
    // measurement can round to 0.
    let calls = stats.last_tick_calls();
    assert_eq!(
        calls.len(),
        1,
        "last tick was the recovery retire — one upstream call"
    );
    assert_eq!(calls[0].vendor, Vendor::Cohere);
    assert_eq!(calls[0].model, expected_model());
    let totals = stats.last_tick_totals();
    assert_eq!(totals.calls, 1);
    assert!(totals.input_tokens > 0);
    assert!(totals.output_tokens > 0);

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
    assert!(archive_dir.is_dir());
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
    );
}

// ---------- live-gated smoke ----------

/// Hits the real Cohere API when `JARVIS_LIVE_LLM=1` is set.
///   `JARVIS_LIVE_LLM=1 cargo test --features llm-cohere \
///       happy_path_tick_live_smoke -- --ignored --nocapture`
#[tokio::test]
#[ignore]
async fn happy_path_tick_live_smoke() {
    if !live_llm_enabled() {
        eprintln!("JARVIS_LIVE_LLM!=1; skipping live smoke");
        return;
    }
    if std::env::var("COHERE_API_KEY")
        .map(|v| v.is_empty())
        .unwrap_or(true)
    {
        panic!("COHERE_API_KEY must be set in the environment for the live smoke");
    }

    let client: Arc<dyn ModelClient> = Arc::new(CohereClient::new());
    let decide = LlmDecide::new(client, CompleteOptions::default());
    // Cohere rejects requests whose `messages[]` carries only a system
    // turn ("invalid request: message must be at least 1 token long");
    // seed one trigger so the prompt renderer emits a user message too.
    let bundle = ContextBundle {
        mandate: Mandate::new(
            "Reply with idle for at least 1000ms.",
            Duration::from_secs(60),
            Some(1),
        ),
        triggers: vec![jarvis_node::trigger::Trigger::ScheduledWake],
        recent_outputs: vec![],
        recent_evidence: vec![],
        correction: None,
    };
    let dec = decide
        .decide(bundle)
        .await
        .expect("live cohere decide should succeed");
    let calls = decide.last_tick_calls();
    assert!(!calls.is_empty());
    assert_eq!(calls[0].vendor, Vendor::Cohere);
    assert!(calls[0].latency_ms > 0);
    eprintln!("live cohere decision: {dec:?}; stats: {:?}", calls[0]);
}
