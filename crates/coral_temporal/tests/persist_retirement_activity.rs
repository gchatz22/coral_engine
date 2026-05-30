//! Hermetic coverage for the `persist_retirement` activity body.
//!
//! Runs as its own integration test binary so it has a fresh per-process
//! `OnceLock<Arc<dyn AgentStorage>>` and can call `install_agent_storage`
//! without colliding with any other test.
//!
//! The activity method itself takes an `ActivityContext` (constructible
//! only from inside a worker), so this test exercises the inner free
//! function [`coral_temporal::activities::persist_retirement_inner`]
//! directly. The live test in `tests/workflow_loop.rs` covers the
//! `ctx.info().scheduled_time` plumbing end-to-end.

use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use coral_node::storage::{AgentStorage, MemoryStorage};
use coral_temporal::activities::{persist_retirement_inner, PersistRetirementInput};
use coral_temporal::worker::install_agent_storage;
use coral_temporal::workflow::FsHandle;

/// Process-wide handle to the `MemoryStorage` we installed. Both tests
/// in this file share the same backend so each can call
/// [`install_agent_storage`] at most once (it panics on double-install
/// by design — see worker.rs). Multiple tests in one binary share the
/// process; we install exactly once via this `OnceLock`.
static SHARED_STORAGE: OnceLock<Arc<MemoryStorage>> = OnceLock::new();

/// Install the shared MemoryStorage backend exactly once and return a
/// concretely-typed `Arc<MemoryStorage>` clone so the test can read
/// keys back without going through the trait-object boundary. After
/// the first call, subsequent calls return a clone of the already-
/// installed backend.
fn install_or_reuse_storage() -> Arc<MemoryStorage> {
    SHARED_STORAGE
        .get_or_init(|| {
            let storage: Arc<MemoryStorage> = Arc::new(MemoryStorage::new());
            // Trait-object Arc clone for the install API; the typed
            // clone is what we keep for read-back.
            install_agent_storage(storage.clone() as Arc<dyn AgentStorage>);
            storage
        })
        .clone()
}

#[tokio::test]
async fn persist_retirement_writes_file_with_reason_and_deterministic_timestamp() {
    let storage = install_or_reuse_storage();

    let prefix = "graphs/g-pinned/agents/a-pinned";
    let input = PersistRetirementInput {
        fs_handle: FsHandle {
            prefix: prefix.into(),
        },
        reason: "hermetic-test: scripted retire".into(),
    };

    // `scheduled_time` is the deterministic clock the activity body
    // reads off `ctx.info().scheduled_time`. Pin a known instant so the
    // resulting `retirement.json` is byte-identical regardless of when
    // the test runs.
    let pinned: SystemTime = UNIX_EPOCH + Duration::from_secs(1_700_000_000); // 2023-11-14T22:13:20Z

    persist_retirement_inner(&input, Some(pinned))
        .await
        .expect("persist_retirement_inner succeeds against MemoryStorage");

    // retirement.json must land under the agent's prefix with the
    // supplied reason and the pinned timestamp serialized as RFC 3339.
    let key = format!("{prefix}/retirement.json");
    let bytes = storage
        .get(&key)
        .await
        .expect("MemoryStorage::get must not error on hermetic test")
        .expect("retirement.json must exist after persist_retirement_inner");
    let v: serde_json::Value = serde_json::from_slice(&bytes).expect("retirement.json is JSON");
    assert_eq!(
        v.get("reason").and_then(|x| x.as_str()),
        Some("hermetic-test: scripted retire"),
        "wrong reason: {v}",
    );
    // RFC 3339 string. The pinned instant is 1_700_000_000 epoch seconds
    // = 2023-11-14T22:13:20Z. chrono's serde shape for `DateTime<Utc>`
    // emits `Z` as the UTC offset (compact RFC 3339 form).
    assert_eq!(
        v.get("retired_at").and_then(|x| x.as_str()),
        Some("2023-11-14T22:13:20Z"),
        "wrong retired_at: {v}",
    );

    // mandate.json must NOT exist — `AgentFs::attach` (used by the
    // activity body) skips the mandate write that `new_with_storage`
    // performs. The retirement-signal short-circuit has no Mandate in
    // scope, and the file we write doesn't need one. Pin the property
    // here so a regression that swaps the constructor back to
    // `new_with_storage` (and silently materialises an empty
    // `mandate.json`) fails loudly.
    let mandate_key = format!("{prefix}/mandate.json");
    let mandate = storage
        .get(&mandate_key)
        .await
        .expect("MemoryStorage::get must not error");
    assert!(
        mandate.is_none(),
        "attach must not write mandate.json; got: {mandate:?}",
    );
}

/// Edge case: the activity body falls back to `Utc::now()` when the SDK
/// hasn't populated `scheduled_time`. Pin the fallback shape so a
/// future refactor that, say, hard-fails on a missing schedule doesn't
/// silently change the contract.
#[tokio::test]
async fn persist_retirement_falls_back_to_wall_clock_when_scheduled_time_absent() {
    let storage = install_or_reuse_storage();

    // Distinct prefix per test so the two tests in this file don't
    // race on the same `retirement.json` key.
    let prefix = "graphs/g-fallback/agents/a-fallback";
    let input = PersistRetirementInput {
        fs_handle: FsHandle {
            prefix: prefix.into(),
        },
        reason: "fallback path".into(),
    };

    persist_retirement_inner(&input, None)
        .await
        .expect("fallback path completes");

    // Can't pin the timestamp (wall-clock), but pin shape: file
    // exists, reason round-trips, `retired_at` is RFC 3339-ish UTC.
    let key = format!("{prefix}/retirement.json");
    let bytes = storage
        .get(&key)
        .await
        .expect("get must not error")
        .expect("retirement.json must exist after fallback path");
    let v: serde_json::Value = serde_json::from_slice(&bytes).expect("JSON");
    assert_eq!(
        v.get("reason").and_then(|x| x.as_str()),
        Some("fallback path"),
    );
    let stamp = v
        .get("retired_at")
        .and_then(|x| x.as_str())
        .expect("retired_at present");
    // chrono RFC 3339 always carries an explicit offset suffix.
    assert!(
        stamp.ends_with("+00:00") || stamp.ends_with('Z'),
        "retired_at not UTC-shaped: {stamp:?}"
    );
}
