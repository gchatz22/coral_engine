//! Agent health — Healthy/Unhealthy state, retry budget, and `health.json`.
//!
//! Per `scratch/post_bootstrap_followups.md` § A1, both inference failures
//! (parse/transport/rate-limit-after-backoff) and tool-call failures share
//! a single per-tick retry budget. When the budget is exhausted on a tick,
//! the agent transitions to `Unhealthy`, the tick aborts, and `health.json`
//! captures the failing decision/call, the retry trail, and the last
//! error. The run loop does **not** halt: the agent stays subscribed to
//! its trigger queue, and a subsequent successful tick flips state back to
//! `Healthy` and archives the prior incident under `health/<timestamp>.json`.
//! Repeated failure while already `Unhealthy` updates `health.json` in
//! place. `retirement.json` is orthogonal and untouched by this path.
//!
//! This ticket (JAR2-18) ships the module in isolation. The agent-loop
//! wiring lives in JAR2-19 (A1.6 inference retries) and JAR2-25 (A2.4
//! tool-call retries); both call into the small public surface here so the
//! state machine is implemented once.
//!
//! # On-disk layout
//!
//! ```text
//! <root>/
//!   health.json                           — current incident (Unhealthy only)
//!   health/<ISO-8601-timestamp>.json      — archived prior incidents
//! ```
//!
//! `health.json` is removed when state flips to `Healthy`. Archive
//! filenames use the `transitioned_at` timestamp of the incident being
//! archived, so audit can reconstruct the order in which failures
//! happened.
//!
//! # Atomic writes
//!
//! Unlike `src/fs.rs`, this module writes the live `health.json` via
//! write-to-temp + rename so a crash mid-write cannot leave a corrupt
//! file. Archive writes use the same path. (`src/fs.rs` uses plain
//! `fs::write` today — fixing that is out of scope for this ticket.)

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use thiserror::Error;

const HEALTH_FILE: &str = "health.json";
const ARCHIVE_DIR: &str = "health";
const SCHEMA_VERSION: u32 = 1;

/// Typed errors raised by `HealthTracker`. Callers in JAR2-19/JAR2-25 match
/// on these to distinguish budget exhaustion from real I/O failures.
#[derive(Debug, Error)]
pub enum HealthError {
    /// `record_failure` was called after the per-tick budget was already
    /// spent for the supplied kind. The caller should stop the tick and
    /// invoke `transition_to_unhealthy` with a populated `HealthIncident`.
    #[error("retry budget exhausted for {kind:?}")]
    BudgetExhausted { kind: FailureKind },
    /// `health.json` parsed at `open` time but had a schema `version` we
    /// do not understand. We refuse to silently downgrade an Unhealthy
    /// agent to Healthy.
    #[error("unsupported health.json schema version: {0}")]
    UnsupportedVersion(u32),
    /// `serde_json` failed to (de)serialize a record.
    #[error("health serde error: {0}")]
    Serde(#[from] serde_json::Error),
    /// Wrapped `std::io::Error` with the path that caused it.
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

impl HealthError {
    fn io(path: impl Into<PathBuf>, source: io::Error) -> Self {
        HealthError::Io {
            path: path.into(),
            source,
        }
    }
}

/// Cheap discriminant for budget bookkeeping. The rich payload that gets
/// archived lives on `HealthIncident::failing` so the budget counter does
/// not couple to the on-disk schema.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum FailureKind {
    /// Inference failure: parse error, transport error, or rate-limit
    /// after backoff. Sourced from the `Decide` adapter (JAR2-19).
    Inference,
    /// Tool-call failure: `Tool::call` returned an error or the underlying
    /// MCP server reported one. Sourced from tool dispatch (JAR2-25).
    ToolCall,
}

/// Per-tick retry budget. Each kind has an independent counter; either
/// overflowing trips exhaustion. Defaults are 1 inference retry and 3
/// tool-call retries per the plan in `post_bootstrap_followups.md` § A1.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryBudget {
    pub max_inference: u32,
    pub max_tool: u32,
}

impl Default for RetryBudget {
    fn default() -> Self {
        Self {
            max_inference: 1,
            max_tool: 3,
        }
    }
}

impl RetryBudget {
    pub fn new(max_inference: u32, max_tool: u32) -> Self {
        Self {
            max_inference,
            max_tool,
        }
    }
}

/// One entry in a retry trail. Built by the caller (JAR2-19/JAR2-25); the
/// tracker just round-trips it through `health.json`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attempt {
    pub attempt: u32,
    pub at: DateTime<Utc>,
    pub error: String,
}

/// Vendor-agnostic description of which call exhausted its budget. The
/// `details` blob is intentionally untyped JSON: an Anthropic 429 and an
/// MCP tool error have very different shapes, and the tracker's job is to
/// archive faithfully, not to normalize.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailingCall {
    #[serde(rename = "type")]
    pub kind: FailureKind,
    pub details: serde_json::Value,
}

/// The contents of a single Unhealthy incident. Built by the caller on
/// budget exhaustion and handed to `transition_to_unhealthy`; round-tripped
/// through `health.json` and `health/<timestamp>.json`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthIncident {
    pub failing: FailingCall,
    pub retry_trail: Vec<Attempt>,
    pub last_error: String,
    pub transitioned_at: DateTime<Utc>,
}

/// Current health state. `Healthy` is the default; `Unhealthy` carries the
/// active incident so a reader does not need to peek at `health.json`
/// separately.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HealthState {
    Healthy,
    Unhealthy {
        since: DateTime<Utc>,
        incident: HealthIncident,
    },
}

/// On-disk envelope for `health.json`. Versioned so a future schema bump
/// is cheap. Kept private to the module — readers go through
/// `HealthTracker::state()`, not direct serde.
#[derive(Debug, Serialize, Deserialize)]
struct HealthRecord {
    version: u32,
    state: RecordState,
    since: DateTime<Utc>,
    incident: HealthIncident,
}

#[derive(Debug, Serialize, Deserialize)]
enum RecordState {
    Unhealthy,
}

/// Tracks Healthy/Unhealthy transitions and owns the per-tick retry
/// budget. One tracker per agent; not `Send`-shared — the run loop owns
/// the only `&mut`.
#[derive(Debug)]
pub struct HealthTracker {
    root: PathBuf,
    budget: RetryBudget,
    state: HealthState,
    inference_used: u32,
    tool_used: u32,
}

impl HealthTracker {
    /// Open a tracker rooted at `root`. If `<root>/health.json` exists it
    /// is rehydrated as the starting `Unhealthy` state — restart-safe so
    /// we do not silently flip Unhealthy agents to Healthy.
    pub fn open(root: &Path, budget: RetryBudget) -> Result<Self, HealthError> {
        let live = root.join(HEALTH_FILE);
        let state = if live.exists() {
            let bytes = fs::read(&live).map_err(|e| HealthError::io(&live, e))?;
            let record: HealthRecord = serde_json::from_slice(&bytes)?;
            if record.version != SCHEMA_VERSION {
                return Err(HealthError::UnsupportedVersion(record.version));
            }
            HealthState::Unhealthy {
                since: record.since,
                incident: record.incident,
            }
        } else {
            HealthState::Healthy
        };

        Ok(Self {
            root: root.to_path_buf(),
            budget,
            state,
            inference_used: 0,
            tool_used: 0,
        })
    }

    /// Borrow the current state.
    pub fn state(&self) -> &HealthState {
        &self.state
    }

    /// Reset the per-tick counters. Called at the start of every tick by
    /// the run loop, regardless of prior outcome. Does not touch state.
    pub fn begin_tick(&mut self) {
        self.inference_used = 0;
        self.tool_used = 0;
    }

    /// Record one failed attempt against the budget for `kind`.
    ///
    /// Returns `Ok(())` if there is still room in the budget; the caller
    /// should retry. Returns `Err(BudgetExhausted)` once the per-tick
    /// allowance for `kind` is spent — the caller should then stop the
    /// tick and call `transition_to_unhealthy` with a populated
    /// `HealthIncident`.
    ///
    /// `error` is currently advisory (not stored on the tracker — the
    /// caller assembles the retry trail and the final `last_error`). It
    /// is part of the signature so future tracing-span work can hook in
    /// without an API break.
    pub fn record_failure(&mut self, kind: FailureKind, error: &str) -> Result<(), HealthError> {
        let _ = error;
        match kind {
            FailureKind::Inference => {
                self.inference_used = self.inference_used.saturating_add(1);
                if self.inference_used > self.budget.max_inference {
                    return Err(HealthError::BudgetExhausted { kind });
                }
            }
            FailureKind::ToolCall => {
                self.tool_used = self.tool_used.saturating_add(1);
                if self.tool_used > self.budget.max_tool {
                    return Err(HealthError::BudgetExhausted { kind });
                }
            }
        }
        Ok(())
    }

    /// Mark the agent `Unhealthy` and persist `incident` to
    /// `health.json`. If the agent is already `Unhealthy`, the existing
    /// `health.json` is updated in place — the prior incident is **not**
    /// archived (archival happens on recovery).
    pub fn transition_to_unhealthy(&mut self, incident: HealthIncident) -> Result<(), HealthError> {
        let since = incident.transitioned_at;
        let record = HealthRecord {
            version: SCHEMA_VERSION,
            state: RecordState::Unhealthy,
            since,
            incident: incident.clone(),
        };
        self.write_live(&record)?;
        self.state = HealthState::Unhealthy { since, incident };
        Ok(())
    }

    /// Mark the agent `Healthy` after a successful tick. If the agent
    /// was previously `Unhealthy`, the live `health.json` is moved into
    /// `health/<transitioned_at>.json` (archive) and removed from its
    /// live location. Per-tick counters are also reset.
    pub fn mark_tick_success(&mut self) -> Result<(), HealthError> {
        if let HealthState::Unhealthy { incident, .. } = &self.state {
            self.archive_current(&incident.transitioned_at)?;
        }
        self.state = HealthState::Healthy;
        self.inference_used = 0;
        self.tool_used = 0;
        Ok(())
    }

    // ---- helpers --------------------------------------------------------

    fn write_live(&self, record: &HealthRecord) -> Result<(), HealthError> {
        let live = self.root.join(HEALTH_FILE);
        let bytes = serde_json::to_vec_pretty(record)?;
        atomic_write(&live, &bytes)
    }

    fn archive_current(&self, transitioned_at: &DateTime<Utc>) -> Result<(), HealthError> {
        let live = self.root.join(HEALTH_FILE);
        if !live.exists() {
            return Ok(());
        }
        let archive_dir = self.root.join(ARCHIVE_DIR);
        fs::create_dir_all(&archive_dir).map_err(|e| HealthError::io(&archive_dir, e))?;

        // ISO-8601 with seconds precision and a trailing `Z` — matches
        // the spec literally. Filenames are unique because a tracker can
        // only have one Unhealthy incident at a time and `transitioned_at`
        // is captured by the caller per-incident.
        let stamp = transitioned_at.format("%Y-%m-%dT%H:%M:%SZ").to_string();
        let archive = archive_dir.join(format!("{stamp}.json"));

        let bytes = fs::read(&live).map_err(|e| HealthError::io(&live, e))?;
        // Atomic-write the archive copy first, then delete the live file.
        // If the rename succeeds but the unlink fails, a future open()
        // still rehydrates from the live file — safer than the inverse.
        atomic_write(&archive, &bytes)?;
        fs::remove_file(&live).map_err(|e| HealthError::io(&live, e))?;
        Ok(())
    }
}

/// Write `bytes` to `path` via a sibling tempfile + rename, so a crash
/// mid-write cannot leave the destination in a half-written state.
fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), HealthError> {
    let tmp = match path.file_name() {
        Some(name) => {
            let mut s = name.to_os_string();
            s.push(".tmp");
            path.with_file_name(s)
        }
        // Edge case: `path` had no file name component. We never construct
        // such a path internally, but guard anyway.
        None => return Err(HealthError::io(path, io::Error::other("missing file name"))),
    };
    fs::write(&tmp, bytes).map_err(|e| HealthError::io(&tmp, e))?;
    fs::rename(&tmp, path).map_err(|e| HealthError::io(path, e))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn sample_incident(kind: FailureKind, when: DateTime<Utc>) -> HealthIncident {
        HealthIncident {
            failing: FailingCall {
                kind,
                details: json!({"call": "demo", "code": 500}),
            },
            retry_trail: vec![
                Attempt {
                    attempt: 1,
                    at: when,
                    error: "first".into(),
                },
                Attempt {
                    attempt: 2,
                    at: when,
                    error: "second".into(),
                },
            ],
            last_error: "second".into(),
            transitioned_at: when,
        }
    }

    fn fresh(budget: RetryBudget) -> (TempDir, HealthTracker) {
        let tmp = TempDir::new().unwrap();
        let tracker = HealthTracker::open(tmp.path(), budget).unwrap();
        (tmp, tracker)
    }

    #[test]
    fn empty_start_yields_healthy_and_no_file() {
        let (tmp, tracker) = fresh(RetryBudget::default());
        assert!(matches!(tracker.state(), HealthState::Healthy));
        assert!(!tmp.path().join("health.json").exists());
        assert!(!tmp.path().join("health").exists());
    }

    #[test]
    fn record_failure_under_budget_stays_healthy_no_file() {
        let (tmp, mut tracker) = fresh(RetryBudget::new(2, 3));
        tracker
            .record_failure(FailureKind::Inference, "boom")
            .unwrap();
        tracker
            .record_failure(FailureKind::Inference, "boom2")
            .unwrap();
        assert!(matches!(tracker.state(), HealthState::Healthy));
        assert!(!tmp.path().join("health.json").exists());
    }

    #[test]
    fn budget_exhaustion_then_transition_writes_health_json() {
        let (tmp, mut tracker) = fresh(RetryBudget::new(1, 3));
        // First failure consumed.
        tracker
            .record_failure(FailureKind::Inference, "first")
            .unwrap();
        // Second failure must trip exhaustion.
        let err = tracker
            .record_failure(FailureKind::Inference, "second")
            .unwrap_err();
        assert!(matches!(
            err,
            HealthError::BudgetExhausted {
                kind: FailureKind::Inference
            }
        ));

        let when = ts("2026-05-04T12:34:56Z");
        let incident = sample_incident(FailureKind::Inference, when);
        tracker.transition_to_unhealthy(incident.clone()).unwrap();

        let live = tmp.path().join("health.json");
        assert!(live.is_file());
        let v: serde_json::Value = serde_json::from_slice(&fs::read(&live).unwrap()).unwrap();
        assert_eq!(v.get("version").and_then(|x| x.as_u64()), Some(1));
        assert_eq!(v.get("state").and_then(|x| x.as_str()), Some("Unhealthy"));
        assert_eq!(
            v.get("since").and_then(|x| x.as_str()),
            Some("2026-05-04T12:34:56Z")
        );
        let inc = v.get("incident").unwrap();
        assert_eq!(
            inc.get("failing").and_then(|f| f.get("type")),
            Some(&json!("Inference"))
        );

        match tracker.state() {
            HealthState::Unhealthy { since, incident: i } => {
                assert_eq!(since, &when);
                assert_eq!(i, &incident);
            }
            HealthState::Healthy => panic!("state should be Unhealthy"),
        }
    }

    #[test]
    fn recovery_archives_and_clears_live_health_json() {
        let (tmp, mut tracker) = fresh(RetryBudget::new(0, 0));
        let when = ts("2026-05-04T12:34:56Z");
        let _ = tracker.record_failure(FailureKind::Inference, "x");
        tracker
            .transition_to_unhealthy(sample_incident(FailureKind::Inference, when))
            .unwrap();
        assert!(tmp.path().join("health.json").exists());

        tracker.mark_tick_success().unwrap();
        assert!(matches!(tracker.state(), HealthState::Healthy));
        assert!(!tmp.path().join("health.json").exists());

        let archive = tmp.path().join("health").join("2026-05-04T12:34:56Z.json");
        assert!(archive.is_file(), "archive should exist at {archive:?}");
        let v: serde_json::Value = serde_json::from_slice(&fs::read(&archive).unwrap()).unwrap();
        assert_eq!(v.get("state").and_then(|x| x.as_str()), Some("Unhealthy"));
    }

    #[test]
    fn repeated_failure_while_unhealthy_updates_in_place_no_archive() {
        let (tmp, mut tracker) = fresh(RetryBudget::new(0, 0));
        let when1 = ts("2026-05-04T12:00:00Z");
        let when2 = ts("2026-05-04T13:00:00Z");

        tracker
            .transition_to_unhealthy(sample_incident(FailureKind::Inference, when1))
            .unwrap();
        // No archive directory yet — recovery has not happened.
        assert!(!tmp.path().join("health").exists());

        // Second transition while still Unhealthy: live file updates,
        // archive directory still absent.
        let mut second = sample_incident(FailureKind::ToolCall, when2);
        second.last_error = "newer".into();
        tracker.transition_to_unhealthy(second.clone()).unwrap();

        match tracker.state() {
            HealthState::Unhealthy { since, incident } => {
                assert_eq!(since, &when2);
                assert_eq!(incident.last_error, "newer");
                assert_eq!(incident.failing.kind, FailureKind::ToolCall);
            }
            HealthState::Healthy => panic!("state should still be Unhealthy"),
        }
        assert!(!tmp.path().join("health").exists());

        // Live file reflects the newer incident.
        let v: serde_json::Value =
            serde_json::from_slice(&fs::read(tmp.path().join("health.json")).unwrap()).unwrap();
        assert_eq!(
            v.get("since").and_then(|x| x.as_str()),
            Some("2026-05-04T13:00:00Z")
        );
        let inc = v.get("incident").unwrap();
        assert_eq!(
            inc.get("failing").and_then(|f| f.get("type")),
            Some(&json!("ToolCall"))
        );
        assert_eq!(
            inc.get("last_error").and_then(|x| x.as_str()),
            Some("newer")
        );
    }

    #[test]
    fn retirement_json_is_never_touched_by_health_path() {
        let (tmp, mut tracker) = fresh(RetryBudget::new(0, 0));
        let retirement = tmp.path().join("retirement.json");
        let original = b"{\"reason\":\"sentinel\"}";
        fs::write(&retirement, original).unwrap();

        // Unhealthy.
        tracker
            .transition_to_unhealthy(sample_incident(
                FailureKind::Inference,
                ts("2026-05-04T12:00:00Z"),
            ))
            .unwrap();
        assert_eq!(fs::read(&retirement).unwrap(), original);

        // Repeated failure.
        tracker
            .transition_to_unhealthy(sample_incident(
                FailureKind::ToolCall,
                ts("2026-05-04T13:00:00Z"),
            ))
            .unwrap();
        assert_eq!(fs::read(&retirement).unwrap(), original);

        // Recovery.
        tracker.mark_tick_success().unwrap();
        assert_eq!(fs::read(&retirement).unwrap(), original);
    }

    #[test]
    fn begin_tick_resets_counters_without_touching_state() {
        let (_tmp, mut tracker) = fresh(RetryBudget::new(1, 1));
        tracker.record_failure(FailureKind::Inference, "x").unwrap();
        tracker.record_failure(FailureKind::ToolCall, "y").unwrap();
        // One more of either would now trip exhaustion.
        assert!(tracker.record_failure(FailureKind::Inference, "z").is_err());
        // begin_tick resets — state stays Healthy, counters fresh.
        tracker.begin_tick();
        assert!(matches!(tracker.state(), HealthState::Healthy));
        tracker
            .record_failure(FailureKind::Inference, "fresh")
            .unwrap();
        tracker
            .record_failure(FailureKind::ToolCall, "fresh")
            .unwrap();
    }

    #[test]
    fn mixed_failures_share_independent_budgets_both_can_exhaust() {
        // Inference path exhausts.
        let (_tmp, mut tracker) = fresh(RetryBudget::new(1, 3));
        tracker.record_failure(FailureKind::ToolCall, "t").unwrap();
        tracker.record_failure(FailureKind::ToolCall, "t").unwrap();
        tracker.record_failure(FailureKind::ToolCall, "t").unwrap();
        tracker.record_failure(FailureKind::Inference, "i").unwrap();
        let err = tracker
            .record_failure(FailureKind::Inference, "i2")
            .unwrap_err();
        assert!(matches!(
            err,
            HealthError::BudgetExhausted {
                kind: FailureKind::Inference
            }
        ));

        // Tool-call path exhausts.
        let (_tmp, mut tracker) = fresh(RetryBudget::new(1, 3));
        tracker.record_failure(FailureKind::Inference, "i").unwrap();
        for _ in 0..3 {
            tracker.record_failure(FailureKind::ToolCall, "t").unwrap();
        }
        let err = tracker
            .record_failure(FailureKind::ToolCall, "t")
            .unwrap_err();
        assert!(matches!(
            err,
            HealthError::BudgetExhausted {
                kind: FailureKind::ToolCall
            }
        ));
    }

    #[test]
    fn schema_round_trip_via_health_json() {
        let (tmp, mut tracker) = fresh(RetryBudget::new(0, 0));
        let when = ts("2026-05-04T12:34:56Z");
        let incident = sample_incident(FailureKind::ToolCall, when);
        tracker.transition_to_unhealthy(incident.clone()).unwrap();

        // Re-open against the same root: state rehydrates Unhealthy with
        // the same incident, and `health.json` parses back into our
        // private envelope deeply.
        let reopened = HealthTracker::open(tmp.path(), RetryBudget::default()).unwrap();
        match reopened.state() {
            HealthState::Unhealthy { since, incident: i } => {
                assert_eq!(since, &when);
                assert_eq!(i, &incident);
            }
            HealthState::Healthy => panic!("expected Unhealthy after reopen"),
        }

        let bytes = fs::read(tmp.path().join("health.json")).unwrap();
        let record: HealthRecord = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(record.version, SCHEMA_VERSION);
        assert_eq!(record.since, when);
        assert_eq!(record.incident, incident);
    }

    #[test]
    fn open_rejects_unknown_schema_version() {
        let tmp = TempDir::new().unwrap();
        let live = tmp.path().join("health.json");
        let bogus = json!({
            "version": 999,
            "state": "Unhealthy",
            "since": "2026-05-04T12:34:56Z",
            "incident": {
                "failing": { "type": "Inference", "details": {} },
                "retry_trail": [],
                "last_error": "x",
                "transitioned_at": "2026-05-04T12:34:56Z"
            }
        });
        fs::write(&live, serde_json::to_vec(&bogus).unwrap()).unwrap();

        let err = HealthTracker::open(tmp.path(), RetryBudget::default()).unwrap_err();
        assert!(matches!(err, HealthError::UnsupportedVersion(999)));
    }
}
