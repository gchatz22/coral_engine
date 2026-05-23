//! `jarvis_temporal` — stage 0.2 Temporal Rust SDK smoke.
//!
//! This crate currently hosts only the `temporal-smoke` binary (see
//! `src/bin/temporal_smoke.rs`). The smoke exercises the SDK primitive
//! set `scratch/agent_runtime.md` § 4 depends on so we discover gaps
//! before stage 3 commits to the substrate. Findings live in
//! `scratch/temporal_rust_sdk_smoke.md`.
//!
//! The smoke deletes itself when stage 3 lands the real `AgentWorkflow`
//! per `scratch/temporal_staged_plan.md` § 5 stage 0 — nothing in this
//! crate is intended as production code.
//!
//! The library surface intentionally stays empty: tests live alongside
//! the binary so the smoke is self-contained, and stage 3 will pick the
//! library shape it actually needs (`AgentWorkflow`, activity bodies,
//! worker binary) rather than inheriting whatever the smoke leaves
//! behind.
