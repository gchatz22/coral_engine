//! Stage 0.3 (JAR2-42) worker stub.
//!
//! This binary exists so the `crates/jarvis_temporal/Dockerfile` and the
//! `worker` service in `docker-compose.yml` have something concrete to
//! build and run. Real Temporal worker code lands in stage 3 per
//! `scratch/temporal_staged_plan.md` § 5.

fn main() {
    println!("jarvis worker stub — implementation lands in stage 3");
}
