//! `jarvis_graph` — structural DB for the Jarvis Engine.
//!
//! This crate owns the *structural* state layer described in
//! `scratch/durability_substrate.md` § 2: what graphs exist, what agents
//! are in each graph, which edges connect them, what tools are
//! registered, and the authored mandate per agent. Working memory
//! (outputs, evidence, notes, claims, health) stays on disk under the
//! per-agent FS; execution state (trigger queue, scheduler cursor,
//! in-flight ticks) lives in Temporal. The three layers do not bleed
//! into each other.
//!
//! ## Stage 1 scope
//!
//! Stage 1.1 (this ticket, JAR2-46) is the crate stub: workspace
//! registration, `sqlx` dep choice, and the `DATABASE_URL` env-var
//! convention. Schema migrations, Rust types, and the CRUD API land in
//! stages 1.2 / 1.3 / 1.4 respectively (`scratch/temporal_staged_plan.md`
//! § 5 stage 1).
//!
//! ## Configuration
//!
//! Connection is via the standard `DATABASE_URL` env var. The dev
//! default (set in `.env.example` from JAR2-42) targets the Postgres
//! instance brought up by the repo's `docker-compose.yml`:
//!
//! ```text
//! DATABASE_URL=postgres://jarvis:jarvis@localhost:5432/jarvis_structural
//! ```
//!
//! The crate itself does not read the env var — callers (the worker
//! binary, `jarvis apply`, tests via `#[sqlx::test]`) construct a
//! `PgPool` and hand it in. This keeps the library free of process-wide
//! configuration coupling.

#[cfg(test)]
mod tests {
    /// Sanity check that the crate links and that `sqlx`'s Postgres
    /// driver is in scope. The latter catches feature-flag regressions
    /// (e.g. dropping `postgres` from the default feature list) without
    /// needing a live DB at test time.
    #[test]
    fn sqlx_postgres_driver_in_scope() {
        // `Postgres` is the marker type for the Postgres backend. If the
        // `postgres` feature is dropped from `Cargo.toml`, this stops
        // compiling — exactly the regression we want to catch.
        let _phantom: std::marker::PhantomData<sqlx::Postgres> = std::marker::PhantomData;
    }
}
