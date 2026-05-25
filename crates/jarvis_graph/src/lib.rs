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
//! Stage 1.2 (JAR2-47) lands the schema + migrations. Subsequent stages
//! land Rust types (1.3, JAR2-48), the `GraphStore` CRUD API (1.4,
//! JAR2-49), and the round-trip integration test (1.5, JAR2-50). See
//! `scratch/temporal_staged_plan.md` § 5 stage 1.
//!
//! ## Schema
//!
//! See `migrations/0001_initial.sql` for the authoritative definition.
//! Tables: `graphs`, `agents`, `edges`, `tools`, `agent_tools`.
//! Per-table design notes (UUID PKs, `ON DELETE CASCADE`, decisions on
//! `mandate_ref` / `tools.kind` / edge same-graph) live in that file's
//! header comment.
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
//!
//! ## Applying migrations
//!
//! Apps apply the schema by calling [`MIGRATOR.run(&pool).await`]:
//!
//! ```ignore
//! use sqlx::postgres::PgPoolOptions;
//! let pool = PgPoolOptions::new().connect(&std::env::var("DATABASE_URL")?).await?;
//! jarvis_graph::MIGRATOR.run(&pool).await?;
//! ```
//!
//! `#[sqlx::test]` applies the same migrator automatically against an
//! ephemeral per-test database, so unit tests don't have to call it
//! themselves.
//!
//! ## Compile-time-checked queries (`sqlx` offline mode)
//!
//! The CRUD API in [`store`] uses `sqlx::query!` / `sqlx::query_as!`
//! macros that verify SQL against the schema at compile time. The
//! `.sqlx/` directory at the workspace root caches the macro
//! expansions so `cargo build` works without a live `DATABASE_URL` (CI
//! relies on this).
//!
//! Regenerate the cache after schema or query changes:
//!
//! ```sh
//! cargo install sqlx-cli --no-default-features --features postgres,rustls
//! # Then, with the dev Postgres up and migrations applied:
//! export DATABASE_URL=postgres://jarvis:jarvis@localhost:5432/jarvis_structural
//! cd crates/jarvis_graph && sqlx migrate run
//! cd ../.. && cargo sqlx prepare --workspace
//! ```
//!
//! Commit the `.sqlx/` directory along with the code change — CI does
//! not have a build-time Postgres.
//!
//! [`MIGRATOR.run(&pool).await`]: sqlx::migrate::Migrator::run

pub mod store;
pub mod types;
pub mod yaml;
pub use store::GraphStore;
pub use types::{AgentRecord, Edge, Graph, ToolRecord};

/// Embedded migration set under `migrations/`. Apps call
/// `MIGRATOR.run(&pool).await` to apply the schema; the macro reads the
/// migration files at compile time so the binary doesn't need them on
/// disk at runtime.
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

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

    /// Sanity check that the migrator embeds the expected migration set.
    /// Catches the "forgot to add the migration file" regression without
    /// needing a live DB.
    #[test]
    fn migrator_includes_initial_migration() {
        let names: Vec<&str> = super::MIGRATOR
            .iter()
            .map(|m| m.description.as_ref())
            .collect();
        // `sqlx::migrate!` derives the description from the filename
        // (drops the leading version + underscore). `0001_initial.sql`
        // -> "initial".
        assert!(
            names.contains(&"initial"),
            "expected an 'initial' migration, got: {:?}",
            names
        );
    }
}

/// Integration-style test that exercises the migrator against a live
/// Postgres via `#[sqlx::test]` (which spins up an ephemeral per-test
/// DB and applies `MIGRATOR` before running the body). Asserts that
/// every expected table exists by querying `information_schema`.
///
/// Behind `#[cfg(test)]` so it doesn't ship in the library; gated to
/// require `DATABASE_URL` at test time, which is the same gate every
/// other stage-1 test will use. Skipped automatically when run without
/// a running Postgres (the macro errors out — see the crate README /
/// `.env.example` for setup).
#[cfg(test)]
mod migration_tests {
    use sqlx::{PgPool, Row};

    /// Asserts every table the schema declares exists after migration.
    /// One test per behavior would be over-fragmented at this stage —
    /// the single roll-up assertion is what 1.2's acceptance criterion
    /// asks for ("Tables exist with the right columns").
    #[sqlx::test(migrator = "super::MIGRATOR")]
    async fn schema_creates_all_expected_tables(pool: PgPool) -> sqlx::Result<()> {
        let expected = ["graphs", "agents", "edges", "tools", "agent_tools"];
        let rows = sqlx::query(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'public' AND table_type = 'BASE TABLE'",
        )
        .fetch_all(&pool)
        .await?;
        let actual: std::collections::HashSet<String> = rows
            .iter()
            .map(|r| r.get::<String, _>("table_name"))
            .collect();
        for table in expected {
            assert!(
                actual.contains(table),
                "expected table `{}` after migration, found: {:?}",
                table,
                actual,
            );
        }
        Ok(())
    }

    /// Asserts the migrator is idempotent: running it twice on the same
    /// DB is a no-op (the `_sqlx_migrations` tracker rejects re-runs of
    /// already-applied versions). This is the acceptance criterion
    /// "Re-running migrations is idempotent."
    #[sqlx::test(migrator = "super::MIGRATOR")]
    async fn migrator_is_idempotent(pool: PgPool) -> sqlx::Result<()> {
        // The first run happened via the test harness. Run it again
        // explicitly and confirm it doesn't error.
        super::MIGRATOR
            .run(&pool)
            .await
            .map_err(sqlx::Error::from)?;
        Ok(())
    }
}
