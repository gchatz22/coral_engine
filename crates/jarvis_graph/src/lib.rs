//! Structural DB for the Jarvis Engine: what graphs exist, what agents
//! are in each graph, which edges connect them, what tools are
//! registered, and the authored mandate per agent. Working memory lives
//! on the per-agent FS; execution state lives in Temporal. Callers
//! construct a `PgPool` from `DATABASE_URL` and hand it in. Apps apply
//! the schema by calling [`MIGRATOR.run(&pool).await`]; `#[sqlx::test]`
//! applies it automatically per-test. Queries use compile-time-checked
//! `sqlx::query!` / `sqlx::query_as!` macros backed by the workspace
//! `.sqlx/` offline cache.
//!
//! [`MIGRATOR.run(&pool).await`]: sqlx::migrate::Migrator::run

pub mod store;
pub mod types;
pub mod yaml;
pub use store::{GraphStore, GraphStoreError};
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
        // Guards against dropping the `postgres` feature from `Cargo.toml`.
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
        // (drops the leading version + underscore).
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
#[cfg(test)]
mod migration_tests {
    use sqlx::{PgPool, Row};

    /// Asserts every table the schema declares exists after migration.
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
    /// already-applied versions).
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
