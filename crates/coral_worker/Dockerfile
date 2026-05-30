# Worker container for `coral_worker`.
#
# Build from the repo root so the workspace is in context:
#   docker compose build worker
# or:
#   docker build -t coral-worker:dev -f crates/coral_worker/Dockerfile .

# ---- Build stage --------------------------------------------------------
# Pin Rust to the same version as `rust-toolchain.toml`. Bookworm
# base keeps glibc compatible with the runtime image below.
FROM rust:1.88-bookworm AS builder

WORKDIR /build

COPY Cargo.toml Cargo.lock rust-toolchain.toml rustfmt.toml ./
COPY crates ./crates

# Compile both LLM vendors in so the container can install a `Decide`
# backend at runtime; the active vendor is picked from `CORAL_MODEL_VENDOR`
# / whichever API key is set (see
# `coral_temporal::worker::build_decide_from_env`). A no-vendor build still
# compiles but exits at boot when it tries to install a `Decide` impl.
RUN cargo build --release -p coral_worker --bin worker --features llm-anthropic,llm-cohere

# ---- Runtime stage ------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# `tini` reaps zombies and forwards signals so `docker stop` propagates
# SIGTERM to the worker cleanly. Cheap and standard.
RUN apt-get update \
    && apt-get install -y --no-install-recommends tini ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Non-root user for the worker. Per-agent FS bind-mount is owned by the
# host user in dev; if uid 1000 doesn't match the host user, the operator
# overrides via `--user` or fixes ownership on the host.
RUN useradd --create-home --uid 1000 coral
USER coral
WORKDIR /home/coral

COPY --from=builder /build/target/release/worker /usr/local/bin/coral-worker

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/coral-worker"]
