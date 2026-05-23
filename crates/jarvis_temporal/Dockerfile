# Worker container scaffold for `jarvis_temporal`.
#
# Stage 0.3 (JAR2-42): this builds the stub `worker` binary that prints
# and exits. Real workflow code lands in stage 3. The image exists today
# so the production-shape compose path is wired end-to-end and the
# Dockerfile is exercised in CI / by hand.
#
# Build from the repo root so the workspace is in context:
#   docker compose build worker
# or:
#   docker build -t jarvis-worker:dev -f crates/jarvis_temporal/Dockerfile .

# ---- Build stage --------------------------------------------------------
# Pin Rust to the same version as `rust-toolchain.toml` (1.88). Bookworm
# base keeps glibc compatible with the runtime image below.
FROM rust:1.88-bookworm AS builder

WORKDIR /build

# Copy the workspace. The stub worker has no dependencies beyond std, so
# we accept a single COPY + build (no cargo-chef dep-cache layer yet —
# that becomes worthwhile in stage 3 when real deps land).
COPY Cargo.toml Cargo.lock rust-toolchain.toml rustfmt.toml ./
COPY crates ./crates

RUN cargo build --release -p jarvis_temporal --bin worker

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
RUN useradd --create-home --uid 1000 jarvis
USER jarvis
WORKDIR /home/jarvis

COPY --from=builder /build/target/release/worker /usr/local/bin/jarvis-worker

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/jarvis-worker"]
