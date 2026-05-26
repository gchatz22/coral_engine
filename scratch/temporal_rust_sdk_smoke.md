# Temporal Rust SDK smoke (stage 0.2 / JAR2-41)

*Status: findings record. Updated by the JAR2-41 smoke run; supersedes the "verify before committing" caveat in `scratch/durability_substrate.md` § 4.1 and § 8 decision 3. The smoke binary lives at `crates/jarvis_temporal/src/bin/temporal_smoke.rs`; this doc is what the maintainer reads when deciding whether stage 3 can build on this substrate or needs a different one.*

---

## 1. SDK pin + bring-up

**Crate: `temporalio-sdk` v0.4.0** (with the sibling crates `temporalio-client`, `temporalio-common`, `temporalio-sdk-core`, `temporalio-macros` at the same `=0.4.0` pin). Latest published 2026-04-29; all releases since 2026-02 are tagged "Pre-release" on GitHub and the SDK's own `lib.rs` opens with:

> This crate defines a Public Preview Temporal Rust SDK. Currently defining activities and running an activity-only worker is the most stable code. **Workflow definitions exist and running a workflow worker works, but the API is still very unstable.**

This is the most honest production-readiness signal the SDK ships, and it's the one to quote when talking to the maintainer about substrate risk. Quoted verbatim here so the doc doesn't paper over it.

**Why these versions:**
- v0.4.0 is the **latest** crates.io release as of this smoke (2026-05-23). Going one minor older buys us nothing — every 0.x release labels itself "Pre-release" and the API churn between 0.3 and 0.4 is real per the GitHub release notes. Pinning to the latest costs us no stability we'd otherwise have.
- All five sibling crates are pinned with `=` so a future `cargo update` doesn't silently shift them under us. Stage 3 will revisit this pin deliberately when it actually depends on the SDK in production code.

**Repo layout note worth recording:** the Rust SDK ships from its own repo `temporalio/sdk-rust` (not the shared `temporalio/sdk-core` repo that earlier scratch docs assumed). The crates path is `crates/sdk` inside that repo. `sdk-core` exists as a separate crates.io package (`temporalio-sdk-core`) and is the lower-level building block; the SDK depends on it.

**Bring-up choice for the smoke: `temporal server start-dev`** (the `temporal` CLI's bundled dev server). Install:

```
brew install temporal
temporal server start-dev   # binds 7233 (gRPC) + 8233 (web UI)
```

In-memory, single binary, sub-second startup. The Docker `temporalio/temporal-server:auto-setup` image is the heavier alternative; JAR2-42 (Docker dev env) brings that path in. The smoke is server-agnostic — it talks to whatever's on `TEMPORAL_ADDRESS` (default `http://localhost:7233`) — so JAR2-42's docker-compose supersedes the dev-server choice transparently.

**Build-time dependency surprise:** the SDK transitively depends on `prost-wkt-types`, whose build script invokes `protoc`. CI and dev environments need `protoc` on PATH. Install: `brew install protobuf` on macOS, `apt-get install protobuf-compiler` on Debian/Ubuntu. JAR2-42 should land this in the Docker image; for now the smoke doc notes it as a prerequisite. The error mode is loud (`Could not find 'protoc'`), so it's not a silent gotcha — but it's a real toolchain ask that the workspace didn't have before.

---

## 2. Per-primitive verdict

Each row maps to a primitive `scratch/agent_runtime.md` § 4 lists. "Worked" means the SDK had a clean, named primitive and the smoke binary exercised it end-to-end against a real Temporal Server. Lines in the binary are the source of evidence.

| Primitive | Verdict | Evidence (line in `temporal_smoke.rs`) | Notes |
|---|---|---|---|
| Workflow definition (`#[workflow]` + `#[workflow_methods]` + `#[run]`) | **WORKED** | `AgentLoopWorkflow` line 142, `SmokeChildWorkflow` line 279, `ContinueAsNewSmokeWorkflow` line 291 | Three workflow types defined and registered cleanly. Workflow state struct fields become the workflow's in-memory state. |
| Activity definition + static worker registration (`#[activities]` + `#[activity]`) | **WORKED** | `SmokeActivities` line 72; registration line 369 (`register_activities(SmokeActivities)`) | Five activities defined on one impl block; one `register_activities` call wires them. |
| Worker (`Worker::new`, `worker.run()`) | **WORKED-WITH-CAVEATS** | `build_worker` line 361 | The `Worker` value holds `Box<dyn WorkerInterceptor>` which is not `Send`, so a `Worker` **cannot move into `tokio::spawn`**. The smoke runs `worker.run()` on the main task and drives the test from a separately-spawned task that talks to the worker via the Temporal server. Documented inline in the binary; CI test uses the same pattern. Stage 3 will hit the same constraint and want a clear answer (run the worker as the main task of the worker binary, drive triggers in via the client). |
| Signal handler (`#[signal]` on a workflow method) | **WORKED** | `external_signal` line 156, `retire` line 163 | Sync `#[signal]` methods take `&mut self` and a `SyncWorkflowContext`. The signal pushes onto `pending_triggers: Vec<String>`, which the `run` body observes via `wait_condition`. End-to-end: client `handle.signal(...)` → worker → workflow state mutation → race outcome. |
| Durable timer (`ctx.timer(Duration)`) | **WORKED** | `ctx.timer(Duration::from_millis(50))` line 173, `ctx.timer(Duration::from_secs(5))` line 184 | Returns `impl CancellableFuture<TimerResult>`. Composes with `workflows::select!` (see next row). |
| `wait_condition` racing a signal against a timer | **WORKED-WITH-CAVEATS** | `wait_condition` + `select!` lines 183–190 | The race must use the SDK's deterministic `temporalio_sdk::workflows::select!`, **not** `tokio::select!`. The SDK README is explicit: "Do not use `tokio` or `futures` concurrency primitives directly in workflow code. … `tokio::select!`, `tokio::spawn`, `futures::select!` introduce nondeterministic behavior that will break workflow replay." The SDK also ships a runtime nondeterminism detector that fails workflow tasks with a descriptive error if a non-SDK wake-up is observed. **This is correct shape**, but it's a tax: every workflow-level concurrency construct in stage 3 has to be expressed via `temporalio_sdk::workflows::{select!, join!, join_all}`, not the Rust-async primitives our team is fluent in. Worth calling out in stage 3.1 code review guidance. |
| `start_child_workflow` with `parent_close_policy=Abandon` | **WORKED** | `ChildWorkflowOptions { parent_close_policy: ParentClosePolicy::Abandon, .. }` lines 214–219; `ctx.child_workflow(...)` line 222 | Path lives at `temporalio_common::protos::temporal::api::enums::v1::ParentClosePolicy::Abandon` — a deep proto-derived path that's awkward to type but it's the real one (verified by reading `crates/sdk/src/workflow_context/options.rs` v0.4.0 line 21). The smoke deliberately does NOT `.result().await` the child, letting the parent return while the child keeps running — that's the whole point of `Abandon`. Stage 5's parent–child topology can use this directly. |
| `continue_as_new` (`ctx.continue_as_new(&carryover, opts)`) | **WORKED** | `ContinueAsNewSmokeWorkflow` line 291; `ctx.continue_as_new(&(current + 1, max), ContinueAsNewOptions::default())` line 302 | API returns `Err(WorkflowTermination::continue_as_new(...))` — calling it terminates the current run, the SDK schedules a fresh run with the new carryover. The smoke iterates current→max with `max=3` and verifies the final return value. **For stage 3.11**: the trigger should be `ctx.continue_as_new_suggested()` (or `ctx.history_length()`), which the SDK exposes — not a turn-count heuristic. Both are visible on `WorkflowContext` per `workflow_context.rs` lines 618 and 650. |
| Dynamic activity registration (one activity routing arbitrary tool names) | **MISSING** | `tool_echo` + `tool_reverse` lines 98–117 as the static workaround | **No `#[activity(dynamic = true)]`, no `register_activity_by_name`, no `unknown_activity_handler` on `WorkerOptions`.** Verified by reading `crates/sdk/src/activities.rs` v0.4.0 lines 353–420 — only `ActivityImplementer` (compile-time) and `register_activity[ies]` are exposed. The Go/Python SDKs have dynamic dispatch; Rust does not. **Stage 3.7's `execute_tool` design depends on this.** Workarounds, in order of preference: (a) register every known tool by name at build time — works for a closed set of MCP tools, doesn't work for dynamically-loaded tools; (b) one `execute_tool` activity that takes `(tool_name: String, args: serde_json::Value)` and does its own dispatch inside the activity body — loses Temporal's per-activity-type retry config but keeps stage 3.7's shape; (c) upstream contribution to add dynamic registration. Option (b) is the realistic stage 3.7 path until (c) lands; the smoke doc records this as a **blocker on the original stage 3.7 design**, not on Temporal itself. |
| `signal_external_workflow` (parent ← child output signaling, stage 5.4) | **WORKED-WITH-CAVEATS** | `crates/jarvis_temporal/src/workflow.rs::signal_parent_child_output` — child's `Decision::EmitOutput` arm fires through this | **Stage 5.4 / JAR2-81 exercised this end-to-end against `temporal server start-dev`** via `crates/jarvis_temporal/tests/child_parent_signal.rs` (happy path + parent-unreachable failure path). The API surface differs from what the JAR2-41 row above assumed: there is **no single `ctx.signal_external_workflow(workflow_id, signal_name, payload)` method**. The real shape is the two-step typed chain documented in row 8 of § 3 below. The signal-name string therefore can't be threaded through dynamically — it's bound at compile time via the `SignalDefinition` marker the `#[signal]` macro generates. |

---

## 3. Surprises and workarounds

Calling out the non-obvious things future agents working on stage 3+ will benefit from knowing — in rough order of how much they bit me.

### 3.1 `Worker` is not `Send`

The `Worker` type owns interceptor state behind a `Box<dyn WorkerInterceptor>` which is not `Send`. Practical implication: `tokio::spawn(async move { worker.run().await })` fails to compile.

**Workaround in the smoke**: run `worker.run().await` on the main task and drive the test from a separately-`tokio::spawn`-ed driver task that uses the **client** (which is `Send + Clone`) to talk to the worker via the server. The driver calls `worker.shutdown_handle()` (a `Fn()` that initiates worker shutdown) when it's done, which makes `worker.run()` return so the main task can exit.

**Stage 3 implication**: the worker binary's top-level shape will be one worker per process, running on the main `tokio` runtime task. This is fine — that's the production shape anyway — but the in-process hermetic test pattern that puts the worker and the test in the same async block has to live with the spawn-from-a-helper-task pattern. Document this in stage 3.2 (`AgentWorkflow` skeleton) so reviewers don't ask "why aren't we spawning the worker?"

### 3.2 Macros require `futures` + `futures-util` as direct deps

The `#[activities]`, `#[workflow]`, `#[workflow_methods]` macros and the `temporalio_sdk::workflows::select!` macro expand to paths like `futures_util::future::FutureExt::boxed` and `futures::future::join_all`. The SDK does NOT re-export these — the macro-using crate has to add both as direct dependencies. The error message ("could not find `futures_util` in the list of imported crates") points at the macro invocation, not the SDK, so the first reaction is "wait, the SDK should have brought this in." It doesn't, and there's no documentation that says it must — I discovered it by reading the SDK example tomls and then by reading the macro expansion. Worth filing upstream as either a docs fix or a re-export.

### 3.3 `start_activity` returns the future directly, not `Result<future>`

The signature is `pub fn start_activity<AD>(...) -> impl CancellableFuture<Result<AD::Output, ActivityExecutionError>>`. So `ctx.start_activity(...).await?` works; `ctx.start_activity(...)?.await?` does not (no `Try` impl on the wrapper future). Took one compile cycle to see this; the official examples use `.await?` without the inner `?`. Stage 3 reviewers will catch this on second sight; calling it out so they don't have to.

### 3.4 `register_activities` takes the bare value, not `Arc<T>`

The macro impls `ActivityImplementer for SmokeActivities` (the bare type). `register_activities` wraps in `Arc` internally — passing `Arc<SmokeActivities>` is a type error. This complicates external observation of the registered instance (e.g. an invocation counter): the registered instance is owned by the SDK, not the caller. The smoke works around it with a `static AtomicUsize`; stage 3's activities will likely want a process-wide `OnceLock<Arc<AgentCore>>` shared by all activity bodies for the same reason.

### 3.5 `Worker::new` returns `Box<dyn Error>`, not `Send + Sync`

`Worker::new(...) -> Result<Worker, Box<dyn std::error::Error>>`. The error type doesn't implement `Send + Sync` and so doesn't auto-convert into `anyhow::Error` via `?`. Wrap with `.map_err(|e| anyhow::anyhow!("{e}"))`. This is a small papercut; worth a small upstream PR to tighten the error bound.

### 3.6 `ChildWorkflowOptions.parent_close_policy` lives behind a deep proto path

The full type path is `temporalio_common::protos::temporal::api::enums::v1::ParentClosePolicy::Abandon`. This is real — it's a proto-derived enum — but typing it out is awkward, and no shorter alias is re-exported by `temporalio-sdk`. Stage 3 + stage 5 code that touches this will benefit from a local type alias near the place that uses it.

### 3.7 No `WorkflowEnvironment` (hermetic test runtime)

The Python SDK (`temporalio.testing.WorkflowEnvironment`), Go SDK (`testsuite.WorkflowTestSuite`), and Java SDK (`TestWorkflowEnvironment`) all ship a hermetic in-process Temporal server for tests. **The Rust SDK does not** in v0.4.0. I verified by reading the top-level `lib.rs` re-exports (no `WorkflowEnvironment` symbol) and grepping the repo (`gh search code --repo temporalio/sdk-rust "WorkflowEnvironment"` returns no hits).

**Stage 3 implication**: hermetic-by-default workflow tests are not possible until upstream ships this primitive (or we build one ourselves, which is a non-trivial project). The pragmatic substitute: gate workflow tests behind `TEMPORAL_LIVE_TEST=1` and run them in a CI job that spins up `temporal server start-dev` as a service container. The smoke binary's `live_temporal_smoke` test follows this pattern. Stage 3 CI will need a slow-path job with the same env-var gating until upstream catches up.

This is the single biggest gap relative to where the Go/Java/Python SDKs are. It doesn't block stage 3 directly (we can use the env-var-gated live path), but it makes fast hermetic feedback loops impossible at the workflow level — only at the `AgentCore` level (the pure logic that stage 2.5's `MemoryStorage` already enables hermetic-style testing for).

### 3.8 `tokio` patch surface

The SDK pins `tokio = "1.47"`. Our `jarvis_node` uses tokio with semver-compatible features and no specific version pin — they unify cleanly. Cargo.lock confirms a single `tokio 1.52` build was selected; no patch surface conflicts. Worth re-checking when stage 3 lands real production code, but for now there's no friction.

### 3.9 Edition 2024 in `temporalio-sdk`

The SDK is `edition = "2024"`. Our workspace pins Rust 1.88 (per `rust-toolchain.toml`), which supports edition 2024 (stabilized in 1.85), so this is a non-issue today. Calling it out so future-us doesn't trip on it if anyone tries to lower the MSRV.

### 3.10 `signal_external_workflow` is a two-step typed chain (JAR2-81 finding)

The JAR2-41 smoke verified `ExternalWorkflowHandle` and `WorkflowContext::external_workflow(...)` exist (row 8 of § 2). Stage 5.4 / JAR2-81 was the first live exercise — that work surfaced a real API divergence from what § 5 stage 5.4 of `scratch/temporal_staged_plan.md` and the JAR2-81 ticket pseudocode assumed.

**There is no `ctx.signal_external_workflow(workflow_id, signal_name, payload).await` method.** The actual call shape, verified by reading `~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/temporalio-sdk-0.4.0/src/workflow_context.rs:799-810` (the `external_workflow` constructor) and `.../src/workflow_context.rs:2007-2030` (`ExternalWorkflowHandle::signal`), is:

```rust
ctx.external_workflow(workflow_id, run_id)            // -> ExternalWorkflowHandle
   .signal(MyWorkflow::external_signal, payload)      // SignalDef: SignalDefinition
   .await                                              // -> Result<SignalExternalOk, Failure>
```

Two consequences:

1. **The signal name is bound at compile time** via the `SignalDefinition` marker the `#[signal]` macro generates (e.g. `AgentWorkflow::external_signal` is the const auto-generated from the `#[signal] fn external_signal(...)` method). There is no `handle.signal_by_name(&str, payload)` path; the macro-emitted const is the only way to address the signal. **Implication for `ParentRef.signal: String`** (`crates/jarvis_temporal/src/workflow.rs`): it's informational in v1 — the workflow body ignores it because the dispatch target is the compile-time `AgentWorkflow::external_signal` marker regardless. The field stays on the wire for future non-`AgentWorkflow` targets that would use a different `SignalDefinition`.

2. **The result is `Result<SignalExternalOk, Failure>`** (`SignalExternalWfResult` in `temporalio-sdk-0.4.0/src/lib.rs:1049`). A failed signal-to-nonexistent-workflow surfaces as `Err(Failure)` after server acknowledgment — JAR2-81's failure-mode live test (`crates/jarvis_temporal/tests/child_parent_signal.rs::child_continues_normally_when_parent_signal_fails`) confirms this: the child's `signal(...)` call returns `Err`, the `signal_parent_child_output` helper in `workflow.rs` logs + continues per Stage 5 Project decision 10, and the child completes normally.

**No upstream issue filed** — the divergence is doc-only (the SDK works as designed; the assumed name was speculative in the smoke + ticket text). If a future ticket needs name-keyed dispatch against a workflow type that isn't statically importable from inside the worker crate, that's the upstream filing trigger; today every signal target is `AgentWorkflow`, which is in scope.

---

## 4. What the smoke does NOT cover

Out-of-scope on purpose; recording so stage 3+ knows what to add to its own integration tests.

- **Authentication / TLS** — local plaintext only. Production deployments will need `temporalio_client::ConnectionOptions::tls(...)`.
- **`signal_external_workflow`** — exists in the API and was first exercised end-to-end by Stage 5.4 (JAR2-81); see § 2 row 8 and § 3.10 above.
- **Update handlers** (`#[update]` + `WorkflowExecuteUpdateOptions`) — exists, used by the upstream `message_passing` example, not in scope for the agent-runtime primitive list.
- **Query handlers** (`#[query]`) — exists, not in scope for stage 0.
- **Local activities** (`start_local_activity`) — exists, may be useful for stage 3.5/3.6 (cheap activities that don't need a full activity worker round-trip), out of scope here.
- **Heartbeats** (`ActivityContext::record_heartbeat`) — exists, stage 3.7 will need this for long-running tool calls.
- **Activity cancellation** — exists, deferred until stage 6's human-override path needs it.
- **Worker scaling** (multiple workers per task queue) — single worker only.
- **Performance benchmarking** — explicitly out of scope per the ticket.

---

## 5. Recommendation for stage 3+

The maintainer makes the call; here is the case the smoke supports.

**Recommendation: PROCEED, with two upstream contributions queued.**

Reasoning, in order:

1. **Every load-bearing primitive `agent_runtime.md` § 4 lists is present and works** end-to-end. The race-signal-vs-timer pattern that's the heart of the agent loop, continue-as-new, child workflows with `Abandon`, signals, activities, the worker — all green. There is no primitive missing that blocks stage 3 from starting.

2. **Two real gaps** are recorded — dynamic activity registration (stage 3.7) and `WorkflowEnvironment` (stage 3 CI). Both have workarounds the smoke validates:
   - **Stage 3.7**: one `execute_tool(tool_name, args)` activity that does its own dispatch inside the activity body. Loses Temporal's per-activity-type retry config; we'd live with that or layer our own retry policy on top.
   - **Stage 3 hermetic tests**: defer to `AgentCore`-level testing (already enabled by stage 2.5's `MemoryStorage`) for fast feedback; gate workflow integration tests behind `TEMPORAL_LIVE_TEST=1` and run them in a slow CI job with a service-container Temporal Server.

3. **The "very unstable" API warning is real but not blocking.** The 0.3 → 0.4 changes happened on a 5-week cadence; the breakage profile is the kind a focused stage-3 sprint can absorb. The alternative substrates (Restate, custom-sqlite-journal) each carry their own risks — for Restate, an early Rust SDK with a smaller user base; for custom, building durable-execution primitives badly that Temporal already builds well. The smoke evidence says Temporal Rust is closer to ready than `scratch/durability_substrate.md` § 4.1 credited.

4. **Two upstream contributions** to file ahead of stage 3, in parallel with the early stage-3 tickets:
   - **Dynamic activity dispatch** (or, weaker: `unknown_activity_handler` on `WorkerOptions`). This is the single most important missing primitive for our stage-3.7 shape, and the Go/Python SDKs already have it — the gap is closeable.
   - **Hermetic test environment** (a Rust `WorkflowEnvironment` analogous to Python's). Lower priority because the workaround (env-gated live tests) is acceptable; but the unblocking value is high.

5. **What we do NOT do**: switch substrate. The Restate Rust SDK has its own 0.x churn risk; reverifying it would re-cost a smoke cycle. Custom-sqlite-journal would have us building durable execution from scratch for months. Neither option's downside is offset by avoiding the Temporal warts the smoke surfaced.

**If the maintainer disagrees with PROCEED**: the smoke binary is the artifact that backs whatever call you want to make. The verdict table above is the negotiable ground.

---

## 6. How to run the smoke

For future-us / future-agent reproducing the run:

```bash
# Prereqs (once, macOS):
brew install temporal protobuf

# Bring up Temporal Server in one terminal:
temporal server start-dev   # binds :7233 (gRPC) + :8233 (web UI)

# Run the smoke binary in another:
cd /path/to/jarvis-engine
cargo run -p jarvis_temporal --bin temporal-smoke

# Or run the env-gated `#[tokio::test]`:
TEMPORAL_LIVE_TEST=1 cargo test -p jarvis_temporal --bin temporal-smoke -- live_temporal_smoke --nocapture

# Default `cargo test` is hermetic — the live test no-ops without the env var:
cargo test --workspace   # green, fast, no Temporal Server required
```

Output: a verdict table on stdout, `MISSING:` lines on stderr for any gaps, exit 0 if the binary ran to completion. The verdict table is the artifact of record; the doc you're reading is the durable interpretation of it.

**Note on workflow ID collisions**: every `cargo run` suffixes workflow IDs with `epoch_ms`, so iterative runs don't collide on Temporal's default reuse policy. The Temporal Web UI at <http://localhost:8233> shows the full history of every run — useful for debugging when the smoke fails.

---

## 7. References

- The smoke binary: `crates/jarvis_temporal/src/bin/temporal_smoke.rs`.
- SDK repo: <https://github.com/temporalio/sdk-rust> (note: NOT `sdk-core`, which is a separate repo).
- SDK crate: <https://crates.io/crates/temporalio-sdk> (v0.4.0, 2026-04-29).
- API docs: <https://docs.rs/temporalio-sdk/0.4.0/temporalio_sdk/>.
- SDK README (in-repo): `temporalio/sdk-rust/crates/sdk/README.md` — has the "Workflow API still very unstable" disclaimer.
- SDK examples: `temporalio/sdk-rust/crates/sdk/examples/{hello_world,message_passing,timer_examples,continue_as_new,child_workflows}` — the smoke's primitive demonstrations are mechanically derived from these.
- Stage 0 ticket: Linear JAR2-41.
- Related: `scratch/temporal_staged_plan.md` § 5 stage 0 (parent plan), `scratch/durability_substrate.md` § 4.1 + § 8 decision 3 (the caveats this smoke retires), `scratch/agent_runtime.md` § 4 (the primitive list the smoke had to cover).
