//! `decide_llm` — pieces that turn a model's response into a `Decision`.
//!
//! The kernel's `Decide` trait (see `crate::decision`) is vendor-neutral; this
//! module hosts the bits that bridge a `ModelClient` response back to that
//! trait. JAR2-17 lands the schema + parser. Later tickets add the prompt
//! renderer (JAR2-16), the `LlmDecide` adapter (JAR2-19), and the
//! correction/re-prompt loop on top.
//!
//! Everything here is gated on `llm-anthropic` because it depends on
//! `crate::model_client`'s `ToolSpec`/`ToolCall` types, which only exist
//! when that feature is enabled. The gating choice mirrors the rest of the
//! LLM stack and keeps the default build network-free.

#[cfg(feature = "llm-anthropic")]
pub mod schema;

#[cfg(feature = "llm-anthropic")]
pub use schema::{decision_tools, parse_decision, DecisionParseError};
