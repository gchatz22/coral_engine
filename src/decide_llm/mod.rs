//! `decide_llm` — pieces that turn a model's response into a `Decision`.
//!
//! The kernel's `Decide` trait (see `crate::decision`) is vendor-neutral; this
//! module hosts the bits that bridge a `ModelClient` response back to that
//! trait. JAR2-17 lands the schema + parser. Later tickets add the prompt
//! renderer (JAR2-16), the `LlmDecide` adapter (JAR2-19), and the
//! correction/re-prompt loop on top.
//!
//! Available whenever any LLM impl is enabled (`llm-anthropic` or
//! `llm-cohere`). The schema itself is vendor-neutral — it only depends on
//! the always-compiled `ToolSpec`/`ToolCall` trait types in
//! `crate::model_client` — but there is no `LlmDecide` consumer to feed
//! when no vendor is built, so we gate the whole module on at-least-one
//! impl to keep the default build network-free.

#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
pub mod schema;

#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
pub use schema::{decision_tools, parse_decision, DecisionParseError};
