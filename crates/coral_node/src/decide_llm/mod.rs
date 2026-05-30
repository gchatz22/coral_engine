//! `decide_llm` — turns a model's response into a `Decision`.
//!
//! Gated on at-least-one LLM impl (`llm-anthropic` or `llm-cohere`) so the
//! default build stays network-free.

#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
pub mod llm_decide;

#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
pub mod prompt;

#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
pub mod schema;

#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
pub use llm_decide::LlmDecide;

#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
pub use prompt::render;

#[cfg(any(feature = "llm-anthropic", feature = "llm-cohere"))]
pub use schema::{decision_tools, parse_decision, DecisionParseError};
