//! Vendor-neutral trait for "ask a model what to say next". Only the shapes
//! both vendors share live on the trait; vendor-specific knobs live on the
//! impls.

use serde::{Deserialize, Serialize};

#[cfg(feature = "llm-anthropic")]
pub mod anthropic;

#[cfg(feature = "llm-cohere")]
pub mod cohere;

/// One conversational turn handed to a `ModelClient`.
///
/// Roles match the four Anthropic-style buckets the prompt renderer
/// produces. Each impl is responsible for translating the trait shape into
/// its own wire format — for example, the Anthropic impl moves `system`
/// turns to a top-level `system` field and rewraps `tool` turns as `user`
/// messages carrying `tool_result` content blocks.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    pub role: Role,
    pub content: Vec<ContentBlock>,
}

impl Message {
    pub fn system(text: impl Into<String>) -> Self {
        Self {
            role: Role::System,
            content: vec![ContentBlock::Text { text: text.into() }],
        }
    }

    pub fn user(text: impl Into<String>) -> Self {
        Self {
            role: Role::User,
            content: vec![ContentBlock::Text { text: text.into() }],
        }
    }

    pub fn assistant(text: impl Into<String>) -> Self {
        Self {
            role: Role::Assistant,
            content: vec![ContentBlock::Text { text: text.into() }],
        }
    }
}

/// Conversational role.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    System,
    User,
    Assistant,
    /// A tool's reply to a prior `assistant` `tool_use` block. Vendors
    /// disagree on how this is represented on the wire (Anthropic wraps it
    /// in a `user` message with a `tool_result` block); the trait shape
    /// keeps it as a first-class role and the impl does the rewriting.
    Tool,
}

/// One block of content inside a `Message`.
///
/// `tool_use` and `tool_result` are first-class so an `assistant` turn can
/// mix narration with a tool call, and a `tool` turn can reply to a
/// specific call by id. `id` correlation is the contract: a `ToolResult`'s
/// `tool_use_id` must match the `id` of the `ToolUse` it answers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentBlock {
    Text {
        text: String,
    },
    ToolUse {
        id: String,
        name: String,
        input: serde_json::Value,
    },
    ToolResult {
        tool_use_id: String,
        content: String,
    },
}

/// A tool the model is allowed to call.
///
/// Field shape mirrors Anthropic's `tools[]` entry verbatim
/// (`name + description + input_schema`); Cohere's wire format is a near
/// trivial transformation of the same three fields.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
}

/// Vendor-neutral sampling knobs.
///
/// `max_tokens` is required because Anthropic requires it on the wire;
/// Cohere accepts but does not require it, so this field is the strictest
/// common contract. Keep this struct minimal — vendor-specific knobs live
/// on impl config, not here.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompleteOptions {
    pub max_tokens: u32,
    pub temperature: Option<f32>,
}

impl Default for CompleteOptions {
    fn default() -> Self {
        Self {
            max_tokens: 1024,
            temperature: None,
        }
    }
}

/// Input to `ModelClient::complete`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompleteRequest {
    pub messages: Vec<Message>,
    pub tools: Vec<ToolSpec>,
    pub options: CompleteOptions,
}

/// Output of `ModelClient::complete`.
///
/// `tool_calls` is a flat projection of `content` for callers that only
/// care about "did the model want to invoke a tool?". The same call appears
/// once in `content` (as a `ToolUse` block) and once in `tool_calls`; the
/// duplication is intentional.
///
/// `stats` carries the cost/latency accounting block, composing `usage`
/// plus the model id, vendor tag, and wall-clock latency measured around
/// the upstream call. Raw counts only; pricing translation lives elsewhere.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CompleteResponse {
    pub content: Vec<ContentBlock>,
    pub tool_calls: Vec<ToolCall>,
    pub usage: Usage,
    pub stats: CallStats,
}

/// A single tool invocation projected out of `CompleteResponse::content`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: serde_json::Value,
}

/// Token-count accounting from one `complete` call. Field names match
/// Anthropic's; Cohere returns the same two numbers under different keys
/// and the impl normalizes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    pub input_tokens: u32,
    pub output_tokens: u32,
}

/// Which vendor produced a `CallStats`. Small closed enum rather than a
/// `String` so callers can match on it cheaply and tracing field rendering
/// stays allocation-free.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Vendor {
    Anthropic,
    Cohere,
}

impl Vendor {
    /// Stable string form, used for tracing fields and tests.
    pub const fn as_str(self) -> &'static str {
        match self {
            Vendor::Anthropic => "anthropic",
            Vendor::Cohere => "cohere",
        }
    }
}

/// Per-call cost + latency accounting block.
///
/// Composes `Usage` rather than duplicating the token counts so there is
/// exactly one source of truth for tokens-in / tokens-out. Ships only raw
/// inputs; pricing translation and budget enforcement live elsewhere.
///
/// No `Default` impl: a `CallStats` is only ever constructed by a vendor
/// adapter's `complete` method, which knows the real vendor, latency, and
/// model id. That keeps "stats with a placeholder vendor" unrepresentable.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallStats {
    pub usage: Usage,
    /// Wall-clock latency around the `ModelClient::complete` HTTP call,
    /// measured by the vendor impl with `std::time::Instant`.
    pub latency_ms: u64,
    /// Vendor that produced this call. See `Vendor::as_str` for the
    /// stable tracing string.
    pub vendor: Vendor,
    /// Model id the call hit. Owned `String` because model ids are
    /// configurable via env vars and `with_model`.
    pub model: String,
}

impl CallStats {
    pub fn tokens_in(&self) -> u32 {
        self.usage.input_tokens
    }

    pub fn tokens_out(&self) -> u32 {
        self.usage.output_tokens
    }
}

/// Categorized failure mode from a model adapter.
///
/// The split exists so callers can decide what to do with the error
/// without parsing prose: rate-limit and transport errors are typically
/// retried, auth and parse errors are not, and `Other` is a catch-all for
/// the long tail of 4xx vendor-specific responses.
#[derive(Debug, thiserror::Error)]
pub enum ModelError {
    /// Response body could not be parsed as the expected shape.
    #[error("parse error: {0}")]
    Parse(String),
    /// Network failure or 5xx upstream.
    #[error("transport error: {0}")]
    Transport(String),
    /// Provider rate-limited the request (HTTP 429 or vendor equivalent).
    #[error("rate limit: {0}")]
    RateLimit(String),
    /// Missing or invalid credentials (HTTP 401 / 403).
    #[error("auth error: {0}")]
    Auth(String),
    /// Anything else — typically a 4xx with a vendor-specific shape.
    #[error("model error: {0}")]
    Other(String),
}

/// The trait every model adapter implements. Held as `Arc<dyn ModelClient>`
/// by callers — the substitution boundary across vendors.
#[async_trait::async_trait]
pub trait ModelClient: Send + Sync {
    async fn complete(&self, req: CompleteRequest) -> Result<CompleteResponse, ModelError>;
}

// `dyn ModelClient: Send + Sync` is load-bearing for sharing across tasks.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync + ?Sized>() {}
    assert_send_sync::<dyn ModelClient>();
};

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn message_constructors_produce_single_text_block() {
        let m = Message::user("hi");
        assert_eq!(m.role, Role::User);
        assert_eq!(m.content, vec![ContentBlock::Text { text: "hi".into() }]);
    }

    #[test]
    fn role_serializes_as_snake_case() {
        let r = Role::Assistant;
        let s = serde_json::to_string(&r).unwrap();
        assert_eq!(s, "\"assistant\"");
        let back: Role = serde_json::from_str("\"tool\"").unwrap();
        assert_eq!(back, Role::Tool);
    }

    #[test]
    fn content_block_round_trip() {
        let blocks = vec![
            ContentBlock::Text { text: "hi".into() },
            ContentBlock::ToolUse {
                id: "toolu_1".into(),
                name: "echo".into(),
                input: json!({"msg": "hi"}),
            },
            ContentBlock::ToolResult {
                tool_use_id: "toolu_1".into(),
                content: "ok".into(),
            },
        ];
        let s = serde_json::to_string(&blocks).unwrap();
        let back: Vec<ContentBlock> = serde_json::from_str(&s).unwrap();
        assert_eq!(blocks, back);
    }

    #[test]
    fn complete_options_default_has_nonzero_max_tokens() {
        let o = CompleteOptions::default();
        assert!(o.max_tokens > 0);
        assert!(o.temperature.is_none());
    }

    #[test]
    fn model_error_displays_categorized_message() {
        let e = ModelError::RateLimit("slow down".into());
        assert!(e.to_string().contains("rate limit"));
        assert!(e.to_string().contains("slow down"));
        let e = ModelError::Auth("bad key".into());
        assert!(e.to_string().contains("auth"));
    }

    #[test]
    fn model_error_implements_std_error() {
        fn assert_error<E: std::error::Error>() {}
        assert_error::<ModelError>();
    }
}
