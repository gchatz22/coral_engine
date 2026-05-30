//! Hand-rolled VCR-style fixture infrastructure for vendor LLM tests.
//!
//! Spawns a loopback HTTP/1.1 listener that serves pre-recorded vendor
//! responses for each `POST` the system under test makes. Per-test
//! fixture: one listener, a `Vec<ResponseFixture>` popped FIFO. Vendor
//! clients are pointed at the mock via `with_base_url`, so the production
//! HTTP path runs end-to-end against recorded wire bytes — closing the
//! `CallStats::latency_ms` / `vendor` gap that pure `parse_response` unit
//! tests leave open. The mock sleeps a few ms before responding so the
//! millisecond-resolution `latency_ms` measurement clears zero.
//!
//! Fixture file format: a JSON document with a `responses` array of
//! `{ "status": u16, "body": <vendor-shaped JSON> }` entries; other
//! fields (`synthesized`, `vendor`, `note`) are reviewer-facing only.

#![allow(dead_code)] // Each test file uses a subset of helpers.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Wall-clock pause the mock server takes before replying, in
/// milliseconds. Chosen to be just above 1ms so the vendor adapter's
/// `latency_ms` measurement clears zero deterministically without
/// noticeably slowing the test suite.
const MOCK_RESPONSE_DELAY_MS: u64 = 5;

/// A single recorded response the mock server will serve.
#[derive(Clone, Debug)]
pub struct ResponseFixture {
    pub status: u16,
    pub body: Vec<u8>,
}

/// One captured request as the mock server saw it on the wire.
#[derive(Clone, Debug)]
pub struct CapturedRequest {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl CapturedRequest {
    /// Parse the body as JSON. Panics with a helpful message on
    /// malformed JSON (tests should never produce one).
    pub fn json(&self) -> Value {
        serde_json::from_slice(&self.body)
            .unwrap_or_else(|e| panic!("captured request body is not JSON: {e}\n{:?}", self.body))
    }
}

/// A running mock server. Drop the handle to stop the listener.
pub struct MockServer {
    addr: SocketAddr,
    state: Arc<Mutex<MockState>>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

#[derive(Default)]
struct MockState {
    fixtures: Vec<ResponseFixture>,
    captured: Vec<CapturedRequest>,
    /// Set to `true` once the listener task observes the state mutex was
    /// cleared by `Drop`; the accept loop checks this to exit cleanly.
    shutdown: bool,
}

impl MockServer {
    /// Spawn a listener on loopback port 0. The returned `base_url` is
    /// suitable for `AnthropicClient::with_base_url` / `CohereClient::with_base_url`.
    ///
    /// `responses` is consumed FIFO — one entry per request the system
    /// under test will make. If the test makes more requests than there
    /// are fixtures, the server replies with HTTP 599 and the test will
    /// surface as a `ModelError` (callers should assert the failure or
    /// add more fixtures).
    pub async fn spawn(responses: Vec<ResponseFixture>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let state = Arc::new(Mutex::new(MockState {
            fixtures: responses,
            captured: Vec::new(),
            shutdown: false,
        }));
        let state_clone = Arc::clone(&state);

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    accept = listener.accept() => {
                        match accept {
                            Ok((stream, _peer)) => {
                                let s = Arc::clone(&state_clone);
                                tokio::spawn(async move {
                                    if let Err(e) = handle_connection(stream, s).await {
                                        eprintln!("mock server: connection error: {e}");
                                    }
                                });
                            }
                            Err(e) => {
                                eprintln!("mock server: accept error: {e}");
                                break;
                            }
                        }
                    }
                    // Periodically check for shutdown so we don't block
                    // an idle listener forever between tests.
                    _ = tokio::time::sleep(Duration::from_millis(50)) => {
                        if state_clone.lock().unwrap().shutdown {
                            break;
                        }
                    }
                }
            }
        });

        Self {
            addr,
            state,
            handle: Some(handle),
        }
    }

    /// `http://127.0.0.1:<port>` — pass directly to `with_base_url` on
    /// either vendor client. Both vendor adapters POST to the base URL
    /// as-is (no path appended), so this is a complete URL.
    pub fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// All captured requests, in arrival order.
    pub fn captured(&self) -> Vec<CapturedRequest> {
        self.state.lock().unwrap().captured.clone()
    }

    /// Number of fixtures still queued to be served. Useful for asserting
    /// the test consumed exactly the expected number of upstream calls.
    pub fn remaining(&self) -> usize {
        self.state.lock().unwrap().fixtures.len()
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        // Signal shutdown; the accept loop polls this every 50ms.
        self.state.lock().unwrap().shutdown = true;
        if let Some(h) = self.handle.take() {
            h.abort();
        }
    }
}

/// Handle one inbound connection: parse a single HTTP request, pop the
/// next fixture, sleep briefly (for `latency_ms` to clear zero), write
/// the recorded response.
async fn handle_connection(
    mut stream: TcpStream,
    state: Arc<Mutex<MockState>>,
) -> std::io::Result<()> {
    // Read until headers complete, then read Content-Length bytes.
    let mut buf: Vec<u8> = Vec::with_capacity(4096);
    let mut tmp = [0u8; 2048];
    let header_end;
    loop {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            // Client closed mid-headers; treat as no-op.
            return Ok(());
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(idx) = find_subslice(&buf, b"\r\n\r\n") {
            header_end = idx + 4;
            break;
        }
        if buf.len() > 1024 * 1024 {
            // Defensive: don't grow unboundedly on a malformed request.
            return Ok(());
        }
    }

    let header_bytes = &buf[..header_end];
    let head = std::str::from_utf8(header_bytes).unwrap_or("");
    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap_or("");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("").to_string();

    let mut headers: Vec<(String, String)> = Vec::new();
    let mut content_length: usize = 0;
    for line in lines {
        if line.is_empty() {
            break;
        }
        if let Some((k, v)) = line.split_once(':') {
            let k = k.trim().to_string();
            let v = v.trim().to_string();
            if k.eq_ignore_ascii_case("content-length") {
                content_length = v.parse().unwrap_or(0);
            }
            headers.push((k, v));
        }
    }

    // Drain the body. We already have everything past `header_end` in `buf`.
    let mut body = buf[header_end..].to_vec();
    while body.len() < content_length {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    body.truncate(content_length);

    // Pop the next fixture and capture the request.
    let fixture = {
        let mut st = state.lock().unwrap();
        st.captured.push(CapturedRequest {
            method,
            path,
            headers,
            body,
        });
        if st.fixtures.is_empty() {
            None
        } else {
            Some(st.fixtures.remove(0))
        }
    };

    // Sleep briefly so the vendor adapter's `latency_ms` measurement
    // (millisecond resolution) clears zero. Keep the delay tiny so the
    // hermetic suite stays fast.
    tokio::time::sleep(Duration::from_millis(MOCK_RESPONSE_DELAY_MS)).await;

    let (status, body) = match fixture {
        Some(f) => (f.status, f.body),
        None => (
            599u16,
            br#"{"error":"mock server: no more fixtures queued"}"#.to_vec(),
        ),
    };
    let status_text = match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        _ => "Unknown",
    };
    let response = format!(
        "HTTP/1.1 {status} {status_text}\r\n\
         content-type: application/json\r\n\
         content-length: {len}\r\n\
         connection: close\r\n\r\n",
        status = status,
        status_text = status_text,
        len = body.len(),
    );
    stream.write_all(response.as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await?;
    let _ = stream.shutdown().await;
    Ok(())
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Read a fixture JSON file from `tests/fixtures/llm/<vendor>/<name>.json`
/// and return its `responses[]` array as `ResponseFixture`s.
///
/// The file format is documented at the top of this module.
pub fn load_fixture(vendor: &str, name: &str) -> Vec<ResponseFixture> {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("tests/fixtures/llm");
    path.push(vendor);
    path.push(format!("{name}.json"));
    let bytes =
        std::fs::read(&path).unwrap_or_else(|e| panic!("failed to read fixture {path:?}: {e}"));
    let v: Value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|e| panic!("fixture {path:?} is not JSON: {e}"));
    let responses = v
        .get("responses")
        .and_then(|x| x.as_array())
        .unwrap_or_else(|| panic!("fixture {path:?} missing `responses` array"));
    responses
        .iter()
        .map(|r| {
            let status = r.get("status").and_then(|x| x.as_u64()).unwrap_or(200) as u16;
            let body_value = r
                .get("body")
                .unwrap_or_else(|| panic!("fixture {path:?} response missing `body`"));
            let body = serde_json::to_vec(body_value).expect("re-serialize body");
            ResponseFixture { status, body }
        })
        .collect()
}

/// Return `true` if the live-test env gate is enabled. Used by the
/// `#[ignore]`-d live tests at the bottom of each per-vendor file.
pub fn live_llm_enabled() -> bool {
    std::env::var("JARVIS_LIVE_LLM")
        .map(|v| v == "1")
        .unwrap_or(false)
}
