//! `compute-evidence-id` — recompute canonical sha256 EvidenceIds.
//!
//! A small audit helper for fixtures. The smoke fixture
//! (`examples/smoke/decisions.jsonl`) embeds the hex EvidenceId of an
//! `(echo, {"hello":"smoke"}, {"echoed":{"hello":"smoke"}})` triple. If the
//! canonical-JSON encoder in `src/evidence.rs` ever changes, that hash goes
//! stale silently — runtime smoke fails, but only late. This binary lets you
//! recompute fixture hashes ahead of time and lists the ids the fixture
//! declares for cross-checking.
//!
//! # Usage
//!
//! ```text
//! compute-evidence-id <tool-name> <args-json> <result-json>
//! compute-evidence-id --from-file <decisions.jsonl>
//! ```
//!
//! In single-triple mode the canonical sha256 hex is written to stdout
//! followed by a newline (and nothing else). In `--from-file` mode each
//! `write_output` entry's declared evidence ids are listed alongside the
//! immediately preceding `call_tool`, so a reviewer can eyeball whether the
//! fixture's static hash still matches what `EvidenceId::new` would produce
//! today. The `--from-file` parser tolerates lines whose JSON *shape* it
//! doesn't recognize (unknown `type` discriminators, extra fields), but it
//! does **not** tolerate invalid JSON: a malformed line aborts the walk so
//! drift in the fixture format is surfaced immediately rather than silently
//! skipped.

use std::fs;
use std::path::Path;
use std::process::ExitCode;

use anyhow::{anyhow, Context, Result};

use coral_node::evidence::EvidenceId;

const USAGE: &str = "\
compute-evidence-id — recompute canonical sha256 EvidenceIds.

USAGE:
    compute-evidence-id <tool-name> <args-json> <result-json>
    compute-evidence-id --from-file <decisions.jsonl>

ARGS:
    <tool-name>      Tool name string (e.g. `echo`).
    <args-json>      JSON value for the tool's args.
    <result-json>    JSON value for the tool's result.
    <decisions.jsonl>
                     Path to a scripted decisions file. Each `write_output`
                     entry's declared evidence ids are listed alongside the
                     immediately preceding `call_tool`. Blank lines and
                     `#`-prefixed comments are skipped.

EXAMPLES:
    compute-evidence-id echo '{\"hello\":\"smoke\"}' '{\"echoed\":{\"hello\":\"smoke\"}}'
    compute-evidence-id --from-file examples/smoke/decisions.jsonl
";

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("compute-evidence-id: {e:#}");
            ExitCode::from(1)
        }
    }
}

fn run() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "-h" || a == "--help") {
        print!("{USAGE}");
        return Ok(());
    }

    match args.as_slice() {
        [flag, path] if flag == "--from-file" => audit_file(Path::new(path)),
        [tool, args_json, result_json] => single(tool, args_json, result_json),
        _ => {
            eprint!("{USAGE}");
            std::process::exit(2);
        }
    }
}

/// Mode 1: hash a single `(tool, args, result)` triple.
fn single(tool: &str, args_json: &str, result_json: &str) -> Result<()> {
    let args: serde_json::Value = serde_json::from_str(args_json).context("parsing <args-json>")?;
    let result: serde_json::Value =
        serde_json::from_str(result_json).context("parsing <result-json>")?;
    let id = EvidenceId::new(tool, &args, &result);
    println!("{id}");
    Ok(())
}

/// Mode 2: walk a `decisions.jsonl` and pair each `write_output`'s declared
/// evidence ids with the immediately preceding `call_tool` for audit.
///
/// We match on the `type` discriminator over a generic `serde_json::Value`
/// rather than deserializing into the crate's `Decision` enum, so a fixture
/// with extra fields or unknown variants doesn't abort the walk. Invalid
/// JSON, however, is a hard failure: an audit tool that silently skipped
/// malformed lines could miss exactly the kind of drift it exists to catch.
fn audit_file(path: &Path) -> Result<()> {
    let text = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;

    // Tracks the most recent CallTool seen in the script. Tuple is (tool
    // name, args). Reset to None after each WriteOutput is reported, so a
    // stray WriteOutput with no preceding CallTool is flagged rather than
    // silently associated with a much earlier one.
    let mut last_call: Option<(String, serde_json::Value)> = None;
    let mut emitted = 0usize;

    for (i, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let value: serde_json::Value = serde_json::from_str(line)
            .with_context(|| format!("parsing line {} of {}", i + 1, path.display()))?;
        let kind = value.get("type").and_then(|v| v.as_str()).unwrap_or("");
        match kind {
            "call_tool" => {
                let name = value
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let args = value
                    .get("args")
                    .cloned()
                    .unwrap_or(serde_json::Value::Null);
                last_call = Some((name, args));
            }
            "write_output" => {
                let citations = value
                    .get("citations")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();
                if citations.is_empty() {
                    continue;
                }
                emitted += 1;
                println!("write_output @ line {}", i + 1);
                match &last_call {
                    Some((tool, args)) => {
                        println!("  preceding call_tool: name={tool} args={args}");
                    }
                    None => {
                        println!("  (no preceding call_tool seen)");
                    }
                }
                for id in &citations {
                    let id_str = id.as_str().unwrap_or("<non-string>");
                    println!("  declared evidence id: {id_str}");
                }
                last_call = None;
            }
            _ => {}
        }
    }

    if emitted == 0 {
        return Err(anyhow!(
            "no write_output entries found in {}",
            path.display()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::Write;

    /// Static drift detection: the smoke fixture
    /// (`examples/smoke/decisions.jsonl`) embeds this hex. If
    /// `EvidenceId::new`'s canonical encoding changes, this test fails so we
    /// notice before the runtime smoke run does.
    #[test]
    fn matches_smoke_fixture_hash() {
        let id = EvidenceId::new(
            "echo",
            &json!({"hello":"smoke"}),
            &json!({"echoed":{"hello":"smoke"}}),
        );
        assert_eq!(
            id.as_str(),
            "1d6a153a69d110156ca44ed281f859ca09d9875747e3ed16b9964c52632fd96e"
        );
    }

    /// `--from-file` must hard-fail on a malformed JSON line, naming the
    /// offending line number and path. An audit tool that silently skipped
    /// such lines would defeat its own purpose.
    #[test]
    fn audit_file_hard_fails_on_invalid_json() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            tmp,
            r#"{{"type":"call_tool","name":"echo","args":{{"hello":"smoke"}}}}"#
        )
        .unwrap();
        writeln!(tmp, "this is not json").unwrap();
        writeln!(tmp, r#"{{"type":"write_output","citations":["abc"]}}"#).unwrap();

        let err = audit_file(tmp.path()).expect_err("expected hard fail on invalid JSON");
        let chain = format!("{err:#}");
        assert!(
            chain.contains("parsing line 2"),
            "error should name the offending line: {chain}"
        );
        assert!(
            chain.contains(&tmp.path().display().to_string()),
            "error should name the offending file: {chain}"
        );
    }

    /// Blank lines and `#`-comments are still skipped silently — they're not
    /// the kind of "drift" the strict mode is meant to catch.
    #[test]
    fn audit_file_tolerates_blank_and_comment_lines() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        writeln!(tmp, "# header comment").unwrap();
        writeln!(tmp).unwrap();
        writeln!(
            tmp,
            r#"{{"type":"call_tool","name":"echo","args":{{"hello":"smoke"}}}}"#
        )
        .unwrap();
        writeln!(tmp, r#"{{"type":"write_output","citations":["abc"]}}"#).unwrap();

        audit_file(tmp.path()).expect("blank/comment lines should not be a hard failure");
    }
}
