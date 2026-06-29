#!/usr/bin/env bash
# Validate the durable artifacts produced by a node-run / node-run-mcp
# runbook smoke. ULID-tolerant: assertions key off content, not filenames.
#
# Usage:
#   assert_smoke.sh <fs_root> <expected_evidence_sha> <expected_tool> \
#       <expected_args_json> <expected_output_content>
set -euo pipefail

if [[ $# -ne 5 ]]; then
    echo "usage: $0 <fs_root> <sha> <tool> <args_json> <content>" >&2
    exit 64
fi

fs_root="$1"
sha="$2"
tool="$3"
args="$4"
content="$5"

fail() {
    echo "assert_smoke[$fs_root]: $*" >&2
    exit 1
}

# 1. retirement.json — the mandate's step_cap backstop should have retired the agent.
[[ -f "$fs_root/retirement.json" ]] || fail "missing retirement.json"
got_reason=$(jq -r '.reason' "$fs_root/retirement.json")
[[ "$got_reason" == "step_cap (1) reached" ]] \
    || fail "retirement.reason: expected 'step_cap (1) reached', got '$got_reason'"

# 2. health.json — the agent should have stayed Healthy through retirement.
[[ -f "$fs_root/health.json" ]] || fail "missing health.json"
got_state=$(jq -r '.state' "$fs_root/health.json")
[[ "$got_state" == "Healthy" ]] \
    || fail "health.state: expected 'Healthy', got '$got_state'"

# 3. evidence/<slug>-<hash>.json — interpretable slug filename; assert on the
#    record's content, not its name (there is exactly one record — the single
#    scripted tool call — so glob it). The `id` field still carries the full
#    content sha, so the canonical-triple check stays exact and deterministic.
ev_file=$(find "$fs_root/evidence" -maxdepth 1 -type f -name '*.json' \
    ! -name '_tail.json' | head -n1)
[[ -n "$ev_file" && -f "$ev_file" ]] || fail "missing evidence record under evidence/"
got_id=$(jq -r '.id' "$ev_file")
[[ "$got_id" == "$sha" ]] \
    || fail "evidence.id: expected '$sha', got '$got_id' — canonical triple drifted?"
got_tool=$(jq -r '.tool' "$ev_file")
[[ "$got_tool" == "$tool" ]] \
    || fail "evidence.tool: expected '$tool', got '$got_tool'"
got_args=$(jq -cS '.args' "$ev_file")
expected_args=$(jq -cS . <<< "$args")
[[ "$got_args" == "$expected_args" ]] \
    || fail "evidence.args: expected $expected_args, got $got_args"

# 4. outputs/output.md — the single, kept-current Output. A stable filename
#    (not content-addressed), pure prose: the body IS the file content, and
#    citations live in the DB reference graph, not the file.
out_file="$fs_root/outputs/output.md"
[[ -f "$out_file" ]] || fail "missing outputs/output.md"
got_content=$(cat "$out_file")
[[ "$got_content" == "$content" ]] \
    || fail "output body: expected '$content', got '$got_content'"

echo "assert_smoke[$fs_root]: OK"
