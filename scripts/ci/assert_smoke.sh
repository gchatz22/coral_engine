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

# 1. retirement.json — the mandate's max_ticks cap should have retired the agent.
[[ -f "$fs_root/retirement.json" ]] || fail "missing retirement.json"
got_reason=$(jq -r '.reason' "$fs_root/retirement.json")
[[ "$got_reason" == "max_ticks (3) reached" ]] \
    || fail "retirement.reason: expected 'max_ticks (3) reached', got '$got_reason'"

# 2. health.json — the agent should have stayed Healthy through retirement.
[[ -f "$fs_root/health.json" ]] || fail "missing health.json"
got_state=$(jq -r '.state' "$fs_root/health.json")
[[ "$got_state" == "Healthy" ]] \
    || fail "health.state: expected 'Healthy', got '$got_state'"

# 3. evidence/<sha>.json — content-addressed, deterministic when the canonical
#    triple in the fixture matches what the tool returned.
ev_file="$fs_root/evidence/$sha.json"
[[ -f "$ev_file" ]] || fail "missing evidence/$sha.json — canonical triple drifted?"
got_tool=$(jq -r '.tool' "$ev_file")
[[ "$got_tool" == "$tool" ]] \
    || fail "evidence.tool: expected '$tool', got '$got_tool'"
got_args=$(jq -cS '.args' "$ev_file")
expected_args=$(jq -cS . <<< "$args")
[[ "$got_args" == "$expected_args" ]] \
    || fail "evidence.args: expected $expected_args, got $got_args"

# 4. outputs/<ulid>.json — filename is random, body fields are stable.
# JAR2-54 introduced a co-located tail-index sidecar (`outputs/_tail.json`,
# per `scratch/agent_storage.md` § 7.1). Exclude underscore-prefixed
# bookkeeping files so the count and selector keep matching the ULID output.
out_count=$(find "$fs_root/outputs" -mindepth 1 -maxdepth 1 -type f -name '*.json' -not -name '_*' | wc -l | tr -d ' ')
[[ "$out_count" == "1" ]] \
    || fail "expected exactly 1 ULID file under outputs/ (excluding _* sidecars), found $out_count"
out_file=$(find "$fs_root/outputs" -mindepth 1 -maxdepth 1 -type f -name '*.json' -not -name '_*' | head -n1)
got_content=$(jq -r '.content' "$out_file")
[[ "$got_content" == "$content" ]] \
    || fail "output.content: expected '$content', got '$got_content'"
got_evidence_idx=$(jq -r --arg sha "$sha" '.evidence | index($sha)' "$out_file")
[[ "$got_evidence_idx" != "null" ]] \
    || fail "output.evidence does not contain expected sha $sha"

echo "assert_smoke[$fs_root]: OK"
