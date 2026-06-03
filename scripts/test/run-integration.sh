#!/usr/bin/env bash
# Integration test orchestrator for feature 005-producer-consumer-test.
#
# Single entrypoint — no manual steps required (SC-004 / T027). Runs the pytest
# integration suite, emits JUnit XML for CI, enforces the 5-minute full-suite
# budget (SC-005 / T028), and supports a repeatability mode that runs the suite
# multiple times and asserts identical pass/fail results (FR-009 / T026).
#
# Usage:
#   scripts/test/run-integration.sh                 # full suite, once
#   scripts/test/run-integration.sh -k message_flow # pytest filter passthrough
#   REPEAT=3 scripts/test/run-integration.sh        # repeatability check (T026)
#   MAX_SUITE_SECONDS=300 scripts/test/run-integration.sh
set -euo pipefail

REPEAT="${REPEAT:-1}"
MAX_SUITE_SECONDS="${MAX_SUITE_SECONDS:-300}"   # SC-005: under 5 minutes
RESULTS_DIR="${RESULTS_DIR:-test-results}"
PYTEST_ARGS=("$@")

mkdir -p "$RESULTS_DIR"

run_once() {
    local run_index="$1"
    local junit="$RESULTS_DIR/junit-run${run_index}.xml"
    echo "──────────────────────────────────────────────"
    echo "▶ Integration suite — run ${run_index}/${REPEAT}"
    echo "──────────────────────────────────────────────"
    # Per-run JUnit so the repeatability check can diff outcomes.
    if pytest --junitxml="$junit" "${PYTEST_ARGS[@]}"; then
        echo "PASS" > "$RESULTS_DIR/result-run${run_index}.txt"
        return 0
    else
        echo "FAIL" > "$RESULTS_DIR/result-run${run_index}.txt"
        return 1
    fi
}

suite_start=$(date +%s)
overall_rc=0
declare -a outcomes=()

for i in $(seq 1 "$REPEAT"); do
    if run_once "$i"; then
        outcomes+=("PASS")
    else
        outcomes+=("FAIL")
        overall_rc=1
    fi
done

suite_end=$(date +%s)
elapsed=$(( suite_end - suite_start ))

echo
echo "=============================================="
echo " Suite summary"
echo "=============================================="
for i in $(seq 1 "$REPEAT"); do
    echo "  run ${i}: ${outcomes[$((i-1))]}"
done
echo "  total elapsed: ${elapsed}s (budget ${MAX_SUITE_SECONDS}s)"

# T026: repeatability — every run must produce the same pass/fail verdict.
if [ "$REPEAT" -gt 1 ]; then
    first="${outcomes[0]}"
    for o in "${outcomes[@]}"; do
        if [ "$o" != "$first" ]; then
            echo "✗ REPEATABILITY FAILURE: results differ across runs (${outcomes[*]})" >&2
            exit 2
        fi
    done
    echo "✓ Repeatability: all ${REPEAT} runs agree (${first})"
fi

# T028 / SC-005: enforce the full-suite timing budget.
if [ "$elapsed" -gt "$MAX_SUITE_SECONDS" ]; then
    echo "✗ TIMING FAILURE: suite took ${elapsed}s, exceeds ${MAX_SUITE_SECONDS}s budget" >&2
    exit 3
fi
echo "✓ Timing: suite completed in ${elapsed}s (under ${MAX_SUITE_SECONDS}s budget)"

exit "$overall_rc"
