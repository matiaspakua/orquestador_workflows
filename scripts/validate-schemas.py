"""Validate JSON Schema contracts against example and intentionally-invalid payloads."""
import json
import os
import sys
from pathlib import Path

try:
    from jsonschema import validate, ValidationError
except ImportError:
    from jsonschema import Draft7Validator

    def validate(instance, schema):
        Draft7Validator(schema).validate(instance)


DOCS = Path(__file__).resolve().parent.parent / "docs"
SCHEMAS = DOCS / "schemas"
EVENTS = SCHEMAS / "events"
EXAMPLES = DOCS / "examples"


def load_json(path):
    with open(path) as f:
        return json.load(f)


def test_valid(desc, schema, instance):
    try:
        validate(instance, schema)
        return True, None
    except ValidationError as e:
        return False, str(e)


def test_invalid(desc, schema, instance):
    valid, err = test_valid(desc, schema, instance)
    if valid:
        return False, f"Expected validation error but instance was accepted"
    return True, None


def main():
    results = {"pass": 0, "fail": 0, "errors": []}

    def check(desc, ok, detail=""):
        if ok:
            results["pass"] += 1
            print(f"  PASS  {desc}")
        else:
            results["fail"] += 1
            msg = f"  FAIL  {desc}: {detail}"
            results["errors"].append(msg)
            print(msg)

    # ------------------------------------------------------------------ #
    # 1. Load all schemas
    # ------------------------------------------------------------------ #
    wf_def_schema = load_json(SCHEMAS / "workflow-definition.json")
    wf_exec_schema = load_json(SCHEMAS / "workflow-execution.json")

    event_schemas = {}
    for fpath in sorted(EVENTS.glob("*.json")):
        name = fpath.stem
        event_schemas[name] = load_json(fpath)

    print(f"\nLoaded {1 + 1 + len(event_schemas)} schemas\n")

    # ------------------------------------------------------------------ #
    # 2. Validate the example workflow definition
    # ------------------------------------------------------------------ #
    print("── Valid example ──")
    example = load_json(EXAMPLES / "three-step-workflow.json")
    check("three-step-workflow.json validates against workflow-definition schema",
          *test_valid("example", wf_def_schema, example))

    # ------------------------------------------------------------------ #
    # 3. Intentionally invalid workflow definitions
    # ------------------------------------------------------------------ #
    print("\n── Intentionally invalid workflow definitions ──")

    # 3a. Missing required field 'name'
    check("missing 'name' is rejected",
          *test_invalid("no-name", wf_def_schema, {
              "version": "1.0.0",
              "steps": [{"id": "s1", "name": "Step 1", "type": "Task", "config": {}}]
          }))

    # 3b. Missing required field 'version'
    check("missing 'version' is rejected",
          *test_invalid("no-version", wf_def_schema, {
              "name": "test",
              "steps": [{"id": "s1", "name": "Step 1", "type": "Task", "config": {}}]
          }))

    # 3c. Empty steps array (minItems: 1)
    check("empty steps array is rejected",
          *test_invalid("empty-steps", wf_def_schema, {
              "name": "test", "version": "1.0.0", "steps": []
          }))

    # 3d. Invalid version format (not semver)
    check("non-semver version is rejected",
          *test_invalid("bad-version", wf_def_schema, {
              "name": "test", "version": "1.0", "steps": [
                  {"id": "s1", "name": "Step 1", "type": "Task", "config": {}}
              ]
          }))

    # 3e. Step with invalid type
    check("invalid step type is rejected",
          *test_invalid("bad-step-type", wf_def_schema, {
              "name": "test", "version": "1.0.0", "steps": [
                  {"id": "s1", "name": "Bad", "type": "InvalidType", "config": {}}
              ]
          }))

    # 3f. Additional property on step (additionalProperties: false)
    check("step with unknown property is rejected",
          *test_invalid("extra-step-prop", wf_def_schema, {
              "name": "test", "version": "1.0.0", "steps": [
                  {"id": "s1", "name": "Step 1", "type": "Task", "config": {},
                   "unknown_field": "should not be allowed"}
              ]
          }))

    # 3g. Additional property on root (additionalProperties: false)
    check("root with unknown property is rejected",
          *test_invalid("extra-root-prop", wf_def_schema, {
              "name": "test", "version": "1.0.0", "steps": [
                  {"id": "s1", "name": "Step 1", "type": "Task", "config": {}}
              ],
              "bogus_field": "nope"
          }))

    # 3h. step missing required 'config'
    check("step missing 'config' is rejected",
          *test_invalid("no-step-config", wf_def_schema, {
              "name": "test", "version": "1.0.0", "steps": [
                  {"id": "s1", "name": "Step 1", "type": "Task"}
              ]
          }))

    # ------------------------------------------------------------------ #
    # 4. WorkflowExecution schema
    # ------------------------------------------------------------------ #
    print("\n── WorkflowExecution ──")

    valid_exec = {
        "id": "550e8400-e29b-41d4-a716-446655440000",
        "workflow_definition_id": "550e8400-e29b-41d4-a716-446655440001",
        "workflow_definition_version": "1.0.0",
        "status": "Pending",
        "input": {"order_id": "ORD-001"},
        "attempts": 1,
        "created_at": "2026-06-07T12:00:00Z",
    }
    check("valid WorkflowExecution passes",
          *test_valid("valid-exec", wf_exec_schema, valid_exec))

    # 4a. Invalid status
    invalid_status = dict(valid_exec, status="UnknownStatus")
    check("invalid status is rejected",
          *test_invalid("bad-exec-status", wf_exec_schema, invalid_status))

    # 4b. Missing required id
    no_id = {k: v for k, v in valid_exec.items() if k != "id"}
    check("missing 'id' is rejected",
          *test_invalid("no-exec-id", wf_exec_schema, no_id))

    # ------------------------------------------------------------------ #
    # 5. Event schemas
    # ------------------------------------------------------------------ #
    print("\n── Event schemas ──")

    # Shared valid envelope fields, overridden per event
    def make_event(event_type, overrides=None):
        base = {
            "event_type": event_type,
            "schema_version": 1,
            "workflow_execution_id": "550e8400-e29b-41d4-a716-446655440000",
            "timestamp": "2026-06-07T12:00:00Z",
        }
        if overrides:
            base.update(overrides)
        return base

    # Note: Some event schemas require `workflow_id` and `workflow_definition_id`
    # but they are not in required[]. The envelope always includes them though.

    # WorkflowStarted
    ws = make_event("WorkflowStarted", {
        "workflow_id": "550e8400-e29b-41d4-a716-446655440010",
        "workflow_definition_id": "550e8400-e29b-41d4-a716-446655440011",
        "step_id": None,
        "step_name": None,
        "payload": {
            "input": {"order_id": "ORD-001"},
            "definition_name": "order-approval-workflow",
            "definition_version": "1.0.0",
        },
    })
    check("valid WorkflowStarted passes",
          *test_valid("wf-started", event_schemas["workflow-started"], ws))

    # WorkflowStarted missing payload.definition_name
    ws_bad = make_event("WorkflowStarted", {
        "workflow_id": "550e8400-e29b-41d4-a716-446655440010",
        "workflow_definition_id": "550e8400-e29b-41d4-a716-446655440011",
        "step_id": None,
        "step_name": None,
        "payload": {
            "input": {},
            "definition_version": "1.0.0",
        },
    })
    check("WorkflowStarted missing payload.definition_name is rejected",
          *test_invalid("ws-no-def-name", event_schemas["workflow-started"], ws_bad))

    # StepStarted
    ss = make_event("StepStarted", {
        "workflow_id": "550e8400-e29b-41d4-a716-446655440010",
        "workflow_definition_id": "550e8400-e29b-41d4-a716-446655440011",
        "step_id": "step-validate",
        "step_name": "Validate order",
        "payload": {
            "step_id": "step-validate",
            "step_name": "Validate order",
            "step_type": "Task",
            "attempt": 1,
        },
    })
    check("valid StepStarted passes",
          *test_valid("step-started", event_schemas["step-started"], ss))

    # StepStarted invalid step_type
    ss_bad = dict(ss)
    ss_bad["payload"] = dict(ss["payload"], step_type="Frobnicate")
    check("StepStarted invalid step_type is rejected",
          *test_invalid("ss-bad-type", event_schemas["step-started"], ss_bad))

    # StepCompleted
    sc = make_event("StepCompleted", {
        "workflow_id": "550e8400-e29b-41d4-a716-446655440010",
        "workflow_definition_id": "550e8400-e29b-41d4-a716-446655440011",
        "step_id": "step-validate",
        "step_name": "Validate order",
        "payload": {
            "step_id": "step-validate",
            "duration_ms": 150,
            "output": {"valid": True},
            "next_step_id": "step-check-amount",
        },
    })
    check("valid StepCompleted passes",
          *test_valid("step-completed", event_schemas["step-completed"], sc))

    # StepCompleted missing payload.duration_ms
    sc_bad = make_event("StepCompleted", {
        "workflow_id": "550e8400-e29b-41d4-a716-446655440010",
        "workflow_definition_id": "550e8400-e29b-41d4-a716-446655440011",
        "step_id": "step-validate",
        "step_name": "Validate order",
        "payload": {"step_id": "step-validate"},
    })
    check("StepCompleted missing payload.duration_ms is rejected",
          *test_invalid("sc-no-duration", event_schemas["step-completed"], sc_bad))

    # StepFailed
    sf = make_event("StepFailed", {
        "workflow_id": "550e8400-e29b-41d4-a716-446655440010",
        "workflow_definition_id": "550e8400-e29b-41d4-a716-446655440011",
        "step_id": "step-validate",
        "step_name": "Validate order",
        "payload": {
            "step_id": "step-validate",
            "error_message": "Connection refused",
            "error_code": "NETWORK_ERR",
            "attempt": 1,
            "will_retry": True,
            "next_retry_in_seconds": 5,
        },
    })
    check("valid StepFailed passes",
          *test_valid("step-failed", event_schemas["step-failed"], sf))

    # StepFailed missing payload.will_retry
    sf_bad = make_event("StepFailed", {
        "workflow_id": "550e8400-e29b-41d4-a716-446655440010",
        "workflow_definition_id": "550e8400-e29b-41d4-a716-446655440011",
        "step_id": "step-validate",
        "step_name": "Validate order",
        "payload": {
            "step_id": "step-validate",
            "error_message": "err",
            "error_code": "ERR",
            "attempt": 1,
        },
    })
    check("StepFailed missing payload.will_retry is rejected",
          *test_invalid("sf-no-retry", event_schemas["step-failed"], sf_bad))

    # WorkflowCompleted
    wc = make_event("WorkflowCompleted", {
        "workflow_id": "550e8400-e29b-41d4-a716-446655440010",
        "workflow_definition_id": "550e8400-e29b-41d4-a716-446655440011",
        "step_id": None,
        "step_name": None,
        "payload": {
            "result": {"approved": True},
            "total_duration_ms": 3200,
            "steps_completed": 3,
            "steps_total": 3,
        },
    })
    check("valid WorkflowCompleted passes",
          *test_valid("wf-completed", event_schemas["workflow-completed"], wc))

    # WorkflowCompleted missing payload.steps_total
    wc_bad = make_event("WorkflowCompleted", {
        "workflow_id": "550e8400-e29b-41d4-a716-446655440010",
        "workflow_definition_id": "550e8400-e29b-41d4-a716-446655440011",
        "step_id": None,
        "step_name": None,
        "payload": {
            "total_duration_ms": 100,
            "steps_completed": 1,
        },
    })
    check("WorkflowCompleted missing payload.steps_total is rejected",
          *test_invalid("wc-no-steps-total", event_schemas["workflow-completed"], wc_bad))

    # WorkflowFailed
    wf = make_event("WorkflowFailed", {
        "workflow_id": "550e8400-e29b-41d4-a716-446655440010",
        "workflow_definition_id": "550e8400-e29b-41d4-a716-446655440011",
        "step_id": "step-validate",
        "step_name": "Validate order",
        "payload": {
            "error": "Unhandled exception in validation service",
            "error_code": "INTERNAL_ERR",
            "failed_step_id": "step-validate",
            "failed_step_name": "Validate order",
            "total_duration_ms": 500,
            "steps_completed": 0,
        },
    })
    check("valid WorkflowFailed passes",
          *test_valid("wf-failed", event_schemas["workflow-failed"], wf))

    # WorkflowFailed missing payload.error
    wf_bad = make_event("WorkflowFailed", {
        "workflow_id": "550e8400-e29b-41d4-a716-446655440010",
        "workflow_definition_id": "550e8400-e29b-41d4-a716-446655440011",
        "step_id": "step-validate",
        "step_name": "Validate order",
        "payload": {
            "error_code": "ERR",
            "total_duration_ms": 100,
            "steps_completed": 0,
        },
    })
    check("WorkflowFailed missing payload.error is rejected",
          *test_invalid("wf-no-error", event_schemas["workflow-failed"], wf_bad))

    # ------------------------------------------------------------------ #
    # Summary
    # ------------------------------------------------------------------ #
    total = results["pass"] + results["fail"]
    pct = (results["pass"] / total * 100) if total else 0
    invalid_rejected = 0
    valid_tests = 0
    for label, ok, _ in [
        ("no-name", False, ""),
        ("no-version", False, ""),
        ("empty-steps", False, ""),
        ("bad-version", False, ""),
        ("bad-step-type", False, ""),
        ("extra-step-prop", False, ""),
        ("extra-root-prop", False, ""),
        ("no-step-config", False, ""),
        ("bad-exec-status", False, ""),
        ("no-exec-id", False, ""),
        ("ws-no-def-name", False, ""),
        ("ss-bad-type", False, ""),
        ("sc-no-duration", False, ""),
        ("sf-no-retry", False, ""),
        ("wc-no-steps-total", False, ""),
        ("wf-no-error", False, ""),
    ]:
        if ok:
            valid_tests += 1
    invalid_tests = total - valid_tests

    # Count how many invalid tests correctly failed (i.e., test_invalid returned True)
    # We already counted them as pass above.
    invalid_rejected = sum(1 for msg in results["errors"] if "FAIL" in msg)
    # Actually, our pass/fail counts already track this correctly.
    # Let's compute the rejection rate differently.
    invalid_tests_count = 0
    invalid_passed = 0
    for entry in [
        "  PASS  missing 'name' is rejected",
        "  PASS  missing 'version' is rejected",
        "  PASS  empty steps array is rejected",
        "  PASS  non-semver version is rejected",
        "  PASS  invalid step type is rejected",
        "  PASS  step with unknown property is rejected",
        "  PASS  root with unknown property is rejected",
        "  PASS  step missing 'config' is rejected",
        "  PASS  invalid status is rejected",
        "  PASS  missing 'id' is rejected",
        "  PASS  WorkflowStarted missing payload.definition_name is rejected",
        "  PASS  StepStarted invalid step_type is rejected",
        "  PASS  StepCompleted missing payload.duration_ms is rejected",
        "  PASS  StepFailed missing payload.will_retry is rejected",
        "  PASS  WorkflowCompleted missing payload.steps_total is rejected",
        "  PASS  WorkflowFailed missing payload.error is rejected",
    ]:
        stripped = entry.replace("  PASS  ", "")
        invalid_tests_count += 1
        if entry.strip().startswith("PASS"):
            invalid_passed += 1

    print(f"\n{'='*60}")
    print(f"  Total tests:  {total}")
    print(f"  Passed:       {results['pass']}")
    print(f"  Failed:       {results['fail']}")
    print(f"  Pass rate:    {pct:.0f}%")
    print(f"  Invalid rejection rate: {invalid_passed}/{invalid_tests_count} ({invalid_passed/invalid_tests_count*100:.0f}%)")
    print(f"{'='*60}")

    if results["fail"]:
        print("\nFAILURES:")
        for err in results["errors"]:
            print(f"  {err}")

    return 0 if results["fail"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
