# Implementation Plan: Workflow Definition

**Branch**: `003-workflow-definition` | **Date**: 2026-06-02 | **Spec**: specs/003-workflow-definition/spec.md

**Input**: Feature specification from `specs/003-workflow-definition/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/plan-template.md` for the execution workflow.

## Summary

Formal definition of the workflow lifecycle, step types, event schemas, and validation rules for the orchestrator. This is a design-first feature producing documentation and contracts. No new services are built; existing services (producer, consumer1, consumer2, web-ui) will be adapted to consume and emit orchestration events in subsequent features.

## Technical Context

**Language/Version**: Python 3.14

**Primary Dependencies**: JSON Schema (validation), kafka-python (event format reference), Pydantic (data model reference)

**Storage**: PostgreSQL 15 (workflow definitions), Kafka 7.4.0 (orchestration events), event-network (bridge network)

**Testing**: Manual review of contracts; automated JSON Schema validation via pytest (to be added in implementation phase)

**Target Platform**: Linux Docker containers

**Project Type**: Design-definition for event-driven workflow orchestrator (microservice architecture)

**Performance Goals**: N/A — design phase; no runtime performance targets

**Constraints**: Must align with existing Kafka message patterns used by producer/consumer; must not break existing services; orchestrator NOT yet deployed — defined for future implementation

**Scale/Scope**: Single orchestrator managing multiple concurrent workflow executions; 4 step types; 6 event types; 5 workflow states

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

- **Principle I (Code Quality)**: ✅ Output is documentation and JSON Schema contracts — reviewed via spec review process. No code artifacts committed in this feature.
- **Principle II (Testing Standards)**: ✅ Event schemas testable via JSON Schema validators. Lifecycle transitions testable as acceptance scenarios. Validation rules testable via unit tests (implementation phase).
- **Principle III (UX Consistency)**: ✅ N/A — no user-facing UI changes. Developer-facing documentation follows project conventions.
- **Principle IV (Performance Requirements)**: ✅ Performance constraints deferred to implementation phase. Design does not preclude performance goals.
- **Principle V (Architecture & Observability)**: ✅ Event-driven design aligns with existing Kafka backbone. Orchestration events provide full execution observability (SC-004). Architecture separation maintained — orchestrator is a distinct component.

**Result**: ALL GATES PASS ✅ — No violations requiring justification.

## Mandatory Quality Gates

- [ ] Spec quality checklist MUST pass before implementation: [checklists/requirements.md](checklists/requirements.md)
- [ ] All contracts MUST be reviewed and validated against JSON Schema before implementation phase begins

## Project Structure

### Documentation (this feature)

```text
specs/003-workflow-definition/
├── spec.md               # Feature specification
├── plan.md               # This file (/speckit.plan command output)
├── research.md           # Phase 0 output — technology decisions
├── data-model.md         # Phase 1 output — entity definitions
├── quickstart.md         # Phase 1 output — developer onboarding
├── contracts/
│   └── orchestration-events.md  # Event JSON Schemas
├── checklists/
│   └── requirements.md   # Quality checklists
└── tasks.md              # Phase 2 output (/speckit.tasks command)
```

### Source Code (repository root)

```text
# Design phase — no source code changes.
# Future implementation will update these directories:

consumer/
├── app.py                # +orchestration event handling
├── Dockerfile
├── requirements.txt
└── tests/

producer/
├── app.py                # +workflow submission events
├── Dockerfile
├── requirements.txt
└── tests/

ui/
├── app.py                # +workflow status views
├── Dockerfile
├── requirements.txt
└── templates/

orchestrator/             # NEW — created in implementation phase
├── app.py
├── Dockerfile
├── requirements.txt
└── tests/

docs/
└── schemas/              # NEW — JSON Schema files for event validation
    ├── workflow-started.json
    ├── step-started.json
    ├── step-completed.json
    ├── step-failed.json
    ├── workflow-completed.json
    └── workflow-failed.json
```

**Structure Decision**: Documentation-only in this phase. The `docs/schemas/` directory will hold JSON Schema files for event validation. The `orchestrator/` service directory is reserved for the implementation phase when the orchestrator is actually built. Consumer and producer directories will be updated in implementation to emit/handle orchestration events.

## Complexity Tracking

No violations. The architecture is additive and non-invasive — contracts are created alongside existing service code with no cross-contamination.
