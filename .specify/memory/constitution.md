<!--
  Sync Impact Report
  Version change: 0.0.0 → 1.0.0
  Modified principles: N/A (initial creation)
  Added sections: Core Principles (I-V), Technology & Infrastructure Standards, Development Workflow & Quality Gates, Governance
  Removed sections: N/A
  Templates requiring updates:
    - .specify/templates/tasks-template.md ✅ updated (tests no longer optional per Principle II)
    - .specify/templates/spec-template.md ✅ updated
    - .specify/templates/plan-template.md ✅ updated
    - .specify/templates/checklist-template.md ✅ updated
  Follow-up TODOs: None
-->

# Orquestador de Workflows Constitution

## Core Principles

### I. Code Quality
MUST use static analysis tools (linters, type checkers) as the first line of defense. All source code MUST pass linting and type-checking before commit. Code review is mandatory for every pull request — no unreviewed code merges to main. Duplicated code MUST be extracted and reused; automated duplication gates MUST be in place. Dead code, commented-out blocks, debug artifacts, and unresolved TODOs MUST NOT be committed.

### II. Testing Standards
Tests are a first-class deliverable, not an afterthought. Every new feature and bug fix MUST include tests. Unit tests MUST cover at least 80% of business logic. Integration tests MUST validate inter-service contracts (Kafka topics, API payloads) and MUST run in every CI pipeline. End-to-end tests MUST cover critical user journeys defined in the feature specification. All test suites MUST pass before merge. The Red-Green-Refactor cycle is the expected rhythm: write a failing test first, then implement, then refactor.

### III. User Experience Consistency
All user-facing interfaces MUST adhere to a unified design language — consistent layout, typography, color, spacing, and interaction patterns. Every interactive state (loading, empty, error, success) MUST be explicitly handled and visually distinct. Error messages MUST be human-readable, locale-aware, and action-oriented. Backend error responses MUST use a consistent schema so the frontend can render them uniformly.

### IV. Performance Requirements
Every endpoint and processing pipeline MUST have defined, measured, and enforced latency and throughput budgets. Database queries MUST be indexed; N+1 query patterns MUST be eliminated before merge. Message processing (Kafka consumer/producer) MUST be benchmarked for throughput and rebalancing behavior. Resource limits (CPU, memory) MUST be set per container. Performance regressions detected in CI MUST block deployment.

### V. Architecture & Observability
Services MUST communicate through well-defined, versioned contracts. Every service MUST expose a health-check endpoint. Structured, machine-parseable logging is mandatory across all components. Distributed tracing MUST be implemented for cross-service workflow tracking. Kafka schemas MUST be versioned and backward-compatible. Architectural changes MUST be documented in diagrams and reviewed against the existing topology before implementation.

## Technology & Infrastructure Standards

Docker and Docker Compose are the mandated deployment and development environment. Kafka with Schema Registry is the sole event backbone; direct service-to-service calls MUST be justified and approved. PostgreSQL is the primary data store; any alternative store requires explicit architectural approval. All containers MUST declare CPU and memory limits. Environment configuration MUST be externalized via `.env` files, never hardcoded. Secrets MUST never be committed to the repository.

## Development Workflow & Quality Gates

Development follows a feature-branch workflow. Every feature branch MUST originate from `main` and be merged via pull request. The CI pipeline MUST run: lint, type-check, unit tests, integration tests, and duplication checks. A PR MAY merge only when all CI stages pass and at least one reviewer has approved. Breaking changes to contracts or schemas MUST be accompanied by a migration plan attached to the PR. Deployment requires all quality gates to pass on the target branch.

## Governance

This Constitution defines non-negotiable principles for all project work. Amendments require a documented proposal, team review, and a migration plan for any existing violations. The Constitution version follows semantic versioning: MAJOR for principle removal or redefinition, MINOR for new principles or materially expanded guidance, PATCH for clarifications and wording refinements. A compliance review MUST be conducted at the start of each feature cycle. All templates, commands, and guidance files in `.specify/` MUST align with this Constitution; discrepancies SHOULD be raised as issues for resolution.

**Version**: 1.0.0 | **Ratified**: 2026-06-02 | **Last Amended**: 2026-06-02
