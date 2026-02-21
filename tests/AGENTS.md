# tests/AGENTS.md

## Scope
Test strategy and expectations for this repository.

## Critical Rule
Any behavior change in pipeline logic must include matching test updates and documentation updates in the same workstream.

## Expectations
- deterministic outputs for identical inputs
- schema validation tests for mapped required fields
- provenance/immutability contract tests
- probability normalization correctness tests
- fixture-based tests only (no live network dependencies)

## Navigation
- test root: `tests/`
- core docs for expected behavior: `docs/spec/pipeline_v3/`
- architecture references: `docs/architecture/`
