# docs/AGENTS.md

## Scope
Documentation standards and navigation for everything under `docs/`.

## Critical Rule
Any code behavior change must update relevant docs in the same change set. Documentation is not optional follow-up work.

## Path Rule
Never use absolute local filesystem paths in docs. Use repository-relative paths only.

## Navigation
- Docs index: `docs/README.md`
- Architecture map: `docs/architecture/README.md`
- V3 spec authority: `docs/spec/pipeline_v3/`
- Source acquisition details: `docs/spec/data_sources.md`

## Update Expectations
When editing docs:
- keep links valid and cross-linked
- update related index pages if new docs are added
- keep wording deterministic and implementation-aligned
