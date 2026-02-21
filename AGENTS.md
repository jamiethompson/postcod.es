# AGENTS.md

Purpose: this file is the agent entrypoint for this repository.
Use it as a roadmap to the docs, then execute work with strict reproducibility and provenance.

## 1. Start Here (Required Reading Order)
1. `docs/README.md`
2. `docs/agent/start-here.md`
3. `docs/spec/pipeline_v3/spec.md`
4. `docs/spec/pipeline_v3/data_model.md`
5. `docs/spec/pipeline_v3/canonicalisation.md`

If behavior in code differs from spec, treat it as a defect and document the delta.

## 2. Documentation Roadmap
- V3 product/behavior spec: `docs/spec/pipeline_v3/spec.md`
- V3 schema and table contracts: `docs/spec/pipeline_v3/data_model.md`
- Determinism and canonical rules: `docs/spec/pipeline_v3/canonicalisation.md`
- Source acquisition + licensing context: `docs/spec/data_sources.md`
- Agent onboarding: `docs/agent/start-here.md`
- Codebase map: `docs/agent/codebase-map.md`
- Operational runbook (ingest/build/publish): `docs/agent/runbook.md`
- Legacy phase docs (historical only): `docs/spec/phase_1/`, `docs/spec/phase_2-open-names/`

## 3. Non-Negotiable Engineering Rules
- No guessing: unknown fields/semantics must be marked unknown and validated explicitly.
- Reproducibility first: same inputs must produce same outputs.
- Raw data is immutable: never mutate raw source snapshots.
- Provenance is mandatory: derived records must trace to source run(s) and method.
- Deterministic execution: stable ordering + explicit tie-breaks only.
- Fail fast on schema/geometry/CRS issues.
- This repo is a pipeline only; do not add API-serving scope here.

## 4. Change Requirements
For meaningful behavior changes (join logic, scoring, normalization, radius/thresholds, CRS, pass semantics):
1. Update spec/docs in `docs/` in the same change.
2. Never place absolute local filesystem paths in docs; use repository-relative paths.
3. State rationale.
4. Provide before/after metrics or counts where applicable.
5. Confirm determinism impact.
6. Add/adjust tests (fixture-based; no live-download dependency).

## 5. Commit Standards
- Commit at logical checkpoints whenever it makes sense.
- Prefer atomic commits grouped by concern (schema, ingest, transforms, tests, docs).
- Use Conventional Commits for every commit message (`type(scope): summary`).

## 6. Decision Rule
If a change reduces transparency, obscures provenance, weakens reproducibility, or introduces hidden assumptions, do not merge it.

Clarity over cleverness. Traceability over speed. Correctness over convenience.
