# Agent Start Here

## Objective
Produce deterministic, replayable postcode/street outputs from open-source ingests with strict provenance.

## Golden Path
1. Read V3 spec docs:
   - `docs/spec/pipeline_v3/spec.md`
   - `docs/spec/pipeline_v3/data_model.md`
   - `docs/spec/pipeline_v3/canonicalisation.md`
2. Validate local runtime assumptions:
   - DB migrations applied
   - Manifest source names and schema mappings align with actual raw payload fields
3. Run in this sequence:
   - `pipeline db migrate`
   - `pipeline ingest source --manifest <source_manifest.json>` (repeat by source)
   - `pipeline bundle create --manifest <bundle_manifest.json>`
   - `pipeline build run --bundle-id <bundle_id> [--rebuild|--resume]`
   - `pipeline build verify --build-run-id <build_run_id>`
   - `pipeline build publish --build-run-id <build_run_id> --actor <name>`

## Critical Contracts
- Raw layer is immutable.
- `derived.postcode_street_candidates` is append-only evidence.
- Pass 3 promotion is insert-only with lineage links.
- Probability normalization is exact by formula and stored with deterministic residual correction.

## When Unsure
- Prefer explicit failure over implicit behavior.
- Capture unknowns in docs + validation.
- Keep all time and ordering deterministic.
