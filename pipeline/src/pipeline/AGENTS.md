# pipeline/src/pipeline/AGENTS.md

## Scope
Python runtime internals for ingest/build/verify/publish.

## Critical Rule
Keep docs in lockstep with behavior changes. Update:
- `docs/spec/pipeline_v3/spec.md`
- `docs/spec/pipeline_v3/data_model.md`
- `docs/spec/pipeline_v3/canonicalisation.md`
- relevant pages under `docs/architecture/`

## Module Map
- `cli.py`: command surface and run flow
- `manifest.py`: source/bundle manifest validation
- `ingest/workflows.py`: raw ingest into `raw.*`
- `build/workflows.py`: pass execution 0a..8, provenance, finalisation, publish
- `db/migrations.py`: migration execution
- `util/normalise.py`: canonical text/postcode normalization

## Common Pitfalls
- nondeterministic ordering in SQL
- mutating append-only evidence tables
- schema mapping drift between manifests/raw payload fields
- config changes without docs/test updates
