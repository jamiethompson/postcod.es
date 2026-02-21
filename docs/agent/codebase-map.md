# Codebase Map

## Main Runtime Modules
- CLI: `pipeline/src/pipeline/cli.py`
- Manifest parsing/validation: `pipeline/src/pipeline/manifest.py`
- Ingest workflows (raw ingestion): `pipeline/src/pipeline/ingest/workflows.py`
- Build workflows (pass execution/finalization/publish): `pipeline/src/pipeline/build/workflows.py`
- DB migrations runner: `pipeline/src/pipeline/db/migrations.py`
- Normalization utilities: `pipeline/src/pipeline/util/normalise.py`

## SQL and Config
- SQL migrations: `pipeline/sql/migrations/`
- Source schema mapping config: `pipeline/config/source_schema.yaml`
- Frequency weights config: `pipeline/config/frequency_weights.yaml`
- Normalization config: `pipeline/config/normalisation.yaml`

## Manifests and Data Inputs
- Real manifests: `data/manifests/real_v3/`
- Smoke manifests: `data/manifests/v3_smoke/`
- Local source files: `data/source_files/`

## Tests
- Test suite root: `tests/`
- Focus on deterministic behavior, schema validation, provenance contracts, and pass semantics.
