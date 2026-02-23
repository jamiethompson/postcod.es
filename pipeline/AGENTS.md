# pipeline/AGENTS.md

## Scope
Implementation guidance for `pipeline/` (configs, SQL migrations, runtime modules).

## Critical Rule
If code in `pipeline/` changes behavior, update matching docs in `docs/spec/pipeline_v3/` and `docs/architecture/` in the same commit series.

## Fast Navigation
- CLI: `pipeline/src/pipeline/cli.py`
- Build logic: `pipeline/src/pipeline/build/workflows.py`
- Ingest logic: `pipeline/src/pipeline/ingest/workflows.py`
- Manifest contracts: `pipeline/src/pipeline/manifest.py`
- Migrations: `pipeline/sql/migrations/`
- Runtime configs: `pipeline/config/`

## Change Checklist
- migration required?
- manifest/source schema mappings updated?
- determinism/canonicalisation still valid?
- tests/docs updated together?
