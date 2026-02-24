# data/AGENTS.md

## Scope
Manifests and local source-file conventions under `data/`.

## Critical Rule
Manifest/source contract changes must be reflected in docs (`docs/spec/...` and `docs/architecture/...`) and code (`pipeline/src/pipeline/manifest.py`, `pipeline/config/source_schema.yaml`) together.

## Conventions
- source manifests live under `data/manifests/`
- keep source naming aligned with `pipeline/src/pipeline/manifest.py`
- use repository-relative `file_path` values in manifests; do not commit absolute local filesystem paths
- update bundle manifests when source keys change

## Useful References
- source acquisition: `docs/spec/data_sources.md`
- architecture dataset pages: `docs/architecture/datasets/`
