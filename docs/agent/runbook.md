# Operational Runbook

## 1) Migrate
```bash
pipeline --dsn "dbname=postcodes_v3" db migrate
```

## 2) Ingest Sources
```bash
pipeline --dsn "dbname=postcodes_v3" ingest source --manifest /path/to/source_manifest.json
```
Repeat for each source in the target profile.

## 3) Create Bundle
```bash
pipeline --dsn "dbname=postcodes_v3" bundle create --manifest /path/to/bundle_manifest.json
```

## 4) Build
```bash
pipeline --dsn "dbname=postcodes_v3" build run --bundle-id <bundle_id> [--rebuild|--resume]
```
Use `--resume` only for the same bundle/run lineage.

## 5) Verify
```bash
pipeline --dsn "dbname=postcodes_v3" build verify --build-run-id <build_run_id>
```

## 6) Publish
```bash
pipeline --dsn "dbname=postcodes_v3" build publish --build-run-id <build_run_id> --actor <name>
```

## Observability Queries
- Build status: `meta.build_run`
- Pass checkpoints: `meta.build_pass_checkpoint`
- Ingest provenance: `meta.ingest_run`, `meta.ingest_run_file`

## Failure Policy
- Fail fast on schema/field mismatches.
- Do not patch raw data; fix logic/mapping and rebuild.
- Record behavior changes in `docs/spec/pipeline_v3/` docs in the same PR.
- Keep architecture docs in sync: `docs/architecture/`.
