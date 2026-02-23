# Pass 0a: Raw Ingest Validation

## Purpose
Validate bundle sources exist and have non-zero ingest metadata row counts before transformations.

## Inputs
- `meta.build_bundle_source`
- `meta.ingest_run`

## Outputs
- pass checkpoint `0a_raw_ingest` with per-source row count summary

## Value Added
- fast fail for missing/empty source runs
- deterministic baseline counts for observability
- explicit replay contract: raw snapshots are rebuild caches backed by archived source files + file hashes

## Related
- Bundle contract: [`../../spec/pipeline_v3/data_model.md`](../../spec/pipeline_v3/data_model.md)
