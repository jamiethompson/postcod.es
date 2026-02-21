# Pipeline V3 Data Model

## Meta Layer

### `meta.ingest_run`
Tracks ingest runs by source and release.

### `meta.ingest_run_file`
Child rows for multi-file ingest provenance.

### `meta.build_bundle`
Deterministic source selection envelope by profile.

### `meta.build_bundle_source`
Source-to-ingest-run links for each bundle.

Bundle rule:
- most sources have exactly one ingest run per bundle
- `ppd` may have multiple ingest runs (baseline + yearly/monthly updates)

### `meta.build_run`
Execution record for a bundle build.

### `meta.build_pass_checkpoint`
Per-pass completion checkpoints.

### `meta.canonical_hash`
Deterministic object hashes per build run.

### `meta.dataset_publication`
Published dataset pointer log.

## Raw and Stage Layers

- `raw.*` holds immutable source snapshots.
- `stage.*` holds typed, normalised rows that build passes consume.

## Core Layer

- `core.postcodes`
- `core.postcodes_meta`
- `core.streets_usrn`

## Derived Layer

### `derived.postcode_street_candidates`
Append-only evidence table.

Required contract:
- insert-only table
- no update/delete of evidence rows
- candidate rows include `source_name`, `ingest_run_id`, `produced_build_run_id`

### `derived.postcode_street_candidate_lineage`
Promotion lineage mapping parent evidence rows to child evidence rows.

### `derived.postcode_streets_final`
One row per final postcode-street record.

### `derived.postcode_streets_final_candidate`
Relational link from final record to all contributing candidate rows.

### `derived.postcode_streets_final_source`
Relational source summary by final record.

## Internal Layer

### `internal.unit_index`
Disambiguation-only table. Never exposed to API reader role.

## API Projection Layer

- `api.postcode_street_lookup__<dataset_version>`
- `api.postcode_lookup__<dataset_version>`
- stable views: `api.postcode_street_lookup`, `api.postcode_lookup`

## Cross-links
- Architecture relationships: [`../../architecture/relationships-overview.md`](../../architecture/relationships-overview.md)
- Dataset lineage pages: [`../../architecture/datasets/README.md`](../../architecture/datasets/README.md)
- Stage/pass pages: [`../../architecture/stages/README.md`](../../architecture/stages/README.md)
