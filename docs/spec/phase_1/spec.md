# Implementation Spec

Open Data Street Inference Import Pipeline (Phase 1)

Datasets: ONSUD + OS Open UPRN + OS Open Roads

## 1. Objectives

Primary objective:
- Ingest three open datasets.
- Build deterministic core and derived tables.
- Produce: `UPRN -> postcode -> nearest named road -> confidence score`.

Quality objectives:
- Deterministic rebuilds from identical inputs.
- Full provenance by dataset release identifiers.
- Programmatic gate checks at every stage.
- No hidden state and no implicit defaults.

### 1.1 Change Note

- 2026-02-20: Nearest-road implementation switched from GiST KNN (`<->`) to deterministic `ST_DWithin + ST_Distance` ordering due reproducible PostgreSQL/PostGIS runtime failure (`index returned tuples in wrong order`) on national-scale data.
- 2026-02-20: Added explicit Phase 1 resume checkpoints (`meta.release_set_stage_checkpoint`) and `pipeline run phase1 --resume` stage restart semantics.

## 2. Scope and Non-goals

Phase 1 includes:
- Dataset ingest and registration.
- Core table construction.
- Spatial nearest named road inference.
- Distance-based confidence scoring.
- Metrics and canonical hash persistence.

Phase 1 excludes:
- PPD, EPC, LLM logic.
- API/serving layer.
- Enumeration endpoints.
- NI data integration.

## 3. Operational Model

The workflow is explicit and separated:
- Ingest commands populate ingest-layer tables only.
- Build command populates release-schema core/derived tables only.
- Activate command repoints stable views only.

### 3.1 CLI Contract

- `pipeline db migrate`
- `pipeline ingest onsud --manifest <path>`
- `pipeline ingest open-uprn --manifest <path>`
- `pipeline ingest open-roads --manifest <path>`
- `pipeline release create --onsud-release <id> --open-uprn-release <id> --open-roads-release <id>`
- `pipeline run phase1 --release-set-id <id> [--resume] [--rebuild]`
- `pipeline release activate --release-set-id <id> --actor <name>`

Rules:
- `pipeline run phase1` never performs ingest work.
- If release set status is `built` and `--rebuild` is absent, `run phase1` is a no-op.
- `--resume` continues only incomplete build stages for that release set using persisted stage checkpoints.
- Without `--resume`, a `created` release set starts as a clean build (drop/recreate release tables, clear checkpoints).
- `--resume` and `--rebuild` are mutually exclusive.
- Checkpoints are written after each table build boundary (not only after aggregate phases).

## 4. Data Model

## 4.1 `meta` schema

### `meta.dataset_release`
Required fields include:
- `dataset_key`, `release_id` (composite key)
- `source_url`, `licence`, `file_path`
- `expected_sha256`, `actual_sha256`
- `retrieved_at`, `manifest_json`
- `source_row_count`, `loaded_row_count` (CSV datasets)
- `source_feature_count`, `loaded_feature_count` (Open Roads)
- `source_layer_name`, `srid_confirmed`

### `meta.pipeline_run`
Tracks run start/end, status, stage, release set linkage.

### `meta.release_set_stage_checkpoint`
Persisted build checkpoints for resumable Phase 1 runs:
- `release_set_id`, `stage_name` (composite key)
- `run_id`, `completed_at`

Allowed `stage_name` values:
- `release_tables_created`
- `core_uprn_postcode_built`
- `core_uprn_point_built`
- `core_road_segment_built`
- `derived_uprn_street_spatial_built`
- `metrics_stored`
- `canonical_hashes_stored`
- `release_marked_built`

### `meta.release_set`
- `release_set_id`
- `onsud_release_id`, `open_uprn_release_id`, `open_roads_release_id`
- `physical_schema`, `status`
- Hard uniqueness constraint:
  - `UNIQUE (onsud_release_id, open_uprn_release_id, open_roads_release_id)`

### `meta.release_activation_log`
Audit record for view promotion actions (`who`, `when`, `from`, `to`).

### `meta.dataset_metrics`
Metric key-value records linked to `run_id` and `release_set_id`.

### `meta.canonical_hash`
- `release_set_id`
- `object_name`
- `projection` (ordered JSON array of columns)
- `row_count`
- `sha256`
- `computed_at`
- `run_id`

Primary key:
- `(release_set_id, object_name, run_id)`

Allowed `object_name` values are locked:
- `core_uprn_postcode`
- `core_uprn_point`
- `core_road_segment`
- `derived_uprn_street_spatial`

## 4.2 `raw` schema

### `raw.onsud_row`
- `dataset_key`, `release_id`, `source_row_num`
- `uprn`, `postcode`
- `extras_jsonb`

### `raw.open_uprn_row`
- `dataset_key`, `release_id`, `source_row_num`
- `uprn`, `latitude`, `longitude`, `easting`, `northing`
- `extras_jsonb`

## 4.3 `stage` schema

### `stage.open_roads_segment` (locked contract)
Required columns:
- `dataset_key text not null` (must be `open_roads`)
- `release_id text not null`
- `segment_id bigint not null` (ingest-generated, deterministic within release)
- `name_display text`
- `name_norm text`
- `geom_bng geometry(MultiLineString,27700) not null`

Required constraints:
- `CHECK (dataset_key = 'open_roads')`
- `UNIQUE (release_id, segment_id)`
- `NOT NULL release_id`

Required indexes:
- btree on `(release_id)`
- GiST on `geom_bng`

Build linkage rule:
- `pipeline run phase1` must read only rows where:
  - `stage.open_roads_segment.release_id = meta.release_set.open_roads_release_id`

## 4.4 Versioned physical schemas and stable views

For each release set, build into `rs_<release_set_id>` tables:
- `core_uprn_postcode`
- `core_uprn_point`
- `core_road_segment`
- `derived_uprn_street_spatial`

Stable consumer views:
- `core.uprn_postcode`
- `core.uprn_point`
- `core.road_segment`
- `derived.uprn_street_spatial`

Only `pipeline release activate` may repoint these views.

## 5. Ingest Rules

General ingest rules:
- Manifest-driven field mapping; no schema guessing.
- SHA256 mismatch: hard fail.
- Duplicate `(dataset_key, release_id)` with different hash: hard fail.
- Duplicate `(dataset_key, release_id)` with same hash: no-op with clear log.
- Missing required mapped columns: hard fail.

Open Roads ingest rule:
- `pipeline ingest open-roads` handles loading into `stage.open_roads_segment` and persists source/loaded feature counts.

## 6. Spatial Inference Rules

- Metric spatial ops use BNG only (`SRID 27700`).
- Distance calculations never use WGS84 geometry.
- Validity gate validates `geom_bng` specifically.

Nearest-road query contract:
- Candidate roads are filtered with `ST_DWithin(point, road, radius_m)`.
- Candidate ordering is `ST_Distance(point, road) ASC, segment_id ASC`.
- GiST KNN operator (`<->`) is not used in Phase 1 runtime queries.

Deterministic tie-breaking:
1. Distance ascending.
2. `segment_id` ascending.

Confidence score bands are fixed:
- `<=15m => 0.70`
- `<=30m => 0.55`
- `<=60m => 0.40`
- `<=150m => 0.25`
- `>150m => 0.00`

No named road within radius:
- `method = 'none_within_radius'`
- `confidence_score = 0.00`

## 7. Normalisation Rules

`postcode_norm`:
- Uppercase
- Remove spaces
- Remove non-alphanumeric

`name_norm` (Phase 1 minimal and frozen):
- Uppercase
- Trim leading/trailing whitespace
- Collapse internal whitespace to single spaces
- Strip punctuation: `.` `,` `'` `-`

## 8. Metrics Definitions

Set definitions are fixed:
- `total_uprns_onsud` = count of non-null UPRNs in `raw.onsud_row` for the release
- `uprns_with_coordinates` = count of distinct UPRNs present in both release core postcode and core point tables
- `uprns_resolved_named_road` = count of UPRNs in derived table where `method='open_roads_nearest'`

Formulas are fixed:
- `coordinate_coverage_pct = uprns_with_coordinates / total_uprns_onsud * 100`
- `resolution_pct = uprns_resolved_named_road / uprns_with_coordinates * 100`

## 9. Gate Criteria (Programmatic)

Registration gate:
- dataset release row exists with expected hash values.

CSV ingest gate:
- source data-row count (header excluded) equals loaded row count exactly.

Open Roads gate:
- source feature count equals loaded feature count.
- all `geom_bng` valid.
- `SRID = 27700`.

Loaded feature count query is locked:

```sql
SELECT COUNT(*) AS loaded_feature_count
FROM stage.open_roads_segment
WHERE release_id = :open_roads_release_id;
```

For PostgreSQL/psycopg execution, the parameter style may be adapted, but the logical query must be identical.

Core gate:
- core table row counts are non-zero.
- join coverage is computed and logged.

Spatial gate:
- resolution percent and distance P50/P90/P99 logged.
- no NULL `method` values.

Metrics gate:
- all mandatory metric keys persisted for the run.

Activation gate:
- stable views point to new release set after activation.
- activation log row exists.

## 10. Canonical Hash Rules

- Canonical hash ordering uses stable key ordering only:
  - `core_uprn_postcode`: `ORDER BY uprn ASC`
  - `core_uprn_point`: `ORDER BY uprn ASC`
  - `core_road_segment`: `ORDER BY segment_id ASC`
  - `derived_uprn_street_spatial`: `ORDER BY uprn ASC`
- Never rely on text-collation ordering for canonical hash row order.
- Projection definition used for each hash must be persisted.

## 11. Test Requirements

Required tests include:
- Two Open Roads releases in staging do not mix by `release_id`.
- Duplicate `(release_id, segment_id)` in staging fails.
- `loaded_feature_count` is sourced from locked stage query and persisted in metadata.
- Deterministic tie-break fixture for equal-distance roads.
- Activation rollback safety test for failed transaction.
- Reproducibility test: same inputs produce same canonical hashes.
