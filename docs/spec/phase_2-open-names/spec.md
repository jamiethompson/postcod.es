# Software Requirements Specification
## Phase 2: Open Names Augmentation + Postcode Street Enumeration

**Document ID:** SRS-PIPELINE-002  
**Version:** 1.1  
**Date:** February 20, 2026

## 1. Scope Lock

Phase 2 is a controlled extension of the existing pipeline.

- Ingest remains explicit and manifest-driven.
- Build remains deterministic and resumable.
- Activation remains explicit and transactional.
- No serving/API work is included.

Phase 2 is not optional in release composition:

- `pipeline release create` requires all four releases:
  - `onsud_release_id`
  - `open_uprn_release_id`
  - `open_roads_release_id`
  - `open_names_release_id`

## 2. CLI Contract

- `pipeline ingest open-names --manifest <path>`
- `pipeline run phase2-open-names --release-set-id <id> [--rebuild] [--resume] [--open-roads-radius-m <m>] [--open-names-radius-m <m>]`
- `pipeline release activate --release-set-id <id> --actor <actor> [--ack-warnings]`

Rules:

- `pipeline run phase2-open-names` performs no ingest work.
- `--rebuild` and `--resume` are mutually exclusive.
- If a release set is already `built`/`active`, run is a no-op unless `--rebuild` is set.

## 3. Build Checkpoints (Locked Order)

1. `release_tables_created`
2. `core_uprn_postcode_built`
3. `core_uprn_point_built`
4. `core_road_segment_built`
5. `core_open_names_entry_built`
6. `core_postcode_unit_seed_built`
7. `derived_uprn_street_spatial_built`
8. `derived_postcode_street_built`
9. `metrics_stored`
10. `warnings_stored`
11. `canonical_hashes_stored`
12. `release_marked_built`

`warnings_stored` is intentionally before `canonical_hashes_stored`.

## 4. Core Data Model

Physical tables are built in `rs_<release_set_id>`.

### 4.1 `core_open_names_entry`

- `entry_id text primary key`
- `name_display text`
- `name_norm text`
- `name2_display text`
- `name2_norm text`
- `name1_lang text`
- `local_type text not null`
- `postcode_district_norm text`
- `geom_bng geometry(Point,27700) not null`
- `open_names_release_id text not null`

### 4.2 `core_postcode_unit_seed`

Built from ONSUD postcode-unit grid references.

- `postcode_unit_norm text primary key`
- `postcode_sector_norm text`
- `postcode_district_norm text`
- `postcode_area_norm text`
- `easting double precision not null`
- `northing double precision not null`
- `geom_bng geometry(Point,27700) not null`
- `onsud_release_id text not null`

Hard failures:

- Missing seed for any postcode unit present in `core_uprn_postcode`

Seed selection rule (locked):

- ONSUD can contain multiple coordinate pairs per postcode unit in real releases.
- `core_postcode_unit_seed` derives one deterministic representative seed per postcode unit using:
  - `AVG(postcode_unit_easting::numeric)`
  - `AVG(postcode_unit_northing::numeric)`
- This is deterministic, auditable, and avoids non-reproducible row-choice behavior.
- Diagnostic metrics track seed multiplicity (`postcode_unit_seed_multi_coord_count`, `postcode_unit_seed_max_distinct_coords`).

## 5. UPRN Street Reconciliation (`derived_uprn_street_spatial`)

## 5.1 Inputs and Tie-Breaking

- Open Roads nearest lookup radius default: `150m`
- Open Names nearest lookup radius default: `200m`
- Open Roads tie-break: distance ascending, `segment_id` ascending
- Open Names tie-break: distance ascending, `entry_id` ascending
- KNN operator `<->` is not used; deterministic `ST_Distance` ordering is used.

Road-number detection regex is locked:

- `^(A|B|M)[0-9]{1,4}[A-Z]?$`
- Applied to stripped token (`upper`, non-alphanumeric removed)

## 5.2 Reconciliation Rules

For rows with `method = 'open_roads_nearest'`:

1. No Open Names in range: keep Open Roads.
2. Open Roads is numbered road and Open Names in range: replace with Open Names.
3. Both present and `name_norm` equal: mark corroborated.
4. Both present and differ: keep Open Roads.

For rows with `method = 'none_within_radius'`:

- No Open Names lookup is used.
- Open Names fields are null.

## 5.3 Output Columns (Phase 2)

- `street_open_roads`
- `street_open_names`
- `street_display`
- `street_norm`
- `open_roads_distance_m`
- `open_names_distance_m`
- `confidence_score`
- `method`
- `name_source`
- `corroborated`
- `sources text[]` (fixed order provenance)

`open_names_distance_m` semantics are locked:

- Distance to nearest in-range Open Names entry
- `NULL` when no Open Names entry exists within `open_names_radius_m`

## 5.4 Provenance Order (`sources`)

Always populated in this exact order:

1. `onsud:<release_id>`
2. `open_uprn:<release_id>`
3. `open_roads:<release_id>`
4. `open_names:<release_id>`

## 6. Postcode Street Enumeration (`derived_postcode_street`)

Single normalized table:

- `postcode_level text` in (`unit`,`sector`,`district`,`area`)
- `postcode_value_norm text`
- `entry_id text`
- `name_display text`
- `name_norm text`
- `name2_display text`
- `name2_norm text`
- `association_method text` in (`district_direct`,`spatial_voronoi`)
- `sources text[]`
- release IDs

Primary key:

- `(postcode_level, postcode_value_norm, entry_id)`

Lookup index:

- `(postcode_level, postcode_value_norm)`

## 6.1 Association Contract

Two candidate methods are generated:

- `district_direct`: `core_open_names_entry.postcode_district_norm = core_postcode_unit_seed.postcode_district_norm`
- `spatial_voronoi`: Open Names point contained by postcode-unit Voronoi cell

De-dup precedence is locked:

1. `district_direct`
2. `spatial_voronoi`

## 6.2 Voronoi Contract

- Seed source: `core_postcode_unit_seed.geom_bng`
- Clipping: convex hull of seeds buffered by `pipeline.config.VORONOI_HULL_BUFFER_M`
- Buffer value is parameter-bound SQL (`hull_buffer_m`), never inlined
- Each postcode unit seed must map to exactly one Voronoi cell, else hard fail

Reference: `docs/spec/phase_2-open-names/voronoi_method.md`

## 7. Metrics (Mandatory)

Phase 1 metrics remain mandatory plus Phase 2 keys.

Additional keys:

- `open_names_entries_loaded`
- `uprns_corroborated`
- `corroboration_pct`
- `uprns_numbered_road_replaced`
- `numbered_road_replacement_pct`
- `uprns_genuine_disagreement`
- `disagreement_pct`
- `postcode_units_with_streets`
- `postcode_units_without_streets`
- `open_names_search_radius_m`
- `total_resolved`

Formula lock:

- `total_uprns_onsud = COUNT(non-null uprn in raw.onsud_row for onsud release)`
- `uprns_with_coordinates = COUNT(DISTINCT uprn in core_uprn_postcode ∩ core_uprn_point)`
- `total_resolved = COUNT(*) WHERE method='open_roads_nearest'`
- `coordinate_coverage_pct = uprns_with_coordinates / total_uprns_onsud * 100`
- `resolution_pct = total_resolved / uprns_with_coordinates * 100`
- `corroboration_pct = uprns_corroborated / total_resolved * 100`
- `numbered_road_replacement_pct = uprns_numbered_road_replaced / total_resolved * 100`
- `disagreement_pct = uprns_genuine_disagreement / total_resolved * 100`

## 8. Warning + Activation Gate

If `disagreement_pct > 5.0`:

- Persist warning row in `meta.pipeline_run_warning`
- Set `requires_ack = true`
- Activation is blocked until acknowledged

Acknowledgement path:

- `pipeline release activate ... --ack-warnings`
- Writes `acknowledged_by` and `acknowledged_at`

## 9. Canonical Hash Contract

Stored in `meta.canonical_hash` per run.

Phase 2 object names:

- `core_uprn_postcode`
- `core_uprn_point`
- `core_road_segment`
- `core_open_names_entry`
- `core_postcode_unit_seed`
- `derived_uprn_street_spatial`
- `derived_postcode_street`

Hash rules:

- Explicit projection list stored as ordered JSON array
- Operational timestamps excluded
- Deterministic key ordering only (numeric keys or `COLLATE "C"` for text keys)

## 10. Out of Scope

- API serving design
- NI dataset onboarding
- LLM disagreement adjudication
- Cross-source linguistic equivalence (`street_equivalence_norm`)

Any behaviour change to this contract must be recorded in:

- `docs/spec/phase_2-open-names/changes.md`
