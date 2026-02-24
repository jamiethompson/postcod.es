# Pass 0b: Stage Normalisation

## Purpose
Transform raw payloads into typed/stable stage contracts consumed by later passes.

## Inputs
- `raw.*` tables selected by bundle ingest runs
- `pipeline/config/source_schema.yaml`

## Outputs
- `stage.onspd_postcode`
- `stage.streets_usrn_input`
- `stage.open_names_road_feature`
- `stage.open_names_postcode_feature`
- `stage.open_names_transport_network`
- `stage.open_names_populated_place`
- `stage.open_names_landcover`
- `stage.open_names_landform`
- `stage.open_names_hydrography`
- `stage.open_names_other`
- `stage.v_open_names_features_all` (analysis union view)
- `stage.open_roads_segment`
- `stage.uprn_point`
- `stage.open_lids_toid_usrn`
- `stage.open_lids_uprn_usrn`
- `stage.nsul_uprn_postcode`
- optional NI/PPD stage tables
- checkpoint metric: `stage.open_lids_relation_count`
- checkpoint metric: `qa.open_names_postcode_duplicate_keys`
- checkpoint metrics: `qa.open_names_feature_family_row_counts.<family>`

## Determinism/Validation
- required mapped fields validated per source
- required mapped field presence checks run against the full raw ingest run in a single aggregate scan per source (fail-fast on mapping drift without N field-by-field table scans)
- GB street-source gate examples:
  - `os_open_usrn`: `usrn`
  - `os_open_names`: `feature_id`, `street_name`
  - `os_open_roads`: `segment_id`, `road_name`
- Open Names postcode enrichment extract:
  - `LOCAL_TYPE='Postcode'` rows stage to `stage.open_names_postcode_feature`
  - postcode key derives from normalised `NAME1`
  - deterministic duplicate handling uses `source_row_num` ordering
- Open Names non-road/non-postcode features are routed by `TYPE` through config map:
  - `pipeline/config/open_names_type_families.yaml`
  - each family row carries `linkage_policy` for downstream eligibility decisions
- Open Roads geometry bytes are decoded from GeoPackage payloads into `geom_bng` and validated as SRID 27700
- heavy-volume sources (`os_open_uprn`, `os_open_lids`, `nsul`) use set-based SQL transforms
- explicit relation typing for LIDS (`toid_usrn`, `uprn_usrn`)
- pass-local `work_mem` is raised for large sort/dedupe transforms to reduce temp-file spill
- `(ingest_run_id, source_row_num)` indexes support deterministic replay/debug and source-row traceability
- `stage.*` tables are `UNLOGGED` to reduce write amplification; they are rebuildable from `raw.*`
- pass start truncates all `stage.*` tables to prevent historical-row/index accumulation across build runs
- final `ANALYZE` refreshes planner stats for all `stage.*` relations before Pass 1+
- `raw.*` tables are `UNLOGGED` in this development profile; authoritative replay comes from archived source files + `meta.ingest_run_file`

## Value Added
- converts heterogeneous schemas into deterministic internal contracts
- removes Python row-loop bottlenecks for the largest source feeds
- surfaces schema drift early

## Related
- Dataset pages: [`../datasets/README.md`](../datasets/README.md)
- Determinism rules: [`../../spec/pipeline_v3/canonicalisation.md`](../../spec/pipeline_v3/canonicalisation.md)
