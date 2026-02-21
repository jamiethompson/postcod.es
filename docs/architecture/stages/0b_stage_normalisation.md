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
- `stage.open_roads_segment`
- `stage.uprn_point`
- `stage.open_lids_toid_usrn`
- `stage.open_lids_uprn_usrn`
- `stage.nsul_uprn_postcode`
- optional NI/PPD stage tables
- checkpoint metric: `stage.open_lids_relation_count`

## Determinism/Validation
- required mapped fields validated per source
- heavy-volume sources (`os_open_uprn`, `os_open_lids`, `nsul`) use set-based SQL transforms
- explicit relation typing for LIDS (`toid_usrn`, `uprn_usrn`)
- `(ingest_run_id, source_row_num)` indexes support deterministic replay/debug and source-row traceability
- `stage.*` tables are `UNLOGGED` to reduce write amplification; they are rebuildable from `raw.*`
- pass start truncates all `stage.*` tables to prevent historical-row/index accumulation across build runs
- `raw.*` tables are `UNLOGGED` in this development profile; authoritative replay comes from archived source files + `meta.ingest_run_file`

## Value Added
- converts heterogeneous schemas into deterministic internal contracts
- removes Python row-loop bottlenecks for the largest source feeds
- surfaces schema drift early

## Related
- Dataset pages: [`../datasets/README.md`](../datasets/README.md)
- Determinism rules: [`../../spec/pipeline_v3/canonicalisation.md`](../../spec/pipeline_v3/canonicalisation.md)
