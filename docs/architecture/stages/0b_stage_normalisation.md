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
- `stage.open_lids_pair`
- `stage.open_lids_toid_usrn`
- `stage.open_lids_uprn_usrn`
- `stage.nsul_uprn_postcode`
- optional NI/PPD stage tables

## Determinism/Validation
- required mapped fields validated per source
- heavy-volume sources (`os_open_uprn`, `os_open_lids`, `nsul`) use set-based SQL transforms
- explicit relation typing for LIDS (`toid_usrn`, `uprn_usrn`)
- raw reads are ordered by `source_row_num` with `(ingest_run_id, source_row_num)` indexes

## Value Added
- converts heterogeneous schemas into deterministic internal contracts
- removes Python row-loop bottlenecks for the largest source feeds
- surfaces schema drift early

## Related
- Dataset pages: [`../datasets/README.md`](../datasets/README.md)
- Determinism rules: [`../../spec/pipeline_v3/canonicalisation.md`](../../spec/pipeline_v3/canonicalisation.md)
