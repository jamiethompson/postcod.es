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
- `stage.oli_identifier_pair`
- `stage.oli_toid_usrn`
- `stage.oli_uprn_usrn`
- `stage.nsul_uprn_postcode`
- optional NI/PPD stage tables

## Determinism/Validation
- required mapped fields validated per source
- stream/batch loading to avoid memory variability
- explicit relation typing for LIDS (`toid_usrn`, `uprn_usrn`)

## Value Added
- converts heterogeneous schemas into deterministic internal contracts
- surfaces schema drift early

## Related
- Dataset pages: [`../datasets/README.md`](../datasets/README.md)
- Determinism rules: [`../../spec/pipeline_v3/canonicalisation.md`](../../spec/pipeline_v3/canonicalisation.md)
