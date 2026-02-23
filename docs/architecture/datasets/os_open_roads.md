# OS Open Roads Dataset Lineage

## Role In The Graph
Open Roads provides fallback street evidence where stronger candidate types do not exist.

## Ingest Contract
- Source key: `os_open_roads`
- Raw table: `raw.os_open_roads_row`
- Stage table: `stage.open_roads_segment`
- Primary pass usage: Pass `5_gb_spatial_fallback`

## Stage Normalisation
- Normalised fields:
  - `segment_id`, `road_id`
  - `road_name`, `road_name_casefolded`
  - optional `usrn`
  - optional `postcode_norm`
- Required mapping gate:
  - `source_schema.yaml` must resolve `segment_id` and `road_name`.

## Downstream Transformations
- Pass 2 can infer missing USRN canonical names by joining `road_id` (TOID) to LIDS TOID->USRN links.
- Pass 5 emits `spatial_os_open_roads` low-confidence candidates only for postcodes without high-confidence evidence.

## Value Added
- Improves coverage without overriding stronger evidence.
- Preserves confidence transparency by explicitly tagging fallback provenance.

## Related Docs
- Pass 5 details: [`../stages/5_gb_spatial_fallback.md`](../stages/5_gb_spatial_fallback.md)
- Confidence model: [`../../spec/pipeline_v3/spec.md`](../../spec/pipeline_v3/spec.md)
