# DfI Highway Dataset Lineage (Optional NI)

## Role In The Graph
DfI Highway contributes NI spatial road-segment fallback evidence.

## Ingest Contract
- Source key: `dfi_highway`
- Raw table: `raw.dfi_highway_row`
- Stage table: `stage.dfi_road_segment`

## Downstream Transformations
- Pass 6 emits `spatial_dfi_highway` candidates.

## Value Added
- Adds NI fallback coverage where direct NI evidence is absent.

## Related Docs
- Pass 6 details: [`../stages/6_ni_candidates.md`](../stages/6_ni_candidates.md)
- NI confidence constraints: [`../../spec/pipeline_v3/spec.md`](../../spec/pipeline_v3/spec.md)
