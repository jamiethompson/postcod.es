# OSNI Gazetteer Dataset Lineage (Optional NI)

## Role In The Graph
OSNI Gazetteer is NI-specific street evidence input for NI-enabled profiles.

## Ingest Contract
- Source key: `osni_gazetteer`
- Raw table: `raw.osni_gazetteer_row`
- Stage table: `stage.osni_street_point`

## Downstream Transformations
- Pass 6 emits `osni_gazetteer_direct` candidates.

## Value Added
- Extends NI street evidence coverage under explicit NI confidence constraints.

## Related Docs
- Pass 6 details: [`../stages/6_ni_candidates.md`](../stages/6_ni_candidates.md)
- Candidate type rules: [`../../spec/pipeline_v3/spec.md`](../../spec/pipeline_v3/spec.md)
