# OS Open Names Dataset Lineage

## Role In The Graph
Open Names contributes named road features and optional TOID references, creating medium-confidence street evidence by postcode and enabling TOID-confirmed promotion.

## Ingest Contract
- Source key: `os_open_names`
- Raw table: `raw.os_open_names_row`
- Stage table: `stage.open_names_road_feature`
- Primary pass usage: Pass `3_open_names_candidates`

## Stage Normalisation
- Normalised fields include:
  - `feature_id`
  - `toid` (when present)
  - `street_name_raw`, `street_name_casefolded`
  - `postcode_norm` (when available)
- Road/transport filtering is applied during staging.

## Downstream Transformations
- Pass 3 inserts `names_postcode_feature` candidates.
- Pass 3 appends `open_lids_toid_usrn` candidates when TOID resolves via LIDS.
- Pass 3 records append-only lineage in `derived.postcode_street_candidate_lineage`.

## Value Added
- Adds broad coverage of named road features.
- Supplies structured evidence that can be upgraded to high confidence with TOID confirmation.

## Related Docs
- Pass 3 details: [`../stages/3_open_names_candidates.md`](../stages/3_open_names_candidates.md)
- LIDS bridge: [`os_open_lids.md`](os_open_lids.md)
- Candidate immutability contract: [`../../spec/pipeline_v3/spec.md`](../../spec/pipeline_v3/spec.md)
