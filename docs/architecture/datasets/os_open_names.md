# OS Open Names Dataset Lineage

## Role In The Graph
Open Names contributes:
- named road features and optional TOID references for street candidates
- postcode feature place/admin context for postcode enrichment

## Ingest Contract
- Source key: `os_open_names`
- Raw table: `raw.os_open_names_row`
- Stage table: `stage.open_names_road_feature`
- Stage table: `stage.open_names_postcode_feature`
- Primary pass usage: Pass `3_open_names_candidates`

## Stage Normalisation
- Normalised fields include:
  - `feature_id`
  - `toid` (when present)
  - `street_name_raw`, `street_name_casefolded`
  - `postcode_norm` (when available in the source release)
- Postcode feature normalisation includes:
  - `postcode_norm`, `postcode_display` (derived from `NAME1` where `LOCAL_TYPE='Postcode'`)
  - place/admin label fields: `populated_place`, `district_borough`, `county_unitary`, `region`, `country`
  - parsed place/admin identifier fields:
    - `place_type`, `place_toid`
    - `region_toid`
    - `county_unitary_type`, `county_unitary_toid`
    - `district_borough_type`, `district_borough_toid`
  - deterministic source ordering via `source_row_num`
- Road/transport filtering is applied during staging.
- Required mapping gate:
  - `source_schema.yaml` must resolve `feature_id` and `street_name`.

## Downstream Transformations
- Pass 1 enriches postcode place/admin values from Open Names postcode features:
  - deterministic winner per postcode via lowest `source_row_num`
  - `core.postcodes.place` is set from `populated_place` (open-data derived)
  - `core.postcodes.*_toid` and `*_type` fields are parsed deterministically from Open Names URIs
  - duplicate postcode keys are logged in pass QA metrics
- Pass 3 inserts `names_postcode_feature` candidates.
- Pass 3 appends `open_lids_toid_usrn` candidates when TOID resolves via LIDS.
- Pass 3 records append-only lineage in `derived.postcode_street_candidate_lineage`.

## Value Added
- Adds broad coverage of named road features.
- Provides open-data place/admin context for postcode-level enrichment.
- Supplies structured evidence that can be upgraded to high confidence with TOID confirmation.

## Related Docs
- Pass 3 details: [`../stages/3_open_names_candidates.md`](../stages/3_open_names_candidates.md)
- LIDS bridge: [`os_open_lids.md`](os_open_lids.md)
- Candidate immutability contract: [`../../spec/pipeline_v3/spec.md`](../../spec/pipeline_v3/spec.md)
