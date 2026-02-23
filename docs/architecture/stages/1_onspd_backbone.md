# Pass 1: ONSPD Backbone

## Purpose
Build canonical postcode entities from staged ONSPD rows.

## Inputs
- `stage.onspd_postcode`
- `stage.open_names_postcode_feature`

## Outputs
- `core.postcodes`
- `core.postcodes_meta`

## Execution Notes
- set-based insert ordered by canonical postcode normalization key
- post-insert `ANALYZE` keeps downstream join planning stable (Pass 3/4/5)
- postcode place/admin enrichment uses Open Names postcode features:
  - deterministic winner per postcode by ascending `source_row_num`
  - `place` is populated from Open Names `populated_place`
  - admin labels are mapped to `*_name` fields
  - URI-derived identifiers and types are mapped to `*_toid` / `*_type` fields
  - duplicate Open Names postcode keys are reported in pass QA metrics

## Value Added
- authoritative postcode backbone
- unified geographic/admin context for subsequent joins

## Related
- Dataset: [`../datasets/onspd.md`](../datasets/onspd.md)
