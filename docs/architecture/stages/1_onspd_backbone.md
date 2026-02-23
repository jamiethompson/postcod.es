# Pass 1: ONSPD Backbone

## Purpose
Build canonical postcode entities from staged ONSPD rows.

## Inputs
- `stage.onspd_postcode`

## Outputs
- `core.postcodes`
- `core.postcodes_meta`

## Execution Notes
- set-based insert ordered by canonical postcode normalization key
- post-insert `ANALYZE` keeps downstream join planning stable (Pass 3/4/5)
- `post_town` and `locality` are copied from `stage.onspd_postcode` into `core.postcodes` and `core.postcodes_meta` without inference.

## Value Added
- authoritative postcode backbone
- unified geographic/admin context for subsequent joins

## Related
- Dataset: [`../datasets/onspd.md`](../datasets/onspd.md)
