# ONSPD Dataset Lineage

## Role In The Graph
ONSPD is the definitive postcode backbone. It validates postcode existence and contributes canonical postcode metadata used by all later joins.

## Ingest Contract
- Source key: `onspd`
- Raw table: `raw.onspd_row`
- Manifest mapping source: `pipeline/config/source_schema.yaml`
- Primary pass usage: Pass `1_onspd_backbone`

## Stage Normalisation
- Stage table: `stage.onspd_postcode`
- Main fields:
  - `postcode_norm`, `postcode_display`
  - `status`, `lat`, `lon`, `easting`, `northing`
  - `country_iso2`, `country_iso3`, `subdivision_code`
  - `post_town`, `locality` (when present in source payload)
  - `street_enrichment_available`
- Limitations:
  - `post_town` and `locality` are passthrough attributes only.
  - If a source release omits these fields, `stage.onspd_postcode` and downstream outputs retain `NULL`.

## Downstream Transformations
- Pass 1 writes:
  - `core.postcodes`
  - `core.postcodes_meta`
- Used by passes 3/4/5/6/7 for postcode validation and join gating.

## Value Added
- Converts raw postcode records into canonical and display-safe forms.
- Centralizes country/subdivision context for profile-specific behavior.
- Prevents downstream candidate generation for invalid/unresolvable postcodes.

## Related Docs
- Pass 1 details: [`../stages/1_onspd_backbone.md`](../stages/1_onspd_backbone.md)
- Canonical postcode rules: [`../../spec/pipeline_v3/canonicalisation.md`](../../spec/pipeline_v3/canonicalisation.md)
- Relationship map: [`../relationships-overview.md`](../relationships-overview.md)
