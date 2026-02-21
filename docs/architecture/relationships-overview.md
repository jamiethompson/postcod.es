# Dataset Relationship Overview

## Core Graph

```text
ONSPD -> core.postcodes
OS Open USRN -> core.streets_usrn
OS Open Names + ONSPD -> candidates (names_postcode_feature)
OS Open Names + LIDS (TOID->USRN) -> candidates (oli_toid_usrn)
OS Open UPRN + NSUL + LIDS (UPRN->USRN) -> candidates (uprn_usrn)
OS Open Roads + core.postcodes -> fallback candidates (spatial_os_open_roads)
Optional: PPD -> gap-fill candidates (ppd_parse_*)
All candidates + weights -> derived.postcode_streets_final
Final + provenance joins -> api projections
```

## Relationship Types
- Validation relationship:
  - ONSPD validates and normalises postcode existence and country/subdivision context.
- Canonical street relationship:
  - USRN is the canonical street key (`core.streets_usrn`).
- Direct semantic relationship:
  - Open Names road features link to postcodes and sometimes TOIDs.
- Identifier bridge relationship:
  - LIDS resolves `TOID -> USRN` and `UPRN -> USRN`.
- Property density relationship:
  - NSUL ties UPRN to postcode, enabling postcode/USRN aggregation with LIDS.
- Spatial fallback relationship:
  - Open Roads provides low-confidence fallback where high-confidence evidence is absent.

## Where Each Relationship Is Materialised
- Raw snapshots: `raw.*`
- Typed normalisation: `stage.*`
- Canonical entities: `core.postcodes`, `core.streets_usrn`
- Evidence graph: `derived.postcode_street_candidates`, `derived.postcode_street_candidate_lineage`
- Final resolved output: `derived.postcode_streets_final`
- Provenance joins: `derived.postcode_streets_final_candidate`, `derived.postcode_streets_final_source`
- API shapes: `api.postcode_street_lookup__<dataset_version>`, `api.postcode_lookup__<dataset_version>`

## Related Docs
- Pass-by-pass detail: [`stages/README.md`](stages/README.md)
- Dataset-specific lineage: [`datasets/README.md`](datasets/README.md)
- Value added by stage: [`value-added-by-stage.md`](value-added-by-stage.md)
- Spec authority: [`../spec/pipeline_v3/spec.md`](../spec/pipeline_v3/spec.md)
