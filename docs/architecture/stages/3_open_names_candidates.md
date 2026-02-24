# Pass 3: Open Names Candidates

## Purpose
Create medium-confidence street candidates and append-only TOID-confirmed promotions.

## Inputs
- `stage.open_names_road_feature`
- `stage.open_lids_toid_usrn`
- `core.postcodes`

## Outputs
- base candidates: `candidate_type=names_postcode_feature`
- promoted candidates: `candidate_type=open_lids_toid_usrn`
- lineage: `derived.postcode_street_candidate_lineage`

## Contract
- candidate table is immutable evidence
- promotions are insert-only; parent rows are never mutated
- TOID linkage key uses `COALESCE(related_toid, feature_toid, toid)` from Open Names road stage
- pass runs conditionally:
  - if no staged Open Names road rows exist, pass returns zero rows with explicit skip metric
  - if staged Open Names rows and core postcode rows exist but base insert is zero, build fails fast

## Value Added
- broad named-road evidence
- high-confidence confirmation via TOID->USRN bridge

## Related
- Spec contract: [`../../spec/pipeline_v3/spec.md`](../../spec/pipeline_v3/spec.md)
- Dataset pages: [`../datasets/os_open_names.md`](../datasets/os_open_names.md), [`../datasets/os_open_lids.md`](../datasets/os_open_lids.md)
