# Pass 3: Open Names Candidates

## Purpose
Create medium-confidence street candidates and append-only TOID-confirmed promotions.

## Inputs
- `stage.open_names_road_feature`
- `stage.oli_toid_usrn`
- `core.postcodes`

## Outputs
- base candidates: `candidate_type=names_postcode_feature`
- promoted candidates: `candidate_type=oli_toid_usrn`
- lineage: `derived.postcode_street_candidate_lineage`

## Contract
- candidate table is immutable evidence
- promotions are insert-only; parent rows are never mutated

## Value Added
- broad named-road evidence
- high-confidence confirmation via TOID->USRN bridge

## Related
- Spec contract: [`../../spec/pipeline_v3/spec.md`](../../spec/pipeline_v3/spec.md)
- Dataset pages: [`../datasets/os_open_names.md`](../datasets/os_open_names.md), [`../datasets/os_open_lids.md`](../datasets/os_open_lids.md)
