# Pass 4: UPRN Reinforcement

## Purpose
Generate high-confidence `uprn_usrn` candidates by aggregating property-level evidence.

## Inputs
- `stage.nsul_uprn_postcode`
- `stage.oli_uprn_usrn`
- `core.postcodes`
- `core.streets_usrn`

## Outputs
- `derived.postcode_street_candidates` rows (`candidate_type=uprn_usrn`, `confidence=high`)

## Value Added
- strongest GB evidence class from UPRN-linked observations
- frequency signal (`uprn_count`) for ranking/probability

## Related
- Datasets: [`../datasets/os_open_uprn.md`](../datasets/os_open_uprn.md), [`../datasets/nsul.md`](../datasets/nsul.md), [`../datasets/os_open_lids.md`](../datasets/os_open_lids.md)
