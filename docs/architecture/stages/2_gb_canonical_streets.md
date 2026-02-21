# Pass 2: GB Canonical Streets

## Purpose
Build `core.streets_usrn` as canonical street dictionary keyed by USRN.

## Inputs
- `stage.streets_usrn_input`
- `stage.open_names_road_feature` + `stage.open_lids_toid_usrn` (for inferred fallback names)

## Outputs
- `core.streets_usrn`

## Value Added
- canonical USRN street name layer
- inferred USRN naming where direct USRN names are missing

## Related
- Datasets: [`../datasets/os_open_usrn.md`](../datasets/os_open_usrn.md), [`../datasets/os_open_names.md`](../datasets/os_open_names.md), [`../datasets/os_open_lids.md`](../datasets/os_open_lids.md)
