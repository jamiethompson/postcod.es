# Pass 2: GB Canonical Streets

## Purpose
Build `core.streets_usrn` as canonical street dictionary keyed by USRN.

## Inputs
- `stage.streets_usrn_input`
- `stage.open_names_road_feature` + `stage.open_lids_toid_usrn` (for inferred fallback names)

## Outputs
- `core.streets_usrn`

## Execution Shape
- set-based direct insert from `stage.streets_usrn_input`
- inferred path pre-aggregates Open Names by `toid` before joining to LIDS to reduce join volume
- set-based inferred insert (Open Names + LIDS) for USRNs not already present
- inferred name ranking uses deterministic tie-breaks by evidence count then casefolded/name lexical order
- stage join indexes on `stage.open_names_road_feature(build_run_id, toid)` and `(build_run_id, postcode_norm)` support Pass 2/3 joins
- post-pass `ANALYZE` keeps `core.streets_usrn` statistics current for Pass 4 joins

## Value Added
- canonical USRN street name layer
- inferred USRN naming where direct USRN names are missing

## Related
- Datasets: [`../datasets/os_open_usrn.md`](../datasets/os_open_usrn.md), [`../datasets/os_open_names.md`](../datasets/os_open_names.md), [`../datasets/os_open_lids.md`](../datasets/os_open_lids.md)
