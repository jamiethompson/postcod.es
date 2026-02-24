# Pass 2: GB Canonical Streets

## Purpose
Build `core.streets_usrn` as canonical street dictionary keyed by USRN.

## Inputs
- `stage.streets_usrn_input`
- `stage.open_names_road_feature` + `stage.open_roads_segment` + `stage.open_lids_toid_usrn` (for inferred fallback names)

## Outputs
- `core.streets_usrn`

## Execution Shape
- set-based direct insert from `stage.streets_usrn_input`
- inferred path pre-aggregates TOID-name evidence from Open Names and Open Roads before joining to LIDS
- Open Names TOID key for inferred path is resolved as `COALESCE(related_toid, feature_toid, toid)`
- set-based inferred insert (Open Names/Open Roads + LIDS) for USRNs not already present
- inferred name ranking uses deterministic tie-breaks by evidence count, then name quality rank, then source priority, then casefolded/name lexical order
- road-number labels are down-ranked in inferred naming, and excluded when postal-plausible names exist for the same USRN
- stage join indexes on `stage.open_names_road_feature(build_run_id, toid)` and `(build_run_id, postcode_norm)` support Pass 2/3 joins
- post-pass `ANALYZE` keeps `core.streets_usrn` statistics current for Pass 4 joins

## Value Added
- canonical USRN street name layer
- inferred USRN naming where direct USRN names are missing
- additional name recovery when Open Names TOID coverage is sparse but Open Roads has matching TOIDs

## Related
- Datasets: [`../datasets/os_open_usrn.md`](../datasets/os_open_usrn.md), [`../datasets/os_open_names.md`](../datasets/os_open_names.md), [`../datasets/os_open_lids.md`](../datasets/os_open_lids.md)
