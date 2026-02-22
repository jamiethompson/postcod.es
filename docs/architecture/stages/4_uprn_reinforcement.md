# Pass 4: UPRN Reinforcement

## Purpose
Generate high-confidence `uprn_usrn` candidates by aggregating property-level evidence.

## Inputs
- `stage.nsul_uprn_postcode`
- `stage.open_lids_uprn_usrn`
- `core.postcodes`
- `core.streets_usrn`

## Outputs
- `derived.postcode_street_candidates` rows (`candidate_type=uprn_usrn`, `confidence=high`)

## Execution Shape
- pass-local `work_mem` is raised to avoid temp-file-heavy plans on large NSUL/LIDS joins
- UPRN evidence is aggregated by `(postcode_norm, usrn)` from `stage.nsul_uprn_postcode` + `stage.open_lids_uprn_usrn`
- `ingest_run_id` provenance is sourced directly from `meta.build_bundle_source` (`os_open_lids`) instead of per-row aggregation
- postcode resolution joins via `stage.onspd_postcode.postcode_norm -> postcode_display` before `core.postcodes` join
- deterministic candidate insertion order remains `postcode`, then `usrn`

## Value Added
- strongest GB evidence class from UPRN-linked observations
- frequency signal (`uprn_count`) for ranking/probability

## Related
- Datasets: [`../datasets/os_open_uprn.md`](../datasets/os_open_uprn.md), [`../datasets/nsul.md`](../datasets/nsul.md), [`../datasets/os_open_lids.md`](../datasets/os_open_lids.md)
