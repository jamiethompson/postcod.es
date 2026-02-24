# OS Open USRN Dataset Lineage

## Role In The Graph
OS Open USRN defines canonical street identity (`USRN`) and street naming used as the final street key in outputs.

## Ingest Contract
- Source key: `os_open_usrn`
- Raw table: `raw.os_open_usrn_row`
- Stage output: `stage.streets_usrn_input`
- Primary pass usage: Pass `2_gb_canonical_streets`

## Stage Normalisation
- Core normalised fields:
  - `usrn`
  - `street_name`
  - `street_name_casefolded`
  - `street_type` / `street_status` metadata (when available; uppercased)
- Required mapping gate:
  - `source_schema.yaml` must resolve `usrn` before Pass 0b continues.
  - `street_name` is used when present; missing direct names are recovered in Pass 2 via Open Names + LIDS evidence.
  - metadata rows without direct street names are still staged by `usrn` for Pass 2 inferred-name propagation.

## Downstream Transformations
- Pass 2 writes `core.streets_usrn`.
- If direct USRN names are sparse, pass 2 infers missing USRN names from Open Names + LIDS TOID bridges.
- Passes 3/4/7 use `core.streets_usrn` for canonical name matching.

## Value Added
- Provides a stable street key for provenance and de-duplication.
- Anchors candidate evidence to canonical street names.

## Related Docs
- Pass 2 details: [`../stages/2_gb_canonical_streets.md`](../stages/2_gb_canonical_streets.md)
- Open Names linkage: [`os_open_names.md`](os_open_names.md)
- LIDS bridge: [`os_open_lids.md`](os_open_lids.md)
