# OS Open UPRN Dataset Lineage

## Role In The Graph
Open UPRN contributes property-level identity used with NSUL and LIDS to create high-confidence postcode/USRN evidence.

## Ingest Contract
- Source key: `os_open_uprn`
- Raw table: `raw.os_open_uprn_row`
- Stage table: `stage.uprn_point`
- Primary pass usage: indirect, via pass `4_uprn_reinforcement`

## Stage Normalisation
- Normalised fields:
  - `uprn`
  - optional `postcode_norm`

## Downstream Transformations
- Combined with:
  - `stage.nsul_uprn_postcode` (UPRN->postcode)
  - `stage.open_lids_uprn_usrn` (UPRN->USRN)
- Pass 4 aggregates evidence into `uprn_usrn` high-confidence candidates.

## Value Added
- Supports strongest GB candidate type by linking property-level and street-level identifiers.

## Related Docs
- Pass 4 details: [`../stages/4_uprn_reinforcement.md`](../stages/4_uprn_reinforcement.md)
- NSUL linkage: [`nsul.md`](nsul.md)
- LIDS linkage: [`os_open_lids.md`](os_open_lids.md)
