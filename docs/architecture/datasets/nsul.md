# NSUL Dataset Lineage

## Role In The Graph
NSUL provides UPRN->postcode relationships used with LIDS UPRN->USRN links to generate high-confidence street evidence.

## Ingest Contract
- Source key: `nsul`
- Raw table: `raw.nsul_row`
- Stage table: `stage.nsul_uprn_postcode`

## Stage Normalisation
- Normalised fields:
  - `uprn`
  - `postcode_norm`

## Downstream Transformations
- Pass 4 joins NSUL and LIDS on UPRN, then aggregates postcode/USRN pairs.
- Output candidate type: `uprn_usrn` (high confidence).

## Value Added
- Adds postcode side of the UPRN linkage chain.
- Enables frequency-like reinforcement based on property counts.

## Related Docs
- Pass 4 details: [`../stages/4_uprn_reinforcement.md`](../stages/4_uprn_reinforcement.md)
- Open UPRN context: [`os_open_uprn.md`](os_open_uprn.md)
- LIDS context: [`os_open_lids.md`](os_open_lids.md)
