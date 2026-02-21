# OS Open LIDS Dataset Lineage

## Role In The Graph
LIDS is the identifier bridge dataset. It resolves relationships between TOID/UPRN and USRN.

## Ingest Contract
- Source key: `os_open_lids`
- Raw table: `raw.os_open_lids_row`
- Stage tables:
  - `stage.open_lids_pair` (`id_1`, `id_2`, `relation_type`)
  - `stage.open_lids_toid_usrn`
  - `stage.open_lids_uprn_usrn`

## Stage Normalisation
- Generic identifier pairs are normalised first:
  - `id_1`, `id_2`, `relation_type`
- Relation typing is explicit (`toid_usrn` or `uprn_usrn`) after deterministic inference.
- Typed rows are materialised into dedicated stage tables for downstream joins.

## Downstream Transformations
- Pass 2: helps infer missing canonical USRN names from Open Names TOIDs.
- Pass 3: confirms TOID-based Open Names evidence, generating `open_lids_toid_usrn` candidates.
- Pass 4: contributes UPRN->USRN links for high-confidence `uprn_usrn` candidates.

## Value Added
- Supplies the key bridge between feature identifiers and canonical street identifiers.
- Converts generic identifier pairs into explicit typed relationships for deterministic joins.

## Related Docs
- Pass 0b staging details: [`../stages/0b_stage_normalisation.md`](../stages/0b_stage_normalisation.md)
- Pass 3 details: [`../stages/3_open_names_candidates.md`](../stages/3_open_names_candidates.md)
- Pass 4 details: [`../stages/4_uprn_reinforcement.md`](../stages/4_uprn_reinforcement.md)
