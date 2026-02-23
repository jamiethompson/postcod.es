# Pass 6: NI Candidates (Profile-Dependent)

## Purpose
Generate NI-specific candidate types when NI sources are present.

## Inputs
- `stage.osni_street_point`
- `stage.dfi_road_segment`
- `core.postcodes`

## Outputs
- `osni_gazetteer_direct` candidates
- `spatial_dfi_highway` candidates

## Value Added
- extends NI coverage under explicit confidence constraints

## Related
- Datasets: [`../datasets/osni_gazetteer.md`](../datasets/osni_gazetteer.md), [`../datasets/dfi_highway.md`](../datasets/dfi_highway.md)
