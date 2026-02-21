# Pass 5: GB Spatial Fallback

## Purpose
Add low-confidence fallback candidates for postcodes lacking high-confidence evidence.

## Inputs
- `stage.open_roads_segment`
- `core.postcodes`
- existing candidates

## Outputs
- `derived.postcode_street_candidates` rows (`candidate_type=spatial_os_open_roads`, `confidence=low`)

## Value Added
- coverage recovery with explicit low-confidence tagging

## Related
- Dataset: [`../datasets/os_open_roads.md`](../datasets/os_open_roads.md)
