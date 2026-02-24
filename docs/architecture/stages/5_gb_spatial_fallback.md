# Pass 5: GB Spatial Fallback

## Purpose
Add low-confidence fallback candidates for postcodes lacking high-confidence evidence.

## Inputs
- `stage.open_roads_segment`
- optional `stage.ppd_parsed_address` (tie-break only)
- `core.postcodes`
- existing candidates

## Outputs
- `derived.postcode_street_candidates` rows (`candidate_type=spatial_os_open_roads`, `confidence=low`)

## Ranking Policy
- radius remains `150m` for spatial candidate generation (EPSG:27700 distance)
- confidence remains `low`
- deterministic ranking order:
  1. postal-name suitability (`postal_plausible` over `unknown` over `road_number`)
  2. optional PPD street-name match score (tie-break only)
  3. distance ascending
  4. segment id ascending (`COLLATE "C"`)
- if any `postal_plausible` candidate exists for a postcode, `road_number` candidates are excluded
- road-number-only wins are allowed only as last-resort fallback and are explicitly flagged in evidence JSON
- PPD never creates candidates; it only ranks candidates already found via spatial matching

## Evidence Fields
- `segment_id`
- `distance_m`
- `name_quality_class`
- `name_quality_reason`
- `fallback_policy`
- `ppd_match_score`
- `ppd_matched_street`
- `tie_break_basis`
- `road_number_only_output`

## QA Counters
- `pass5_candidates_postal_plausible`
- `pass5_candidates_road_number`
- `pass5_road_number_only_wins`
- `pass5_ppd_tie_break_applied_count`
- `pass5_ppd_match_exact_count`
- `pass5_ppd_match_partial_count`
- `pass5_ppd_match_none_count`

## Value Added
- coverage recovery with explicit low-confidence tagging

## Related
- Dataset: [`../datasets/os_open_roads.md`](../datasets/os_open_roads.md)
