# Pass 7: PPD Gap Fill (Profile-Dependent)

## Purpose
Add lower-confidence candidates from parsed transactional/self-reported addresses.

## Inputs
- `stage.ppd_parsed_address`
- `core.streets_usrn`

## Outputs
- `derived.postcode_street_candidates` (`ppd_parse_matched` / `ppd_parse_unmatched`)
- `internal.unit_index`

## Value Added
- gap filling without overriding stronger spatial evidence
- expanded coverage with transparent provenance tags

## Related
- Dataset: [`../datasets/ppd.md`](../datasets/ppd.md)
