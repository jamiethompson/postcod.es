# PPD Dataset Lineage (Optional)

## Role In The Graph
PPD is a gap-fill source for lower-confidence address-derived street evidence.

## Ingest Contract
- Source key: `ppd`
- Raw table: `raw.ppd_row`
- Stage table: `stage.ppd_parsed_address`
- Bundle rule: may include multiple ingest runs (baseline + updates), applied in deterministic ingest-time order.

## Stage Normalisation
- Normalised fields:
  - `row_hash`
  - `postcode_norm`
  - `house_number`
  - `street_token_raw`, `street_token_casefolded`

## Downstream Transformations
- Pass 7 performs token matching against canonical streets.
- Generates `ppd_parse_matched` or `ppd_parse_unmatched` candidate types.
- Used as additive gap-fill only; does not override stronger evidence.

## Value Added
- Improves coverage where core spatial joins have sparse evidence.
- Preserves confidence transparency through explicit low/none-like candidate typing.

## Related Docs
- Pass 7 details: [`../stages/7_ppd_gap_fill.md`](../stages/7_ppd_gap_fill.md)
- PPD baseline/update rule: [`../../spec/pipeline_v3/spec.md`](../../spec/pipeline_v3/spec.md)
