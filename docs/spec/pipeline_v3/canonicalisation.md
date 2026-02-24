# Pipeline V3 Canonicalisation and Determinism Rules

## Postcode Normalisation

1. Uppercase.
2. Remove non-alphanumeric characters.
3. Require minimum structure for UK postcode canonical form.
4. Store display form with single space before final three characters.

## Street Name Normalisation

1. Trim whitespace.
2. Unicode NFKC normalisation.
3. Uppercase canonical form.
4. Strip configured punctuation.
5. Collapse internal whitespace.
6. Apply configured token aliases deterministically.

## Pass 5 Name-Quality Classifier

Open Roads fallback names are normalised before quality classing:
1. Trim whitespace.
2. Uppercase.
3. Collapse internal whitespace.

Classifier outputs:
- `postal_plausible`
- `road_number`
- `unknown`

Road-number detection uses deterministic UK route-label regex matching (for example: `A390`, `B1249`, `M1`, `A1(M)`, `A 390`).

## Open Names URI/Type Extraction

For postcode place/admin enrichment fields:
- trim whitespace first; blank becomes null
- URI identifier fields (`*_toid`) use the last URI path segment
- type fields use fragment token after `#` when present, otherwise last URI path segment
- preserve token case from source extraction
- do not prefix extracted identifiers (no synthetic `osgb` prefix)

## Null and Empty Handling

- Empty strings map to null.
- Null and empty-string duplicates are forbidden semantically.

## Probability and Rounding

- Base probability uses exact formula from the main spec.
- Store to fixed precision `numeric(6,4)`.
- Apply residual correction to deterministic rank 1 row per postcode.

## Deterministic Ranking Keys

Probability ranking (descending) uses:
1. unrounded probability desc
2. confidence rank desc (`high` > `medium` > `low` > `none`)
3. canonical street name `COLLATE "C"` asc
4. USRN asc (nulls last)

Pass-5 candidate ranking (ascending) uses:
1. name-quality rank (`postal_plausible` > `unknown` > `road_number`)
2. PPD match score desc (when enabled)
3. distance asc (nulls last)
4. segment id `COLLATE "C"` asc

## JSON and Array Ordering

- `streets_json` is materialised with deterministic ordered aggregation.
- API projection source arrays are ordered lexicographically by:
  - `source_name`
  - `ingest_run_id`
  - `candidate_type`

## Timezone

All metadata timestamps are UTC.

## Cross-links
- Pass finalisation behavior: [`../../architecture/stages/8_finalisation.md`](../../architecture/stages/8_finalisation.md)
- Value-added summary: [`../../architecture/value-added-by-stage.md`](../../architecture/value-added-by-stage.md)
