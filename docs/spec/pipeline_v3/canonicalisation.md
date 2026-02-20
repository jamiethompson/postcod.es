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

## JSON and Array Ordering

- `streets_json` is materialised with deterministic ordered aggregation.
- API projection source arrays are ordered lexicographically by:
  - `source_name`
  - `ingest_run_id`
  - `candidate_type`

## Timezone

All metadata timestamps are UTC.
