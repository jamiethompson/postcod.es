# Phase 1 `name_norm` Specification

Status: Locked for Phase 1.

`name_norm` exists for deterministic grouping and hashing within the Phase 1 pipeline.
It is intentionally minimal and must not include linguistic expansion rules.

## Rules (in order)

1. Convert to uppercase.
2. Trim leading and trailing whitespace.
3. Collapse all internal whitespace runs to a single space.
4. Remove these punctuation characters exactly:
   - `.`
   - `,`
   - `'`
   - `-`

## Explicitly Out of Scope

- Abbreviation expansion (for example `ST` -> `STREET`).
- Language-aware equivalence.
- Article removal.
- Any fuzzy matching.

If any of the above is required, it belongs to a separate Phase 2 normalization function.
