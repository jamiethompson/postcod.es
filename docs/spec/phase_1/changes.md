# Phase 1 Behaviour Changes

This file records behavioural deviations from the original Phase 1 specification.

---

## Date
2026-02-21

## Change ID
PH1-ID-001

## What changed

- Added human-readable deterministic `bundle_id` generation.
- Kept `ingest_run_id` and `build_run_id` as UUID values.
- Added formal ID generation contracts in `spec.md` Sections 5.1 and 5.2.
- Added frozen vocabulary requirements:
  - `ADJECTIVES_64`
  - `BUNDLE_NOUNS_256`

## Why it changed

Operators need IDs that can be read at a glance while preserving deterministic traceability and reproducibility guarantees.

## Before behaviour

- Pipeline run IDs were specified as UUIDs (`meta.pipeline_run.run_id`).
- No formal bundle ID format was defined.
- No readable token structure existed for run identifiers.

## After behaviour

- Bundle IDs follow `v<yyyymm>_<adjective>_<bundle_noun>_<hash6>`.
- Ingest run IDs are UUIDv4.
- Build run IDs are UUIDv4.
- `bundle_id` derives from explicit deterministic inputs plus explicit timestamp.

## Observed or expected metric impact

- No expected change to spatial inference accuracy metrics (coverage, resolution, distance percentiles).
- Improved operational observability and trace readability in logs and metadata tables.

## Determinism confirmation

- `bundle_id` output is deterministic for a given `(seed, timestamp)` pair.
- Ingest/build run identifiers are intentionally non-deterministic UUIDs.
- Bundle ID generators must not create implicit timestamps internally.

## `spec.md` updated

Yes. `docs/spec/phase_1/spec.md` updated with Sections 5.1 and 5.2 plus revised metadata table definitions.
