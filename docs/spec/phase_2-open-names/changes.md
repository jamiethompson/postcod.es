## 2026-02-20 — CHG-0001

What changed:
- Locked Voronoi hull buffer as code-level constant:
  - `pipeline.config.VORONOI_HULL_BUFFER_M = 20000.0`
- Added explicit Voronoi SQL contract requiring bound parameter usage for `hull_buffer_m`.

Why it changed:
- Prevent silent drift in Voronoi clipping behavior that would invalidate deterministic outputs and canonical hashes.

Before behavior:
- Buffer value existed only in planning/docs context and could be inlined ad hoc in SQL.

After behavior:
- Buffer value is governed by a named constant in code.
- SQL contract uses parameter binding for buffer application.
- Governance requirements are explicit for any constant change.

Observed / expected metric impact:
- No immediate metric change at lock time.
- Future constant changes are expected to affect enumeration coverage and must be measured.

Determinism confirmation:
- Fixed constant + fixed seed set yields stable clipped geometry.
- Determinism validated through contract tests.

Spec update confirmation:
- Updated `/Users/jamie/code/postcod.es/docs/spec/phase_2-open-names/spec.md` and added `/Users/jamie/code/postcod.es/docs/spec/phase_2-open-names/voronoi_method.md`.

## 2026-02-20 — CHG-0002

What changed:
- Implemented full Phase 2 build flow:
  - `pipeline run phase2-open-names`
  - mandatory Open Names release on `pipeline release create`
  - checkpointed/resumable stage sequence through warnings and canonical hashes
- Added Phase 2 core/derived release-schema tables:
  - `core_open_names_entry`
  - `core_postcode_unit_seed`
  - Phase 2 shape of `derived_uprn_street_spatial`
  - `derived_postcode_street`
- Added activation warning gate with auditable acknowledgement (`--ack-warnings`).
- Added Phase 2 metrics and canonical hash set.
- Added Phase 2 E2E fixture scripts.

Why it changed:
- Move from Phase 1 baseline to two-source reconciliation and postcode street enumeration while retaining deterministic, resumable operations.

Before behavior:
- Build/runtime supported Phase 1 only.
- No Open Names build stages, no warnings gate, no enumeration output.

After behavior:
- Phase 2 pipeline runs end-to-end with explicit checkpoints and activation gating.

Observed / expected metric impact:
- New metrics expose corroboration, replacement, disagreement, and enumeration coverage.

Determinism confirmation:
- Canonical hashes extended to Phase 2 objects.
- Ordering rules for hash projections use deterministic keys / `COLLATE "C"` where text sorting is required.

Spec update confirmation:
- Updated:
  - `/Users/jamie/code/postcod.es/docs/spec/phase_2-open-names/spec.md`
  - `/Users/jamie/code/postcod.es/docs/spec/phase_2-open-names/prd.md`

## 2026-02-20 — CHG-0003

What changed:
- Replaced hard-fail duplicate-seed rule in `core_postcode_unit_seed` with deterministic representative seed derivation per postcode unit:
  - `AVG(postcode_unit_easting::numeric)`
  - `AVG(postcode_unit_northing::numeric)`
- Added diagnostics metrics:
  - `postcode_unit_seed_multi_coord_count`
  - `postcode_unit_seed_max_distinct_coords`

Why it changed:
- Real ONSUD release `ONSUD_NOV_2025` contains widespread multi-coordinate postcode units, so hard-failing duplicates blocks all national builds.

Before behavior:
- Build failed on first postcode unit with >1 distinct seed coordinate pair.

After behavior:
- Build derives one deterministic seed per postcode unit and proceeds.
- Multiplicity is measured and persisted as quality diagnostics.

Observed / expected metric impact:
- Build completion becomes possible on real national data.
- New metrics expose seed multiplicity scale for monitoring.

Determinism confirmation:
- Numeric-average aggregation is deterministic for fixed input rows.
- No row-order-dependent seed selection is used.

Spec update confirmation:
- Updated `/Users/jamie/code/postcod.es/docs/spec/phase_2-open-names/spec.md`.
