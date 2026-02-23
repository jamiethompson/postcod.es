## 2026-02-20 — CHG-0001

What changed:
- Replaced nearest-road candidate ordering in Phase 1 derived build from GiST KNN (`<->`) to deterministic `ST_DWithin + ST_Distance + segment_id`.

Why it changed:
- Full-scale real dataset runs reproduced PostgreSQL/PostGIS runtime failure: `index returned tuples in wrong order` during KNN nearest-neighbor evaluation.
- Failure was reproduced in isolation against dedicated `iso_knn` tables and a single point fixture, and persisted after `REINDEX`.

Before behavior:
- `ORDER BY point_geom <-> road_geom, segment_id ASC` with no explicit `ST_DWithin` in the lateral nearest-road selector.

After behavior:
- `WHERE ST_DWithin(point_geom, road_geom, radius_m)`
- `ORDER BY ST_Distance(point_geom, road_geom) ASC, segment_id ASC`

Observed / expected metric impact:
- Expected semantic output: unchanged for rows with a nearest named segment within radius.
- Expected operational impact: slower runtime in dense urban areas due full distance ordering within radius candidates.

Determinism confirmation:
- Tie-break remains stable and explicit (`distance`, then `segment_id`).

Spec update confirmation:
- Updated `docs/spec/phase_1/spec.md` to reflect runtime query contract and dated change note.

## 2026-02-20 — CHG-0002

What changed:
- Added stage checkpoint persistence in `meta.release_set_stage_checkpoint`.
- Added resumable build mode: `pipeline run phase1 --resume`.

Why it changed:
- Full-scale builds can fail late in the process (for example, during derived spatial inference) after long-running successful stages.
- Restarting from zero is operationally expensive and slows deterministic troubleshooting.

Before behavior:
- `pipeline run phase1` behaved as a single-shot build with no persisted stage boundary checkpoints.
- Failure required a full rerun from the first build step.

After behavior:
- Successful stage completions are persisted per `release_set_id`.
- `pipeline run phase1 --resume` skips completed stages and continues from the first incomplete stage.
- Default non-resume run on a `created` release set performs a clean rebuild by dropping release tables and clearing checkpoints.
- `--resume` and `--rebuild` are mutually exclusive.

Observed / expected metric impact:
- No semantic change to output rows or metrics for a successful end-to-end run.
- Operational improvement: failure recovery time reduced by avoiding recomputation of completed stages.

Determinism confirmation:
- Stage checkpoints only skip previously completed deterministic stages for the same `release_set_id`.
- Rebuild path remains explicit via `--rebuild`.

Spec update confirmation:
- Updated `docs/spec/phase_1/spec.md` with resume CLI and checkpoint table contract.

## 2026-02-20 — CHG-0003

What changed:
- Refined resume checkpoints from coarse phase-level boundaries to table-level build boundaries.

Why it changed:
- Long-running builds need restart points between individual core/derived table builds, not only between aggregate phases.

Before behavior:
- Core build was checkpointed once after all core tables were built.
- Derived build was checkpointed once after derived phase completion.

After behavior:
- Checkpoints are now written after each table build:
  - `core_uprn_postcode_built`
  - `core_uprn_point_built`
  - `core_road_segment_built`
  - `derived_uprn_street_spatial_built`
- Existing legacy checkpoint names (`core_built`, `derived_built`) remain accepted by constraint for backward compatibility.

Observed / expected metric impact:
- No change to output semantics or metric values.
- Operational improvement: finer-grained resume points reduce rebuild time after late-stage failures.

Determinism confirmation:
- Each checkpoint still marks completion of deterministic SQL steps for one release set.
- Resume continues by skipping only completed table-level checkpoints.

Spec update confirmation:
- Updated `docs/spec/phase_1/spec.md` to lock table-level checkpoint behavior.
