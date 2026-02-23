# Voronoi Method (Phase 2)

This document locks the Voronoi clipping contract used by postcode street enumeration.

## Locked Constant

- `pipeline.config.VORONOI_HULL_BUFFER_M = 20000.0`

This value is behavior-defining and hash-impacting. It must not be changed silently.

## SQL Contract

Voronoi clipping uses PostGIS-native operations with a bound parameter:

- `ST_ConvexHull(ST_Collect(seed_geom_bng))`
- `ST_Buffer(..., %(hull_buffer_m)s)`
- `ST_VoronoiPolygons(..., (SELECT gb_clip_geom ...))`

`hull_buffer_m` is bound at runtime. The buffer must not be inlined as a SQL literal.

## Governance

Any change to `VORONOI_HULL_BUFFER_M` requires all of the following before implementation:

1. Prior entry in `docs/spec/phase_2-open-names/changes.md`
2. Canonical hash re-baseline for affected objects
3. Before/after metric comparison for enumeration coverage

## Determinism Notes

- Seed inputs are ordered deterministically.
- Voronoi clipping geometry is deterministic for a fixed seed set and `VORONOI_HULL_BUFFER_M`.
- Runtime ties in downstream spatial joins must have explicit tie-breakers.
