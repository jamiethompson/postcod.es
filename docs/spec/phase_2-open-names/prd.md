# PRD: Phase 2 Open Names Augmentation

**Phase:** 2  
**Date:** February 20, 2026

## Goal

Phase 2 adds Open Names as a second signal to Phase 1 street inference and adds postcode-level street enumeration.

Deliverables:

1. Improve UPRN street assignment quality with two-source reconciliation.
2. Persist transparent provenance and disagreement signals.
3. Build deterministic postcode street listings for unit/sector/district/area.
4. Keep the build resumable and operationally auditable at national scale.

## Product Decisions (Locked)

- Open Names is mandatory in Phase 2 release creation.
- Ingest is explicit; `run phase2-open-names` does no ingest.
- Build is checkpointed and resumable.
- Warning acknowledgement is required for activation when disagreement rate is high.
- Voronoi clipping uses code constant `VORONOI_HULL_BUFFER_M` via SQL parameter binding.

## User Outcomes

Pipeline operators can:

- ingest four datasets explicitly
- create a release set deterministically
- run/repair builds with `--resume`
- inspect warnings before activation
- activate only with explicit acknowledgement when required

Downstream consumers can:

- read final street name + source-specific street names
- inspect corroboration/disagreement indicators
- query streets by postcode hierarchy level

## Core Behaviour

### UPRN reconciliation

For each UPRN with an Open Roads match:

- nearest Open Names in range is considered
- numbered-road labels can be replaced by Open Names names
- corroboration is recorded when normalized names agree
- disagreements are preserved, not auto-corrected

For UPRNs without Open Roads match:

- method remains unresolved (`none_within_radius`)
- Open Names distance/name fields are null

### Enumeration

`derived_postcode_street` is a single normalized table keyed by:

- `(postcode_level, postcode_value_norm, entry_id)`

Association methods:

- `district_direct`
- `spatial_voronoi`

Deterministic precedence:

- `district_direct` wins over `spatial_voronoi`

## Quality and Operations

Mandatory metrics include:

- Phase 1 coverage and distance metrics
- corroboration/replacement/disagreement metrics
- postcode-units with/without streets

Activation gate:

- if `disagreement_pct > 5%`, warning must be acknowledged

Determinism checks:

- canonical hashes stored for all Phase 2 core/derived objects
- same inputs must reproduce identical hashes

## Non-goals

- API work
- NI support
- LLM adjudication
- heavy multilingual equivalence logic

## References

- `docs/spec/phase_2-open-names/spec.md`
- `docs/spec/phase_2-open-names/voronoi_method.md`
- `docs/spec/phase_2-open-names/changes.md`
