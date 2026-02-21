# Product Requirements Document

## Product Name
Open Data Street Inference Import Pipeline (MVP)

## Status
Draft, Engineering Build Phase 1

## Owner
Internal

## Purpose

Build a reproducible, versioned data pipeline that ingests three UK open datasets and produces a derived dataset mapping:

UPRN → postcode → inferred street name → confidence score

The pipeline must:

- Use only open-licence datasets
- Be deterministic and rebuildable
- Track provenance for every output
- Provide measurable quality metrics
- Operate independently of any serving layer

This PRD covers import, transformation and derived dataset production only.

## Problem Statement

The UK open data ecosystem provides:

- UPRN ↔ postcode mapping (ONSUD)
- UPRN ↔ coordinates (OS Open UPRN)
- Road geometry and names (OS Open Roads)

However:

- These datasets are separate
- They are not joined
- They are not normalised
- They are not spatially resolved to street level
- They are not versioned in a reproducible operational form

We require a structured import pipeline that:

- Normalises and joins these datasets
- Performs spatial nearest-road inference
- Produces a stable derived dataset
- Enables quality auditing before further enrichment layers

## Goals

### Primary Goals

- **G1. Deterministic rebuild**  
  Re-running the pipeline with the same dataset releases produces identical derived outputs.

- **G2. Version traceability**  
  Every derived row must reference exact dataset release identifiers.

- **G3. Spatial street inference**  
  For each UPRN with coordinates, assign nearest named road with a distance-based confidence score.

- **G4. Coverage metrics**  
  Quantify join coverage and resolution quality.

- **G5. Clean layered architecture**  
  Separate raw imports from core normalised tables and derived outputs.

## Non-goals

- No address enumeration
- No property number inference
- No PAF or proprietary dataset integration
- No PPD or EPC integration
- No API or serving layer
- No polygon boundary generation
- No LLM augmentation

This is a foundation layer only.

## Users

### Primary User
- Internal engineering team

### Secondary User
- Data quality auditor / product architect

This pipeline is not a public-facing product.

## Success Metrics

### Operational Metrics

- 100% reproducible rebuild using release identifiers
- 95%+ of UPRNs with coordinates resolve to a named road (subject to dataset coverage)
- Pipeline completes within acceptable runtime window (target: under 2 hours full rebuild on standard hardware)

### Data Quality Metrics

- Report UPRN total count
- Report UPRNs with coordinate coverage percentage
- Report UPRNs resolved to named road percentage
- Report P50, P90, P99 distance to nearest named road

## Data Sources

### ONSUD
Provides:
- UPRN ↔ postcode backbone

### OS Open UPRN
Provides:
- UPRN ↔ coordinates

### OS Open Roads
Provides:
- Named road geometries

All datasets must be ingested under Open Government Licence (OGL).

## Functional Requirements

### FR1. Dataset Registration

Each dataset import must register:

- dataset_key
- release_id
- source_url
- sha256
- retrieval timestamp
- licence

### FR2. Raw Data Storage

- Raw imports must be immutable
- Raw schema must preserve original fields
- No transformation in raw tables

### FR3. Core Table Construction

#### FR3.1 UPRN ↔ Postcode
Build `core.uprn_postcode`:

- uprn (PK)
- postcode_norm
- onsud_release_id

#### FR3.2 UPRN ↔ Coordinates
Build `core.uprn_point`:

- uprn (PK)
- geom (Point, SRID 4326)
- lat, lon, easting, northing
- open_uprn_release_id

#### FR3.3 Road Segments
Build `core.road_segment`:

- segment_id
- name_display
- name_norm
- geom
- open_roads_release_id

### FR4. Normalisation

#### Postcode
- Uppercase
- Remove spaces
- Remove non-alphanumeric characters

#### Street
- Uppercase
- Trim
- Collapse whitespace

### FR5. Street Inference

For each UPRN with coordinates:

1. Identify nearest named road segment using spatial KNN
2. Compute distance in metres
3. Apply threshold radius (default 150m)
4. Assign:
    - street_display
    - street_norm
    - distance_m
    - method = `open_roads_nearest`
    - confidence_score (distance-based banding)

### FR6. Confidence Scoring

Distance bands:

- <=15m → 0.70
- <=30m → 0.55
- <=60m → 0.40
- <=150m → 0.25
- >150m → 0.00

### FR7. Derived Output Table

`derived.uprn_street_spatial`:

- uprn (PK)
- postcode_norm
- street_display
- street_norm
- distance_m
- confidence_score
- method
- sources[]
- onsud_release_id
- open_uprn_release_id
- open_roads_release_id
- computed_at

### FR8. Metrics Generation

After each build, compute:

- Total UPRNs (ONSUD)
- UPRNs with coordinates
- Coordinate coverage percentage
- UPRNs resolved to named road
- Resolution percentage
- Distance percentiles

Store in `meta.dataset_metrics`.

### FR9. Bundle and Run Identifier Strategy

The pipeline must use:

- Human-readable deterministic IDs for release bundles (`bundle_id`)
- UUIDs for ingest runs (`ingest_run_id`)
- UUIDs for build runs (`build_run_id`)

Requirements:

- `bundle_id` must be generated from deterministic seeds plus explicit timestamps.
- `bundle_id` must include a date tag and short hash component.
- `bundle_id` must include adjective+noun tokens for readability.
- `bundle_id` uses a frozen noun list.
- `ingest_run_id` and `build_run_id` remain UUID values.

## Non-functional Requirements

### NFR1. Performance
- Use database-side spatial joins (PostGIS)
- Avoid loading full geometry datasets into Python memory

### NFR2. Determinism
- No random ordering
- Stable ordering in spatial joins
- `bundle_id` generation must not use implicit wall-clock defaults inside generator functions

### NFR3. Idempotency
- Re-running build for same release overwrites derived outputs safely

### NFR4. Observability
- Log row counts at every stage
- Fail fast on schema mismatch
- Track pipeline run status

### NFR5. Indexing
- GiST index on geometry columns
- B-tree on UPRN primary keys
- B-tree on postcode_norm

## Risks

- **R1. Spatial mismatch**  
  Industrial estates may produce higher distances.

- **R2. Unnamed roads**  
  Some UPRNs may not resolve to named roads.

- **R3. Dataset schema changes**  
  Must validate required columns at ingest.

- **R4. Runtime constraints**  
  Large spatial joins may require batching or tuning.

## Future Extensions (Out of Scope)

- PPD street signal layer
- EPC enrichment
- OLI feature classification
- LLM-assisted candidate ranking
- Postcode-level street dominance modelling

## Acceptance Criteria

- Pipeline successfully imports all three datasets
- Derived table populated with expected row counts
- Coverage and distance metrics recorded
- Full rebuild reproducible
- All outputs traceable to dataset release identifiers
