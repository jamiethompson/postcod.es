# Implementation Spec

Open Data Street Inference Import Pipeline (MVP)

Datasets: ONSUD + OS Open UPRN + OS Open Roads

Purpose: Build a reproducible, versioned import and transformation pipeline that produces a UPRN to inferred street mapping using only open datasets.

## 1. Objectives

Primary objective:
- Ingest 3 open datasets
- Normalise and join them
- Produce a deterministic, rebuildable derived table:
    - `UPRN -> postcode -> nearest named road -> confidence score`

Secondary objectives:
- Full dataset versioning and provenance
- Deterministic rebuilds
- Metrics collection for quality auditing
- No mutation of raw data after ingest

Non-goals:
- No address enumeration
- No PAF or AddressBase
- No PPD or EPC
- No serving layer

## 2. Technology Stack

Language:
- Python 3.11+

Database:
- PostgreSQL + PostGIS extension

Core libraries:
- psycopg (database)
- SQLAlchemy (optional)
- pandas (CSV handling)
- geopandas (if needed for geometry)
- shapely
- pyproj
- click or argparse (CLI)

Notes:
- Spatial processing must be delegated to PostGIS where possible
- Avoid loading large geometries into Python memory

## 3. Directory Structure

- `pipeline/`
    - `pyproject.toml`
    - `src/`
        - `cli.py`
        - `config.py`
        - `datasets/`
            - `onsud.py`
            - `open_uprn.py`
            - `open_roads.py`
        - `ingest/`
            - `raw_load.py`
        - `transform/`
            - `build_core.py`
            - `build_derived.py`
            - `metrics.py`
        - `util/`
            - `normalise.py`
            - `hashing.py`
    - `sql/`
        - `schema.sql`
        - `indexes.sql`
- `data/`
    - `raw/`
        - `onsud/`
        - `open_uprn/`
        - `open_roads/`

## 4. Dataset Specifications

## 4.1 ONSUD (ONS UPRN Directory)

Purpose:
- UPRN to postcode backbone

Required fields:
- UPRN
- Postcode (unit level)

Import requirements:
- Load full dataset
- Do not filter rows during raw ingest
- Preserve all original columns

Derived extraction (`core.uprn_postcode`):
- `uprn` (bigint primary key)
- `postcode_norm` (text)
- `onsud_release_id` (text)

## 4.2 OS Open UPRN

Purpose:
- UPRN to coordinates

Required fields:
- UPRN
- Easting
- Northing
- Latitude
- Longitude

Derived extraction (`core.uprn_point`):
- `uprn` (bigint primary key)
- `geom` (Point, SRID 4326)
- `lat`
- `lon`
- `easting`
- `northing`
- `open_uprn_release_id`

## 4.3 OS Open Roads

Purpose:
- Named road geometries for nearest-neighbour inference

Required:
- Geometry (LineString or MultiLineString)
- Road name field

Derived extraction (`core.road_segment`):
- `segment_id` (bigserial primary key)
- `name_display` (text nullable)
- `name_norm` (text nullable)
- `geom` (geometry)
- `open_roads_release_id`

## 5. Versioning and Provenance

Table: `meta.dataset_release`

Fields:
- `dataset_key` (`onsud|open_uprn|open_roads`)
- `release_id`
- `source_url`
- `sha256`
- `retrieved_at`
- `licence`
- `file_path`

Table: `meta.pipeline_run`

Fields:
- `run_id` (uuid)
- `started_at`
- `finished_at`
- `status`
- `release_map` (json)
- `log_path`

Rules:
- Every derived build references exact `release_id` values
- Raw data is immutable
- Rebuilds must be deterministic

## 6. Normalisation Rules

`postcode_norm`:
- Uppercase
- Remove spaces
- Remove non-alphanumeric

`street_norm`:
- Uppercase
- Trim
- Collapse whitespace
- Preserve original name in `street_display`

`UPRN`:
- Cast to bigint after validation

## 7. Ingest Workflow

Step 1: Register dataset release
- Compute SHA256 of archive
- Insert into `meta.dataset_release`

Step 2: Load raw table
- Use `COPY` for CSV
- Use `ogr2ogr` or PostGIS loader for shapefiles
- Record row counts

Step 3: Build core tables
- Extract required fields
- Apply normalisation
- Create indexes

Step 4: Validate joins
- Count UPRNs in ONSUD
- Count matching UPRNs in Open UPRN
- Report coverage percentage

## 8. Street Inference Algorithm (Phase 1)

Goal:
- Assign nearest named road to each UPRN

Process:
1. Join `core.uprn_postcode` with `core.uprn_point`
2. For each UPRN with coordinates:
    - Use PostGIS KNN operator (`<->`) to find nearest road segment
    - Filter to segments with non-null `name_display`
    - Compute `ST_Distance` in metres
3. Apply search radius threshold (default `150m`)
4. Assign:
    - `street_display`
    - `street_norm`
    - `distance_m`
    - `method = 'open_roads_nearest'`
    - `confidence_score` (distance-based banding)
5. Insert into `derived.uprn_street_spatial`

Confidence bands:
- `<= 15m` -> `0.70`
- `<= 30m` -> `0.55`
- `<= 60m` -> `0.40`
- `<= 150m` -> `0.25`
- `> 150m` -> `0.00`

If no named road within radius:
- `street_display = NULL`
- `confidence_score = 0.00`
- `method = 'none_within_radius'`

## 9. Metrics Collection

Compute after build:
- Total UPRNs (ONSUD)
- UPRNs with coordinates
- Coordinate coverage percentage
- UPRNs resolved to named road
- Resolution percentage
- Distance percentiles (P50, P90, P99)

Insert into `meta.dataset_metrics`

## 10. CLI Contract

- `pipeline ingest onsud --release-id <id> --file <path>`
- `pipeline ingest open-uprn --release-id <id> --file <path>`
- `pipeline ingest open-roads --release-id <id> --file <path>`

- `pipeline build core --onsud <id> --open-uprn <id> --open-roads <id>`

- `pipeline build derived street-spatial --onsud <id> --open-uprn <id> --open-roads <id>`

- `pipeline metrics compute --onsud <id> --open-uprn <id> --open-roads <id>`

## 11. Acceptance Criteria

- Rebuild produces identical row counts and deterministic output
- 95%+ of UPRNs with coords resolve to some named road (subject to dataset reality)
- All tables indexed appropriately
- No raw data mutation after import
- All outputs traceable to `release_id` values