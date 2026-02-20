# PRD: OS Open Names Integration

**Status:** Proposed  
**Phase:** 1.5 — post Phase 1 pipeline stable  
**Author:** Jamie  
**Date:** February 2026

---

## Problem

The Phase 1 street inference pipeline assigns a street name to every UPRN by finding the nearest named road segment in OS Open Roads within a 150m radius. This approach works well but has two known failure modes that Open Names can directly address.

**Failure mode 1 — road numbers instead of street names.**  
OS Open Roads sometimes labels road segments with their road number rather than their colloquial name. A UPRN on the A40 through Acton receives `A40` as its street name from Open Roads. The street residents know is `Western Avenue`. This produces technically correct but practically useless street names for any UPRN on a numbered road.

**Failure mode 2 — unverified single-source inference.**  
The current pipeline has one signal per UPRN. There is no way to distinguish a confident correct inference from a confident wrong inference — the confidence score reflects distance to road geometry only, not whether the name is actually right. A UPRN 12m from a road segment gets a 0.70 confidence score whether or not that road is the street the UPRN is addressed to.

Open Names is a purpose-built street and place gazetteer. It carries street names as primary facts rather than geometry labels. Adding it as a second signal addresses both problems.

---

## Goals

1. Detect and replace road number labels (`A40`, `B1234`) with colloquial street names from Open Names where available.
2. Introduce a corroboration signal — a boolean indicating that Open Roads and Open Names agree on the street name for a given UPRN.
3. Store both source names in the pipeline output so the resolution method is transparent and auditable.
4. Do not degrade Phase 1 street inference results. Open Names is augmentation, not replacement. Where Open Names has no entry, Phase 1 behaviour is unchanged.
5. Remain entirely on OGL data. No new licences.

---

## Non-goals

- Replacing the spatial KNN inference approach. Open Roads geometry remains the primary mechanism.
- Full address completion or delivery-point resolution. This is still street-level inference.
- Northern Ireland. Deferred pending CRS confirmation gate, same as all NI work.
- Abbreviation expansion or cross-source string equivalence matching. That is Phase 2 `street_equivalence_norm` work, not this.

---

## Data source

**OS Open Names**  
Publisher: Ordnance Survey  
Licence: OGL v3.0  
Format: CSV (zipped) or GeoPackage  
Coverage: Great Britain  
Update frequency: Approximately six-monthly  
URL: `osdatahub.os.uk/downloads/open/OpenNames`

Relevant fields:

| Field | Description |
|-------|-------------|
| `ID` | Unique identifier for the named place entry |
| `NAMES_URI` | URI identifier |
| `NAME1` | Primary name |
| `NAME1_LANG` | Language of NAME1 (blank = English, `wel` = Welsh) |
| `NAME2` | Secondary name (e.g. Welsh form where NAME1 is English) |
| `TYPE` | Top-level type: `transportNetwork`, `populatedPlace`, etc. |
| `LOCAL_TYPE` | Specific type: `Road`, `Named Road`, `Numbered Road`, `Street`, etc. |
| `GEOMETRY_X` | Easting (BNG, SRID 27700) |
| `GEOMETRY_Y` | Northing (BNG, SRID 27700) |
| `POSTCODE_DISTRICT` | Postcode district associated with this entry |
| `POPULATED_PLACE` | Associated settlement name |
| `DISTRICT_BOROUGH` | Administrative district |
| `COUNTY_UNITARY` | County or unitary authority |

For street inference, filter to entries where `LOCAL_TYPE` IN (`Road`, `Named Road`, `Street`). Exclude `Numbered Road` entries — these are the road number labels you are trying to replace, not the names you want.

---

## Schema changes

### New ingest table

Add to the manifest-driven ingest system. Open Names is a first-class dataset with its own `dataset_key`, release tracking, SHA256 verification, and `meta.dataset_release` row.

```sql
-- raw.open_names_row
CREATE TABLE raw.open_names_row (
    id              bigserial PRIMARY KEY,
    dataset_key     text            NOT NULL,
    release_id      text            NOT NULL,
    source_row_num  bigint          NOT NULL,
    entry_id        text,
    name1           text,
    name1_lang      text,
    name2           text,
    type            text,
    local_type      text,
    geometry_x      double precision,
    geometry_y      double precision,
    postcode_district text,
    populated_place text,
    extras_jsonb    jsonb,
    FOREIGN KEY (dataset_key, release_id)
        REFERENCES meta.dataset_release (dataset_key, release_id)
);
```

### New core table

Add `open_names_entry` to the versioned physical schema per release set, alongside `uprn_postcode`, `uprn_point`, and `road_segment`.

```sql
-- rs_<id>.open_names_entry
CREATE TABLE open_names_entry (
    id                  bigserial PRIMARY KEY,
    entry_id            text            NOT NULL,
    name_display        text            NOT NULL,
    name_norm           text            NOT NULL,
    name2_display       text,
    name2_norm          text,
    local_type          text            NOT NULL,
    geom_bng            geometry(Point, 27700) NOT NULL,
    postcode_district   text,
    populated_place     text
);

CREATE INDEX ON open_names_entry USING GIST (geom_bng);
CREATE INDEX ON open_names_entry (name_norm);
CREATE INDEX ON open_names_entry (postcode_district);
```

Only rows where `local_type IN ('Road', 'Named Road', 'Street')` are loaded into this table. All other Open Names entry types are discarded at load time. This is enforced in the loader, not via a view filter.

### Changes to `uprn_street_spatial`

The following fields are added. All are nullable — absence means Open Names had no entry for this UPRN.

```sql
-- New columns on rs_<id>.uprn_street_spatial

street_open_roads       text,       -- raw name from Open Roads segment (was: street_display)
street_open_names       text,       -- raw name from nearest Open Names entry (nullable)
street_display          text,       -- final resolved display name
street_norm             text,       -- name_norm applied to street_display
name_source             text,       -- 'open_roads' | 'open_names' | 'corroborated'
corroborated            boolean,    -- true if Open Roads and Open Names agree
open_names_distance_m   double precision  -- distance from UPRN to Open Names entry point (nullable)
```

`street_display` and `street_norm` remain the stable consumer-facing fields. The resolution method is transparent via `name_source` and both raw source values are preserved.

### Release set manifest

`meta.release_set` gains a fourth release ID column:

```sql
ALTER TABLE meta.release_set
    ADD COLUMN open_names_release_id text REFERENCES meta.dataset_release(release_id);
```

The unique constraint is updated:

```sql
ALTER TABLE meta.release_set
    DROP CONSTRAINT uq_release_set_inputs;

ALTER TABLE meta.release_set
    ADD CONSTRAINT uq_release_set_inputs
    UNIQUE (onsud_release_id, open_uprn_release_id, open_roads_release_id, open_names_release_id);
```

Open Names is optional for backwards compatibility with Phase 1 release sets — the column is nullable. A release set with `open_names_release_id IS NULL` behaves exactly as Phase 1 and produces `street_open_names = NULL`, `name_source = 'open_roads'`, `corroborated = false` throughout.

---

## CLI changes

New ingest command:

```
pipeline ingest open-names --manifest <path>
```

Follows the same pattern as existing ingest commands. Manifest schema is identical.

`pipeline release-set create` gains an optional argument:

```
pipeline release-set create \
    --onsud-release <id> \
    --open-uprn-release <id> \
    --open-roads-release <id> \
    --open-names-release <id>    # optional
```

`pipeline build derived street-spatial` is updated to use Open Names if the release set includes it. If `open_names_release_id` is null, behaviour is identical to Phase 1.

`pipeline run phase1` is unchanged. Open Names augmentation runs as part of the spatial inference stage when present — it is not a separate pipeline stage.

---

## Resolution logic

The reconciliation logic runs per UPRN after the Open Roads KNN join. It is deterministic and documented here as the executable specification.

### Step 1 — Open Roads result

Proceed as Phase 1. For each UPRN, find the nearest named road segment in Open Roads within 150m. Assign `street_open_roads` and `confidence_score` per existing confidence bands. If no road within 150m, set `method = 'none_within_radius'` and skip to output with no Open Names lookup.

### Step 2 — Open Names lookup

For each UPRN that has an Open Roads result, find the nearest `open_names_entry` within a search radius.

Search radius for Open Names lookup: **200m**. This is wider than the Open Roads radius because Open Names representative points are centroids of named streets, not road edge geometry. A street centroid may be further from a UPRN at the end of the street than the road segment edge is.

```sql
SELECT
    entry_id,
    name_display,
    name_norm,
    ST_Distance(geom_bng, $uprn_geom_bng) AS distance_m
FROM open_names_entry
WHERE ST_DWithin(geom_bng, $uprn_geom_bng, 200)
ORDER BY geom_bng <-> $uprn_geom_bng, entry_id
LIMIT 1;
```

If no Open Names entry within 200m, set `street_open_names = NULL` and proceed to output with `name_source = 'open_roads'`.

### Step 3 — Numbered road detection

Check whether the Open Roads name matches the numbered road pattern:

```python
import re
NUMBERED_ROAD = re.compile(r'^[AaBbMm]\d+', re.IGNORECASE)

is_numbered = bool(NUMBERED_ROAD.match(street_open_roads_norm))
```

### Step 4 — Reconciliation

```
IF street_open_names IS NULL:
    street_display = street_open_roads
    name_source    = 'open_roads'
    corroborated   = false

ELSE IF is_numbered AND street_open_names IS NOT NULL:
    street_display = street_open_names
    name_source    = 'open_names'
    corroborated   = false
    -- numbered road replaced by Open Names name

ELSE IF name_norm(street_open_roads) == name_norm(street_open_names):
    street_display = street_open_roads  -- prefer Open Roads form for display
    name_source    = 'corroborated'
    corroborated   = true

ELSE:
    -- genuine disagreement between named sources
    street_display = street_open_roads  -- Open Roads is primary
    name_source    = 'open_roads'
    corroborated   = false
    -- disagreement is visible via street_open_names != NULL and corroborated = false
```

Genuine disagreements are not resolved automatically. They are preserved in the output and flagged for the LLM review pipeline (Phase 2). The consumer sees the Open Roads name. The disagreement is visible and auditable.

### Step 5 — Output

Apply `name_norm` to `street_display` to produce `street_norm`. Populate all fields. `computed_at` timestamp as per Phase 1.

---

## Confidence model

The distance-based confidence score from Phase 1 is unchanged. Open Names corroboration is expressed separately as a boolean, not folded into the numeric score. This keeps the two signals independent and interpretable.

A consumer who wants to weight corroborated results more highly can do so in their own logic. The API will expose `corroborated` as a field on the street enrichment response.

The only confidence score modification: if `name_source = 'open_names'` (numbered road replacement), the confidence score is set to the Open Names distance band rather than the Open Roads distance band:

```
open_names_distance_m ≤ 50m  → 0.70
open_names_distance_m ≤ 100m → 0.55
open_names_distance_m ≤ 200m → 0.40
```

These bands are wider than Open Roads bands because Open Names point geometry is a centroid, not an edge. This is documented in the confidence model specification.

---

## New metrics

Add to `meta.dataset_metrics` for any release set that includes Open Names:

| Metric key | Description |
|------------|-------------|
| `open_names_entries_loaded` | Total Open Names entries loaded into `open_names_entry` |
| `uprns_corroborated` | UPRNs where Open Roads and Open Names agree |
| `corroboration_pct` | `uprns_corroborated` / `uprns_resolved_named_road` × 100 |
| `uprns_numbered_road_replaced` | UPRNs where Open Roads number was replaced by Open Names name |
| `numbered_road_replacement_pct` | `uprns_numbered_road_replaced` / total resolved UPRNs × 100 |
| `uprns_genuine_disagreement` | UPRNs where both sources present but names differ |
| `disagreement_pct` | `uprns_genuine_disagreement` / total resolved UPRNs × 100 |

The `disagreement_pct` metric is the key quality signal for this feature. A high disagreement rate indicates either a data quality issue or a flaw in the reconciliation logic and should trigger investigation before the release set is activated.

---

## Gate criteria

| Gate | Pass condition |
|------|---------------|
| Open Names ingest | `ogrinfo`-reported feature count for selected entries equals loaded `open_names_entry` count. Both counts recorded. |
| Open Names content | `open_names_entry` contains only `local_type IN ('Road', 'Named Road', 'Street')` rows. Verify with count query. |
| Spatial inference | All existing Phase 1 gate criteria pass unchanged. |
| New metrics | All new metric keys present in `meta.dataset_metrics` for release sets with Open Names. |
| Disagreement rate | `disagreement_pct` logged. If > 5%, emit a warning and require explicit confirmation before activation. Not a hard block but must be acknowledged. |
| Backwards compatibility | A release set with `open_names_release_id = NULL` produces output identical to Phase 1 canonical hashes. Verified by test. |

---

## Test cases

**Numbered road replacement:**  
UPRN fixture with nearest Open Roads segment = `A40`. Open Names entry `Western Avenue` within 200m. Expected output: `street_display = 'Western Avenue'`, `name_source = 'open_names'`, `corroborated = false`.

**Corroboration:**  
UPRN fixture with Open Roads = `HIGH STREET` and Open Names nearest entry = `High Street`. After `name_norm` both resolve to `HIGH STREET`. Expected: `corroborated = true`, `name_source = 'corroborated'`, `street_display = 'HIGH STREET'`.

**Genuine disagreement:**  
UPRN fixture where Open Roads = `BACK LANE` and Open Names nearest entry = `STATION ROAD`. Neither is a numbered road. Expected: `street_display = 'BACK LANE'`, `name_source = 'open_roads'`, `corroborated = false`, `street_open_names = 'Station Road'`.

**No Open Names entry:**  
UPRN fixture where no Open Names entry within 200m. Expected: `street_open_names = NULL`, `name_source = 'open_roads'`, `corroborated = false`. Confidence score unchanged from Phase 1.

**No Open Roads result:**  
UPRN fixture with `method = 'none_within_radius'`. Open Names lookup does not run. All Open Names fields NULL.

**Backwards compatibility:**  
Release set with `open_names_release_id = NULL`. Full pipeline run. Canonical hashes must match equivalent Phase 1 release set hashes exactly.

**Welsh name:**  
UPRN fixture in a Welsh postcode where Open Names entry has `NAME1_LANG = 'wel'`. Verify `name_display` preserves Welsh form. Verify `name_norm` applies the same rules as English names (uppercase, trim, collapse whitespace, explicit punctuation strip). No translation or substitution.

---

## Documentation deliverables

- `/Users/jamie/code/postcod.es/docs/spec/phase_1/open_names.md` — this PRD, updated to reflect final implementation decisions
- `/Users/jamie/code/postcod.es/docs/spec/phase_1/name_norm.md` — updated to confirm `name_norm` rules apply identically to Open Names and Open Roads names
- `/Users/jamie/code/postcod.es/docs/spec/phase_1/confidence_model.md` — updated to document Open Names distance bands and the numbered road replacement case
- `/Users/jamie/code/postcod.es/docs/spec/data_sources.md` — Open Names added as a data source with licence, URL, update frequency, and field mapping

---

## Out of scope

- `street_equivalence_norm` for cross-source matching. Phase 2.
- LLM-assisted review of genuine disagreements. Phase 2.
- Welsh/English name equivalence resolution. Phase 2.
- Any use of Open Names `populatedPlace`, `districtBorough`, or `countyUnitary` fields for administrative geography. ONSUD already provides this more reliably.
- Open Names entries with `LOCAL_TYPE` other than `Road`, `Named Road`, `Street`. Locality names, settlements, and water features are out of scope for street inference.

---

## Open questions

**Search radius for Open Names lookup.** 200m is proposed based on the characteristic that Open Names points are street centroids rather than road edges. This may need tuning after first data run. The metric `open_names_distance_m` is stored precisely so the radius can be evaluated empirically and adjusted in a subsequent release.

**Multiple Open Names entries within radius.** The spec takes the single nearest entry. It is possible that two named streets are equidistant from a UPRN and the nearest Open Names entry is the wrong one. For Phase 1.5 this is acceptable — the tie-breaking rule (distance then `entry_id`) is deterministic. If the disagreement rate metric surfaces this as a significant problem, a future release can introduce a candidate set approach.

**Open Names update frequency.** OS publishes Open Names approximately every six months. This is less frequent than the Open Roads update cadence. A release set that updates Open Roads without updating Open Names will use a stale Open Names release. This is valid — the pipeline explicitly tracks which Open Names release is in each release set — but it means corroboration rates may drift between releases if the underlying data diverges. Monitor via `disagreement_pct` metric.

---

*This PRD describes the intended behaviour. Any deviation during implementation must be recorded in `changes.md` with a rationale.*