# Pipeline V3 Specification

## 1. Scope

Pipeline V3 is a raw-first, deterministic, replayable build pipeline for postcode and street intelligence.

Key properties:
- all source ingests are archived with file-level hashes
- all build outputs are reproducible from a bundle of ingest runs
- all derived records have relational provenance links
- API projections are versioned and published by atomic view switch

## 2. Pass Sequence

Build pass order is fixed:
1. `0a_raw_ingest`
2. `0b_stage_normalisation`
3. `1_onspd_backbone`
4. `2_gb_canonical_streets`
5. `3_open_names_candidates`
6. `4_uprn_reinforcement`
7. `5_gb_spatial_fallback`
8. `6_ni_candidates`
9. `7_ppd_gap_fill`
10. `8_finalisation`

### 2.2 Postcode Place/Admin Enrichment

- `post_town` and `locality` are removed from the V3 postcode contract.
- Pass `1_onspd_backbone` enriches postcode rows from Open Names postcode features (`LOCAL_TYPE='Postcode'`) joined by canonical postcode key.
- Deterministic winner rule for duplicate Open Names postcode matches:
  - choose the lowest `source_row_num` per `postcode_norm`.
- Missing Open Names matches do not drop postcode rows; enrichment fields remain null.
- QA metrics track duplicate postcode keys and place coverage counts per build run.

Field mapping contract:
- `POPULATED_PLACE -> place`
- `POPULATED_PLACE_TYPE -> place_type` using URI fragment token after `#` (fallback: last URI path segment)
- `POPULATED_PLACE_URI -> place_toid` using last URI path segment
- `REGION -> region_name`
- `REGION_URI -> region_toid` using last URI path segment
- `COUNTY_UNITARY -> county_unitary_name`
- `COUNTY_UNITARY_URI -> county_unitary_toid` using last URI path segment
- `COUNTY_UNITARY_TYPE -> county_unitary_type` using last URI path segment
- `DISTRICT_BOROUGH -> district_borough_name`
- `DISTRICT_BOROUGH_URI -> district_borough_toid` using last URI path segment
- `DISTRICT_BOROUGH_TYPE -> district_borough_type` using last URI path segment

Normalisation rules:
- trim whitespace; empty string maps to null
- no synthetic `osgb` prefixing
- preserve extracted token case
- preserve source casing for `place`

### 2.1 PPD Baseline + Updates Rule

- The 4.2GB PPD full baseline is ingested once and retained.
- Subsequent yearly and monthly PPD update files are ingested as additional PPD runs.
- A build bundle may include multiple PPD ingest runs:
  - one baseline run
  - zero or more yearly/monthly update runs
- Stage normalisation applies PPD runs in deterministic ingest timestamp order.
- Non-PPD sources remain single-run-per-source within a bundle.
- Bundles must not include sources outside the selected build profile.
- Build profile naming keeps PPD independent from NI:
  - `gb_core`: GB core only
  - `gb_core_ppd`: GB core + PPD
  - `core_ni`: GB core + NI (without PPD)

### 2.3 Open Names Feature Family Staging

- Pass `0b_stage_normalisation` keeps Open Names roads and postcodes as dedicated stage tables:
  - `stage.open_names_road_feature`
  - `stage.open_names_postcode_feature`
- Non-road and non-postcode Open Names features are staged into typed family tables:
  - `stage.open_names_transport_network`
  - `stage.open_names_populated_place`
  - `stage.open_names_landcover`
  - `stage.open_names_landform`
  - `stage.open_names_hydrography`
  - `stage.open_names_other`
- Type-family routing is config-driven via `pipeline/config/open_names_type_families.yaml`.
- Each family row carries deterministic `linkage_policy` (`eligible`, `context_only`, `excluded`).
- `stage.v_open_names_features_all` provides a deterministic union across all non-road/non-postcode family tables.

### 2.4 Future Feature-Cascade Track (Planning)

- A future heuristic track (`H6`) is reserved for deterministic feature-cascade promotion using selected non-road Open Names families.
- Scope for this release is staging + evidence readiness only; production candidate logic remains unchanged for non-road feature families.

## 3. Candidate Evidence Contract

`derived.postcode_street_candidates` is an immutable evidence log.

### 3.1 Pass 3 Promotion Semantics (Append-Only)

- `names_postcode_feature` candidates are immutable evidence rows.
- TOID confirmation creates a new `open_lids_toid_usrn` candidate row.
- Promotion lineage is recorded in `derived.postcode_street_candidate_lineage`.
- Existing candidate rows are never updated for `candidate_type`, `confidence`, `usrn`, or `evidence_ref`.

### 3.2 Pass 3 Conditional Gate

- Pass 3 base evidence resolves Open Names linkage using:
  - `COALESCE(related_toid, feature_toid, toid)` as deterministic TOID key.
- If no Open Names road rows are staged for the build, pass 3 is skipped with zero outputs and explicit checkpoint metric:
  - `qa.pass3_skipped_no_open_names_rows`
- If staged Open Names road rows and core postcode rows exist but pass 3 inserts zero base candidates, the build fails fast.

## 4. Confidence and Candidate Types

Candidate type enum:
- `names_postcode_feature`
- `open_lids_toid_usrn`
- `uprn_usrn`
- `spatial_os_open_roads`
- `osni_gazetteer_direct`
- `spatial_dfi_highway`
- `ppd_parse_matched`
- `ppd_parse_unmatched`

Confidence enum:
- `high`
- `medium`
- `low`
- `none`

NI confidence cap:
- NI candidate types cannot exceed `medium` in this release.

### 4.1 Pass 5 Spatial Fallback Policy

- Pass 5 remains low-confidence fallback (`confidence='low'`) for GB postcodes lacking high-confidence candidates.
- Spatial candidate generation uses Open Roads geometry with radius `150m` (EPSG:27700).
- If no spatial candidates exist for a postcode, postcode-key fallback candidates from Open Roads are permitted.
- Name-quality guardrail classifies road names as:
  - `postal_plausible`
  - `road_number`
  - `unknown`
- If any `postal_plausible` candidate exists for a postcode, `road_number` candidates are excluded.
- Candidate ranking is deterministic and ordered by:
  1. name-quality rank (`postal_plausible` > `unknown` > `road_number`)
  2. optional PPD street-match score (tie-break only)
  3. distance ascending
  4. segment id ascending (`COLLATE "C"`)
- Optional PPD tie-break is enabled only when:
  - the build includes source `ppd`
  - staged PPD rows exist for the build
  - environment flag `PIPELINE_PASS5_ENABLE_PPD_TIE_BREAK` is not disabled
- PPD never creates pass-5 candidates; it only ranks existing pass-5 candidates.
- Pass-5 evidence JSON includes:
  - `distance_m`
  - `name_quality_class`
  - `name_quality_reason`
  - `fallback_policy`
  - `ppd_match_score`
  - `ppd_matched_street`
  - `tie_break_basis`
- Pass-5 checkpoint counters include:
  - `pass5_candidates_postal_plausible`
  - `pass5_candidates_road_number`
  - `pass5_road_number_only_wins`
  - `pass5_ppd_tie_break_applied_count`
  - `pass5_ppd_match_exact_count`
  - `pass5_ppd_match_partial_count`
  - `pass5_ppd_match_none_count`

## 5. Frequency and Probability

### 5.1 Probability Formula (Exact)

- `weighted_score(postcode, street) = sum(candidate_weight for contributing candidates)`.
- `total_weight(postcode) = sum(weighted_score(postcode, *))`.
- `probability(postcode, street) = weighted_score(postcode, street) / total_weight(postcode)`.

### 5.2 Storage Rule

- Probabilities are rounded to fixed scale (`numeric(6,4)`).
- Deterministic residual correction is applied to rank 1 street so stored probabilities sum to exactly `1.0000` per postcode.
- Builds fail if `total_weight(postcode) <= 0` for any postcode with final rows.

## 6. Publish Contract

- Build writes versioned physical API tables only:
  - `api.postcode_lookup__<dataset_version>`
  - `api.postcode_street_lookup__<dataset_version>`
- Publish updates stable views in one transaction:
  - `api.postcode_lookup`
  - `api.postcode_street_lookup`
- Publication metadata is persisted transactionally.
- Publish rollback leaves previous published version untouched.

## 7. Provenance

Final outputs use relational provenance:
- `derived.postcode_streets_final_candidate`
- `derived.postcode_streets_final_source`

Arrays and JSON payloads are projection-only conveniences in `api.*` tables/views.

## 8. Architecture Cross-links
- Architecture index: [`../../architecture/README.md`](../../architecture/README.md)
- Dataset lineage pages: [`../../architecture/datasets/README.md`](../../architecture/datasets/README.md)
- Stage/pass pages: [`../../architecture/stages/README.md`](../../architecture/stages/README.md)
