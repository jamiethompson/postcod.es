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

### 2.1 PPD Baseline + Updates Rule

- The 4.2GB PPD full baseline is ingested once and retained.
- Subsequent yearly and monthly PPD update files are ingested as additional PPD runs.
- A build bundle may include multiple PPD ingest runs:
  - one baseline run
  - zero or more yearly/monthly update runs
- Stage normalisation applies PPD runs in deterministic ingest timestamp order.
- Non-PPD sources remain single-run-per-source within a bundle.
- Build profile naming keeps PPD independent from NI:
  - `gb_core`: GB core only
  - `gb_core_ppd`: GB core + PPD
  - `core_ni`: GB core + NI (without PPD)

## 3. Candidate Evidence Contract

`derived.postcode_street_candidates` is an immutable evidence log.

### 3.1 Pass 3 Promotion Semantics (Append-Only)

- `names_postcode_feature` candidates are immutable evidence rows.
- TOID confirmation creates a new `oli_toid_usrn` candidate row.
- Promotion lineage is recorded in `derived.postcode_street_candidate_lineage`.
- Existing candidate rows are never updated for `candidate_type`, `confidence`, `usrn`, or `evidence_ref`.

## 4. Confidence and Candidate Types

Candidate type enum:
- `names_postcode_feature`
- `oli_toid_usrn`
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
