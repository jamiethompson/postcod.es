# Value Added By Stage

This page explains what new product value is created at each pass, not just what tables are written.

## Pass 0a: Raw Ingest Validation
- Inputs: bundle-selected ingest runs (`meta.ingest_run`).
- Output: validated source presence and row-count baseline.
- Value added:
  - confirms build bundle completeness before transformations
  - establishes reproducible volume expectations per source

## Pass 0b: Stage Normalisation
- Inputs: immutable raw payloads (`raw.*`) + schema mapping config.
- Output: typed/normalized rows in `stage.*`.
- Value added:
  - converts heterogeneous source schemas to deterministic internal contracts
  - enforces required-field gates before downstream joins
  - materialises `LIDS` relation typing (`id_1`, `id_2`, `relation_type`)

## Pass 1: ONSPD Backbone
- Inputs: `stage.onspd_postcode`.
- Output: `core.postcodes`, `core.postcodes_meta`.
- Value added:
  - creates authoritative postcode validation layer
  - provides postcode centroid/admin metadata context for all later joins

## Pass 2: Canonical Streets (USRN)
- Inputs: `stage.streets_usrn_input`, `stage.open_names_road_feature`, `stage.open_lids_toid_usrn`.
- Output: `core.streets_usrn`.
- Value added:
  - produces canonical USRN-keyed street dictionary
  - fills gaps by inferring USRN names from Open Names + LIDS TOID mapping when direct names are absent

## Pass 3: Open Names Candidates
- Inputs: `stage.open_names_road_feature`, `stage.open_lids_toid_usrn`, `core.*`.
- Output: `derived.postcode_street_candidates` + lineage rows.
- Value added:
  - creates medium-confidence postcode/street evidence from named features
  - upgrades TOID-confirmed evidence via append-only promotion (`open_lids_toid_usrn`)
  - preserves full evidence chain (immutable parent + promoted child + lineage)

## Pass 4: UPRN Reinforcement
- Inputs: `stage.nsul_uprn_postcode`, `stage.open_lids_uprn_usrn`, `core.*`.
- Output: high-confidence `uprn_usrn` candidates.
- Value added:
  - adds strong evidence using property-level frequency aggregation
  - ties street inference to observed property density per postcode

## Pass 5: GB Spatial Fallback
- Inputs: `stage.open_roads_segment`, `core.postcodes`, current candidates.
- Output: low-confidence `spatial_os_open_roads` candidates.
- Value added:
  - closes obvious holes where no high-confidence candidate exists
  - improves postcode coverage while preserving confidence transparency

## Pass 6: NI Candidates (Optional Profile)
- Inputs: `stage.osni_street_point`, `stage.dfi_road_segment`, `core.postcodes`.
- Output: NI-specific candidate types.
- Value added:
  - extends coverage for NI builds with explicitly capped confidence

## Pass 7: PPD Gap Fill (Optional Profile)
- Inputs: `stage.ppd_parsed_address`, `core.streets_usrn`.
- Output: `ppd_parse_*` candidates, `internal.unit_index`.
- Value added:
  - uses transactional/self-reported evidence to fill gaps only
  - never overrides stronger core spatial evidence

## Pass 8: Finalisation
- Inputs: all candidates + weights config.
- Output: final tables + API versioned projections + deterministic hashes.
- Value added:
  - resolves competing evidence into ranked final street outputs
  - computes exact probabilities with deterministic rounding correction
  - produces API-ready materialisations with relational provenance backing

## Cross-links
- Stage details: [`stages/README.md`](stages/README.md)
- Dataset lineage pages: [`datasets/README.md`](datasets/README.md)
- Probability contract: [`../spec/pipeline_v3/spec.md`](../spec/pipeline_v3/spec.md)
