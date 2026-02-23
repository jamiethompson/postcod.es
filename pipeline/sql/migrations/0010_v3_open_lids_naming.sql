BEGIN;

DO $$
BEGIN
    IF to_regclass('stage.oli_identifier_pair') IS NOT NULL
       AND to_regclass('stage.open_lids_pair') IS NULL THEN
        ALTER TABLE stage.oli_identifier_pair
            RENAME TO open_lids_pair;
    END IF;

    IF to_regclass('stage.oli_toid_usrn') IS NOT NULL
       AND to_regclass('stage.open_lids_toid_usrn') IS NULL THEN
        ALTER TABLE stage.oli_toid_usrn
            RENAME TO open_lids_toid_usrn;
    END IF;

    IF to_regclass('stage.oli_uprn_usrn') IS NOT NULL
       AND to_regclass('stage.open_lids_uprn_usrn') IS NULL THEN
        ALTER TABLE stage.oli_uprn_usrn
            RENAME TO open_lids_uprn_usrn;
    END IF;
END $$;

DO $$
BEGIN
    IF to_regclass('stage.oli_identifier_pair_pkey') IS NOT NULL
       AND to_regclass('stage.open_lids_pair_pkey') IS NULL THEN
        ALTER INDEX stage.oli_identifier_pair_pkey
            RENAME TO open_lids_pair_pkey;
    END IF;

    IF to_regclass('stage.oli_toid_usrn_pkey') IS NOT NULL
       AND to_regclass('stage.open_lids_toid_usrn_pkey') IS NULL THEN
        ALTER INDEX stage.oli_toid_usrn_pkey
            RENAME TO open_lids_toid_usrn_pkey;
    END IF;

    IF to_regclass('stage.oli_uprn_usrn_pkey') IS NOT NULL
       AND to_regclass('stage.open_lids_uprn_usrn_pkey') IS NULL THEN
        ALTER INDEX stage.oli_uprn_usrn_pkey
            RENAME TO open_lids_uprn_usrn_pkey;
    END IF;

    IF to_regclass('stage.idx_stage_oli_identifier_pair_run_relation') IS NOT NULL
       AND to_regclass('stage.idx_stage_open_lids_pair_run_relation') IS NULL THEN
        ALTER INDEX stage.idx_stage_oli_identifier_pair_run_relation
            RENAME TO idx_stage_open_lids_pair_run_relation;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS stage.open_lids_pair (
    build_run_id   uuid NOT NULL REFERENCES meta.build_run (build_run_id) ON DELETE CASCADE,
    id_1           text NOT NULL,
    id_2           text NOT NULL,
    relation_type  text NOT NULL,
    ingest_run_id  uuid NOT NULL REFERENCES meta.ingest_run (run_id),
    PRIMARY KEY (build_run_id, id_1, id_2, relation_type),
    CHECK (relation_type IN ('toid_usrn', 'uprn_usrn'))
);

CREATE INDEX IF NOT EXISTS idx_stage_open_lids_pair_run_relation
    ON stage.open_lids_pair (build_run_id, relation_type);

CREATE TABLE IF NOT EXISTS stage.open_lids_toid_usrn (
    build_run_id             uuid NOT NULL REFERENCES meta.build_run (build_run_id) ON DELETE CASCADE,
    toid                     text NOT NULL,
    usrn                     bigint NOT NULL,
    ingest_run_id            uuid NOT NULL REFERENCES meta.ingest_run (run_id),
    PRIMARY KEY (build_run_id, toid, usrn)
);

CREATE TABLE IF NOT EXISTS stage.open_lids_uprn_usrn (
    build_run_id             uuid NOT NULL REFERENCES meta.build_run (build_run_id) ON DELETE CASCADE,
    uprn                     bigint NOT NULL,
    usrn                     bigint NOT NULL,
    ingest_run_id            uuid NOT NULL REFERENCES meta.ingest_run (run_id),
    PRIMARY KEY (build_run_id, uprn, usrn)
);

ALTER TABLE derived.postcode_street_candidates
    DROP CONSTRAINT IF EXISTS postcode_street_candidates_candidate_type_check;

ALTER TABLE derived.postcode_streets_final_source
    DROP CONSTRAINT IF EXISTS postcode_streets_final_source_candidate_type_check;

ALTER TABLE derived.postcode_street_candidates DISABLE TRIGGER USER;

UPDATE derived.postcode_street_candidates
SET candidate_type = 'open_lids_toid_usrn'
WHERE candidate_type = 'oli_toid_usrn';

UPDATE derived.postcode_street_candidates
SET evidence_ref = regexp_replace(evidence_ref, '^oli:', 'open_lids:')
WHERE evidence_ref LIKE 'oli:%';

ALTER TABLE derived.postcode_street_candidates ENABLE TRIGGER USER;

UPDATE derived.postcode_streets_final_source
SET candidate_type = 'open_lids_toid_usrn'
WHERE candidate_type = 'oli_toid_usrn';

ALTER TABLE derived.postcode_streets_final_source
    ADD CONSTRAINT postcode_streets_final_source_candidate_type_check
    CHECK (candidate_type IN (
        'names_postcode_feature',
        'open_lids_toid_usrn',
        'uprn_usrn',
        'spatial_os_open_roads',
        'osni_gazetteer_direct',
        'spatial_dfi_highway',
        'ppd_parse_matched',
        'ppd_parse_unmatched'
    ));

COMMIT;
