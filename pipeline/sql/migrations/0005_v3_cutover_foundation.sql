BEGIN;

CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS derived;
CREATE SCHEMA IF NOT EXISTS internal;
CREATE SCHEMA IF NOT EXISTS api;

-- Hard cutover: remove legacy release-run lifecycle objects.
DROP TABLE IF EXISTS meta.release_activation_log CASCADE;
DROP TABLE IF EXISTS meta.release_set_stage_checkpoint CASCADE;
DROP TABLE IF EXISTS meta.pipeline_run_warning CASCADE;
DROP TABLE IF EXISTS meta.dataset_metrics CASCADE;
DROP TABLE IF EXISTS meta.canonical_hash CASCADE;
DROP TABLE IF EXISTS meta.pipeline_run CASCADE;
DROP TABLE IF EXISTS meta.release_set CASCADE;
DROP TABLE IF EXISTS meta.dataset_release CASCADE;

DROP VIEW IF EXISTS core.uprn_postcode CASCADE;
DROP VIEW IF EXISTS core.uprn_point CASCADE;
DROP VIEW IF EXISTS core.road_segment CASCADE;
DROP VIEW IF EXISTS core.open_names_entry CASCADE;
DROP VIEW IF EXISTS core.postcode_unit_seed CASCADE;
DROP VIEW IF EXISTS derived.uprn_street_spatial CASCADE;
DROP VIEW IF EXISTS derived.postcode_street CASCADE;

CREATE TABLE IF NOT EXISTS meta.ingest_run (
    run_id              uuid PRIMARY KEY,
    source_name         text NOT NULL,
    source_version      text NOT NULL,
    retrieved_at_utc    timestamptz NOT NULL,
    source_url          text,
    processing_git_sha  char(40) NOT NULL,
    record_count        bigint,
    notes               text,
    file_set_sha256     char(64) NOT NULL,
    created_at_utc      timestamptz NOT NULL DEFAULT now(),
    CHECK (source_name IN (
        'onspd',
        'os_open_usrn',
        'os_open_names',
        'os_open_roads',
        'os_open_uprn',
        'os_open_lids',
        'nsul',
        'osni_gazetteer',
        'dfi_highway',
        'ppd'
    )),
    CHECK (processing_git_sha ~ '^[0-9a-f]{40}$'),
    CHECK (file_set_sha256 ~ '^[0-9a-fA-F]{64}$'),
    UNIQUE (source_name, source_version, file_set_sha256)
);

CREATE TABLE IF NOT EXISTS meta.ingest_run_file (
    file_id          bigserial PRIMARY KEY,
    ingest_run_id    uuid NOT NULL,
    file_role        text NOT NULL,
    filename         text NOT NULL,
    layer_name       text NOT NULL DEFAULT '',
    sha256           char(64) NOT NULL,
    size_bytes       bigint NOT NULL,
    row_count        bigint,
    format           text NOT NULL,
    FOREIGN KEY (ingest_run_id)
        REFERENCES meta.ingest_run (run_id)
        ON DELETE CASCADE,
    CHECK (sha256 ~ '^[0-9a-fA-F]{64}$'),
    CHECK (size_bytes >= 0),
    UNIQUE (ingest_run_id, file_role, filename, layer_name)
);

CREATE TABLE IF NOT EXISTS meta.build_bundle (
    bundle_id         uuid PRIMARY KEY,
    build_profile     text NOT NULL,
    bundle_hash       char(64) NOT NULL,
    status            text NOT NULL,
    created_at_utc    timestamptz NOT NULL DEFAULT now(),
    CHECK (build_profile IN ('gb_core', 'gb_core_ppd', 'core_ni')),
    CHECK (bundle_hash ~ '^[0-9a-fA-F]{64}$'),
    CHECK (status IN ('created', 'built', 'failed', 'published'))
);

CREATE TABLE IF NOT EXISTS meta.build_bundle_source (
    bundle_id       uuid NOT NULL,
    source_name     text NOT NULL,
    ingest_run_id   uuid NOT NULL,
    PRIMARY KEY (bundle_id, source_name, ingest_run_id),
    FOREIGN KEY (bundle_id)
        REFERENCES meta.build_bundle (bundle_id)
        ON DELETE CASCADE,
    FOREIGN KEY (ingest_run_id)
        REFERENCES meta.ingest_run (run_id),
    CHECK (source_name IN (
        'onspd',
        'os_open_usrn',
        'os_open_names',
        'os_open_roads',
        'os_open_uprn',
        'os_open_lids',
        'nsul',
        'osni_gazetteer',
        'dfi_highway',
        'ppd'
    ))
);

CREATE INDEX IF NOT EXISTS idx_build_bundle_source_bundle_source
    ON meta.build_bundle_source (bundle_id, source_name);

CREATE TABLE IF NOT EXISTS meta.build_run (
    build_run_id      uuid PRIMARY KEY,
    bundle_id         uuid NOT NULL,
    dataset_version   text NOT NULL,
    status            text NOT NULL,
    current_pass      text NOT NULL,
    error_text        text,
    started_at_utc    timestamptz NOT NULL DEFAULT now(),
    finished_at_utc   timestamptz,
    FOREIGN KEY (bundle_id)
        REFERENCES meta.build_bundle (bundle_id)
        ON DELETE CASCADE,
    CHECK (status IN ('started', 'built', 'failed', 'published'))
);

CREATE TABLE IF NOT EXISTS meta.build_pass_checkpoint (
    build_run_id             uuid NOT NULL,
    pass_name                text NOT NULL,
    completed_at_utc         timestamptz NOT NULL DEFAULT now(),
    row_count_summary_json   jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (build_run_id, pass_name),
    FOREIGN KEY (build_run_id)
        REFERENCES meta.build_run (build_run_id)
        ON DELETE CASCADE,
    CHECK (pass_name IN (
        '0a_raw_ingest',
        '0b_stage_normalisation',
        '1_onspd_backbone',
        '2_gb_canonical_streets',
        '3_open_names_candidates',
        '4_uprn_reinforcement',
        '5_gb_spatial_fallback',
        '6_ni_candidates',
        '7_ppd_gap_fill',
        '8_finalisation'
    ))
);

CREATE TABLE IF NOT EXISTS meta.canonical_hash (
    build_run_id    uuid NOT NULL,
    object_name     text NOT NULL,
    projection      jsonb NOT NULL,
    row_count       bigint NOT NULL,
    sha256          char(64) NOT NULL,
    computed_at_utc timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (build_run_id, object_name),
    FOREIGN KEY (build_run_id)
        REFERENCES meta.build_run (build_run_id)
        ON DELETE CASCADE,
    CHECK (sha256 ~ '^[0-9a-fA-F]{64}$')
);

CREATE TABLE IF NOT EXISTS meta.dataset_publication (
    dataset_version          text PRIMARY KEY,
    build_run_id             uuid NOT NULL UNIQUE,
    published_at_utc         timestamptz NOT NULL DEFAULT now(),
    published_by             text NOT NULL,
    lookup_table_name        text NOT NULL,
    street_lookup_table_name text NOT NULL,
    publish_txid             bigint NOT NULL,
    FOREIGN KEY (build_run_id)
        REFERENCES meta.build_run (build_run_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS raw.onspd_row (
    id              bigserial PRIMARY KEY,
    ingest_run_id   uuid NOT NULL REFERENCES meta.ingest_run (run_id) ON DELETE CASCADE,
    source_row_num  bigint NOT NULL,
    payload_jsonb   jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS raw.os_open_usrn_row (LIKE raw.onspd_row INCLUDING ALL);
CREATE TABLE IF NOT EXISTS raw.os_open_names_row (LIKE raw.onspd_row INCLUDING ALL);
CREATE TABLE IF NOT EXISTS raw.os_open_roads_row (LIKE raw.onspd_row INCLUDING ALL);
CREATE TABLE IF NOT EXISTS raw.os_open_uprn_row (LIKE raw.onspd_row INCLUDING ALL);
CREATE TABLE IF NOT EXISTS raw.os_open_lids_row (LIKE raw.onspd_row INCLUDING ALL);
CREATE TABLE IF NOT EXISTS raw.nsul_row (LIKE raw.onspd_row INCLUDING ALL);
CREATE TABLE IF NOT EXISTS raw.osni_gazetteer_row (LIKE raw.onspd_row INCLUDING ALL);
CREATE TABLE IF NOT EXISTS raw.dfi_highway_row (LIKE raw.onspd_row INCLUDING ALL);
CREATE TABLE IF NOT EXISTS raw.ppd_row (LIKE raw.onspd_row INCLUDING ALL);

CREATE INDEX IF NOT EXISTS idx_raw_onspd_run_id ON raw.onspd_row (ingest_run_id);
CREATE INDEX IF NOT EXISTS idx_raw_os_open_usrn_run_id ON raw.os_open_usrn_row (ingest_run_id);
CREATE INDEX IF NOT EXISTS idx_raw_os_open_names_run_id ON raw.os_open_names_row (ingest_run_id);
CREATE INDEX IF NOT EXISTS idx_raw_os_open_roads_run_id ON raw.os_open_roads_row (ingest_run_id);
CREATE INDEX IF NOT EXISTS idx_raw_os_open_uprn_run_id ON raw.os_open_uprn_row (ingest_run_id);
CREATE INDEX IF NOT EXISTS idx_raw_os_open_lids_run_id ON raw.os_open_lids_row (ingest_run_id);
CREATE INDEX IF NOT EXISTS idx_raw_nsul_run_id ON raw.nsul_row (ingest_run_id);
CREATE INDEX IF NOT EXISTS idx_raw_osni_run_id ON raw.osni_gazetteer_row (ingest_run_id);
CREATE INDEX IF NOT EXISTS idx_raw_dfi_run_id ON raw.dfi_highway_row (ingest_run_id);
CREATE INDEX IF NOT EXISTS idx_raw_ppd_run_id ON raw.ppd_row (ingest_run_id);

CREATE TABLE IF NOT EXISTS stage.onspd_postcode (
    build_run_id                 uuid NOT NULL REFERENCES meta.build_run (build_run_id) ON DELETE CASCADE,
    postcode_norm                text NOT NULL,
    postcode_display             text NOT NULL,
    status                       text NOT NULL,
    lat                          numeric(9,6),
    lon                          numeric(9,6),
    easting                      integer,
    northing                     integer,
    country_iso2                 char(2) NOT NULL,
    country_iso3                 char(3) NOT NULL,
    subdivision_code             text,
    post_town                    text,
    locality                     text,
    street_enrichment_available  boolean NOT NULL,
    onspd_run_id                 uuid NOT NULL REFERENCES meta.ingest_run (run_id),
    PRIMARY KEY (build_run_id, postcode_norm)
);

CREATE TABLE IF NOT EXISTS stage.streets_usrn_input (
    build_run_id          uuid NOT NULL REFERENCES meta.build_run (build_run_id) ON DELETE CASCADE,
    usrn                  bigint NOT NULL,
    street_name           text NOT NULL,
    street_name_casefolded text NOT NULL,
    street_class          text,
    street_status         text,
    usrn_run_id           uuid NOT NULL REFERENCES meta.ingest_run (run_id),
    PRIMARY KEY (build_run_id, usrn)
);

CREATE TABLE IF NOT EXISTS stage.open_names_road_feature (
    build_run_id             uuid NOT NULL REFERENCES meta.build_run (build_run_id) ON DELETE CASCADE,
    feature_id               text NOT NULL,
    toid                     text,
    postcode_norm            text,
    street_name_raw          text NOT NULL,
    street_name_casefolded   text NOT NULL,
    ingest_run_id            uuid NOT NULL REFERENCES meta.ingest_run (run_id),
    PRIMARY KEY (build_run_id, feature_id)
);

CREATE TABLE IF NOT EXISTS stage.open_roads_segment (
    build_run_id             uuid NOT NULL REFERENCES meta.build_run (build_run_id) ON DELETE CASCADE,
    segment_id               text NOT NULL,
    road_id                  text,
    postcode_norm            text,
    usrn                     bigint,
    road_name                text NOT NULL,
    road_name_casefolded     text NOT NULL,
    ingest_run_id            uuid NOT NULL REFERENCES meta.ingest_run (run_id),
    PRIMARY KEY (build_run_id, segment_id)
);

CREATE TABLE IF NOT EXISTS stage.uprn_point (
    build_run_id             uuid NOT NULL REFERENCES meta.build_run (build_run_id) ON DELETE CASCADE,
    uprn                     bigint NOT NULL,
    postcode_norm            text,
    ingest_run_id            uuid NOT NULL REFERENCES meta.ingest_run (run_id),
    PRIMARY KEY (build_run_id, uprn)
);

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

CREATE TABLE IF NOT EXISTS stage.nsul_uprn_postcode (
    build_run_id             uuid NOT NULL REFERENCES meta.build_run (build_run_id) ON DELETE CASCADE,
    uprn                     bigint NOT NULL,
    postcode_norm            text NOT NULL,
    ingest_run_id            uuid NOT NULL REFERENCES meta.ingest_run (run_id),
    PRIMARY KEY (build_run_id, uprn, postcode_norm)
);

CREATE TABLE IF NOT EXISTS stage.osni_street_point (
    build_run_id             uuid NOT NULL REFERENCES meta.build_run (build_run_id) ON DELETE CASCADE,
    feature_id               text NOT NULL,
    postcode_norm            text,
    street_name_raw          text NOT NULL,
    street_name_casefolded   text NOT NULL,
    ingest_run_id            uuid NOT NULL REFERENCES meta.ingest_run (run_id),
    PRIMARY KEY (build_run_id, feature_id)
);

CREATE TABLE IF NOT EXISTS stage.dfi_road_segment (
    build_run_id             uuid NOT NULL REFERENCES meta.build_run (build_run_id) ON DELETE CASCADE,
    segment_id               text NOT NULL,
    postcode_norm            text,
    street_name_raw          text NOT NULL,
    street_name_casefolded   text NOT NULL,
    ingest_run_id            uuid NOT NULL REFERENCES meta.ingest_run (run_id),
    PRIMARY KEY (build_run_id, segment_id)
);

CREATE TABLE IF NOT EXISTS stage.ppd_parsed_address (
    build_run_id             uuid NOT NULL REFERENCES meta.build_run (build_run_id) ON DELETE CASCADE,
    row_hash                 text NOT NULL,
    postcode_norm            text NOT NULL,
    house_number             text,
    street_token_raw         text NOT NULL,
    street_token_casefolded  text NOT NULL,
    ingest_run_id            uuid NOT NULL REFERENCES meta.ingest_run (run_id),
    PRIMARY KEY (build_run_id, row_hash)
);

CREATE TABLE IF NOT EXISTS core.postcodes (
    produced_build_run_id        uuid NOT NULL REFERENCES meta.build_run (build_run_id) ON DELETE CASCADE,
    postcode                     text NOT NULL,
    status                       text NOT NULL,
    lat                          numeric(9,6),
    lon                          numeric(9,6),
    easting                      integer,
    northing                     integer,
    country_iso2                 char(2) NOT NULL,
    country_iso3                 char(3) NOT NULL,
    subdivision_code             text,
    post_town                    text,
    locality                     text,
    street_enrichment_available  boolean NOT NULL,
    multi_street                 boolean NOT NULL DEFAULT false,
    onspd_run_id                 uuid NOT NULL REFERENCES meta.ingest_run (run_id),
    PRIMARY KEY (produced_build_run_id, postcode)
);

CREATE TABLE IF NOT EXISTS core.postcodes_meta (
    produced_build_run_id    uuid NOT NULL,
    postcode                 text NOT NULL,
    meta_jsonb               jsonb NOT NULL DEFAULT '{}'::jsonb,
    onspd_run_id             uuid NOT NULL REFERENCES meta.ingest_run (run_id),
    PRIMARY KEY (produced_build_run_id, postcode),
    FOREIGN KEY (produced_build_run_id, postcode)
        REFERENCES core.postcodes (produced_build_run_id, postcode)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS core.streets_usrn (
    produced_build_run_id    uuid NOT NULL REFERENCES meta.build_run (build_run_id) ON DELETE CASCADE,
    usrn                     bigint NOT NULL,
    street_name              text NOT NULL,
    street_name_casefolded   text NOT NULL,
    street_class             text,
    street_status            text,
    usrn_run_id              uuid NOT NULL REFERENCES meta.ingest_run (run_id),
    PRIMARY KEY (produced_build_run_id, usrn)
);

CREATE TABLE IF NOT EXISTS derived.postcode_street_candidates (
    candidate_id             bigserial PRIMARY KEY,
    produced_build_run_id    uuid NOT NULL,
    postcode                 text NOT NULL,
    street_name_raw          text NOT NULL,
    street_name_canonical    text NOT NULL,
    usrn                     bigint,
    candidate_type           text NOT NULL,
    confidence               text NOT NULL,
    evidence_ref             text NOT NULL,
    source_name              text NOT NULL,
    ingest_run_id            uuid NOT NULL,
    evidence_json            jsonb,
    created_at_utc           timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (produced_build_run_id)
        REFERENCES meta.build_run (build_run_id)
        ON DELETE CASCADE,
    FOREIGN KEY (produced_build_run_id, postcode)
        REFERENCES core.postcodes (produced_build_run_id, postcode)
        ON DELETE CASCADE,
    FOREIGN KEY (produced_build_run_id, usrn)
        REFERENCES core.streets_usrn (produced_build_run_id, usrn)
        ON DELETE SET NULL,
    FOREIGN KEY (ingest_run_id)
        REFERENCES meta.ingest_run (run_id),
    CHECK (candidate_type IN (
        'names_postcode_feature',
        'open_lids_toid_usrn',
        'uprn_usrn',
        'spatial_os_open_roads',
        'osni_gazetteer_direct',
        'spatial_dfi_highway',
        'ppd_parse_matched',
        'ppd_parse_unmatched'
    )),
    CHECK (confidence IN ('high', 'medium', 'low', 'none')),
    CHECK (source_name IN (
        'onspd',
        'os_open_usrn',
        'os_open_names',
        'os_open_roads',
        'os_open_uprn',
        'os_open_lids',
        'nsul',
        'osni_gazetteer',
        'dfi_highway',
        'ppd'
    ))
);

CREATE INDEX IF NOT EXISTS idx_candidate_run_postcode
    ON derived.postcode_street_candidates (produced_build_run_id, postcode);
CREATE INDEX IF NOT EXISTS idx_candidate_run_usrn
    ON derived.postcode_street_candidates (produced_build_run_id, usrn);

CREATE TABLE IF NOT EXISTS derived.postcode_street_candidate_lineage (
    parent_candidate_id      bigint NOT NULL REFERENCES derived.postcode_street_candidates (candidate_id) ON DELETE CASCADE,
    child_candidate_id       bigint NOT NULL REFERENCES derived.postcode_street_candidates (candidate_id) ON DELETE CASCADE,
    relation_type            text NOT NULL,
    produced_build_run_id    uuid NOT NULL REFERENCES meta.build_run (build_run_id) ON DELETE CASCADE,
    PRIMARY KEY (parent_candidate_id, child_candidate_id, relation_type)
);

CREATE TABLE IF NOT EXISTS derived.postcode_streets_final (
    final_id                 bigserial PRIMARY KEY,
    produced_build_run_id    uuid NOT NULL,
    postcode                 text NOT NULL,
    street_name              text NOT NULL,
    usrn                     bigint,
    confidence               text NOT NULL,
    frequency_score          numeric(10,4) NOT NULL,
    probability              numeric(6,4) NOT NULL,
    created_at_utc           timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (produced_build_run_id)
        REFERENCES meta.build_run (build_run_id)
        ON DELETE CASCADE,
    FOREIGN KEY (produced_build_run_id, postcode)
        REFERENCES core.postcodes (produced_build_run_id, postcode)
        ON DELETE CASCADE,
    FOREIGN KEY (produced_build_run_id, usrn)
        REFERENCES core.streets_usrn (produced_build_run_id, usrn)
        ON DELETE SET NULL,
    CHECK (confidence IN ('high', 'medium', 'low', 'none')),
    CHECK (probability >= 0 AND probability <= 1),
    UNIQUE (produced_build_run_id, postcode, street_name)
);

CREATE TABLE IF NOT EXISTS derived.postcode_streets_final_candidate (
    final_id                 bigint NOT NULL REFERENCES derived.postcode_streets_final (final_id) ON DELETE CASCADE,
    candidate_id             bigint NOT NULL REFERENCES derived.postcode_street_candidates (candidate_id) ON DELETE CASCADE,
    produced_build_run_id    uuid NOT NULL REFERENCES meta.build_run (build_run_id) ON DELETE CASCADE,
    link_rank                integer NOT NULL,
    PRIMARY KEY (final_id, candidate_id)
);

CREATE TABLE IF NOT EXISTS derived.postcode_streets_final_source (
    final_id                 bigint NOT NULL REFERENCES derived.postcode_streets_final (final_id) ON DELETE CASCADE,
    source_name              text NOT NULL,
    ingest_run_id            uuid NOT NULL REFERENCES meta.ingest_run (run_id),
    candidate_type           text NOT NULL,
    contribution_weight      numeric(10,4) NOT NULL,
    produced_build_run_id    uuid NOT NULL REFERENCES meta.build_run (build_run_id) ON DELETE CASCADE,
    PRIMARY KEY (final_id, source_name, ingest_run_id, candidate_type),
    CHECK (candidate_type IN (
        'names_postcode_feature',
        'open_lids_toid_usrn',
        'uprn_usrn',
        'spatial_os_open_roads',
        'osni_gazetteer_direct',
        'spatial_dfi_highway',
        'ppd_parse_matched',
        'ppd_parse_unmatched'
    ))
);

CREATE TABLE IF NOT EXISTS internal.unit_index (
    index_id                 bigserial PRIMARY KEY,
    produced_build_run_id    uuid NOT NULL REFERENCES meta.build_run (build_run_id) ON DELETE CASCADE,
    postcode                 text NOT NULL,
    house_number             text NOT NULL,
    street_name              text NOT NULL,
    usrn                     bigint,
    confidence               text NOT NULL,
    source_type              text NOT NULL,
    ingest_run_id            uuid NOT NULL REFERENCES meta.ingest_run (run_id),
    created_at_utc           timestamptz NOT NULL DEFAULT now(),
    CHECK (confidence IN ('high', 'medium', 'low', 'none'))
);

CREATE INDEX IF NOT EXISTS idx_unit_index_lookup
    ON internal.unit_index (produced_build_run_id, postcode, house_number);

CREATE OR REPLACE FUNCTION derived.reject_candidate_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'derived.postcode_street_candidates is append-only; % is not allowed', TG_OP;
END;
$$;

DROP TRIGGER IF EXISTS trg_candidate_no_update ON derived.postcode_street_candidates;
CREATE TRIGGER trg_candidate_no_update
BEFORE UPDATE ON derived.postcode_street_candidates
FOR EACH ROW EXECUTE FUNCTION derived.reject_candidate_mutation();

DROP TRIGGER IF EXISTS trg_candidate_no_delete ON derived.postcode_street_candidates;
CREATE TRIGGER trg_candidate_no_delete
BEFORE DELETE ON derived.postcode_street_candidates
FOR EACH ROW EXECUTE FUNCTION derived.reject_candidate_mutation();

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pipeline_writer') THEN
        CREATE ROLE pipeline_writer;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'api_reader') THEN
        CREATE ROLE api_reader;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'audit_reader') THEN
        CREATE ROLE audit_reader;
    END IF;
END
$$;

REVOKE ALL ON SCHEMA internal FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA internal FROM PUBLIC;

GRANT USAGE ON SCHEMA api TO api_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA api TO api_reader;

GRANT USAGE ON SCHEMA meta TO audit_reader;
GRANT SELECT ON meta.ingest_run, meta.ingest_run_file, meta.build_bundle, meta.build_bundle_source,
               meta.build_run, meta.build_pass_checkpoint, meta.canonical_hash, meta.dataset_publication
    TO audit_reader;
GRANT USAGE ON SCHEMA derived TO audit_reader;
GRANT SELECT ON derived.postcode_street_candidates,
               derived.postcode_street_candidate_lineage,
               derived.postcode_streets_final,
               derived.postcode_streets_final_candidate,
               derived.postcode_streets_final_source
    TO audit_reader;

COMMIT;
