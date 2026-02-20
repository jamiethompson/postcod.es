BEGIN;

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS derived;

CREATE TABLE IF NOT EXISTS meta.dataset_release (
    dataset_key             text        NOT NULL,
    release_id              text        NOT NULL,
    source_url              text        NOT NULL,
    licence                 text        NOT NULL,
    file_path               text        NOT NULL,
    expected_sha256         text        NOT NULL,
    actual_sha256           text        NOT NULL,
    retrieved_at            timestamptz NOT NULL,
    manifest_json           jsonb       NOT NULL DEFAULT '{}'::jsonb,
    source_row_count        bigint,
    loaded_row_count        bigint,
    source_feature_count    bigint,
    loaded_feature_count    bigint,
    source_layer_name       text,
    srid_confirmed          integer,
    PRIMARY KEY (dataset_key, release_id),
    CHECK (dataset_key IN ('onsud', 'open_uprn', 'open_roads')),
    CHECK (expected_sha256 ~ '^[0-9a-fA-F]{64}$'),
    CHECK (actual_sha256 ~ '^[0-9a-fA-F]{64}$')
);

CREATE TABLE IF NOT EXISTS meta.pipeline_run (
    run_id           uuid        PRIMARY KEY,
    release_set_id   uuid,
    started_at       timestamptz NOT NULL,
    finished_at      timestamptz,
    status           text        NOT NULL,
    stage            text        NOT NULL,
    error_text       text,
    CHECK (status IN ('started', 'built', 'active', 'failed'))
);

CREATE TABLE IF NOT EXISTS meta.release_set (
    release_set_id         uuid        PRIMARY KEY,
    onsud_release_id       text        NOT NULL,
    open_uprn_release_id   text        NOT NULL,
    open_roads_release_id  text        NOT NULL,
    physical_schema        text        NOT NULL,
    status                 text        NOT NULL,
    created_at             timestamptz NOT NULL,
    built_at               timestamptz,
    activated_at           timestamptz,
    CONSTRAINT uq_release_set_inputs
        UNIQUE (onsud_release_id, open_uprn_release_id, open_roads_release_id),
    CHECK (status IN ('created', 'built', 'active', 'inactive'))
);

CREATE TABLE IF NOT EXISTS meta.release_activation_log (
    activation_id            bigserial   PRIMARY KEY,
    previous_release_set_id  uuid,
    release_set_id           uuid        NOT NULL,
    actor                    text        NOT NULL,
    activated_at             timestamptz NOT NULL,
    FOREIGN KEY (release_set_id)
        REFERENCES meta.release_set (release_set_id)
);

CREATE TABLE IF NOT EXISTS meta.dataset_metrics (
    run_id          uuid        NOT NULL,
    release_set_id  uuid        NOT NULL,
    metric_key      text        NOT NULL,
    metric_value    numeric     NOT NULL,
    metric_unit     text        NOT NULL,
    computed_at     timestamptz NOT NULL,
    PRIMARY KEY (run_id, metric_key),
    FOREIGN KEY (release_set_id)
        REFERENCES meta.release_set (release_set_id)
);

CREATE TABLE IF NOT EXISTS meta.canonical_hash (
    release_set_id  uuid        NOT NULL,
    object_name     text        NOT NULL,
    projection      jsonb       NOT NULL,
    row_count       bigint      NOT NULL,
    sha256          text        NOT NULL,
    computed_at     timestamptz NOT NULL,
    run_id          uuid        NOT NULL,
    PRIMARY KEY (release_set_id, object_name, run_id),
    FOREIGN KEY (release_set_id)
        REFERENCES meta.release_set (release_set_id),
    CHECK (sha256 ~ '^[0-9a-fA-F]{64}$')
);

CREATE TABLE IF NOT EXISTS raw.onsud_row (
    id              bigserial PRIMARY KEY,
    dataset_key     text      NOT NULL,
    release_id      text      NOT NULL,
    source_row_num  bigint    NOT NULL,
    uprn            bigint,
    postcode        text,
    extras_jsonb    jsonb,
    FOREIGN KEY (dataset_key, release_id)
        REFERENCES meta.dataset_release (dataset_key, release_id)
);

CREATE TABLE IF NOT EXISTS raw.open_uprn_row (
    id              bigserial        PRIMARY KEY,
    dataset_key     text             NOT NULL,
    release_id      text             NOT NULL,
    source_row_num  bigint           NOT NULL,
    uprn            bigint,
    latitude        double precision,
    longitude       double precision,
    easting         double precision,
    northing        double precision,
    extras_jsonb    jsonb,
    FOREIGN KEY (dataset_key, release_id)
        REFERENCES meta.dataset_release (dataset_key, release_id)
);

-- Ingest must normalize incoming LineString geometries to MultiLineString.
CREATE TABLE IF NOT EXISTS stage.open_roads_segment (
    dataset_key    text                          NOT NULL,
    release_id     text                          NOT NULL,
    segment_id     bigint                        NOT NULL,
    name_display   text,
    name_norm      text,
    geom_bng       geometry(MultiLineString,27700) NOT NULL,
    CHECK (dataset_key = 'open_roads'),
    UNIQUE (release_id, segment_id),
    FOREIGN KEY (dataset_key, release_id)
        REFERENCES meta.dataset_release (dataset_key, release_id)
);

CREATE INDEX IF NOT EXISTS idx_stage_open_roads_segment_release_id
    ON stage.open_roads_segment (release_id);

CREATE INDEX IF NOT EXISTS idx_stage_open_roads_segment_geom_bng
    ON stage.open_roads_segment USING gist (geom_bng);

COMMIT;
