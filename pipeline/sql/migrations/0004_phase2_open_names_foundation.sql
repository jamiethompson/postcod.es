BEGIN;

-- Phase boundary cutover: purge prior release-set lifecycle state and rs_* schemas.
DO $$
DECLARE
    schema_row record;
BEGIN
    FOR schema_row IN
        SELECT nspname
        FROM pg_namespace
        WHERE nspname LIKE 'rs_%'
    LOOP
        EXECUTE format('DROP SCHEMA IF EXISTS %I CASCADE', schema_row.nspname);
    END LOOP;
END
$$;

TRUNCATE TABLE meta.release_activation_log RESTART IDENTITY CASCADE;
TRUNCATE TABLE meta.release_set_stage_checkpoint RESTART IDENTITY CASCADE;
TRUNCATE TABLE meta.canonical_hash RESTART IDENTITY CASCADE;
TRUNCATE TABLE meta.dataset_metrics RESTART IDENTITY CASCADE;
TRUNCATE TABLE meta.pipeline_run RESTART IDENTITY CASCADE;
TRUNCATE TABLE meta.release_set RESTART IDENTITY CASCADE;

ALTER TABLE meta.dataset_release
    DROP CONSTRAINT IF EXISTS dataset_release_dataset_key_check;
ALTER TABLE meta.dataset_release
    DROP CONSTRAINT IF EXISTS ck_dataset_release_dataset_key;
ALTER TABLE meta.dataset_release
    ADD CONSTRAINT ck_dataset_release_dataset_key
    CHECK (dataset_key IN ('onsud', 'open_uprn', 'open_roads', 'open_names'));

ALTER TABLE meta.release_set
    ADD COLUMN IF NOT EXISTS open_names_release_id text;
ALTER TABLE meta.release_set
    ALTER COLUMN open_names_release_id SET NOT NULL;

ALTER TABLE meta.release_set
    DROP CONSTRAINT IF EXISTS uq_release_set_inputs;
ALTER TABLE meta.release_set
    DROP CONSTRAINT IF EXISTS uq_release_set_inputs_phase2;
ALTER TABLE meta.release_set
    ADD CONSTRAINT uq_release_set_inputs_phase2
    UNIQUE (
        onsud_release_id,
        open_uprn_release_id,
        open_roads_release_id,
        open_names_release_id
    );

ALTER TABLE meta.release_set_stage_checkpoint
    DROP CONSTRAINT IF EXISTS release_set_stage_checkpoint_stage_name_check;
ALTER TABLE meta.release_set_stage_checkpoint
    ADD CONSTRAINT release_set_stage_checkpoint_stage_name_check
    CHECK (
        stage_name IN (
            'release_tables_created',
            'core_uprn_postcode_built',
            'core_uprn_point_built',
            'core_road_segment_built',
            'core_open_names_entry_built',
            'core_postcode_unit_seed_built',
            'derived_uprn_street_spatial_built',
            'derived_postcode_street_built',
            'metrics_stored',
            'warnings_stored',
            'canonical_hashes_stored',
            'release_marked_built',
            'core_built',
            'derived_built'
        )
    );

CREATE TABLE IF NOT EXISTS meta.pipeline_run_warning (
    warning_id       bigserial   PRIMARY KEY,
    run_id           uuid        NOT NULL,
    release_set_id   uuid        NOT NULL,
    warning_code     text        NOT NULL,
    metric_key       text        NOT NULL,
    metric_value     numeric     NOT NULL,
    threshold_value  numeric     NOT NULL,
    requires_ack     boolean     NOT NULL,
    acknowledged_by  text,
    acknowledged_at  timestamptz,
    created_at       timestamptz NOT NULL DEFAULT now(),
    UNIQUE (run_id, warning_code),
    FOREIGN KEY (run_id)
        REFERENCES meta.pipeline_run (run_id)
        ON DELETE CASCADE,
    FOREIGN KEY (release_set_id)
        REFERENCES meta.release_set (release_set_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_warning_release_set
    ON meta.pipeline_run_warning (release_set_id, requires_ack, acknowledged_at);

ALTER TABLE raw.onsud_row
    ADD COLUMN IF NOT EXISTS postcode_unit_easting double precision;
ALTER TABLE raw.onsud_row
    ADD COLUMN IF NOT EXISTS postcode_unit_northing double precision;

CREATE TABLE IF NOT EXISTS raw.open_names_row (
    id                      bigserial PRIMARY KEY,
    dataset_key             text      NOT NULL,
    release_id              text      NOT NULL,
    source_row_num          bigint    NOT NULL,
    entry_id                text      NOT NULL,
    name1_display           text,
    name1_lang              text,
    name1_norm              text,
    name2_display           text,
    name2_norm              text,
    local_type              text      NOT NULL,
    postcode_district_norm  text,
    easting                 double precision,
    northing                double precision,
    geom_bng                geometry(Point,27700) NOT NULL,
    extras_jsonb            jsonb,
    CHECK (dataset_key = 'open_names'),
    CHECK (local_type IN ('Road', 'Named Road', 'Street')),
    UNIQUE (release_id, entry_id),
    FOREIGN KEY (dataset_key, release_id)
        REFERENCES meta.dataset_release (dataset_key, release_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_open_names_row_release_id
    ON raw.open_names_row (release_id);

CREATE INDEX IF NOT EXISTS idx_raw_open_names_row_release_district
    ON raw.open_names_row (release_id, postcode_district_norm);

CREATE INDEX IF NOT EXISTS idx_raw_open_names_row_geom_bng
    ON raw.open_names_row USING gist (geom_bng);

COMMIT;
