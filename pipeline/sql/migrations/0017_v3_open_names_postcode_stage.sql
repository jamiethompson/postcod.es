BEGIN;

CREATE UNLOGGED TABLE IF NOT EXISTS stage.open_names_postcode_feature (
    build_run_id      uuid NOT NULL REFERENCES meta.build_run (build_run_id) ON DELETE CASCADE,
    source_row_num    bigint NOT NULL,
    feature_id        text,
    postcode_norm     text NOT NULL,
    postcode_display  text NOT NULL,
    populated_place   text,
    district_borough  text,
    county_unitary    text,
    region            text,
    country           text,
    geometry_x        integer,
    geometry_y        integer,
    ingest_run_id     uuid NOT NULL REFERENCES meta.ingest_run (run_id),
    PRIMARY KEY (build_run_id, source_row_num)
);

CREATE INDEX IF NOT EXISTS idx_stage_open_names_postcode_lookup
    ON stage.open_names_postcode_feature (build_run_id, postcode_norm, source_row_num);

COMMIT;
