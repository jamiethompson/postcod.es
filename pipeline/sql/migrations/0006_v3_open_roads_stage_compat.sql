BEGIN;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'stage'
          AND table_name = 'open_roads_segment'
          AND column_name = 'release_id'
    ) THEN
        DROP TABLE stage.open_roads_segment CASCADE;
    END IF;
END
$$;

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

COMMIT;
