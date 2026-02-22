BEGIN;

CREATE INDEX IF NOT EXISTS idx_stage_open_names_run_toid
    ON stage.open_names_road_feature (build_run_id, toid)
    WHERE toid IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_stage_open_names_run_postcode
    ON stage.open_names_road_feature (build_run_id, postcode_norm)
    WHERE postcode_norm IS NOT NULL;

COMMIT;
