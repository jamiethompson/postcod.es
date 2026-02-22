BEGIN;

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
            'derived_uprn_street_spatial_built',
            'metrics_stored',
            'canonical_hashes_stored',
            'release_marked_built',
            'core_built',
            'derived_built'
        )
    );

COMMIT;
