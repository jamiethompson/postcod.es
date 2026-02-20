BEGIN;

CREATE TABLE IF NOT EXISTS meta.release_set_stage_checkpoint (
    release_set_id uuid        NOT NULL,
    stage_name     text        NOT NULL,
    run_id         uuid        NOT NULL,
    completed_at   timestamptz NOT NULL,
    PRIMARY KEY (release_set_id, stage_name),
    FOREIGN KEY (release_set_id)
        REFERENCES meta.release_set (release_set_id)
        ON DELETE CASCADE,
    CHECK (stage_name IN (
        'release_tables_created',
        'core_built',
        'derived_built',
        'metrics_stored',
        'canonical_hashes_stored',
        'release_marked_built'
    ))
);

CREATE INDEX IF NOT EXISTS idx_release_set_stage_checkpoint_release_set
    ON meta.release_set_stage_checkpoint (release_set_id, completed_at DESC);

COMMIT;
