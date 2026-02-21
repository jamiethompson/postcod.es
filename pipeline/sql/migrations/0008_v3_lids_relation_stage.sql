BEGIN;

CREATE TABLE IF NOT EXISTS stage.oli_identifier_pair (
    build_run_id   uuid NOT NULL REFERENCES meta.build_run (build_run_id) ON DELETE CASCADE,
    id_1           text NOT NULL,
    id_2           text NOT NULL,
    relation_type  text NOT NULL,
    ingest_run_id  uuid NOT NULL REFERENCES meta.ingest_run (run_id),
    PRIMARY KEY (build_run_id, id_1, id_2, relation_type),
    CHECK (relation_type IN ('toid_usrn', 'uprn_usrn'))
);

CREATE INDEX IF NOT EXISTS idx_stage_oli_identifier_pair_run_relation
    ON stage.oli_identifier_pair (build_run_id, relation_type);

COMMIT;
