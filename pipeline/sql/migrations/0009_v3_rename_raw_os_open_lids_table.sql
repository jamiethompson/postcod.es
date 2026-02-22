BEGIN;

DO $$
BEGIN
    IF to_regclass('raw.os_open_linked_identifiers_row') IS NOT NULL
       AND to_regclass('raw.os_open_lids_row') IS NULL THEN
        ALTER TABLE raw.os_open_linked_identifiers_row
            RENAME TO os_open_lids_row;
    END IF;
END $$;

DO $$
BEGIN
    IF to_regclass('raw.idx_raw_oli_run_id') IS NOT NULL
       AND to_regclass('raw.idx_raw_os_open_lids_run_id') IS NULL THEN
        ALTER INDEX raw.idx_raw_oli_run_id
            RENAME TO idx_raw_os_open_lids_run_id;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_raw_os_open_lids_run_id
    ON raw.os_open_lids_row (ingest_run_id);

COMMIT;
