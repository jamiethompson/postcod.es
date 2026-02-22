BEGIN;

DROP INDEX IF EXISTS raw.idx_raw_onspd_run_id;
DROP INDEX IF EXISTS raw.idx_raw_os_open_usrn_run_id;
DROP INDEX IF EXISTS raw.idx_raw_os_open_names_run_id;
DROP INDEX IF EXISTS raw.idx_raw_os_open_roads_run_id;
DROP INDEX IF EXISTS raw.idx_raw_os_open_uprn_run_id;
DROP INDEX IF EXISTS raw.idx_raw_os_open_lids_run_id;
DROP INDEX IF EXISTS raw.idx_raw_nsul_run_id;
DROP INDEX IF EXISTS raw.idx_raw_osni_run_id;
DROP INDEX IF EXISTS raw.idx_raw_dfi_run_id;
DROP INDEX IF EXISTS raw.idx_raw_ppd_run_id;

CREATE INDEX IF NOT EXISTS idx_raw_onspd_run_rownum
    ON raw.onspd_row (ingest_run_id, source_row_num);
CREATE INDEX IF NOT EXISTS idx_raw_os_open_usrn_run_rownum
    ON raw.os_open_usrn_row (ingest_run_id, source_row_num);
CREATE INDEX IF NOT EXISTS idx_raw_os_open_names_run_rownum
    ON raw.os_open_names_row (ingest_run_id, source_row_num);
CREATE INDEX IF NOT EXISTS idx_raw_os_open_roads_run_rownum
    ON raw.os_open_roads_row (ingest_run_id, source_row_num);
CREATE INDEX IF NOT EXISTS idx_raw_os_open_uprn_run_rownum
    ON raw.os_open_uprn_row (ingest_run_id, source_row_num);
CREATE INDEX IF NOT EXISTS idx_raw_os_open_lids_run_rownum
    ON raw.os_open_lids_row (ingest_run_id, source_row_num);
CREATE INDEX IF NOT EXISTS idx_raw_nsul_run_rownum
    ON raw.nsul_row (ingest_run_id, source_row_num);
CREATE INDEX IF NOT EXISTS idx_raw_osni_run_rownum
    ON raw.osni_gazetteer_row (ingest_run_id, source_row_num);
CREATE INDEX IF NOT EXISTS idx_raw_dfi_run_rownum
    ON raw.dfi_highway_row (ingest_run_id, source_row_num);
CREATE INDEX IF NOT EXISTS idx_raw_ppd_run_rownum
    ON raw.ppd_row (ingest_run_id, source_row_num);

COMMIT;
