BEGIN;

ALTER TABLE raw.onspd_row SET UNLOGGED;
ALTER TABLE raw.os_open_usrn_row SET UNLOGGED;
ALTER TABLE raw.os_open_names_row SET UNLOGGED;
ALTER TABLE raw.os_open_roads_row SET UNLOGGED;
ALTER TABLE raw.os_open_uprn_row SET UNLOGGED;
ALTER TABLE raw.os_open_lids_row SET UNLOGGED;
ALTER TABLE raw.nsul_row SET UNLOGGED;
ALTER TABLE raw.osni_gazetteer_row SET UNLOGGED;
ALTER TABLE raw.dfi_highway_row SET UNLOGGED;
ALTER TABLE raw.ppd_row SET UNLOGGED;

COMMIT;
