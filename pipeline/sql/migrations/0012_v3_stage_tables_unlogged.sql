BEGIN;

ALTER TABLE stage.onspd_postcode SET UNLOGGED;
ALTER TABLE stage.streets_usrn_input SET UNLOGGED;
ALTER TABLE stage.open_names_road_feature SET UNLOGGED;
ALTER TABLE stage.open_roads_segment SET UNLOGGED;
ALTER TABLE stage.uprn_point SET UNLOGGED;
ALTER TABLE stage.open_lids_toid_usrn SET UNLOGGED;
ALTER TABLE stage.open_lids_uprn_usrn SET UNLOGGED;
ALTER TABLE stage.open_lids_pair SET UNLOGGED;
ALTER TABLE stage.nsul_uprn_postcode SET UNLOGGED;
ALTER TABLE stage.osni_street_point SET UNLOGGED;
ALTER TABLE stage.dfi_road_segment SET UNLOGGED;
ALTER TABLE stage.ppd_parsed_address SET UNLOGGED;

COMMIT;
