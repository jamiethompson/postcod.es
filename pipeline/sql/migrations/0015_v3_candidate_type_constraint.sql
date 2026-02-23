BEGIN;

ALTER TABLE derived.postcode_street_candidates
    DROP CONSTRAINT IF EXISTS postcode_street_candidates_candidate_type_check;

ALTER TABLE derived.postcode_street_candidates
    ADD CONSTRAINT postcode_street_candidates_candidate_type_check
    CHECK (candidate_type IN (
        'names_postcode_feature',
        'open_lids_toid_usrn',
        'uprn_usrn',
        'spatial_os_open_roads',
        'osni_gazetteer_direct',
        'spatial_dfi_highway',
        'ppd_parse_matched',
        'ppd_parse_unmatched'
    ));

COMMIT;
