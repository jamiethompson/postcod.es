BEGIN;

ALTER TABLE meta.ingest_run
    DROP CONSTRAINT IF EXISTS ingest_run_source_name_check;

ALTER TABLE meta.build_bundle_source
    DROP CONSTRAINT IF EXISTS build_bundle_source_source_name_check;

ALTER TABLE derived.postcode_street_candidates
    DROP CONSTRAINT IF EXISTS postcode_street_candidates_source_name_check;

UPDATE meta.ingest_run
SET source_name = 'os_open_lids'
WHERE source_name = 'os_open_linked_identifiers';

UPDATE meta.build_bundle_source
SET source_name = 'os_open_lids'
WHERE source_name = 'os_open_linked_identifiers';

ALTER TABLE derived.postcode_street_candidates DISABLE TRIGGER ALL;
UPDATE derived.postcode_street_candidates
SET source_name = 'os_open_lids'
WHERE source_name = 'os_open_linked_identifiers';
ALTER TABLE derived.postcode_street_candidates ENABLE TRIGGER ALL;

ALTER TABLE meta.ingest_run
    ADD CONSTRAINT ingest_run_source_name_check
    CHECK (source_name IN (
        'onspd',
        'os_open_usrn',
        'os_open_names',
        'os_open_roads',
        'os_open_uprn',
        'os_open_lids',
        'nsul',
        'osni_gazetteer',
        'dfi_highway',
        'ppd'
    ));

ALTER TABLE meta.build_bundle_source
    ADD CONSTRAINT build_bundle_source_source_name_check
    CHECK (source_name IN (
        'onspd',
        'os_open_usrn',
        'os_open_names',
        'os_open_roads',
        'os_open_uprn',
        'os_open_lids',
        'nsul',
        'osni_gazetteer',
        'dfi_highway',
        'ppd'
    ));

ALTER TABLE derived.postcode_street_candidates
    ADD CONSTRAINT postcode_street_candidates_source_name_check
    CHECK (source_name IN (
        'onspd',
        'os_open_usrn',
        'os_open_names',
        'os_open_roads',
        'os_open_uprn',
        'os_open_lids',
        'nsul',
        'osni_gazetteer',
        'dfi_highway',
        'ppd'
    ));

COMMIT;
