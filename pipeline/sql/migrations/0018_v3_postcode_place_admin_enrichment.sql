BEGIN;

ALTER TABLE stage.onspd_postcode
    DROP COLUMN IF EXISTS post_town,
    DROP COLUMN IF EXISTS locality;

ALTER TABLE core.postcodes
    DROP COLUMN IF EXISTS post_town,
    DROP COLUMN IF EXISTS locality;

ALTER TABLE stage.open_names_postcode_feature
    ADD COLUMN IF NOT EXISTS place_type text,
    ADD COLUMN IF NOT EXISTS place_toid text,
    ADD COLUMN IF NOT EXISTS region_toid text,
    ADD COLUMN IF NOT EXISTS county_unitary_toid text,
    ADD COLUMN IF NOT EXISTS county_unitary_type text,
    ADD COLUMN IF NOT EXISTS district_borough_toid text,
    ADD COLUMN IF NOT EXISTS district_borough_type text;

ALTER TABLE core.postcodes
    ADD COLUMN IF NOT EXISTS place text,
    ADD COLUMN IF NOT EXISTS place_type text,
    ADD COLUMN IF NOT EXISTS place_toid text,
    ADD COLUMN IF NOT EXISTS region_name text,
    ADD COLUMN IF NOT EXISTS region_toid text,
    ADD COLUMN IF NOT EXISTS county_unitary_name text,
    ADD COLUMN IF NOT EXISTS county_unitary_toid text,
    ADD COLUMN IF NOT EXISTS county_unitary_type text,
    ADD COLUMN IF NOT EXISTS district_borough_name text,
    ADD COLUMN IF NOT EXISTS district_borough_toid text,
    ADD COLUMN IF NOT EXISTS district_borough_type text;

COMMIT;
