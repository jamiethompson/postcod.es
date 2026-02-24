BEGIN;

ALTER TABLE stage.open_names_road_feature
    ADD COLUMN IF NOT EXISTS related_toid text,
    ADD COLUMN IF NOT EXISTS feature_toid text,
    ADD COLUMN IF NOT EXISTS postcode_district_norm text,
    ADD COLUMN IF NOT EXISTS geom_bng geometry(Point, 27700);

ALTER TABLE stage.open_roads_segment
    ADD COLUMN IF NOT EXISTS geom_bng geometry(LineString, 27700);

CREATE INDEX IF NOT EXISTS idx_stage_open_names_run_related_toid
    ON stage.open_names_road_feature (build_run_id, related_toid)
    WHERE related_toid IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_stage_open_names_run_feature_toid
    ON stage.open_names_road_feature (build_run_id, feature_toid)
    WHERE feature_toid IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_stage_open_names_run_postcode_district
    ON stage.open_names_road_feature (build_run_id, postcode_district_norm)
    WHERE postcode_district_norm IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_stage_open_names_road_geom_gist
    ON stage.open_names_road_feature USING GIST (geom_bng);

CREATE INDEX IF NOT EXISTS idx_stage_open_roads_run_id
    ON stage.open_roads_segment (build_run_id);

CREATE INDEX IF NOT EXISTS idx_stage_open_roads_run_postcode
    ON stage.open_roads_segment (build_run_id, postcode_norm)
    WHERE postcode_norm IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_stage_open_roads_geom_gist
    ON stage.open_roads_segment USING GIST (geom_bng);

CREATE INDEX IF NOT EXISTS idx_stage_ppd_parsed_postcode_street
    ON stage.ppd_parsed_address (build_run_id, postcode_norm, street_token_casefolded);

CREATE UNLOGGED TABLE IF NOT EXISTS stage.open_names_transport_network (
    build_run_id             uuid NOT NULL REFERENCES meta.build_run (build_run_id) ON DELETE CASCADE,
    source_row_num           bigint NOT NULL,
    feature_id               text NOT NULL,
    related_toid             text,
    name1                    text NOT NULL,
    name2                    text,
    type                     text NOT NULL,
    local_type               text NOT NULL,
    postcode_district_norm   text,
    populated_place          text,
    district_borough         text,
    county_unitary           text,
    region                   text,
    country                  text,
    geom_bng                 geometry(Point, 27700),
    linkage_policy           text NOT NULL,
    ingest_run_id            uuid NOT NULL REFERENCES meta.ingest_run (run_id),
    PRIMARY KEY (build_run_id, source_row_num),
    CHECK (linkage_policy IN ('eligible', 'context_only', 'excluded'))
);

CREATE UNLOGGED TABLE IF NOT EXISTS stage.open_names_populated_place (
    build_run_id             uuid NOT NULL REFERENCES meta.build_run (build_run_id) ON DELETE CASCADE,
    source_row_num           bigint NOT NULL,
    feature_id               text NOT NULL,
    related_toid             text,
    name1                    text NOT NULL,
    name2                    text,
    type                     text NOT NULL,
    local_type               text NOT NULL,
    postcode_district_norm   text,
    populated_place          text,
    district_borough         text,
    county_unitary           text,
    region                   text,
    country                  text,
    geom_bng                 geometry(Point, 27700),
    linkage_policy           text NOT NULL,
    ingest_run_id            uuid NOT NULL REFERENCES meta.ingest_run (run_id),
    PRIMARY KEY (build_run_id, source_row_num),
    CHECK (linkage_policy IN ('eligible', 'context_only', 'excluded'))
);

CREATE UNLOGGED TABLE IF NOT EXISTS stage.open_names_landcover (
    build_run_id             uuid NOT NULL REFERENCES meta.build_run (build_run_id) ON DELETE CASCADE,
    source_row_num           bigint NOT NULL,
    feature_id               text NOT NULL,
    related_toid             text,
    name1                    text NOT NULL,
    name2                    text,
    type                     text NOT NULL,
    local_type               text NOT NULL,
    postcode_district_norm   text,
    populated_place          text,
    district_borough         text,
    county_unitary           text,
    region                   text,
    country                  text,
    geom_bng                 geometry(Point, 27700),
    linkage_policy           text NOT NULL,
    ingest_run_id            uuid NOT NULL REFERENCES meta.ingest_run (run_id),
    PRIMARY KEY (build_run_id, source_row_num),
    CHECK (linkage_policy IN ('eligible', 'context_only', 'excluded'))
);

CREATE UNLOGGED TABLE IF NOT EXISTS stage.open_names_landform (
    build_run_id             uuid NOT NULL REFERENCES meta.build_run (build_run_id) ON DELETE CASCADE,
    source_row_num           bigint NOT NULL,
    feature_id               text NOT NULL,
    related_toid             text,
    name1                    text NOT NULL,
    name2                    text,
    type                     text NOT NULL,
    local_type               text NOT NULL,
    postcode_district_norm   text,
    populated_place          text,
    district_borough         text,
    county_unitary           text,
    region                   text,
    country                  text,
    geom_bng                 geometry(Point, 27700),
    linkage_policy           text NOT NULL,
    ingest_run_id            uuid NOT NULL REFERENCES meta.ingest_run (run_id),
    PRIMARY KEY (build_run_id, source_row_num),
    CHECK (linkage_policy IN ('eligible', 'context_only', 'excluded'))
);

CREATE UNLOGGED TABLE IF NOT EXISTS stage.open_names_hydrography (
    build_run_id             uuid NOT NULL REFERENCES meta.build_run (build_run_id) ON DELETE CASCADE,
    source_row_num           bigint NOT NULL,
    feature_id               text NOT NULL,
    related_toid             text,
    name1                    text NOT NULL,
    name2                    text,
    type                     text NOT NULL,
    local_type               text NOT NULL,
    postcode_district_norm   text,
    populated_place          text,
    district_borough         text,
    county_unitary           text,
    region                   text,
    country                  text,
    geom_bng                 geometry(Point, 27700),
    linkage_policy           text NOT NULL,
    ingest_run_id            uuid NOT NULL REFERENCES meta.ingest_run (run_id),
    PRIMARY KEY (build_run_id, source_row_num),
    CHECK (linkage_policy IN ('eligible', 'context_only', 'excluded'))
);

CREATE UNLOGGED TABLE IF NOT EXISTS stage.open_names_other (
    build_run_id             uuid NOT NULL REFERENCES meta.build_run (build_run_id) ON DELETE CASCADE,
    source_row_num           bigint NOT NULL,
    feature_id               text NOT NULL,
    related_toid             text,
    name1                    text NOT NULL,
    name2                    text,
    type                     text NOT NULL,
    local_type               text NOT NULL,
    postcode_district_norm   text,
    populated_place          text,
    district_borough         text,
    county_unitary           text,
    region                   text,
    country                  text,
    geom_bng                 geometry(Point, 27700),
    linkage_policy           text NOT NULL,
    ingest_run_id            uuid NOT NULL REFERENCES meta.ingest_run (run_id),
    PRIMARY KEY (build_run_id, source_row_num),
    CHECK (linkage_policy IN ('eligible', 'context_only', 'excluded'))
);

CREATE INDEX IF NOT EXISTS idx_open_names_transport_network_run_related_toid
    ON stage.open_names_transport_network (build_run_id, related_toid)
    WHERE related_toid IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_open_names_populated_place_run_related_toid
    ON stage.open_names_populated_place (build_run_id, related_toid)
    WHERE related_toid IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_open_names_landcover_run_related_toid
    ON stage.open_names_landcover (build_run_id, related_toid)
    WHERE related_toid IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_open_names_landform_run_related_toid
    ON stage.open_names_landform (build_run_id, related_toid)
    WHERE related_toid IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_open_names_hydrography_run_related_toid
    ON stage.open_names_hydrography (build_run_id, related_toid)
    WHERE related_toid IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_open_names_other_run_related_toid
    ON stage.open_names_other (build_run_id, related_toid)
    WHERE related_toid IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_open_names_transport_network_geom_gist
    ON stage.open_names_transport_network USING GIST (geom_bng);

CREATE INDEX IF NOT EXISTS idx_open_names_populated_place_geom_gist
    ON stage.open_names_populated_place USING GIST (geom_bng);

CREATE INDEX IF NOT EXISTS idx_open_names_landcover_geom_gist
    ON stage.open_names_landcover USING GIST (geom_bng);

CREATE INDEX IF NOT EXISTS idx_open_names_landform_geom_gist
    ON stage.open_names_landform USING GIST (geom_bng);

CREATE INDEX IF NOT EXISTS idx_open_names_hydrography_geom_gist
    ON stage.open_names_hydrography USING GIST (geom_bng);

CREATE INDEX IF NOT EXISTS idx_open_names_other_geom_gist
    ON stage.open_names_other USING GIST (geom_bng);

CREATE OR REPLACE VIEW stage.v_open_names_features_all AS
SELECT
    build_run_id,
    source_row_num,
    feature_id,
    related_toid,
    name1,
    name2,
    type,
    local_type,
    postcode_district_norm,
    populated_place,
    district_borough,
    county_unitary,
    region,
    country,
    geom_bng,
    linkage_policy,
    ingest_run_id,
    'stage.open_names_transport_network'::text AS feature_table,
    'transport_network'::text AS feature_class
FROM stage.open_names_transport_network
UNION ALL
SELECT
    build_run_id,
    source_row_num,
    feature_id,
    related_toid,
    name1,
    name2,
    type,
    local_type,
    postcode_district_norm,
    populated_place,
    district_borough,
    county_unitary,
    region,
    country,
    geom_bng,
    linkage_policy,
    ingest_run_id,
    'stage.open_names_populated_place'::text AS feature_table,
    'populated_place'::text AS feature_class
FROM stage.open_names_populated_place
UNION ALL
SELECT
    build_run_id,
    source_row_num,
    feature_id,
    related_toid,
    name1,
    name2,
    type,
    local_type,
    postcode_district_norm,
    populated_place,
    district_borough,
    county_unitary,
    region,
    country,
    geom_bng,
    linkage_policy,
    ingest_run_id,
    'stage.open_names_landcover'::text AS feature_table,
    'landcover'::text AS feature_class
FROM stage.open_names_landcover
UNION ALL
SELECT
    build_run_id,
    source_row_num,
    feature_id,
    related_toid,
    name1,
    name2,
    type,
    local_type,
    postcode_district_norm,
    populated_place,
    district_borough,
    county_unitary,
    region,
    country,
    geom_bng,
    linkage_policy,
    ingest_run_id,
    'stage.open_names_landform'::text AS feature_table,
    'landform'::text AS feature_class
FROM stage.open_names_landform
UNION ALL
SELECT
    build_run_id,
    source_row_num,
    feature_id,
    related_toid,
    name1,
    name2,
    type,
    local_type,
    postcode_district_norm,
    populated_place,
    district_borough,
    county_unitary,
    region,
    country,
    geom_bng,
    linkage_policy,
    ingest_run_id,
    'stage.open_names_hydrography'::text AS feature_table,
    'hydrography'::text AS feature_class
FROM stage.open_names_hydrography
UNION ALL
SELECT
    build_run_id,
    source_row_num,
    feature_id,
    related_toid,
    name1,
    name2,
    type,
    local_type,
    postcode_district_norm,
    populated_place,
    district_borough,
    county_unitary,
    region,
    country,
    geom_bng,
    linkage_policy,
    ingest_run_id,
    'stage.open_names_other'::text AS feature_table,
    'other'::text AS feature_class
FROM stage.open_names_other;

COMMIT;
