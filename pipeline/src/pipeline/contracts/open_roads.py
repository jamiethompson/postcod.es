"""Locked SQL contracts for Open Roads staging and build linkage."""

OPEN_ROADS_STAGE_TABLE = "stage.open_roads_segment"

ALLOWED_CANONICAL_HASH_OBJECT_NAMES = (
    "core_uprn_postcode",
    "core_uprn_point",
    "core_road_segment",
    "derived_uprn_street_spatial",
)

ALLOWED_CANONICAL_HASH_OBJECT_NAMES_PHASE2 = (
    "core_uprn_postcode",
    "core_uprn_point",
    "core_road_segment",
    "core_open_names_entry",
    "core_postcode_unit_seed",
    "derived_uprn_street_spatial",
    "derived_postcode_street",
)

OPEN_ROADS_LOADED_FEATURE_COUNT_SQL = (
    "SELECT COUNT(*) AS loaded_feature_count "
    "FROM stage.open_roads_segment "
    "WHERE release_id = %(release_id)s;"
)

OPEN_ROADS_PERSIST_LOADED_FEATURE_COUNT_SQL = (
    "UPDATE meta.dataset_release "
    "SET loaded_feature_count = ("
    "SELECT COUNT(*) "
    "FROM stage.open_roads_segment "
    "WHERE release_id = %(release_id)s"
    ") "
    "WHERE dataset_key = 'open_roads' "
    "AND release_id = %(release_id)s;"
)

OPEN_ROADS_BUILD_LINKAGE_SQL = (
    "SELECT s.segment_id, s.name_display, s.name_norm, s.geom_bng "
    "FROM stage.open_roads_segment AS s "
    "WHERE s.release_id = %(open_roads_release_id)s "
    "ORDER BY s.segment_id ASC;"
)


def loaded_feature_count_query() -> str:
    """Return the locked gate query for loaded Open Roads features."""

    return OPEN_ROADS_LOADED_FEATURE_COUNT_SQL


def persist_loaded_feature_count_query() -> str:
    """Return the locked query that writes loaded feature counts into metadata."""

    return OPEN_ROADS_PERSIST_LOADED_FEATURE_COUNT_SQL


def build_linkage_query() -> str:
    """Return the locked stage-to-build linkage query filtered by release_id."""

    return OPEN_ROADS_BUILD_LINKAGE_SQL
