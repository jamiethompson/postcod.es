import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (
    ROOT
    / "pipeline"
    / "sql"
    / "migrations"
    / "0019_v3_open_names_feature_expansion_spatial_fallback.sql"
)


class Migration0019OpenNamesFeatureExpansionContractTests(unittest.TestCase):
    def test_migration_adds_open_names_road_linkage_and_geometry_columns(self) -> None:
        text = MIGRATION.read_text(encoding="utf-8")
        self.assertIn("ALTER TABLE stage.open_names_road_feature", text)
        self.assertIn("ADD COLUMN IF NOT EXISTS related_toid text", text)
        self.assertIn("ADD COLUMN IF NOT EXISTS feature_toid text", text)
        self.assertIn("ADD COLUMN IF NOT EXISTS postcode_district_norm text", text)
        self.assertIn("ADD COLUMN IF NOT EXISTS geom_bng geometry(Point, 27700)", text)

    def test_migration_adds_spatial_support_indexes_and_ppd_tie_break_index(self) -> None:
        text = MIGRATION.read_text(encoding="utf-8")
        self.assertIn("ADD COLUMN IF NOT EXISTS geom_bng geometry(LineString, 27700)", text)
        self.assertIn("idx_stage_open_roads_geom_gist", text)
        self.assertIn("idx_stage_open_roads_run_postcode", text)
        self.assertIn("idx_stage_ppd_parsed_postcode_street", text)

    def test_migration_adds_open_names_type_family_tables_and_union_view(self) -> None:
        text = MIGRATION.read_text(encoding="utf-8")
        self.assertIn("CREATE UNLOGGED TABLE IF NOT EXISTS stage.open_names_transport_network", text)
        self.assertIn("CREATE UNLOGGED TABLE IF NOT EXISTS stage.open_names_populated_place", text)
        self.assertIn("CREATE UNLOGGED TABLE IF NOT EXISTS stage.open_names_landcover", text)
        self.assertIn("CREATE UNLOGGED TABLE IF NOT EXISTS stage.open_names_landform", text)
        self.assertIn("CREATE UNLOGGED TABLE IF NOT EXISTS stage.open_names_hydrography", text)
        self.assertIn("CREATE UNLOGGED TABLE IF NOT EXISTS stage.open_names_other", text)
        self.assertIn("CREATE OR REPLACE VIEW stage.v_open_names_features_all AS", text)


if __name__ == "__main__":
    unittest.main()
