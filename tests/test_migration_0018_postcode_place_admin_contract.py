import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (
    ROOT
    / "pipeline"
    / "sql"
    / "migrations"
    / "0018_v3_postcode_place_admin_enrichment.sql"
)


class Migration0018PostcodePlaceAdminContractTests(unittest.TestCase):
    def test_migration_drops_legacy_town_locality_columns(self) -> None:
        text = MIGRATION.read_text(encoding="utf-8")
        self.assertIn("ALTER TABLE stage.onspd_postcode", text)
        self.assertIn("DROP COLUMN IF EXISTS post_town", text)
        self.assertIn("DROP COLUMN IF EXISTS locality", text)
        self.assertIn("ALTER TABLE core.postcodes", text)

    def test_migration_adds_open_names_parsed_stage_columns(self) -> None:
        text = MIGRATION.read_text(encoding="utf-8")
        self.assertIn("ALTER TABLE stage.open_names_postcode_feature", text)
        self.assertIn("ADD COLUMN IF NOT EXISTS place_type text", text)
        self.assertIn("ADD COLUMN IF NOT EXISTS place_toid text", text)
        self.assertIn("ADD COLUMN IF NOT EXISTS region_toid text", text)
        self.assertIn("ADD COLUMN IF NOT EXISTS county_unitary_toid text", text)
        self.assertIn("ADD COLUMN IF NOT EXISTS county_unitary_type text", text)
        self.assertIn("ADD COLUMN IF NOT EXISTS district_borough_toid text", text)
        self.assertIn("ADD COLUMN IF NOT EXISTS district_borough_type text", text)

    def test_migration_adds_core_postcode_place_admin_columns(self) -> None:
        text = MIGRATION.read_text(encoding="utf-8")
        self.assertIn("ADD COLUMN IF NOT EXISTS place text", text)
        self.assertIn("ADD COLUMN IF NOT EXISTS place_type text", text)
        self.assertIn("ADD COLUMN IF NOT EXISTS place_toid text", text)
        self.assertIn("ADD COLUMN IF NOT EXISTS region_name text", text)
        self.assertIn("ADD COLUMN IF NOT EXISTS region_toid text", text)
        self.assertIn("ADD COLUMN IF NOT EXISTS county_unitary_name text", text)
        self.assertIn("ADD COLUMN IF NOT EXISTS county_unitary_toid text", text)
        self.assertIn("ADD COLUMN IF NOT EXISTS county_unitary_type text", text)
        self.assertIn("ADD COLUMN IF NOT EXISTS district_borough_name text", text)
        self.assertIn("ADD COLUMN IF NOT EXISTS district_borough_toid text", text)
        self.assertIn("ADD COLUMN IF NOT EXISTS district_borough_type text", text)


if __name__ == "__main__":
    unittest.main()
