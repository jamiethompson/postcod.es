import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (
    ROOT
    / "pipeline"
    / "sql"
    / "migrations"
    / "0017_v3_open_names_postcode_stage.sql"
)


class OpenNamesPostcodeStageMigrationContractTests(unittest.TestCase):
    def test_migration_creates_unlogged_stage_table(self) -> None:
        text = MIGRATION.read_text(encoding="utf-8")
        self.assertIn("CREATE UNLOGGED TABLE IF NOT EXISTS stage.open_names_postcode_feature", text)
        self.assertIn("source_row_num", text)
        self.assertIn("postcode_norm", text)
        self.assertIn("populated_place", text)

    def test_migration_adds_postcode_lookup_index(self) -> None:
        text = MIGRATION.read_text(encoding="utf-8")
        self.assertIn("idx_stage_open_names_postcode_lookup", text)
        self.assertIn("(build_run_id, postcode_norm, source_row_num)", text)


if __name__ == "__main__":
    unittest.main()
