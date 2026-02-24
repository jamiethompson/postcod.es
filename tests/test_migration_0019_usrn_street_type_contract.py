import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (
    ROOT
    / "pipeline"
    / "sql"
    / "migrations"
    / "0019_v3_usrn_street_type_metadata.sql"
)


class Migration0019UsrnStreetTypeContractTests(unittest.TestCase):
    def test_migration_renames_street_class_to_street_type(self) -> None:
        text = MIGRATION.read_text(encoding="utf-8")
        self.assertIn("ALTER TABLE stage.streets_usrn_input", text)
        self.assertIn("RENAME COLUMN street_class TO street_type", text)
        self.assertIn("ALTER TABLE core.streets_usrn", text)

    def test_migration_allows_stage_rows_without_direct_street_name(self) -> None:
        text = MIGRATION.read_text(encoding="utf-8")
        self.assertIn("ALTER COLUMN street_name DROP NOT NULL", text)
        self.assertIn("ALTER COLUMN street_name_casefolded DROP NOT NULL", text)


if __name__ == "__main__":
    unittest.main()
