import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / "pipeline" / "src" / "pipeline" / "build" / "workflows.py"
SOURCE_SCHEMA = ROOT / "pipeline" / "config" / "source_schema.yaml"


class OpenNamesPostcodeEnrichmentContractTests(unittest.TestCase):
    def test_stage_normalisation_populates_open_names_postcode_stage_table(self) -> None:
        text = WORKFLOWS.read_text(encoding="utf-8")
        self.assertIn("stage.open_names_postcode_feature", text)
        self.assertIn("qa.open_names_postcode_duplicate_keys", text)

    def test_pass1_uses_deterministic_open_names_winner_by_source_row_num(self) -> None:
        text = WORKFLOWS.read_text(encoding="utf-8")
        self.assertIn("ROW_NUMBER() OVER (", text)
        self.assertIn("PARTITION BY postcode_norm", text)
        self.assertIn("ORDER BY source_row_num ASC", text)

    def test_pass1_sets_place_and_admin_fields_from_open_names(self) -> None:
        text = WORKFLOWS.read_text(encoding="utf-8")
        self.assertIn("sop.place", text)
        self.assertIn("sop.place_type", text)
        self.assertIn("sop.place_toid", text)
        self.assertIn("sop.region_name", text)
        self.assertIn("sop.county_unitary_name", text)
        self.assertIn("sop.district_borough_name", text)

    def test_pass1_logs_place_coverage_metrics(self) -> None:
        text = WORKFLOWS.read_text(encoding="utf-8")
        self.assertIn("qa.postcode_place_populated_count", text)
        self.assertIn("qa.postcode_place_missing_count", text)

    def test_source_schema_maps_open_names_enrichment_fields(self) -> None:
        text = SOURCE_SCHEMA.read_text(encoding="utf-8")
        self.assertIn('"populated_place"', text)
        self.assertIn('"populated_place_type"', text)
        self.assertIn('"populated_place_uri"', text)
        self.assertIn('"region_uri"', text)
        self.assertIn('"district_borough"', text)
        self.assertIn('"district_borough_uri"', text)
        self.assertIn('"district_borough_type"', text)
        self.assertIn('"county_unitary"', text)
        self.assertIn('"county_unitary_uri"', text)
        self.assertIn('"county_unitary_type"', text)


if __name__ == "__main__":
    unittest.main()
