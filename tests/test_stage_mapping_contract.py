import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE_SCHEMA = ROOT / "pipeline" / "config" / "source_schema.yaml"
WORKFLOWS = ROOT / "pipeline" / "src" / "pipeline" / "build" / "workflows.py"


class StageMappingContractTests(unittest.TestCase):
    def test_required_stage_fields_are_enforced_for_gb_street_sources(self) -> None:
        payload = json.loads(SOURCE_SCHEMA.read_text(encoding="utf-8"))
        sources = payload["sources"]

        self.assertEqual(
            set(sources["os_open_usrn"]["required_fields"]),
            {"usrn"},
        )
        self.assertEqual(
            set(sources["os_open_names"]["required_fields"]),
            {
                "feature_id",
                "street_name",
                "local_type",
                "populated_place",
                "populated_place_uri",
                "populated_place_type",
                "region",
                "region_uri",
                "county_unitary",
                "county_unitary_uri",
                "county_unitary_type",
                "district_borough",
                "district_borough_uri",
                "district_borough_type",
            },
        )
        self.assertEqual(
            set(sources["os_open_roads"]["required_fields"]),
            {"segment_id", "road_name"},
        )

    def test_stage_extractors_use_mapped_field_lookup(self) -> None:
        text = WORKFLOWS.read_text(encoding="utf-8")
        self.assertIn('name_raw = _field_value(row, field_map, "street_name")', text)
        self.assertIn('postcode_raw = _field_value(row, field_map, "postcode")', text)
        self.assertIn('toid_raw = _field_value(row, field_map, "toid")', text)
        self.assertIn('usrn_raw = _field_value(row, field_map, "usrn")', text)

    def test_required_field_validation_is_not_limited_to_first_raw_row(self) -> None:
        text = WORKFLOWS.read_text(encoding="utf-8")
        self.assertIn("payload_jsonb ? %s", text)
        self.assertIn("raw_table=raw_table", text)


if __name__ == "__main__":
    unittest.main()
