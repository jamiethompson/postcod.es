import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "pipeline" / "config" / "open_names_type_families.yaml"
WORKFLOWS = ROOT / "pipeline" / "src" / "pipeline" / "build" / "workflows.py"
SOURCE_SCHEMA = ROOT / "pipeline" / "config" / "source_schema.yaml"


class OpenNamesTypeFamilyConfigContractTests(unittest.TestCase):
    def test_config_declares_default_type_map_and_linkage_policy(self) -> None:
        payload = json.loads(CONFIG.read_text(encoding="utf-8"))
        self.assertIn("default_type_family", payload)
        self.assertIn("type_to_family", payload)
        self.assertIn("families", payload)
        self.assertEqual(payload["default_type_family"], "other")
        self.assertIn("transportnetwork", payload["type_to_family"])
        self.assertEqual(
            payload["families"]["transport_network"]["linkage_policy"],
            "eligible",
        )

    def test_workflow_loads_open_names_type_family_config(self) -> None:
        text = WORKFLOWS.read_text(encoding="utf-8")
        self.assertIn("open_names_type_families_config_path()", text)
        self.assertIn("open_names_type_families missing object key type_to_family", text)
        self.assertIn("open_names_type_families missing object key families", text)

    def test_source_schema_maps_type_and_postcode_district_for_open_names(self) -> None:
        text = SOURCE_SCHEMA.read_text(encoding="utf-8")
        self.assertIn('"type": "TYPE"', text)
        self.assertIn('"postcode_district": "POSTCODE_DISTRICT"', text)
        self.assertIn('"street_name_alt": "NAME2"', text)


if __name__ == "__main__":
    unittest.main()
