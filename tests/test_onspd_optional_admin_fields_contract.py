import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SOURCE_SCHEMA = ROOT / "pipeline" / "config" / "source_schema.yaml"
WORKFLOWS = ROOT / "pipeline" / "src" / "pipeline" / "build" / "workflows.py"


class OnspdOptionalAdminFieldsContractTests(unittest.TestCase):
    def test_onspd_field_map_omits_post_town_and_locality(self) -> None:
        payload = json.loads(SOURCE_SCHEMA.read_text(encoding="utf-8"))
        field_map = payload["sources"]["onspd"]["field_map"]
        self.assertNotIn("post_town", field_map)
        self.assertNotIn("locality", field_map)

    def test_stage_loader_no_longer_reads_post_town_or_locality(self) -> None:
        text = WORKFLOWS.read_text(encoding="utf-8")
        self.assertNotIn('post_town_raw = _field_value(row, field_map, "post_town")', text)
        self.assertNotIn('locality_raw = _field_value(row, field_map, "locality")', text)
        self.assertNotIn("post_town", text)
        self.assertNotIn("locality", text)


if __name__ == "__main__":
    unittest.main()
