import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / "pipeline" / "src" / "pipeline" / "build" / "workflows.py"


class PostcodeLookupProjectionContractTests(unittest.TestCase):
    def test_api_projection_includes_place_admin_fields(self) -> None:
        text = WORKFLOWS.read_text(encoding="utf-8")
        self.assertIn("p.place", text)
        self.assertIn("p.place_type", text)
        self.assertIn("p.place_toid", text)
        self.assertIn("p.region_name", text)
        self.assertIn("p.region_toid", text)
        self.assertIn("p.county_unitary_name", text)
        self.assertIn("p.county_unitary_toid", text)
        self.assertIn("p.county_unitary_type", text)
        self.assertIn("p.district_borough_name", text)
        self.assertIn("p.district_borough_toid", text)
        self.assertIn("p.district_borough_type", text)

    def test_api_projection_excludes_post_town_and_locality(self) -> None:
        text = WORKFLOWS.read_text(encoding="utf-8")
        self.assertNotIn("p.post_town", text)
        self.assertNotIn("p.locality", text)

    def test_verify_hash_projection_excludes_post_town_and_locality(self) -> None:
        text = WORKFLOWS.read_text(encoding="utf-8")
        self.assertNotIn("post_town, locality", text)
        self.assertIn("place, place_type, place_toid", text)


if __name__ == "__main__":
    unittest.main()
