import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / "pipeline" / "src" / "pipeline" / "build" / "workflows.py"


class Pass2ToidNameFallbackContractTests(unittest.TestCase):
    def test_pass2_merges_open_names_and_open_roads_toid_name_evidence(self) -> None:
        text = WORKFLOWS.read_text(encoding="utf-8")
        self.assertIn("CREATE TEMP TABLE tmp_toid_name_counts", text)
        self.assertIn("FROM stage.open_names_road_feature AS n", text)
        self.assertIn("FROM stage.open_roads_segment AS r", text)
        self.assertIn("r.road_id AS toid", text)

    def test_pass2_ranking_uses_source_priority_after_evidence_count(self) -> None:
        text = WORKFLOWS.read_text(encoding="utf-8")
        self.assertIn("MIN(n.source_priority)::smallint AS source_priority", text)
        self.assertIn("ORDER BY evidence_count DESC,", text)
        self.assertIn("source_priority ASC", text)


if __name__ == "__main__":
    unittest.main()
