import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / "pipeline" / "src" / "pipeline" / "build" / "workflows.py"


class Pass3AppendOnlyPromotionTests(unittest.TestCase):
    def test_pass3_inserts_promoted_rows_and_lineage(self) -> None:
        text = WORKFLOWS.read_text(encoding="utf-8")
        self.assertIn("def _pass_3_open_names_candidates", text)
        self.assertIn("INSERT INTO derived.postcode_street_candidates", text)
        self.assertIn("'open_lids_toid_usrn'", text)
        self.assertIn("INSERT INTO derived.postcode_street_candidate_lineage", text)
        self.assertIn("promotion_toid_usrn", text)
        self.assertIn("COALESCE(related_toid, feature_toid, toid) AS resolved_toid", text)
        self.assertIn("'related_toid', n.related_toid", text)
        self.assertIn("'feature_toid', n.feature_toid", text)

    def test_pass3_does_not_update_candidate_type(self) -> None:
        text = WORKFLOWS.read_text(encoding="utf-8")
        self.assertNotIn("UPDATE derived.postcode_street_candidates", text)

    def test_pass3_has_conditional_zero_output_guard(self) -> None:
        text = WORKFLOWS.read_text(encoding="utf-8")
        self.assertIn("Pass 3 produced zero base Open Names candidates despite staged input rows", text)
        self.assertIn("qa.pass3_skipped_no_open_names_rows", text)


if __name__ == "__main__":
    unittest.main()
