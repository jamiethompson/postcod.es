import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / "pipeline" / "src" / "pipeline" / "build" / "workflows.py"


class Pass5SpatialFallbackContractTests(unittest.TestCase):
    def test_pass5_uses_150m_spatial_radius_and_name_quality_guardrail(self) -> None:
        text = WORKFLOWS.read_text(encoding="utf-8")
        self.assertIn("PASS5_SPATIAL_RADIUS_M = 150.0", text)
        self.assertIn("ST_DWithin(p.geom_bng, r.geom_bng, %(radius_m)s)", text)
        self.assertIn("name_quality_class = 'postal_plausible'", text)
        self.assertIn("AND p.name_quality_class = 'road_number'", text)

    def test_pass5_emits_ppd_tie_break_and_quality_evidence_fields(self) -> None:
        text = WORKFLOWS.read_text(encoding="utf-8")
        self.assertIn("'ppd_match_score', rs.ppd_match_score", text)
        self.assertIn("'ppd_matched_street', rs.ppd_matched_street", text)
        self.assertIn("'tie_break_basis', %s", text)
        self.assertIn("'name_quality_class', rs.name_quality_class", text)
        self.assertIn("'name_quality_reason', rs.name_quality_reason", text)
        self.assertIn("'fallback_policy', 'postal_name_preferred'", text)

    def test_pass5_exposes_requested_qa_counters(self) -> None:
        text = WORKFLOWS.read_text(encoding="utf-8")
        self.assertIn('"pass5_candidates_postal_plausible"', text)
        self.assertIn('"pass5_candidates_road_number"', text)
        self.assertIn('"pass5_road_number_only_wins"', text)
        self.assertIn('"pass5_ppd_tie_break_applied_count"', text)
        self.assertIn('"pass5_ppd_match_exact_count"', text)
        self.assertIn('"pass5_ppd_match_partial_count"', text)
        self.assertIn('"pass5_ppd_match_none_count"', text)


if __name__ == "__main__":
    unittest.main()
