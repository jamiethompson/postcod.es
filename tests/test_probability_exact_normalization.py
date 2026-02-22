import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / "pipeline" / "src" / "pipeline" / "build" / "workflows.py"


class ProbabilityExactNormalizationTests(unittest.TestCase):
    def test_probability_uses_explicit_denominator(self) -> None:
        text = WORKFLOWS.read_text(encoding="utf-8")
        self.assertIn("(g.weighted_score / t.total_weight) AS raw_probability", text)

    def test_probability_residual_correction_applied_to_rank_one(self) -> None:
        text = WORKFLOWS.read_text(encoding="utf-8")
        self.assertIn("WHEN rn = 1", text)
        self.assertIn("(1.0000 - rounded_sum)", text)

    def test_verify_requires_exact_sum_one(self) -> None:
        text = WORKFLOWS.read_text(encoding="utf-8")
        self.assertIn("HAVING SUM(probability)::numeric(10,4) <> 1.0000", text)


if __name__ == "__main__":
    unittest.main()
