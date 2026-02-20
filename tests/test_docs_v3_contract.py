import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "docs" / "spec" / "pipeline_v3" / "spec.md"


class DocsV3ContractTests(unittest.TestCase):
    def test_spec_locks_append_only_promotion_and_exact_probability(self) -> None:
        text = SPEC.read_text(encoding="utf-8")
        self.assertIn("Pass 3 Promotion Semantics (Append-Only)", text)
        self.assertIn("immutable evidence rows", text)
        self.assertIn("Probability Formula (Exact)", text)
        self.assertIn("probability(postcode, street) = weighted_score(postcode, street) / total_weight(postcode)", text)
        self.assertNotIn("~1.0", text)


if __name__ == "__main__":
    unittest.main()
