import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "pipeline" / "sql" / "migrations" / "0005_v3_cutover_foundation.sql"


class CandidateImmutabilityDbContractTests(unittest.TestCase):
    def test_candidate_trigger_rejects_update_and_delete(self) -> None:
        text = MIGRATION.read_text(encoding="utf-8")
        self.assertIn("CREATE OR REPLACE FUNCTION derived.reject_candidate_mutation", text)
        self.assertIn("append-only", text)
        self.assertIn("CREATE TRIGGER trg_candidate_no_update", text)
        self.assertIn("BEFORE UPDATE ON derived.postcode_street_candidates", text)
        self.assertIn("CREATE TRIGGER trg_candidate_no_delete", text)
        self.assertIn("BEFORE DELETE ON derived.postcode_street_candidates", text)


if __name__ == "__main__":
    unittest.main()
