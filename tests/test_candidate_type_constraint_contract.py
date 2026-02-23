import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MIGRATION = (
    ROOT
    / "pipeline"
    / "sql"
    / "migrations"
    / "0015_v3_candidate_type_constraint.sql"
)


class CandidateTypeConstraintDbContractTests(unittest.TestCase):
    def test_candidate_type_check_constraint_is_restored(self) -> None:
        text = MIGRATION.read_text(encoding="utf-8")
        self.assertIn("postcode_street_candidates_candidate_type_check", text)
        self.assertIn("ADD CONSTRAINT postcode_street_candidates_candidate_type_check", text)
        self.assertIn("'open_lids_toid_usrn'", text)
        self.assertIn("'ppd_parse_unmatched'", text)


if __name__ == "__main__":
    unittest.main()
