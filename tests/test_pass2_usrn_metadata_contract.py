import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / "pipeline" / "src" / "pipeline" / "build" / "workflows.py"


class Pass2UsrnMetadataContractTests(unittest.TestCase):
    def test_pass2_inferred_usrn_rows_reuse_stage_metadata(self) -> None:
        text = WORKFLOWS.read_text(encoding="utf-8")
        self.assertIn("LEFT JOIN stage.streets_usrn_input AS attrs", text)
        self.assertIn("attrs.street_type", text)
        self.assertIn("attrs.street_status", text)
        self.assertIn("COALESCE(attrs.usrn_run_id, ranked.usrn_run_id) AS usrn_run_id", text)

    def test_pass2_direct_insert_requires_canonical_name_fields(self) -> None:
        text = WORKFLOWS.read_text(encoding="utf-8")
        self.assertIn("AND s.street_name IS NOT NULL", text)
        self.assertIn("AND s.street_name_casefolded IS NOT NULL", text)

    def test_stage_usrn_metadata_normalises_to_uppercase(self) -> None:
        text = WORKFLOWS.read_text(encoding="utf-8")
        self.assertIn("street_type_value = street_type_value.upper()", text)
        self.assertIn("street_status_value = street_status_value.upper()", text)


if __name__ == "__main__":
    unittest.main()
