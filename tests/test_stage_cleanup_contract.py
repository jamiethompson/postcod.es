import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / "pipeline" / "src" / "pipeline" / "build" / "workflows.py"


class StageCleanupContractTests(unittest.TestCase):
    def test_stage_cleanup_uses_truncate_not_per_run_delete(self) -> None:
        text = WORKFLOWS.read_text(encoding="utf-8")
        self.assertIn("TRUNCATE TABLE", text)
        self.assertNotIn("DELETE FROM {}.{} WHERE build_run_id = %s", text)

    def test_stage_cleanup_blocks_when_other_build_is_started(self) -> None:
        text = WORKFLOWS.read_text(encoding="utf-8")
        self.assertIn("WHERE status = 'started'", text)
        self.assertIn("Stage truncate is unsafe while another build is in status=started", text)


if __name__ == "__main__":
    unittest.main()
