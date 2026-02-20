import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CLI = ROOT / "pipeline" / "src" / "pipeline" / "cli.py"


class CliV3ContractTests(unittest.TestCase):
    def test_cli_has_v3_commands(self) -> None:
        text = CLI.read_text(encoding="utf-8")
        self.assertIn('add_parser("bundle"', text)
        self.assertIn('add_parser("build"', text)
        self.assertIn('add_parser("source"', text)
        self.assertIn('add_parser("run"', text)
        self.assertIn('add_parser("verify"', text)
        self.assertIn('add_parser("publish"', text)


if __name__ == "__main__":
    unittest.main()
