import ast
import json
import unittest
from decimal import Decimal
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / "pipeline" / "src" / "pipeline" / "build" / "workflows.py"


def _candidate_types_from_workflows() -> tuple[str, ...]:
    text = WORKFLOWS.read_text(encoding="utf-8")
    tree = ast.parse(text)
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "CANDIDATE_TYPES":
                    value = ast.literal_eval(node.value)
                    if isinstance(value, tuple):
                        return tuple(str(item) for item in value)
    raise AssertionError("CANDIDATE_TYPES constant not found in workflows.py")


class WeightConfigContractTests(unittest.TestCase):
    def test_weight_config_has_all_candidate_types_with_positive_values(self) -> None:
        candidate_types = _candidate_types_from_workflows()
        config_path = ROOT / "pipeline" / "config" / "frequency_weights.yaml"
        payload = json.loads(config_path.read_text(encoding="utf-8"))
        weights = payload["weights"]

        self.assertEqual(set(candidate_types), set(weights.keys()))
        for candidate_type in candidate_types:
            self.assertGreater(Decimal(str(weights[candidate_type])), Decimal("0"))


if __name__ == "__main__":
    unittest.main()
