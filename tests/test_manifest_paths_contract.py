import json
import tempfile
import unittest
from pathlib import Path

import sys


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "pipeline" / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from pipeline.manifest import load_source_manifest  # noqa: E402


class ManifestPathsContractTests(unittest.TestCase):
    def _source_manifest_payload(self, file_path: str) -> dict:
        return {
            "source_name": "onspd",
            "source_version": "test-v1",
            "retrieved_at_utc": "2026-02-24T00:00:00Z",
            "source_url": "local://test/onspd",
            "processing_git_sha": "1234567890abcdef1234567890abcdef12345678",
            "notes": "test",
            "files": [
                {
                    "file_role": "primary",
                    "file_path": file_path,
                    "sha256": "d0fd4c2f84f2f115ec6cfb79d9f68a1507f9b8f0f483dba53f0241725790f2ef",
                    "size_bytes": 6,
                    "format": "csv",
                    "row_count_expected": 1,
                }
            ],
        }

    def test_source_manifest_resolves_relative_file_path_from_manifest_dir(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            data_dir = base / "source"
            manifests_dir = base / "manifests"
            data_dir.mkdir(parents=True, exist_ok=True)
            manifests_dir.mkdir(parents=True, exist_ok=True)

            data_file = data_dir / "onspd.csv"
            data_file.write_text("a,b,c\n", encoding="utf-8")

            manifest_path = manifests_dir / "onspd_manifest.json"
            payload = self._source_manifest_payload("../source/onspd.csv")
            manifest_path.write_text(json.dumps(payload), encoding="utf-8")

            manifest = load_source_manifest(manifest_path)
            self.assertEqual(data_file.resolve(), manifest.files[0].file_path)

    def test_source_manifest_resolves_repo_relative_file_path(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo = Path(temp_dir) / "repo"
            (repo / ".git").mkdir(parents=True, exist_ok=True)
            (repo / "data" / "source_files" / "fixture").mkdir(parents=True, exist_ok=True)
            (repo / "data" / "manifests" / "fixture").mkdir(parents=True, exist_ok=True)

            data_file = repo / "data" / "source_files" / "fixture" / "onspd.csv"
            data_file.write_text("a,b,c\n", encoding="utf-8")

            manifest_path = repo / "data" / "manifests" / "fixture" / "onspd_manifest.json"
            payload = self._source_manifest_payload("data/source_files/fixture/onspd.csv")
            manifest_path.write_text(json.dumps(payload), encoding="utf-8")

            manifest = load_source_manifest(manifest_path)
            self.assertEqual(data_file.resolve(), manifest.files[0].file_path)

    def test_repo_manifests_do_not_use_absolute_file_paths(self) -> None:
        manifest_paths = sorted((ROOT / "data" / "manifests").glob("**/*manifest*.json"))
        self.assertTrue(manifest_paths, "Expected manifest fixtures under data/manifests/")

        for manifest_path in manifest_paths:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            files = payload.get("files")
            if isinstance(files, list):
                for file_entry in files:
                    if isinstance(file_entry, dict) and "file_path" in file_entry:
                        self.assertFalse(
                            Path(str(file_entry["file_path"])).is_absolute(),
                            f"{manifest_path} contains absolute files[].file_path",
                        )
            if "file_path" in payload:
                self.assertFalse(
                    Path(str(payload["file_path"])).is_absolute(),
                    f"{manifest_path} contains absolute file_path",
                )


if __name__ == "__main__":
    unittest.main()
