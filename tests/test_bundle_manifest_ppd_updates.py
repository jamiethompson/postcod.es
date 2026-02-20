import json
import tempfile
import unittest
from pathlib import Path

import sys


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "pipeline" / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from pipeline.manifest import ManifestError, load_bundle_manifest  # noqa: E402


class BundleManifestPpdUpdatesTests(unittest.TestCase):
    def _write_manifest(self, payload: dict) -> Path:
        handle = tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".json", delete=False)
        try:
            json.dump(payload, handle)
            handle.flush()
            return Path(handle.name)
        finally:
            handle.close()

    def test_bundle_allows_multiple_ppd_runs(self) -> None:
        payload = {
            "build_profile": "gb_core_ppd",
            "source_runs": {
                "onspd": "11111111-1111-1111-1111-111111111111",
                "os_open_usrn": "22222222-2222-2222-2222-222222222222",
                "os_open_names": "33333333-3333-3333-3333-333333333333",
                "os_open_roads": "44444444-4444-4444-4444-444444444444",
                "os_open_uprn": "55555555-5555-5555-5555-555555555555",
                "os_open_linked_identifiers": "66666666-6666-6666-6666-666666666666",
                "nsul": "77777777-7777-7777-7777-777777777777",
                "ppd": [
                    "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                ],
            },
        }
        path = self._write_manifest(payload)
        manifest = load_bundle_manifest(path)
        self.assertEqual(
            (
                "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            ),
            manifest.source_runs["ppd"],
        )

    def test_bundle_rejects_empty_source_run_list(self) -> None:
        payload = {
            "build_profile": "gb_core",
            "source_runs": {
                "onspd": [],
                "os_open_usrn": "22222222-2222-2222-2222-222222222222",
                "os_open_names": "33333333-3333-3333-3333-333333333333",
                "os_open_roads": "44444444-4444-4444-4444-444444444444",
                "os_open_uprn": "55555555-5555-5555-5555-555555555555",
                "os_open_linked_identifiers": "66666666-6666-6666-6666-666666666666",
                "nsul": "77777777-7777-7777-7777-777777777777",
            },
        }
        path = self._write_manifest(payload)
        with self.assertRaises(ManifestError):
            load_bundle_manifest(path)

    def test_gb_core_ppd_does_not_require_ni_sources(self) -> None:
        payload = {
            "build_profile": "gb_core_ppd",
            "source_runs": {
                "onspd": "11111111-1111-1111-1111-111111111111",
                "os_open_usrn": "22222222-2222-2222-2222-222222222222",
                "os_open_names": "33333333-3333-3333-3333-333333333333",
                "os_open_roads": "44444444-4444-4444-4444-444444444444",
                "os_open_uprn": "55555555-5555-5555-5555-555555555555",
                "os_open_linked_identifiers": "66666666-6666-6666-6666-666666666666",
                "nsul": "77777777-7777-7777-7777-777777777777",
                "ppd": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            },
        }
        path = self._write_manifest(payload)
        manifest = load_bundle_manifest(path)
        self.assertEqual(("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",), manifest.source_runs["ppd"])


if __name__ == "__main__":
    unittest.main()
