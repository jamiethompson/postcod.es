import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pipeline" / "src"))

from pipeline.util.normalise import uri_fragment_or_terminal, uri_terminal_segment


class OpenNamesUriParsingContractTests(unittest.TestCase):
    def test_populated_place_type_uses_fragment_token_when_present(self) -> None:
        value = "http://www.ordnancesurvey.co.uk/xml/codelists/localtype.xml#village"
        self.assertEqual(uri_fragment_or_terminal(value), "village")

    def test_type_falls_back_to_path_terminal_when_fragment_absent(self) -> None:
        value = "http://data.ordnancesurvey.co.uk/ontology/admingeo/UnitaryAuthority"
        self.assertEqual(uri_fragment_or_terminal(value), "UnitaryAuthority")

    def test_uri_terminal_extracts_identifier_without_prefixing(self) -> None:
        value = "http://data.ordnancesurvey.co.uk/id/7000000000041429"
        token = uri_terminal_segment(value)
        self.assertEqual(token, "7000000000041429")
        self.assertFalse(str(token).lower().startswith("osgb"))

    def test_empty_values_map_to_none(self) -> None:
        self.assertIsNone(uri_fragment_or_terminal(None))
        self.assertIsNone(uri_fragment_or_terminal(""))
        self.assertIsNone(uri_fragment_or_terminal("   "))
        self.assertIsNone(uri_terminal_segment(""))

    def test_parser_preserves_token_case(self) -> None:
        county_type = "http://data.ordnancesurvey.co.uk/ontology/admingeo/GreaterLondonAuthority"
        place_type = "http://www.ordnancesurvey.co.uk/xml/codelists/localtype.xml#otherSettlement"
        self.assertEqual(uri_fragment_or_terminal(county_type), "GreaterLondonAuthority")
        self.assertEqual(uri_fragment_or_terminal(place_type), "otherSettlement")


if __name__ == "__main__":
    unittest.main()
