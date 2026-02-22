#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SOURCE_DIR="$ROOT_DIR/data/source_files/e2e"
MANIFEST_DIR="$ROOT_DIR/data/manifests/e2e"
RELEASE_ID="2026-Q1-E2E-P2"

mkdir -p "$SOURCE_DIR" "$MANIFEST_DIR"

cat > "$SOURCE_DIR/onsud_sample.csv" <<'CSV'
ONS_UPRN,ONS_POSTCODE,PC_UNIT_E,PC_UNIT_N,OA_CODE
1001,SW1A 2AA,530268.167,179640.532,E001
1002,SW1A 2AB,530343.656,179675.849,E002
1003,SW1A 2AC,530236.700,179784.382,E003
1004,SW1A 2AD,530165.000,179894.000,E004
CSV

cat > "$SOURCE_DIR/open_uprn_sample.csv" <<'CSV'
UPRN_REF,LAT,LON,EASTING,NORTHING,UPRN_STATUS
1001,51.5007,-0.1246,530268.167,179640.532,ACTIVE
1002,51.5010,-0.1235,530343.656,179675.849,ACTIVE
1003,51.5020,-0.1250,530236.700,179784.382,ACTIVE
1005,51.5030,-0.1260,530165.000,179894.000,ACTIVE
CSV

cat > "$SOURCE_DIR/open_roads_sample.geojson" <<'GEOJSON'
{
  "type": "FeatureCollection",
  "name": "open_roads_sample",
  "features": [
    {
      "type": "Feature",
      "properties": {
        "SRC_ID": 10,
        "ROAD_NAME": "Parliament Street"
      },
      "geometry": {
        "type": "LineString",
        "coordinates": [
          [-0.1253, 51.5001],
          [-0.1232, 51.5014]
        ]
      }
    },
    {
      "type": "Feature",
      "properties": {
        "SRC_ID": 20,
        "ROAD_NAME": "Bridge Street"
      },
      "geometry": {
        "type": "LineString",
        "coordinates": [
          [-0.1244, 51.5016],
          [-0.1231, 51.5024]
        ]
      }
    },
    {
      "type": "Feature",
      "properties": {
        "SRC_ID": 30,
        "ROAD_NAME": "A40"
      },
      "geometry": {
        "type": "LineString",
        "coordinates": [
          [-0.1262, 51.5018],
          [-0.1248, 51.5025]
        ]
      }
    }
  ]
}
GEOJSON

cat > "$SOURCE_DIR/open_names_sample.csv" <<'CSV'
ON_ID,NAME1,NAME1_LANG,NAME2,LOCAL_TYPE,GEOM_X,GEOM_Y,PC_DISTRICT
E1001,Parliament Street,,,Road,530280.000,179650.000,SW1A
E1002,Bridge Street,,,Street,530330.000,179690.000,SW1A
E1003,Western Avenue,,,Road,530225.000,179790.000,SW1A
E1004,Charing Cross,,,PopulatedPlace,530200.000,179700.000,SW1A
CSV

onsud_sha="$(shasum -a 256 "$SOURCE_DIR/onsud_sample.csv" | awk '{print $1}')"
open_uprn_sha="$(shasum -a 256 "$SOURCE_DIR/open_uprn_sample.csv" | awk '{print $1}')"
open_roads_sha="$(shasum -a 256 "$SOURCE_DIR/open_roads_sample.geojson" | awk '{print $1}')"
open_names_sha="$(shasum -a 256 "$SOURCE_DIR/open_names_sample.csv" | awk '{print $1}')"

cat > "$MANIFEST_DIR/onsud_manifest.json" <<EOF
{
  "dataset_key": "onsud",
  "release_id": "$RELEASE_ID",
  "source_url": "https://example.local/onsud-sample",
  "licence": "OGL v3.0",
  "file_path": "$SOURCE_DIR/onsud_sample.csv",
  "expected_sha256": "$onsud_sha",
  "format": "csv",
  "column_map": {
    "uprn": "ONS_UPRN",
    "postcode": "ONS_POSTCODE",
    "postcode_unit_easting": "PC_UNIT_E",
    "postcode_unit_northing": "PC_UNIT_N"
  }
}
EOF

cat > "$MANIFEST_DIR/open_uprn_manifest.json" <<EOF
{
  "dataset_key": "open_uprn",
  "release_id": "$RELEASE_ID",
  "source_url": "https://example.local/open-uprn-sample",
  "licence": "OGL v3.0",
  "file_path": "$SOURCE_DIR/open_uprn_sample.csv",
  "expected_sha256": "$open_uprn_sha",
  "format": "csv",
  "column_map": {
    "uprn": "UPRN_REF",
    "latitude": "LAT",
    "longitude": "LON",
    "easting": "EASTING",
    "northing": "NORTHING"
  }
}
EOF

cat > "$MANIFEST_DIR/open_roads_manifest.json" <<EOF
{
  "dataset_key": "open_roads",
  "release_id": "$RELEASE_ID",
  "source_url": "https://example.local/open-roads-sample",
  "licence": "OGL v3.0",
  "file_path": "$SOURCE_DIR/open_roads_sample.geojson",
  "expected_sha256": "$open_roads_sha",
  "format": "geojson",
  "layer_name": "open_roads_sample",
  "expected_srid": 27700,
  "column_map": {
    "source_id": "src_id",
    "name_display": "road_name"
  }
}
EOF

cat > "$MANIFEST_DIR/open_names_manifest.json" <<EOF
{
  "dataset_key": "open_names",
  "release_id": "$RELEASE_ID",
  "source_url": "https://example.local/open-names-sample",
  "licence": "OGL v3.0",
  "file_path": "$SOURCE_DIR/open_names_sample.csv",
  "expected_sha256": "$open_names_sha",
  "format": "csv",
  "column_map": {
    "entry_id": "ON_ID",
    "name1": "NAME1",
    "name1_lang": "NAME1_LANG",
    "name2": "NAME2",
    "local_type": "LOCAL_TYPE",
    "geometry_x": "GEOM_X",
    "geometry_y": "GEOM_Y",
    "postcode_district": "PC_DISTRICT"
  }
}
EOF

echo "Created Phase 2 source files in $SOURCE_DIR"
echo "Created manifests in $MANIFEST_DIR"
