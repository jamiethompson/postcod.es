# Data Sources

This document is the authoritative procedure for obtaining the latest source files for pipeline builds (Phase 1 + Phase 2):

- ONSUD
- OS Open UPRN
- OS Open Roads
- OS Open Names

Rules:

- Do not guess dataset structure.
- Persist release metadata and checksums in manifests.
- Verify hashes before ingest.
- If a source does not provide an official release identifier, record that as `Unknown` and derive a deterministic local release token from retrieval date + published hash.

## 0. Source Registry

| Dataset | Official discovery endpoint | Download endpoint type | Licence handling | Update frequency |
|---|---|---|---|---|
| ONSUD | `https://geoportal.statistics.gov.uk/api/search/v1/collections/dataset/items?q=ONSUD_LATEST&limit=20` | ArcGIS item `/data` download | Read from ArcGIS item `licenseInfo` and linked ONS licence page | Published as periodic ONSUD releases (item description states ~6-week cadence) |
| OS Open UPRN | `https://api.os.uk/downloads/v1/products/OpenUPRN/downloads` | OS Downloads API artifact URL (`redirect`) | Open OS licence terms from product metadata/docs | Official cadence is not enforced in pipeline; derive from published artifact metadata |
| OS Open Roads | `https://api.os.uk/downloads/v1/products/OpenRoads/downloads` | OS Downloads API artifact URL (`redirect`) | Open OS licence terms from product metadata/docs | Official cadence is not enforced in pipeline; derive from published artifact metadata |
| OS Open Names | `https://api.os.uk/downloads/v1/products/OpenNames/downloads` | OS Downloads API artifact URL (`redirect`) | Open OS licence terms from product metadata/docs | Approximately six-monthly; derive concrete release from artifact metadata |

If licence text or cadence cannot be confirmed at ingest time, record `Unknown` and retain the raw metadata response used for the run.

## 1. Prerequisites

Required tools:

- `curl`
- `python3`
- `shasum`
- `unzip`
- `ogrinfo` (Open Roads inspection)

Suggested directories:

```bash
mkdir -p data/source_files/real data/manifests/real
```

## 2. ONSUD (Latest)

### 2.1 Discover latest ONSUD item

Use the Open Geography Portal search API:

```bash
curl -s 'https://geoportal.statistics.gov.uk/api/search/v1/collections/dataset/items?q=ONSUD_LATEST&limit=20' > /tmp/onsud_search.json
python3 - <<'PY'
import json
from pathlib import Path

payload = json.loads(Path('/tmp/onsud_search.json').read_text())
for feature in payload.get('features', []):
    p = feature.get('properties', {})
    if p.get('title') == 'ONSUD_LATEST':
        print(feature['id'])
        break
else:
    raise SystemExit('Could not find ONSUD_LATEST item id')
PY
```

Store the printed item id as `ONSUD_ITEM_ID`.

### 2.2 Retrieve metadata and download file

```bash
ONSUD_ITEM_ID='<replace_with_id>'
curl -s "https://www.arcgis.com/sharing/rest/content/items/${ONSUD_ITEM_ID}?f=json" > /tmp/onsud_item.json
python3 - <<'PY'
import json
from pathlib import Path
item = json.loads(Path('/tmp/onsud_item.json').read_text())
print('name=', item.get('name'))
print('size=', item.get('size'))
print('modified=', item.get('modified'))
PY

curl -fL --retry 3 --retry-delay 2 \
  -o data/source_files/real/ONSUD_LATEST.zip \
  "https://www.arcgis.com/sharing/rest/content/items/${ONSUD_ITEM_ID}/data"
```

### 2.3 Verify and unpack

```bash
shasum -a 256 data/source_files/real/ONSUD_LATEST.zip
unzip -l data/source_files/real/ONSUD_LATEST.zip | head -n 50
unzip -o data/source_files/real/ONSUD_LATEST.zip -d data/source_files/real/onsud
find data/source_files/real/onsud -type f -name '*.csv' | sort
```

Required ONSUD manifest mappings (explicit, no guessing):

- `uprn`
- `postcode`
- `postcode_unit_easting`
- `postcode_unit_northing`

## 3. OS Open UPRN (Latest CSV)

### 3.1 Discover latest downloadable CSV artifact

```bash
curl -s 'https://api.os.uk/downloads/v1/products/OpenUPRN/downloads' > /tmp/open_uprn_downloads.json
python3 - <<'PY'
import json
from pathlib import Path

items = json.loads(Path('/tmp/open_uprn_downloads.json').read_text())
match = next(
    i for i in items
    if i.get('area') == 'GB' and i.get('format') == 'CSV'
)
print('url=', match['url'])
print('fileName=', match['fileName'])
print('md5=', match['md5'])
print('size=', match['size'])
PY
```

### 3.2 Download and verify

```bash
OPEN_UPRN_URL="$(python3 - <<'PY'
import json
from pathlib import Path
items = json.loads(Path('/tmp/open_uprn_downloads.json').read_text())
match = next(i for i in items if i.get('area') == 'GB' and i.get('format') == 'CSV')
print(match['url'])
PY
)"

OPEN_UPRN_MD5="$(python3 - <<'PY'
import json
from pathlib import Path
items = json.loads(Path('/tmp/open_uprn_downloads.json').read_text())
match = next(i for i in items if i.get('area') == 'GB' and i.get('format') == 'CSV')
print(match['md5'])
PY
)"

curl -fL --retry 3 --retry-delay 2 \
  -o data/source_files/real/open_uprn_latest_csv.zip \
  "${OPEN_UPRN_URL}"

md5 data/source_files/real/open_uprn_latest_csv.zip
echo "Expected md5: ${OPEN_UPRN_MD5}"
```

### 3.3 Unpack and inspect columns

```bash
unzip -o data/source_files/real/open_uprn_latest_csv.zip -d data/source_files/real/open_uprn
OPEN_UPRN_CSV="$(find data/source_files/real/open_uprn -type f -name '*.csv' | head -n 1)"
echo "${OPEN_UPRN_CSV}"
head -n 1 "${OPEN_UPRN_CSV}"
```

Map these required fields explicitly in manifest:

- `uprn`
- `latitude`
- `longitude`
- `easting`
- `northing`

## 4. OS Open Roads (Latest GeoPackage)

### 4.1 Discover latest GeoPackage artifact

```bash
curl -s 'https://api.os.uk/downloads/v1/products/OpenRoads/downloads' > /tmp/open_roads_downloads.json
python3 - <<'PY'
import json
from pathlib import Path

items = json.loads(Path('/tmp/open_roads_downloads.json').read_text())
match = next(
    i for i in items
    if i.get('area') == 'GB' and i.get('format') == 'GeoPackage'
)
print('url=', match['url'])
print('fileName=', match['fileName'])
print('md5=', match['md5'])
print('size=', match['size'])
PY
```

### 4.2 Download and verify

```bash
OPEN_ROADS_URL="$(python3 - <<'PY'
import json
from pathlib import Path
items = json.loads(Path('/tmp/open_roads_downloads.json').read_text())
match = next(i for i in items if i.get('area') == 'GB' and i.get('format') == 'GeoPackage')
print(match['url'])
PY
)"

OPEN_ROADS_MD5="$(python3 - <<'PY'
import json
from pathlib import Path
items = json.loads(Path('/tmp/open_roads_downloads.json').read_text())
match = next(i for i in items if i.get('area') == 'GB' and i.get('format') == 'GeoPackage')
print(match['md5'])
PY
)"

curl -fL --retry 3 --retry-delay 2 \
  -o data/source_files/real/open_roads_latest_gpkg.zip \
  "${OPEN_ROADS_URL}"

md5 data/source_files/real/open_roads_latest_gpkg.zip
echo "Expected md5: ${OPEN_ROADS_MD5}"
```

### 4.3 Unpack and inspect layer/fields

```bash
unzip -o data/source_files/real/open_roads_latest_gpkg.zip -d data/source_files/real/open_roads
OPEN_ROADS_GPKG="$(find data/source_files/real/open_roads -type f -name '*.gpkg' | head -n 1)"
echo "${OPEN_ROADS_GPKG}"
ogrinfo "${OPEN_ROADS_GPKG}"
```

Select the layer used for named road segments and inspect schema:

```bash
ogrinfo -so "${OPEN_ROADS_GPKG}" '<layer_name>'
```

Map these required fields explicitly in manifest:

- `source_id` (stable source identifier column)
- `name_display` (road name display column)

Do not assume field names without inspecting the exact release file.

## 5. OS Open Names (Latest CSV)

### 5.1 Discover latest CSV artifact

```bash
curl -s 'https://api.os.uk/downloads/v1/products/OpenNames/downloads' > /tmp/open_names_downloads.json
python3 - <<'PY'
import json
from pathlib import Path

items = json.loads(Path('/tmp/open_names_downloads.json').read_text())
match = next(i for i in items if i.get('format') == 'CSV')
print('url=', match['url'])
print('fileName=', match['fileName'])
print('md5=', match['md5'])
print('size=', match['size'])
PY
```

### 5.2 Download and verify

```bash
OPEN_NAMES_URL="$(python3 - <<'PY'
import json
from pathlib import Path
items = json.loads(Path('/tmp/open_names_downloads.json').read_text())
match = next(i for i in items if i.get('format') == 'CSV')
print(match['url'])
PY
)"

OPEN_NAMES_MD5="$(python3 - <<'PY'
import json
from pathlib import Path
items = json.loads(Path('/tmp/open_names_downloads.json').read_text())
match = next(i for i in items if i.get('format') == 'CSV')
print(match['md5'])
PY
)"

curl -fL --retry 3 --retry-delay 2 \
  -o data/source_files/real/open_names_latest_csv.zip \
  "${OPEN_NAMES_URL}"

md5 data/source_files/real/open_names_latest_csv.zip
echo "Expected md5: ${OPEN_NAMES_MD5}"
```

### 5.3 Unpack and inspect columns

```bash
unzip -o data/source_files/real/open_names_latest_csv.zip -d data/source_files/real/open_names
OPEN_NAMES_CSV="$(find data/source_files/real/open_names -type f -name '*.csv' | head -n 1)"
echo "${OPEN_NAMES_CSV}"
head -n 1 "${OPEN_NAMES_CSV}"
```

Required Open Names manifest mappings:

- `entry_id`
- `name1`
- `name1_lang`
- `name2`
- `local_type`
- `geometry_x`
- `geometry_y`
- `postcode_district`

Ingest supports CSV only for Open Names.

## 6. Release ID Rules

Use official release identifiers where provided in source metadata.

If not provided:

- Record official identifier as `Unknown` in notes.
- Use deterministic local `release_id` for manifests:
  - `<dataset>-unknown-<YYYYMMDD>-<published_hash_prefix>`
- Example:
  - `open_roads-unknown-20260220-ebbaaaff`

This preserves reproducibility while avoiding guessed semantic versions.

## 7. Manifest Preparation Checklist

For each dataset manifest:

1. `dataset_key` is correct (`onsud`, `open_uprn`, `open_roads`, `open_names`)
2. `release_id` follows rules above
3. `source_url` is the exact URL used to download
4. `file_path` points to the local extracted file used for ingest
5. `expected_sha256` equals `shasum -a 256 <file>`
6. `column_map` is explicit and validated from inspected headers/layers
7. For Open Roads, `layer_name` is set and validated via `ogrinfo`

## 8. Operational Notes

- File sizes are large (hundreds of MB to >1 GB). Use a stable network connection.
- Prefer re-runnable shell scripts over manual ad-hoc commands.
- Keep downloaded archives and manifests together under `data/source_files/real` and `data/manifests/real` for auditability.
