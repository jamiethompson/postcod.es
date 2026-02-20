#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

DSN="${PIPELINE_DSN:-dbname=postcodes_v3}"
RELEASE_ID="2026-Q1-E2E-P2"

if [ ! -d .venv ]; then
  python3 -m venv .venv
fi

. .venv/bin/activate
python -m pip install -e ./pipeline >/dev/null

python -m pipeline.cli --dsn "$DSN" db migrate

./scripts/obtain_phase2_e2e_sources.sh

python -m pipeline.cli --dsn "$DSN" ingest onsud --manifest data/manifests/e2e/onsud_manifest.json
python -m pipeline.cli --dsn "$DSN" ingest open-uprn --manifest data/manifests/e2e/open_uprn_manifest.json
python -m pipeline.cli --dsn "$DSN" ingest open-roads --manifest data/manifests/e2e/open_roads_manifest.json
python -m pipeline.cli --dsn "$DSN" ingest open-names --manifest data/manifests/e2e/open_names_manifest.json

release_json="$(python -m pipeline.cli --dsn "$DSN" release create \
  --onsud-release "$RELEASE_ID" \
  --open-uprn-release "$RELEASE_ID" \
  --open-roads-release "$RELEASE_ID" \
  --open-names-release "$RELEASE_ID")"
release_set_id="$(python - <<'PY' "$release_json"
import json
import sys
print(json.loads(sys.argv[1])["release_set_id"])
PY
)"

run_one="$(python -m pipeline.cli --dsn "$DSN" run phase2-open-names --release-set-id "$release_set_id" --rebuild)"
run_one_id="$(python - <<'PY' "$run_one"
import json
import sys
print(json.loads(sys.argv[1])["run_id"])
PY
)"

run_two="$(python -m pipeline.cli --dsn "$DSN" run phase2-open-names --release-set-id "$release_set_id" --rebuild)"
run_two_id="$(python - <<'PY' "$run_two"
import json
import sys
print(json.loads(sys.argv[1])["run_id"])
PY
)"

python -m pipeline.cli --dsn "$DSN" release activate --release-set-id "$release_set_id" --actor "e2e-script" --ack-warnings

psql "$DSN" -v ON_ERROR_STOP=1 <<SQL
WITH run_a AS (
  SELECT object_name, sha256
  FROM meta.canonical_hash
  WHERE run_id = '$run_one_id'
),
run_b AS (
  SELECT object_name, sha256
  FROM meta.canonical_hash
  WHERE run_id = '$run_two_id'
)
SELECT a.object_name,
       (a.sha256 = b.sha256) AS hashes_match
FROM run_a a
JOIN run_b b USING (object_name)
ORDER BY a.object_name COLLATE "C";
SQL

echo "Phase 2 E2E run complete for release_set_id=$release_set_id"
