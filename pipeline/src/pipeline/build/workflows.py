"""Build bundle, pass execution, verification, and publish workflows for Pipeline V3."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from pipeline.config import (
    frequency_weights_config_path,
    source_schema_config_path,
)
from pipeline.manifest import BUILD_PROFILES, BuildBundleManifest
from pipeline.util.normalise import postcode_display, postcode_norm, street_casefold


class BuildError(RuntimeError):
    """Raised for build lifecycle errors."""


@dataclass(frozen=True)
class BuildBundleResult:
    bundle_id: str
    status: str
    bundle_hash: str


@dataclass(frozen=True)
class BuildRunResult:
    build_run_id: str
    status: str
    dataset_version: str
    message: str


@dataclass(frozen=True)
class VerifyResult:
    build_run_id: str
    status: str
    object_hashes: dict[str, str]


@dataclass(frozen=True)
class PublishResult:
    build_run_id: str
    dataset_version: str
    status: str


PASS_ORDER = (
    "0a_raw_ingest",
    "0b_stage_normalisation",
    "1_onspd_backbone",
    "2_gb_canonical_streets",
    "3_open_names_candidates",
    "4_uprn_reinforcement",
    "5_gb_spatial_fallback",
    "6_ni_candidates",
    "7_ppd_gap_fill",
    "8_finalisation",
)

RAW_TABLE_BY_SOURCE = {
    "onspd": "raw.onspd_row",
    "os_open_usrn": "raw.os_open_usrn_row",
    "os_open_names": "raw.os_open_names_row",
    "os_open_roads": "raw.os_open_roads_row",
    "os_open_uprn": "raw.os_open_uprn_row",
    "os_open_linked_identifiers": "raw.os_open_linked_identifiers_row",
    "nsul": "raw.nsul_row",
    "osni_gazetteer": "raw.osni_gazetteer_row",
    "dfi_highway": "raw.dfi_highway_row",
    "ppd": "raw.ppd_row",
}

CANDIDATE_TYPES = (
    "names_postcode_feature",
    "oli_toid_usrn",
    "uprn_usrn",
    "spatial_os_open_roads",
    "osni_gazetteer_direct",
    "spatial_dfi_highway",
    "ppd_parse_matched",
    "ppd_parse_unmatched",
)


def _load_json_config(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise BuildError(f"Invalid JSON config: {path}") from exc
    if not isinstance(payload, dict):
        raise BuildError(f"Config root must be object: {path}")
    return payload


def _schema_config() -> dict[str, Any]:
    return _load_json_config(source_schema_config_path())


def _weight_config() -> dict[str, Decimal]:
    payload = _load_json_config(frequency_weights_config_path())
    raw_weights = payload.get("weights")
    if not isinstance(raw_weights, dict):
        raise BuildError("frequency_weights config must contain object key 'weights'")

    parsed: dict[str, Decimal] = {}
    for key, value in raw_weights.items():
        if not isinstance(key, str):
            raise BuildError("frequency weight keys must be strings")
        try:
            weight = Decimal(str(value))
        except Exception as exc:  # pragma: no cover
            raise BuildError(f"Invalid frequency weight for {key}: {value}") from exc
        parsed[key] = weight

    missing = sorted(set(CANDIDATE_TYPES) - set(parsed.keys()))
    if missing:
        raise BuildError(
            "frequency_weights missing candidate types: " + ", ".join(missing)
        )

    for candidate_type, weight in parsed.items():
        if weight <= Decimal("0"):
            raise BuildError(
                f"frequency weight must be > 0 for candidate_type={candidate_type}; got {weight}"
            )

    unknown = sorted(set(parsed.keys()) - set(CANDIDATE_TYPES))
    if unknown:
        raise BuildError(
            "frequency_weights has unknown candidate types: " + ", ".join(unknown)
        )

    return {candidate_type: parsed[candidate_type] for candidate_type in CANDIDATE_TYPES}


def _bundle_hash(build_profile: str, source_runs: dict[str, tuple[str, ...]]) -> str:
    normalized_source_runs = {
        source_name: sorted(run_ids)
        for source_name, run_ids in source_runs.items()
    }
    payload = {
        "build_profile": build_profile,
        "source_runs": {
            key: normalized_source_runs[key] for key in sorted(normalized_source_runs.keys())
        },
    }
    encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _dataset_version_from_bundle_hash(bundle_hash: str) -> str:
    return f"v3_{bundle_hash[:12]}"


def _safe_version_suffix(dataset_version: str) -> str:
    suffix = re.sub(r"[^A-Za-z0-9_]", "_", dataset_version)
    return suffix or "v3"


def create_build_bundle(conn: psycopg.Connection, manifest: BuildBundleManifest) -> BuildBundleResult:
    bundle_hash = _bundle_hash(manifest.build_profile, manifest.source_runs)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT bundle_id
            FROM meta.build_bundle
            WHERE build_profile = %s
              AND bundle_hash = %s
            """,
            (manifest.build_profile, bundle_hash),
        )
        existing = cur.fetchone()
        if existing is not None:
            return BuildBundleResult(
                bundle_id=str(existing[0]),
                status="existing",
                bundle_hash=bundle_hash,
            )

    required_sources = BUILD_PROFILES[manifest.build_profile]
    missing = sorted(required_sources - set(manifest.source_runs.keys()))
    if missing:
        raise BuildError(
            "Bundle manifest missing required sources: " + ", ".join(missing)
        )

    with conn.cursor() as cur:
        for source_name in sorted(required_sources):
            run_ids = manifest.source_runs[source_name]
            if source_name == "ppd":
                if len(run_ids) == 0:
                    raise BuildError("Bundle must include at least one ppd ingest run")
            else:
                if len(run_ids) != 1:
                    raise BuildError(
                        f"Source {source_name} must map to exactly one ingest run in a bundle"
                    )

            for run_id in run_ids:
                cur.execute(
                    """
                    SELECT source_name
                    FROM meta.ingest_run
                    WHERE run_id = %s
                    """,
                    (run_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise BuildError(f"Unknown ingest_run_id for source {source_name}: {run_id}")
                if row[0] != source_name:
                    raise BuildError(
                        f"Ingest run/source mismatch: source={source_name} run_id={run_id} row_source={row[0]}"
                    )

    bundle_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO meta.build_bundle (
                bundle_id,
                build_profile,
                bundle_hash,
                status,
                created_at_utc
            ) VALUES (%s, %s, %s, 'created', now())
            """,
            (bundle_id, manifest.build_profile, bundle_hash),
        )

        for source_name, run_ids in manifest.source_runs.items():
            for ingest_run_id in run_ids:
                cur.execute(
                    """
                    INSERT INTO meta.build_bundle_source (
                        bundle_id,
                        source_name,
                        ingest_run_id
                    ) VALUES (%s, %s, %s)
                    """,
                    (bundle_id, source_name, ingest_run_id),
                )

    return BuildBundleResult(bundle_id=bundle_id, status="created", bundle_hash=bundle_hash)


def _load_bundle(
    conn: psycopg.Connection,
    bundle_id: str,
) -> tuple[str, str, str, dict[str, tuple[str, ...]]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT build_profile, bundle_hash, status
            FROM meta.build_bundle
            WHERE bundle_id = %s
            FOR UPDATE
            """,
            (bundle_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise BuildError(f"Bundle not found: {bundle_id}")
        build_profile, bundle_hash, status = row

        cur.execute(
            """
            SELECT source_name, ingest_run_id::text
            FROM meta.build_bundle_source
            WHERE bundle_id = %s
            """,
            (bundle_id,),
        )
        source_rows = cur.fetchall()

    source_runs_map: dict[str, list[str]] = {}
    for source_name, ingest_run_id in source_rows:
        source_runs_map.setdefault(source_name, []).append(ingest_run_id)

    source_runs: dict[str, tuple[str, ...]] = {
        source_name: tuple(sorted(run_ids))
        for source_name, run_ids in source_runs_map.items()
    }

    required = BUILD_PROFILES[build_profile]
    missing = sorted(required - set(source_runs.keys()))
    if missing:
        raise BuildError(
            f"Bundle {bundle_id} missing required sources for profile {build_profile}: {', '.join(missing)}"
        )

    return build_profile, bundle_hash, status, source_runs


def _latest_resumable_run(conn: psycopg.Connection, bundle_id: str) -> tuple[str, str] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT build_run_id::text, dataset_version
            FROM meta.build_run
            WHERE bundle_id = %s
              AND status IN ('started', 'failed')
            ORDER BY started_at_utc DESC
            LIMIT 1
            """,
            (bundle_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return row[0], row[1]


def _load_completed_passes(conn: psycopg.Connection, build_run_id: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT pass_name
            FROM meta.build_pass_checkpoint
            WHERE build_run_id = %s
            """,
            (build_run_id,),
        )
        return {row[0] for row in cur.fetchall()}


def _single_source_run(source_runs: dict[str, tuple[str, ...]], source_name: str) -> str:
    run_ids = source_runs.get(source_name, ())
    if len(run_ids) != 1:
        raise BuildError(
            f"Source {source_name} requires exactly one ingest run in bundle; found {len(run_ids)}"
        )
    return run_ids[0]


def _ordered_run_ids(conn: psycopg.Connection, run_ids: tuple[str, ...]) -> tuple[str, ...]:
    if not run_ids:
        return ()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id::text
            FROM meta.ingest_run
            WHERE run_id = ANY(%s::uuid[])
            ORDER BY retrieved_at_utc ASC, run_id ASC
            """,
            (list(run_ids),),
        )
        ordered = tuple(row[0] for row in cur.fetchall())
    if len(ordered) != len(run_ids):
        raise BuildError("One or more ingest run IDs could not be resolved for ordered execution")
    return ordered


def _create_build_run(conn: psycopg.Connection, bundle_id: str, dataset_version: str) -> str:
    build_run_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO meta.build_run (
                build_run_id,
                bundle_id,
                dataset_version,
                status,
                current_pass,
                started_at_utc
            ) VALUES (%s, %s, %s, 'started', 'initialising', now())
            """,
            (build_run_id, bundle_id, dataset_version),
        )
    return build_run_id


def _set_build_run_pass(conn: psycopg.Connection, build_run_id: str, pass_name: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE meta.build_run
            SET current_pass = %s
            WHERE build_run_id = %s
            """,
            (pass_name, build_run_id),
        )


def _mark_pass_checkpoint(
    conn: psycopg.Connection,
    build_run_id: str,
    pass_name: str,
    row_count_summary: dict[str, int],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO meta.build_pass_checkpoint (
                build_run_id,
                pass_name,
                completed_at_utc,
                row_count_summary_json
            ) VALUES (%s, %s, now(), %s)
            ON CONFLICT (build_run_id, pass_name)
            DO UPDATE SET
                completed_at_utc = EXCLUDED.completed_at_utc,
                row_count_summary_json = EXCLUDED.row_count_summary_json
            """,
            (build_run_id, pass_name, Jsonb(row_count_summary)),
        )


def _mark_build_failed(conn: psycopg.Connection, build_run_id: str, current_pass: str, error_text: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE meta.build_run
            SET status = 'failed',
                current_pass = %s,
                error_text = %s,
                finished_at_utc = now()
            WHERE build_run_id = %s
            """,
            (current_pass, error_text, build_run_id),
        )


def _mark_build_built(conn: psycopg.Connection, bundle_id: str, build_run_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE meta.build_run
            SET status = 'built',
                current_pass = 'complete',
                finished_at_utc = now(),
                error_text = NULL
            WHERE build_run_id = %s
            """,
            (build_run_id,),
        )
        cur.execute(
            """
            UPDATE meta.build_bundle
            SET status = 'built'
            WHERE bundle_id = %s
            """,
            (bundle_id,),
        )


def _load_raw_rows(conn: psycopg.Connection, raw_table: str, ingest_run_id: str) -> list[dict[str, Any]]:
    schema_name, table_name = raw_table.split(".", 1)
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT payload_jsonb
                FROM {}.{}
                WHERE ingest_run_id = %s
                ORDER BY source_row_num ASC
                """
            ).format(sql.Identifier(schema_name), sql.Identifier(table_name)),
            (ingest_run_id,),
        )
        return [row[0] for row in cur.fetchall()]


def _mapped_fields_for_source(schema_config: dict[str, Any], source_name: str) -> tuple[dict[str, str], tuple[str, ...]]:
    sources = schema_config.get("sources")
    if not isinstance(sources, dict):
        raise BuildError("source_schema.yaml missing object key 'sources'")

    source_cfg = sources.get(source_name)
    if not isinstance(source_cfg, dict):
        raise BuildError(f"source_schema.yaml missing source block: {source_name}")

    field_map_raw = source_cfg.get("field_map")
    required_raw = source_cfg.get("required_fields")
    if not isinstance(field_map_raw, dict):
        raise BuildError(f"source_schema.yaml source {source_name} missing field_map object")
    if not isinstance(required_raw, list):
        raise BuildError(f"source_schema.yaml source {source_name} missing required_fields list")

    field_map: dict[str, str] = {}
    for key, value in field_map_raw.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise BuildError(f"source_schema field_map for {source_name} must be string:string")
        field_map[key] = value

    required_fields = []
    for item in required_raw:
        if not isinstance(item, str):
            raise BuildError(f"source_schema required_fields for {source_name} must be strings")
        if item not in field_map:
            raise BuildError(
                f"source_schema required field '{item}' missing from field_map for {source_name}"
            )
        required_fields.append(item)

    return field_map, tuple(required_fields)


def _assert_required_mapped_fields_present(
    source_name: str,
    rows: list[dict[str, Any]],
    field_map: dict[str, str],
    required_fields: tuple[str, ...],
) -> None:
    if not rows:
        raise BuildError(f"Raw source is empty for {source_name}; cannot stage-normalise")

    sample_keys = set(rows[0].keys())
    missing = []
    for key in required_fields:
        mapped = field_map[key]
        if mapped not in sample_keys:
            missing.append(mapped)
    if missing:
        raise BuildError(
            f"Schema mapping unresolved for {source_name}; missing mapped fields in raw rows: "
            + ", ".join(sorted(missing))
        )


def _schema_insert_rows(
    conn: psycopg.Connection,
    query: sql.SQL,
    rows: list[tuple[Any, ...]],
) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(query, rows)
    return len(rows)


def _stage_cleanup(conn: psycopg.Connection, build_run_id: str) -> None:
    tables = (
        "stage.ppd_parsed_address",
        "stage.dfi_road_segment",
        "stage.osni_street_point",
        "stage.nsul_uprn_postcode",
        "stage.oli_uprn_usrn",
        "stage.oli_toid_usrn",
        "stage.uprn_point",
        "stage.open_roads_segment",
        "stage.open_names_road_feature",
        "stage.streets_usrn_input",
        "stage.onspd_postcode",
    )
    with conn.cursor() as cur:
        for table in tables:
            schema_name, table_name = table.split(".", 1)
            cur.execute(
                sql.SQL("DELETE FROM {}.{} WHERE build_run_id = %s").format(
                    sql.Identifier(schema_name),
                    sql.Identifier(table_name),
                ),
                (build_run_id,),
            )


def _pass_0a_raw_ingest(
    conn: psycopg.Connection,
    build_run_id: str,
    source_runs: dict[str, tuple[str, ...]],
) -> dict[str, int]:
    counts: dict[str, int] = {}
    with conn.cursor() as cur:
        for source_name, run_ids in sorted(source_runs.items()):
            raw_table = RAW_TABLE_BY_SOURCE[source_name]
            schema_name, table_name = raw_table.split(".", 1)
            total_row_count = 0
            for ingest_run_id in run_ids:
                cur.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}.{} WHERE ingest_run_id = %s").format(
                        sql.Identifier(schema_name),
                        sql.Identifier(table_name),
                    ),
                    (ingest_run_id,),
                )
                row_count = int(cur.fetchone()[0])
                if row_count <= 0:
                    raise BuildError(
                        "Pass 0a failed: source has no raw rows for "
                        f"source={source_name} run={ingest_run_id}"
                    )
                total_row_count += row_count
            counts[source_name] = total_row_count
    return counts


def _country_enrichment_available(country_iso2: str, subdivision_code: str | None) -> bool:
    if subdivision_code in {"GB-ENG", "GB-SCT", "GB-WLS", "GB-NIR"}:
        return True
    if country_iso2 == "GB":
        return True
    return False


def _populate_stage_onspd(
    conn: psycopg.Connection,
    build_run_id: str,
    ingest_run_id: str,
    field_map: dict[str, str],
    required_fields: tuple[str, ...],
) -> int:
    rows = _load_raw_rows(conn, "raw.onspd_row", ingest_run_id)
    _assert_required_mapped_fields_present("onspd", rows, field_map, required_fields)

    payload: list[tuple[Any, ...]] = []
    for row in rows:
        postcode_raw = row.get(field_map["postcode"])
        postcode_n = postcode_norm(str(postcode_raw) if postcode_raw is not None else None)
        postcode_d = postcode_display(str(postcode_raw) if postcode_raw is not None else None)
        if postcode_n is None or postcode_d is None:
            continue

        status_raw = row.get(field_map["status"])
        status = (str(status_raw).strip().lower() if status_raw is not None else "active") or "active"

        country_iso2 = str(row.get(field_map["country_iso2"], "")).strip().upper()
        country_iso3 = str(row.get(field_map["country_iso3"], "")).strip().upper()
        subdivision_code_raw = row.get(field_map["subdivision_code"])
        subdivision_code = (
            str(subdivision_code_raw).strip().upper() if subdivision_code_raw is not None else None
        )
        subdivision_code = subdivision_code or None

        lat_raw = row.get(field_map["lat"])
        lon_raw = row.get(field_map["lon"])
        easting_raw = row.get(field_map["easting"])
        northing_raw = row.get(field_map["northing"])

        lat: Decimal | None
        lon: Decimal | None
        try:
            lat = Decimal(str(lat_raw)).quantize(Decimal("0.000001")) if lat_raw not in (None, "") else None
            lon = Decimal(str(lon_raw)).quantize(Decimal("0.000001")) if lon_raw not in (None, "") else None
        except Exception:
            lat = None
            lon = None

        try:
            easting = int(float(easting_raw)) if easting_raw not in (None, "") else None
            northing = int(float(northing_raw)) if northing_raw not in (None, "") else None
        except Exception:
            easting = None
            northing = None

        post_town_raw = row.get(field_map["post_town"])
        locality_raw = row.get(field_map["locality"])

        payload.append(
            (
                build_run_id,
                postcode_n,
                postcode_d,
                status,
                lat,
                lon,
                easting,
                northing,
                country_iso2,
                country_iso3,
                subdivision_code,
                str(post_town_raw).strip().upper() if post_town_raw not in (None, "") else None,
                str(locality_raw).strip().upper() if locality_raw not in (None, "") else None,
                _country_enrichment_available(country_iso2, subdivision_code),
                ingest_run_id,
            )
        )

    return _schema_insert_rows(
        conn,
        sql.SQL(
            """
            INSERT INTO stage.onspd_postcode (
                build_run_id,
                postcode_norm,
                postcode_display,
                status,
                lat,
                lon,
                easting,
                northing,
                country_iso2,
                country_iso3,
                subdivision_code,
                post_town,
                locality,
                street_enrichment_available,
                onspd_run_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
        ),
        payload,
    )


def _populate_stage_usrn(
    conn: psycopg.Connection,
    build_run_id: str,
    ingest_run_id: str,
    field_map: dict[str, str],
    required_fields: tuple[str, ...],
) -> int:
    rows = _load_raw_rows(conn, "raw.os_open_usrn_row", ingest_run_id)
    _assert_required_mapped_fields_present("os_open_usrn", rows, field_map, required_fields)

    payload: list[tuple[Any, ...]] = []
    for row in rows:
        usrn_raw = row.get(field_map["usrn"])
        name_raw = row.get(field_map["street_name"])
        if usrn_raw in (None, "") or name_raw in (None, ""):
            continue
        try:
            usrn = int(usrn_raw)
        except Exception:
            continue
        street_name = str(name_raw).strip()
        folded = street_casefold(street_name)
        if not street_name or folded is None:
            continue

        payload.append(
            (
                build_run_id,
                usrn,
                street_name,
                folded,
                str(row.get(field_map.get("street_class", ""), "")).strip() or None,
                str(row.get(field_map.get("street_status", ""), "")).strip() or None,
                ingest_run_id,
            )
        )

    return _schema_insert_rows(
        conn,
        sql.SQL(
            """
            INSERT INTO stage.streets_usrn_input (
                build_run_id,
                usrn,
                street_name,
                street_name_casefolded,
                street_class,
                street_status,
                usrn_run_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (build_run_id, usrn)
            DO UPDATE SET
                street_name = EXCLUDED.street_name,
                street_name_casefolded = EXCLUDED.street_name_casefolded,
                street_class = EXCLUDED.street_class,
                street_status = EXCLUDED.street_status,
                usrn_run_id = EXCLUDED.usrn_run_id
            """
        ),
        payload,
    )


def _populate_stage_open_names(
    conn: psycopg.Connection,
    build_run_id: str,
    ingest_run_id: str,
    field_map: dict[str, str],
    required_fields: tuple[str, ...],
) -> int:
    rows = _load_raw_rows(conn, "raw.os_open_names_row", ingest_run_id)
    _assert_required_mapped_fields_present("os_open_names", rows, field_map, required_fields)

    payload: list[tuple[Any, ...]] = []
    for row in rows:
        feature_id_raw = row.get(field_map["feature_id"])
        street_raw = row.get(field_map["street_name"])
        postcode_raw = row.get(field_map["postcode"])
        toid_raw = row.get(field_map.get("toid", ""))
        if feature_id_raw in (None, "") or street_raw in (None, ""):
            continue

        folded = street_casefold(str(street_raw))
        postcode_n = postcode_norm(str(postcode_raw) if postcode_raw is not None else None)
        if folded is None:
            continue

        payload.append(
            (
                build_run_id,
                str(feature_id_raw).strip(),
                str(toid_raw).strip() if toid_raw not in (None, "") else None,
                postcode_n,
                str(street_raw).strip(),
                folded,
                ingest_run_id,
            )
        )

    return _schema_insert_rows(
        conn,
        sql.SQL(
            """
            INSERT INTO stage.open_names_road_feature (
                build_run_id,
                feature_id,
                toid,
                postcode_norm,
                street_name_raw,
                street_name_casefolded,
                ingest_run_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (build_run_id, feature_id)
            DO UPDATE SET
                toid = EXCLUDED.toid,
                postcode_norm = EXCLUDED.postcode_norm,
                street_name_raw = EXCLUDED.street_name_raw,
                street_name_casefolded = EXCLUDED.street_name_casefolded,
                ingest_run_id = EXCLUDED.ingest_run_id
            """
        ),
        payload,
    )


def _populate_stage_open_roads(
    conn: psycopg.Connection,
    build_run_id: str,
    ingest_run_id: str,
    field_map: dict[str, str],
    required_fields: tuple[str, ...],
) -> int:
    rows = _load_raw_rows(conn, "raw.os_open_roads_row", ingest_run_id)
    _assert_required_mapped_fields_present("os_open_roads", rows, field_map, required_fields)

    payload: list[tuple[Any, ...]] = []
    for row in rows:
        segment_id_raw = row.get(field_map["segment_id"])
        road_name_raw = row.get(field_map["road_name"])
        if segment_id_raw in (None, "") or road_name_raw in (None, ""):
            continue

        folded = street_casefold(str(road_name_raw))
        if folded is None:
            continue

        postcode_n = postcode_norm(str(row.get(field_map.get("postcode", ""), "")) or None)

        usrn_raw = row.get(field_map.get("usrn", ""))
        try:
            usrn = int(usrn_raw) if usrn_raw not in (None, "") else None
        except Exception:
            usrn = None

        payload.append(
            (
                build_run_id,
                str(segment_id_raw).strip(),
                str(row.get(field_map.get("road_id", ""), "")).strip() or None,
                postcode_n,
                usrn,
                str(road_name_raw).strip(),
                folded,
                ingest_run_id,
            )
        )

    return _schema_insert_rows(
        conn,
        sql.SQL(
            """
            INSERT INTO stage.open_roads_segment (
                build_run_id,
                segment_id,
                road_id,
                postcode_norm,
                usrn,
                road_name,
                road_name_casefolded,
                ingest_run_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (build_run_id, segment_id)
            DO UPDATE SET
                road_id = EXCLUDED.road_id,
                postcode_norm = EXCLUDED.postcode_norm,
                usrn = EXCLUDED.usrn,
                road_name = EXCLUDED.road_name,
                road_name_casefolded = EXCLUDED.road_name_casefolded,
                ingest_run_id = EXCLUDED.ingest_run_id
            """
        ),
        payload,
    )


def _populate_stage_open_uprn(
    conn: psycopg.Connection,
    build_run_id: str,
    ingest_run_id: str,
    field_map: dict[str, str],
    required_fields: tuple[str, ...],
) -> int:
    rows = _load_raw_rows(conn, "raw.os_open_uprn_row", ingest_run_id)
    _assert_required_mapped_fields_present("os_open_uprn", rows, field_map, required_fields)

    payload: list[tuple[Any, ...]] = []
    for row in rows:
        uprn_raw = row.get(field_map["uprn"])
        if uprn_raw in (None, ""):
            continue
        try:
            uprn = int(uprn_raw)
        except Exception:
            continue

        postcode_n = postcode_norm(str(row.get(field_map.get("postcode", ""), "")) or None)

        payload.append((build_run_id, uprn, postcode_n, ingest_run_id))

    return _schema_insert_rows(
        conn,
        sql.SQL(
            """
            INSERT INTO stage.uprn_point (
                build_run_id,
                uprn,
                postcode_norm,
                ingest_run_id
            ) VALUES (%s, %s, %s, %s)
            ON CONFLICT (build_run_id, uprn)
            DO UPDATE SET
                postcode_norm = EXCLUDED.postcode_norm,
                ingest_run_id = EXCLUDED.ingest_run_id
            """
        ),
        payload,
    )


def _populate_stage_oli(
    conn: psycopg.Connection,
    build_run_id: str,
    ingest_run_id: str,
    field_map: dict[str, str],
    required_fields: tuple[str, ...],
) -> tuple[int, int]:
    rows = _load_raw_rows(conn, "raw.os_open_linked_identifiers_row", ingest_run_id)
    _assert_required_mapped_fields_present(
        "os_open_linked_identifiers", rows, field_map, required_fields
    )

    toid_payload: list[tuple[Any, ...]] = []
    uprn_payload: list[tuple[Any, ...]] = []

    for row in rows:
        relation_raw = row.get(field_map["relation_type"])
        left_raw = row.get(field_map["left_id"])
        right_raw = row.get(field_map["right_id"])

        relation = str(relation_raw).strip().lower() if relation_raw not in (None, "") else ""
        if left_raw in (None, "") or right_raw in (None, ""):
            continue

        left_id = str(left_raw).strip()
        right_id = str(right_raw).strip()

        if relation in {"toid_usrn", "toid->usrn", "toid_usrn_link"}:
            try:
                usrn = int(right_id)
            except Exception:
                continue
            toid_payload.append((build_run_id, left_id, usrn, ingest_run_id))
        elif relation in {"uprn_usrn", "uprn->usrn", "uprn_usrn_link"}:
            try:
                uprn = int(left_id)
                usrn = int(right_id)
            except Exception:
                continue
            uprn_payload.append((build_run_id, uprn, usrn, ingest_run_id))

    toid_count = _schema_insert_rows(
        conn,
        sql.SQL(
            """
            INSERT INTO stage.oli_toid_usrn (
                build_run_id,
                toid,
                usrn,
                ingest_run_id
            ) VALUES (%s, %s, %s, %s)
            ON CONFLICT (build_run_id, toid, usrn)
            DO NOTHING
            """
        ),
        toid_payload,
    )

    uprn_count = _schema_insert_rows(
        conn,
        sql.SQL(
            """
            INSERT INTO stage.oli_uprn_usrn (
                build_run_id,
                uprn,
                usrn,
                ingest_run_id
            ) VALUES (%s, %s, %s, %s)
            ON CONFLICT (build_run_id, uprn, usrn)
            DO NOTHING
            """
        ),
        uprn_payload,
    )

    return toid_count, uprn_count


def _populate_stage_nsul(
    conn: psycopg.Connection,
    build_run_id: str,
    ingest_run_id: str,
    field_map: dict[str, str],
    required_fields: tuple[str, ...],
) -> int:
    rows = _load_raw_rows(conn, "raw.nsul_row", ingest_run_id)
    _assert_required_mapped_fields_present("nsul", rows, field_map, required_fields)

    payload: list[tuple[Any, ...]] = []
    for row in rows:
        uprn_raw = row.get(field_map["uprn"])
        postcode_raw = row.get(field_map["postcode"])
        if uprn_raw in (None, ""):
            continue
        try:
            uprn = int(uprn_raw)
        except Exception:
            continue
        postcode_n = postcode_norm(str(postcode_raw) if postcode_raw is not None else None)
        if postcode_n is None:
            continue
        payload.append((build_run_id, uprn, postcode_n, ingest_run_id))

    return _schema_insert_rows(
        conn,
        sql.SQL(
            """
            INSERT INTO stage.nsul_uprn_postcode (
                build_run_id,
                uprn,
                postcode_norm,
                ingest_run_id
            ) VALUES (%s, %s, %s, %s)
            ON CONFLICT (build_run_id, uprn, postcode_norm)
            DO NOTHING
            """
        ),
        payload,
    )


def _populate_stage_osni(
    conn: psycopg.Connection,
    build_run_id: str,
    ingest_run_id: str,
    field_map: dict[str, str],
    required_fields: tuple[str, ...],
) -> int:
    rows = _load_raw_rows(conn, "raw.osni_gazetteer_row", ingest_run_id)
    _assert_required_mapped_fields_present("osni_gazetteer", rows, field_map, required_fields)

    payload: list[tuple[Any, ...]] = []
    for row in rows:
        feature_id_raw = row.get(field_map["feature_id"])
        street_raw = row.get(field_map["street_name"])
        if feature_id_raw in (None, "") or street_raw in (None, ""):
            continue

        folded = street_casefold(str(street_raw))
        if folded is None:
            continue

        postcode_n = postcode_norm(str(row.get(field_map.get("postcode", ""), "")) or None)
        payload.append(
            (
                build_run_id,
                str(feature_id_raw).strip(),
                postcode_n,
                str(street_raw).strip(),
                folded,
                ingest_run_id,
            )
        )

    return _schema_insert_rows(
        conn,
        sql.SQL(
            """
            INSERT INTO stage.osni_street_point (
                build_run_id,
                feature_id,
                postcode_norm,
                street_name_raw,
                street_name_casefolded,
                ingest_run_id
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (build_run_id, feature_id)
            DO UPDATE SET
                postcode_norm = EXCLUDED.postcode_norm,
                street_name_raw = EXCLUDED.street_name_raw,
                street_name_casefolded = EXCLUDED.street_name_casefolded,
                ingest_run_id = EXCLUDED.ingest_run_id
            """
        ),
        payload,
    )


def _populate_stage_dfi(
    conn: psycopg.Connection,
    build_run_id: str,
    ingest_run_id: str,
    field_map: dict[str, str],
    required_fields: tuple[str, ...],
) -> int:
    rows = _load_raw_rows(conn, "raw.dfi_highway_row", ingest_run_id)
    _assert_required_mapped_fields_present("dfi_highway", rows, field_map, required_fields)

    payload: list[tuple[Any, ...]] = []
    for row in rows:
        segment_id_raw = row.get(field_map["segment_id"])
        street_raw = row.get(field_map["street_name"])
        if segment_id_raw in (None, "") or street_raw in (None, ""):
            continue

        folded = street_casefold(str(street_raw))
        if folded is None:
            continue
        postcode_n = postcode_norm(str(row.get(field_map.get("postcode", ""), "")) or None)

        payload.append(
            (
                build_run_id,
                str(segment_id_raw).strip(),
                postcode_n,
                str(street_raw).strip(),
                folded,
                ingest_run_id,
            )
        )

    return _schema_insert_rows(
        conn,
        sql.SQL(
            """
            INSERT INTO stage.dfi_road_segment (
                build_run_id,
                segment_id,
                postcode_norm,
                street_name_raw,
                street_name_casefolded,
                ingest_run_id
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (build_run_id, segment_id)
            DO UPDATE SET
                postcode_norm = EXCLUDED.postcode_norm,
                street_name_raw = EXCLUDED.street_name_raw,
                street_name_casefolded = EXCLUDED.street_name_casefolded,
                ingest_run_id = EXCLUDED.ingest_run_id
            """
        ),
        payload,
    )


def _populate_stage_ppd(
    conn: psycopg.Connection,
    build_run_id: str,
    ingest_run_id: str,
    field_map: dict[str, str],
    required_fields: tuple[str, ...],
) -> int:
    rows = _load_raw_rows(conn, "raw.ppd_row", ingest_run_id)
    _assert_required_mapped_fields_present("ppd", rows, field_map, required_fields)

    payload: list[tuple[Any, ...]] = []
    for row in rows:
        row_hash_raw = row.get(field_map["row_hash"])
        postcode_raw = row.get(field_map["postcode"])
        street_raw = row.get(field_map["street"])
        house_number_raw = row.get(field_map["house_number"])

        if row_hash_raw in (None, "") or postcode_raw in (None, "") or street_raw in (None, ""):
            continue

        postcode_n = postcode_norm(str(postcode_raw))
        folded = street_casefold(str(street_raw))
        if postcode_n is None or folded is None:
            continue

        payload.append(
            (
                build_run_id,
                str(row_hash_raw).strip(),
                postcode_n,
                str(house_number_raw).strip() if house_number_raw not in (None, "") else None,
                str(street_raw).strip(),
                folded,
                ingest_run_id,
            )
        )

    return _schema_insert_rows(
        conn,
        sql.SQL(
            """
            INSERT INTO stage.ppd_parsed_address (
                build_run_id,
                row_hash,
                postcode_norm,
                house_number,
                street_token_raw,
                street_token_casefolded,
                ingest_run_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (build_run_id, row_hash)
            DO UPDATE SET
                postcode_norm = EXCLUDED.postcode_norm,
                house_number = EXCLUDED.house_number,
                street_token_raw = EXCLUDED.street_token_raw,
                street_token_casefolded = EXCLUDED.street_token_casefolded,
                ingest_run_id = EXCLUDED.ingest_run_id
            """
        ),
        payload,
    )


def _pass_0b_stage_normalisation(
    conn: psycopg.Connection,
    build_run_id: str,
    source_runs: dict[str, tuple[str, ...]],
) -> dict[str, int]:
    _stage_cleanup(conn, build_run_id)
    schema_config = _schema_config()

    counts: dict[str, int] = {}

    if "onspd" in source_runs:
        field_map, required_fields = _mapped_fields_for_source(schema_config, "onspd")
        ingest_run_id = _single_source_run(source_runs, "onspd")
        counts["stage.onspd_postcode"] = _populate_stage_onspd(
            conn, build_run_id, ingest_run_id, field_map, required_fields
        )

    if "os_open_usrn" in source_runs:
        field_map, required_fields = _mapped_fields_for_source(schema_config, "os_open_usrn")
        ingest_run_id = _single_source_run(source_runs, "os_open_usrn")
        counts["stage.streets_usrn_input"] = _populate_stage_usrn(
            conn, build_run_id, ingest_run_id, field_map, required_fields
        )

    if "os_open_names" in source_runs:
        field_map, required_fields = _mapped_fields_for_source(schema_config, "os_open_names")
        ingest_run_id = _single_source_run(source_runs, "os_open_names")
        counts["stage.open_names_road_feature"] = _populate_stage_open_names(
            conn, build_run_id, ingest_run_id, field_map, required_fields
        )

    if "os_open_roads" in source_runs:
        field_map, required_fields = _mapped_fields_for_source(schema_config, "os_open_roads")
        ingest_run_id = _single_source_run(source_runs, "os_open_roads")
        counts["stage.open_roads_segment"] = _populate_stage_open_roads(
            conn, build_run_id, ingest_run_id, field_map, required_fields
        )

    if "os_open_uprn" in source_runs:
        field_map, required_fields = _mapped_fields_for_source(schema_config, "os_open_uprn")
        ingest_run_id = _single_source_run(source_runs, "os_open_uprn")
        counts["stage.uprn_point"] = _populate_stage_open_uprn(
            conn, build_run_id, ingest_run_id, field_map, required_fields
        )

    if "os_open_linked_identifiers" in source_runs:
        field_map, required_fields = _mapped_fields_for_source(schema_config, "os_open_linked_identifiers")
        ingest_run_id = _single_source_run(source_runs, "os_open_linked_identifiers")
        toid_count, uprn_count = _populate_stage_oli(
            conn, build_run_id, ingest_run_id, field_map, required_fields
        )
        counts["stage.oli_toid_usrn"] = toid_count
        counts["stage.oli_uprn_usrn"] = uprn_count

    if "nsul" in source_runs:
        field_map, required_fields = _mapped_fields_for_source(schema_config, "nsul")
        ingest_run_id = _single_source_run(source_runs, "nsul")
        counts["stage.nsul_uprn_postcode"] = _populate_stage_nsul(
            conn, build_run_id, ingest_run_id, field_map, required_fields
        )

    if "osni_gazetteer" in source_runs:
        field_map, required_fields = _mapped_fields_for_source(schema_config, "osni_gazetteer")
        ingest_run_id = _single_source_run(source_runs, "osni_gazetteer")
        counts["stage.osni_street_point"] = _populate_stage_osni(
            conn, build_run_id, ingest_run_id, field_map, required_fields
        )

    if "dfi_highway" in source_runs:
        field_map, required_fields = _mapped_fields_for_source(schema_config, "dfi_highway")
        ingest_run_id = _single_source_run(source_runs, "dfi_highway")
        counts["stage.dfi_road_segment"] = _populate_stage_dfi(
            conn, build_run_id, ingest_run_id, field_map, required_fields
        )

    if "ppd" in source_runs:
        field_map, required_fields = _mapped_fields_for_source(schema_config, "ppd")
        ppd_run_ids = source_runs["ppd"]
        if len(ppd_run_ids) == 0:
            raise BuildError("Bundle requires at least one ppd ingest run")
        ppd_rows = 0
        for ingest_run_id in _ordered_run_ids(conn, ppd_run_ids):
            ppd_rows += _populate_stage_ppd(
                conn,
                build_run_id,
                ingest_run_id,
                field_map,
                required_fields,
            )
        counts["stage.ppd_parsed_address"] = ppd_rows

    return counts


def _clear_run_outputs(conn: psycopg.Connection, build_run_id: str) -> None:
    with conn.cursor() as cur:
        for table in (
            "internal.unit_index",
            "derived.postcode_streets_final_source",
            "derived.postcode_streets_final_candidate",
            "derived.postcode_street_candidate_lineage",
            "derived.postcode_streets_final",
            "derived.postcode_street_candidates",
            "core.postcodes_meta",
            "core.streets_usrn",
            "core.postcodes",
        ):
            schema_name, table_name = table.split(".", 1)
            column_name = "produced_build_run_id"
            if table == "core.postcodes_meta":
                column_name = "produced_build_run_id"
            cur.execute(
                sql.SQL("DELETE FROM {}.{} WHERE {} = %s").format(
                    sql.Identifier(schema_name),
                    sql.Identifier(table_name),
                    sql.Identifier(column_name),
                ),
                (build_run_id,),
            )

        cur.execute("DELETE FROM meta.canonical_hash WHERE build_run_id = %s", (build_run_id,))
        cur.execute("DELETE FROM meta.build_pass_checkpoint WHERE build_run_id = %s", (build_run_id,))


def _pass_1_onspd_backbone(conn: psycopg.Connection, build_run_id: str) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO core.postcodes (
                produced_build_run_id,
                postcode,
                status,
                lat,
                lon,
                easting,
                northing,
                country_iso2,
                country_iso3,
                subdivision_code,
                post_town,
                locality,
                street_enrichment_available,
                onspd_run_id
            )
            SELECT
                build_run_id,
                postcode_display,
                status,
                lat,
                lon,
                easting,
                northing,
                country_iso2,
                country_iso3,
                subdivision_code,
                post_town,
                locality,
                street_enrichment_available,
                onspd_run_id
            FROM stage.onspd_postcode
            WHERE build_run_id = %s
            ORDER BY postcode_norm COLLATE "C" ASC
            """,
            (build_run_id,),
        )
        inserted_postcodes = cur.rowcount

        cur.execute(
            """
            INSERT INTO core.postcodes_meta (
                produced_build_run_id,
                postcode,
                meta_jsonb,
                onspd_run_id
            )
            SELECT
                build_run_id,
                postcode_display,
                jsonb_build_object(
                    'postcode_norm', postcode_norm,
                    'country_iso2', country_iso2,
                    'country_iso3', country_iso3,
                    'subdivision_code', subdivision_code,
                    'post_town', post_town,
                    'locality', locality,
                    'status', status
                ),
                onspd_run_id
            FROM stage.onspd_postcode
            WHERE build_run_id = %s
            ORDER BY postcode_norm COLLATE "C" ASC
            """,
            (build_run_id,),
        )
        inserted_meta = cur.rowcount

    return {
        "core.postcodes": int(inserted_postcodes),
        "core.postcodes_meta": int(inserted_meta),
    }


def _pass_2_gb_canonical_streets(conn: psycopg.Connection, build_run_id: str) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO core.streets_usrn (
                produced_build_run_id,
                usrn,
                street_name,
                street_name_casefolded,
                street_class,
                street_status,
                usrn_run_id
            )
            SELECT
                build_run_id,
                usrn,
                street_name,
                street_name_casefolded,
                street_class,
                street_status,
                usrn_run_id
            FROM stage.streets_usrn_input
            WHERE build_run_id = %s
            ORDER BY usrn ASC
            """,
            (build_run_id,),
        )
        inserted = cur.rowcount

    return {"core.streets_usrn": int(inserted)}


def _pass_3_open_names_candidates(conn: psycopg.Connection, build_run_id: str) -> dict[str, int]:
    schema_config = _schema_config()
    _mapped_fields_for_source(schema_config, "os_open_names")
    _mapped_fields_for_source(schema_config, "os_open_linked_identifiers")

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO derived.postcode_street_candidates (
                produced_build_run_id,
                postcode,
                street_name_raw,
                street_name_canonical,
                usrn,
                candidate_type,
                confidence,
                evidence_ref,
                source_name,
                ingest_run_id,
                evidence_json
            )
            SELECT
                %s,
                p.postcode,
                n.street_name_raw,
                n.street_name_casefolded,
                NULL,
                'names_postcode_feature',
                'medium',
                'open_names:feature:' || n.feature_id,
                'os_open_names',
                n.ingest_run_id,
                jsonb_build_object('feature_id', n.feature_id, 'toid', n.toid)
            FROM stage.open_names_road_feature AS n
            JOIN core.postcodes AS p
              ON p.produced_build_run_id = %s
             AND replace(p.postcode, ' ', '') = n.postcode_norm
            WHERE n.build_run_id = %s
            ORDER BY n.feature_id COLLATE "C" ASC
            """,
            (build_run_id, build_run_id, build_run_id),
        )
        base_inserted = cur.rowcount

    promotions_inserted = 0
    lineage_inserted = 0

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                parent.candidate_id,
                parent.postcode,
                parent.street_name_raw,
                parent.street_name_canonical,
                parent.evidence_json ->> 'toid' AS toid,
                oli.usrn,
                oli.ingest_run_id
            FROM derived.postcode_street_candidates AS parent
            JOIN stage.oli_toid_usrn AS oli
              ON oli.build_run_id = parent.produced_build_run_id
             AND oli.toid = parent.evidence_json ->> 'toid'
            WHERE parent.produced_build_run_id = %s
              AND parent.candidate_type = 'names_postcode_feature'
              AND parent.evidence_json ->> 'toid' IS NOT NULL
            ORDER BY parent.candidate_id ASC, oli.usrn ASC
            """,
            (build_run_id,),
        )
        promotion_rows = cur.fetchall()

    with conn.cursor() as cur:
        for parent_candidate_id, postcode, street_name_raw, street_name_canonical, toid, usrn, oli_run_id in promotion_rows:
            cur.execute(
                """
                INSERT INTO derived.postcode_street_candidates (
                    produced_build_run_id,
                    postcode,
                    street_name_raw,
                    street_name_canonical,
                    usrn,
                    candidate_type,
                    confidence,
                    evidence_ref,
                    source_name,
                    ingest_run_id,
                    evidence_json
                ) VALUES (%s, %s, %s, %s, %s, 'oli_toid_usrn', 'high', %s, 'os_open_linked_identifiers', %s, %s)
                RETURNING candidate_id
                """,
                (
                    build_run_id,
                    postcode,
                    street_name_raw,
                    street_name_canonical,
                    usrn,
                    f"oli:toid_usrn:{toid}",
                    oli_run_id,
                    Jsonb({"toid": toid, "usrn": usrn}),
                ),
            )
            child_candidate_id = int(cur.fetchone()[0])
            promotions_inserted += 1

            cur.execute(
                """
                INSERT INTO derived.postcode_street_candidate_lineage (
                    parent_candidate_id,
                    child_candidate_id,
                    relation_type,
                    produced_build_run_id
                ) VALUES (%s, %s, 'promotion_toid_usrn', %s)
                ON CONFLICT DO NOTHING
                """,
                (parent_candidate_id, child_candidate_id, build_run_id),
            )
            lineage_inserted += cur.rowcount

    return {
        "derived.postcode_street_candidates_base": int(base_inserted),
        "derived.postcode_street_candidates_promoted": int(promotions_inserted),
        "derived.postcode_street_candidate_lineage": int(lineage_inserted),
    }


def _pass_4_uprn_reinforcement(conn: psycopg.Connection, build_run_id: str) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH aggregate_pairs AS (
                SELECT
                    nsul.postcode_norm,
                    oli.usrn,
                    COUNT(*)::bigint AS uprn_count,
                    (ARRAY_AGG(oli.ingest_run_id ORDER BY oli.ingest_run_id::text ASC))[1] AS oli_ingest_run_id
                FROM stage.nsul_uprn_postcode AS nsul
                JOIN stage.oli_uprn_usrn AS oli
                  ON oli.build_run_id = nsul.build_run_id
                 AND oli.uprn = nsul.uprn
                WHERE nsul.build_run_id = %s
                GROUP BY nsul.postcode_norm, oli.usrn
            )
            INSERT INTO derived.postcode_street_candidates (
                produced_build_run_id,
                postcode,
                street_name_raw,
                street_name_canonical,
                usrn,
                candidate_type,
                confidence,
                evidence_ref,
                source_name,
                ingest_run_id,
                evidence_json
            )
            SELECT
                %s,
                p.postcode,
                s.street_name,
                s.street_name_casefolded,
                a.usrn,
                'uprn_usrn',
                'high',
                'oli:uprn_usrn:' || a.uprn_count::text || '_uprns',
                'os_open_linked_identifiers',
                a.oli_ingest_run_id,
                jsonb_build_object('uprn_count', a.uprn_count)
            FROM aggregate_pairs AS a
            JOIN core.postcodes AS p
              ON p.produced_build_run_id = %s
             AND replace(p.postcode, ' ', '') = a.postcode_norm
            JOIN core.streets_usrn AS s
              ON s.produced_build_run_id = %s
             AND s.usrn = a.usrn
            ORDER BY p.postcode COLLATE "C" ASC, a.usrn ASC
            """,
            (build_run_id, build_run_id, build_run_id, build_run_id),
        )
        inserted = cur.rowcount

    return {"derived.postcode_street_candidates_uprn_usrn": int(inserted)}


def _pass_5_gb_spatial_fallback(conn: psycopg.Connection, build_run_id: str) -> dict[str, int]:
    schema_config = _schema_config()
    _mapped_fields_for_source(schema_config, "os_open_roads")

    with conn.cursor() as cur:
        cur.execute(
            """
            WITH gb_postcodes_without_high AS (
                SELECT p.postcode, replace(p.postcode, ' ', '') AS postcode_norm
                FROM core.postcodes AS p
                WHERE p.produced_build_run_id = %s
                  AND p.country_iso2 = 'GB'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM derived.postcode_street_candidates AS c
                      WHERE c.produced_build_run_id = p.produced_build_run_id
                        AND c.postcode = p.postcode
                        AND c.confidence = 'high'
                  )
            ),
            ranked_segments AS (
                SELECT
                    g.postcode,
                    r.segment_id,
                    r.usrn,
                    r.road_name,
                    r.road_name_casefolded,
                    r.ingest_run_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY g.postcode
                        ORDER BY r.segment_id COLLATE "C" ASC
                    ) AS rn
                FROM gb_postcodes_without_high AS g
                JOIN stage.open_roads_segment AS r
                  ON r.build_run_id = %s
                 AND r.postcode_norm = g.postcode_norm
            )
            INSERT INTO derived.postcode_street_candidates (
                produced_build_run_id,
                postcode,
                street_name_raw,
                street_name_canonical,
                usrn,
                candidate_type,
                confidence,
                evidence_ref,
                source_name,
                ingest_run_id,
                evidence_json
            )
            SELECT
                %s,
                rs.postcode,
                rs.road_name,
                rs.road_name_casefolded,
                rs.usrn,
                'spatial_os_open_roads',
                'low',
                'spatial:os_open_roads:' || rs.segment_id || ':fallback',
                'os_open_roads',
                rs.ingest_run_id,
                jsonb_build_object('segment_id', rs.segment_id)
            FROM ranked_segments AS rs
            WHERE rs.rn = 1
            ORDER BY rs.postcode COLLATE "C" ASC
            """,
            (build_run_id, build_run_id, build_run_id),
        )
        inserted = cur.rowcount

    return {"derived.postcode_street_candidates_spatial_os_open_roads": int(inserted)}


def _pass_6_ni_candidates(conn: psycopg.Connection, build_run_id: str) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO derived.postcode_street_candidates (
                produced_build_run_id,
                postcode,
                street_name_raw,
                street_name_canonical,
                usrn,
                candidate_type,
                confidence,
                evidence_ref,
                source_name,
                ingest_run_id,
                evidence_json
            )
            SELECT
                %s,
                p.postcode,
                n.street_name_raw,
                n.street_name_casefolded,
                NULL,
                'osni_gazetteer_direct',
                'medium',
                'osni_gazetteer:feature:' || n.feature_id,
                'osni_gazetteer',
                n.ingest_run_id,
                jsonb_build_object('feature_id', n.feature_id)
            FROM stage.osni_street_point AS n
            JOIN core.postcodes AS p
              ON p.produced_build_run_id = %s
             AND replace(p.postcode, ' ', '') = n.postcode_norm
            WHERE n.build_run_id = %s
              AND p.subdivision_code = 'GB-NIR'
            ORDER BY n.feature_id COLLATE "C" ASC
            """,
            (build_run_id, build_run_id, build_run_id),
        )
        direct_inserted = cur.rowcount

        cur.execute(
            """
            WITH ni_without_candidates AS (
                SELECT p.postcode, replace(p.postcode, ' ', '') AS postcode_norm
                FROM core.postcodes AS p
                WHERE p.produced_build_run_id = %s
                  AND p.subdivision_code = 'GB-NIR'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM derived.postcode_street_candidates AS c
                      WHERE c.produced_build_run_id = p.produced_build_run_id
                        AND c.postcode = p.postcode
                  )
            ),
            ranked_segments AS (
                SELECT
                    n.postcode,
                    d.segment_id,
                    d.street_name_raw,
                    d.street_name_casefolded,
                    d.ingest_run_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY n.postcode
                        ORDER BY d.segment_id COLLATE "C" ASC
                    ) AS rn
                FROM ni_without_candidates AS n
                JOIN stage.dfi_road_segment AS d
                  ON d.build_run_id = %s
                 AND d.postcode_norm = n.postcode_norm
            )
            INSERT INTO derived.postcode_street_candidates (
                produced_build_run_id,
                postcode,
                street_name_raw,
                street_name_canonical,
                usrn,
                candidate_type,
                confidence,
                evidence_ref,
                source_name,
                ingest_run_id,
                evidence_json
            )
            SELECT
                %s,
                r.postcode,
                r.street_name_raw,
                r.street_name_casefolded,
                NULL,
                'spatial_dfi_highway',
                'low',
                'spatial:dfi_highway:' || r.segment_id || ':fallback',
                'dfi_highway',
                r.ingest_run_id,
                jsonb_build_object('segment_id', r.segment_id)
            FROM ranked_segments AS r
            WHERE r.rn = 1
            ORDER BY r.postcode COLLATE "C" ASC
            """,
            (build_run_id, build_run_id, build_run_id),
        )
        fallback_inserted = cur.rowcount

    return {
        "derived.postcode_street_candidates_osni_gazetteer_direct": int(direct_inserted),
        "derived.postcode_street_candidates_spatial_dfi_highway": int(fallback_inserted),
    }


def _pass_7_ppd_gap_fill(conn: psycopg.Connection, build_run_id: str) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH matched AS (
                SELECT
                    c.postcode,
                    p.house_number,
                    p.street_token_raw,
                    p.ingest_run_id,
                    s.usrn,
                    s.street_name,
                    s.street_name_casefolded
                FROM stage.ppd_parsed_address AS p
                JOIN core.postcodes AS c
                  ON c.produced_build_run_id = %s
                 AND replace(c.postcode, ' ', '') = p.postcode_norm
                LEFT JOIN core.streets_usrn AS s
                  ON s.produced_build_run_id = %s
                 AND s.street_name_casefolded = p.street_token_casefolded
                WHERE p.build_run_id = %s
            )
            INSERT INTO derived.postcode_street_candidates (
                produced_build_run_id,
                postcode,
                street_name_raw,
                street_name_canonical,
                usrn,
                candidate_type,
                confidence,
                evidence_ref,
                source_name,
                ingest_run_id,
                evidence_json
            )
            SELECT
                %s,
                m.postcode,
                m.street_token_raw,
                COALESCE(m.street_name_casefolded, upper(m.street_token_raw)),
                m.usrn,
                CASE WHEN m.usrn IS NULL THEN 'ppd_parse_unmatched' ELSE 'ppd_parse_matched' END,
                CASE WHEN m.usrn IS NULL THEN 'low' ELSE 'medium' END,
                'ppd:row:' || md5(m.postcode || '|' || COALESCE(m.house_number, '') || '|' || m.street_token_raw),
                'ppd',
                m.ingest_run_id,
                jsonb_build_object('house_number', m.house_number)
            FROM matched AS m
            ORDER BY m.postcode COLLATE "C" ASC
            """,
            (build_run_id, build_run_id, build_run_id, build_run_id),
        )
        candidate_inserted = cur.rowcount

        cur.execute(
            """
            WITH matched AS (
                SELECT
                    c.postcode,
                    p.house_number,
                    p.ingest_run_id,
                    s.usrn,
                    COALESCE(s.street_name, p.street_token_raw) AS street_name,
                    CASE WHEN s.usrn IS NULL THEN 'low' ELSE 'medium' END AS confidence,
                    CASE WHEN s.usrn IS NULL THEN 'ppd_parse_unmatched' ELSE 'ppd_parse_matched' END AS source_type
                FROM stage.ppd_parsed_address AS p
                JOIN core.postcodes AS c
                  ON c.produced_build_run_id = %s
                 AND replace(c.postcode, ' ', '') = p.postcode_norm
                LEFT JOIN core.streets_usrn AS s
                  ON s.produced_build_run_id = %s
                 AND s.street_name_casefolded = p.street_token_casefolded
                WHERE p.build_run_id = %s
            )
            INSERT INTO internal.unit_index (
                produced_build_run_id,
                postcode,
                house_number,
                street_name,
                usrn,
                confidence,
                source_type,
                ingest_run_id
            )
            SELECT
                %s,
                postcode,
                COALESCE(house_number, ''),
                street_name,
                usrn,
                confidence,
                source_type,
                ingest_run_id
            FROM matched
            ORDER BY postcode COLLATE "C" ASC
            """,
            (build_run_id, build_run_id, build_run_id, build_run_id),
        )
        unit_index_inserted = cur.rowcount

    return {
        "derived.postcode_street_candidates_ppd": int(candidate_inserted),
        "internal.unit_index": int(unit_index_inserted),
    }


def _confidence_from_rank(conf_rank: int) -> str:
    if conf_rank >= 3:
        return "high"
    if conf_rank == 2:
        return "medium"
    if conf_rank == 1:
        return "low"
    return "none"


def _pass_8_finalisation(conn: psycopg.Connection, build_run_id: str, dataset_version: str) -> dict[str, int]:
    weight_map = _weight_config()

    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS pg_temp.tmp_candidate_weights")
        cur.execute(
            """
            CREATE TEMP TABLE tmp_candidate_weights (
                candidate_type text PRIMARY KEY,
                weight numeric(10,4) NOT NULL
            ) ON COMMIT DROP
            """
        )
        cur.executemany(
            "INSERT INTO tmp_candidate_weights (candidate_type, weight) VALUES (%s, %s)",
            [(candidate_type, weight) for candidate_type, weight in weight_map.items()],
        )

        cur.execute("DROP TABLE IF EXISTS pg_temp.tmp_weighted_candidates")
        cur.execute(
            """
            CREATE TEMP TABLE tmp_weighted_candidates AS
            SELECT
                c.candidate_id,
                c.postcode,
                COALESCE(s.street_name, c.street_name_canonical) AS canonical_street_name,
                c.usrn,
                c.source_name,
                c.ingest_run_id,
                c.candidate_type,
                w.weight::numeric(10,4) AS weight,
                CASE c.confidence
                    WHEN 'high' THEN 3
                    WHEN 'medium' THEN 2
                    WHEN 'low' THEN 1
                    ELSE 0
                END AS conf_rank
            FROM derived.postcode_street_candidates AS c
            JOIN tmp_candidate_weights AS w
              ON w.candidate_type = c.candidate_type
            LEFT JOIN core.streets_usrn AS s
              ON s.produced_build_run_id = c.produced_build_run_id
             AND s.usrn = c.usrn
            WHERE c.produced_build_run_id = %s
            """,
            (build_run_id,),
        )

        cur.execute(
            """
            SELECT postcode
            FROM (
                SELECT postcode, SUM(weight) AS total_weight
                FROM tmp_weighted_candidates
                GROUP BY postcode
            ) AS totals
            WHERE total_weight <= 0
            LIMIT 1
            """
        )
        bad = cur.fetchone()
        if bad is not None:
            raise BuildError(
                f"Finalisation failed: total_weight <= 0 for postcode={bad[0]}"
            )

        cur.execute(
            """
            WITH grouped AS (
                SELECT
                    postcode,
                    canonical_street_name,
                    MIN(usrn) AS usrn,
                    SUM(weight) AS weighted_score,
                    MAX(conf_rank) AS conf_rank
                FROM tmp_weighted_candidates
                GROUP BY postcode, canonical_street_name
            ),
            totals AS (
                SELECT postcode, SUM(weighted_score) AS total_weight
                FROM grouped
                GROUP BY postcode
            ),
            scored AS (
                SELECT
                    g.postcode,
                    g.canonical_street_name,
                    g.usrn,
                    g.weighted_score,
                    g.conf_rank,
                    (g.weighted_score / t.total_weight) AS raw_probability
                FROM grouped AS g
                JOIN totals AS t
                  ON t.postcode = g.postcode
            ),
            rounded AS (
                SELECT
                    s.*,
                    ROUND(s.raw_probability::numeric, 4) AS rounded_probability,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.postcode
                        ORDER BY
                            s.raw_probability DESC,
                            s.conf_rank DESC,
                            s.canonical_street_name COLLATE "C" ASC,
                            s.usrn ASC NULLS LAST
                    ) AS rn,
                    SUM(ROUND(s.raw_probability::numeric, 4)) OVER (
                        PARTITION BY s.postcode
                    ) AS rounded_sum
                FROM scored AS s
            )
            SELECT
                postcode,
                canonical_street_name,
                usrn,
                weighted_score,
                conf_rank,
                CASE
                    WHEN rn = 1
                    THEN ROUND((rounded_probability + (1.0000 - rounded_sum))::numeric, 4)
                    ELSE rounded_probability
                END AS final_probability,
                rn
            FROM rounded
            ORDER BY postcode COLLATE "C" ASC, rn ASC
            """
        )
        final_rows = cur.fetchall()

    inserted_final = 0
    inserted_final_candidate = 0
    inserted_final_source = 0

    with conn.cursor() as cur:
        for postcode, street_name, usrn, weighted_score, conf_rank, probability, _rn in final_rows:
            frequency_score = Decimal(str(weighted_score)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
            probability_decimal = Decimal(str(probability)).quantize(
                Decimal("0.0001"),
                rounding=ROUND_HALF_UP,
            )
            confidence = _confidence_from_rank(int(conf_rank))

            cur.execute(
                """
                INSERT INTO derived.postcode_streets_final (
                    produced_build_run_id,
                    postcode,
                    street_name,
                    usrn,
                    confidence,
                    frequency_score,
                    probability
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING final_id
                """,
                (
                    build_run_id,
                    postcode,
                    street_name,
                    usrn,
                    confidence,
                    frequency_score,
                    probability_decimal,
                ),
            )
            final_id = int(cur.fetchone()[0])
            inserted_final += 1

            cur.execute(
                """
                SELECT candidate_id
                FROM tmp_weighted_candidates
                WHERE postcode = %s
                  AND canonical_street_name = %s
                ORDER BY candidate_id ASC
                """,
                (postcode, street_name),
            )
            candidate_ids = [int(row[0]) for row in cur.fetchall()]
            for rank, candidate_id in enumerate(candidate_ids, start=1):
                cur.execute(
                    """
                    INSERT INTO derived.postcode_streets_final_candidate (
                        final_id,
                        candidate_id,
                        produced_build_run_id,
                        link_rank
                    ) VALUES (%s, %s, %s, %s)
                    """,
                    (final_id, candidate_id, build_run_id, rank),
                )
                inserted_final_candidate += 1

            cur.execute(
                """
                SELECT source_name, ingest_run_id, candidate_type, SUM(weight) AS contribution_weight
                FROM tmp_weighted_candidates
                WHERE postcode = %s
                  AND canonical_street_name = %s
                GROUP BY source_name, ingest_run_id, candidate_type
                ORDER BY source_name COLLATE "C" ASC, ingest_run_id::text ASC, candidate_type COLLATE "C" ASC
                """,
                (postcode, street_name),
            )
            for source_name, ingest_run_id, candidate_type, contribution_weight in cur.fetchall():
                cur.execute(
                    """
                    INSERT INTO derived.postcode_streets_final_source (
                        final_id,
                        source_name,
                        ingest_run_id,
                        candidate_type,
                        contribution_weight,
                        produced_build_run_id
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        final_id,
                        source_name,
                        ingest_run_id,
                        candidate_type,
                        Decimal(str(contribution_weight)).quantize(
                            Decimal("0.0001"), rounding=ROUND_HALF_UP
                        ),
                        build_run_id,
                    ),
                )
                inserted_final_source += 1

        cur.execute(
            """
            UPDATE core.postcodes
            SET multi_street = false
            WHERE produced_build_run_id = %s
            """,
            (build_run_id,),
        )
        cur.execute(
            """
            WITH counts AS (
                SELECT postcode, COUNT(*) AS street_count
                FROM derived.postcode_streets_final
                WHERE produced_build_run_id = %s
                GROUP BY postcode
            )
            UPDATE core.postcodes AS p
            SET multi_street = (c.street_count > 1)
            FROM counts AS c
            WHERE p.produced_build_run_id = %s
              AND p.postcode = c.postcode
            """,
            (build_run_id, build_run_id),
        )

    projection_counts = _create_api_projection_tables(conn, build_run_id, dataset_version)

    return {
        "derived.postcode_streets_final": inserted_final,
        "derived.postcode_streets_final_candidate": inserted_final_candidate,
        "derived.postcode_streets_final_source": inserted_final_source,
        **projection_counts,
    }


def _create_api_projection_tables(
    conn: psycopg.Connection,
    build_run_id: str,
    dataset_version: str,
) -> dict[str, int]:
    suffix = _safe_version_suffix(dataset_version)
    street_table_name = f"postcode_street_lookup__{suffix}"
    lookup_table_name = f"postcode_lookup__{suffix}"

    street_ident = sql.Identifier(street_table_name)
    lookup_ident = sql.Identifier(lookup_table_name)

    with conn.cursor() as cur:
        cur.execute(sql.SQL("DROP TABLE IF EXISTS api.{} CASCADE").format(street_ident))
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE api.{} AS
                SELECT
                    f.postcode,
                    f.street_name,
                    f.usrn,
                    f.confidence,
                    f.frequency_score,
                    f.probability,
                    %s::text AS dataset_version,
                    f.produced_build_run_id
                FROM derived.postcode_streets_final AS f
                WHERE f.produced_build_run_id = %s
                ORDER BY
                    f.postcode COLLATE "C" ASC,
                    f.probability DESC,
                    f.street_name COLLATE "C" ASC,
                    f.usrn ASC NULLS LAST
                """
            ).format(street_ident),
            (dataset_version, build_run_id),
        )

        cur.execute(sql.SQL("DROP TABLE IF EXISTS api.{} CASCADE").format(lookup_ident))
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE api.{} AS
                WITH street_rows AS (
                    SELECT
                        s.postcode,
                        jsonb_agg(
                            jsonb_build_object(
                                'name', s.street_name,
                                'confidence', s.confidence,
                                'probability', s.probability,
                                'usrn', s.usrn
                            )
                            ORDER BY
                                s.probability DESC,
                                CASE s.confidence
                                    WHEN 'high' THEN 3
                                    WHEN 'medium' THEN 2
                                    WHEN 'low' THEN 1
                                    ELSE 0
                                END DESC,
                                s.street_name COLLATE "C" ASC,
                                s.usrn ASC NULLS LAST
                        ) AS streets_json
                    FROM api.{} AS s
                    GROUP BY s.postcode
                ),
                source_rows AS (
                    SELECT
                        dedup.postcode,
                        array_agg(dedup.source_name ORDER BY dedup.source_name COLLATE "C") AS sources
                    FROM (
                        SELECT DISTINCT
                            f.postcode,
                            fs.source_name
                        FROM derived.postcode_streets_final AS f
                        JOIN derived.postcode_streets_final_source AS fs
                          ON fs.final_id = f.final_id
                        WHERE f.produced_build_run_id = %s
                    ) AS dedup
                    GROUP BY dedup.postcode
                )
                SELECT
                    p.postcode,
                    p.status,
                    p.country_iso2,
                    p.country_iso3,
                    p.subdivision_code,
                    p.post_town,
                    p.locality,
                    p.lat,
                    p.lon,
                    p.easting,
                    p.northing,
                    p.street_enrichment_available,
                    p.multi_street,
                    COALESCE(sr.streets_json, '[]'::jsonb) AS streets_json,
                    COALESCE(src.sources, ARRAY['onspd']::text[]) AS sources,
                    %s::text AS dataset_version,
                    p.produced_build_run_id
                FROM core.postcodes AS p
                LEFT JOIN street_rows AS sr
                  ON sr.postcode = p.postcode
                LEFT JOIN source_rows AS src
                  ON src.postcode = p.postcode
                WHERE p.produced_build_run_id = %s
                ORDER BY p.postcode COLLATE "C" ASC
                """
            ).format(lookup_ident, street_ident),
            (build_run_id, dataset_version, build_run_id),
        )

        cur.execute(sql.SQL("SELECT COUNT(*) FROM api.{}").format(street_ident))
        street_count = int(cur.fetchone()[0])
        cur.execute(sql.SQL("SELECT COUNT(*) FROM api.{}").format(lookup_ident))
        lookup_count = int(cur.fetchone()[0])

    return {
        f"api.{street_table_name}": street_count,
        f"api.{lookup_table_name}": lookup_count,
    }


def _pass_handler(
    pass_name: str,
):
    handlers = {
        "0a_raw_ingest": _pass_0a_raw_ingest,
        "0b_stage_normalisation": _pass_0b_stage_normalisation,
        "1_onspd_backbone": _pass_1_onspd_backbone,
        "2_gb_canonical_streets": _pass_2_gb_canonical_streets,
        "3_open_names_candidates": _pass_3_open_names_candidates,
        "4_uprn_reinforcement": _pass_4_uprn_reinforcement,
        "5_gb_spatial_fallback": _pass_5_gb_spatial_fallback,
        "6_ni_candidates": _pass_6_ni_candidates,
        "7_ppd_gap_fill": _pass_7_ppd_gap_fill,
        "8_finalisation": _pass_8_finalisation,
    }
    return handlers[pass_name]


def run_build(
    conn: psycopg.Connection,
    bundle_id: str,
    rebuild: bool,
    resume: bool,
) -> BuildRunResult:
    if rebuild and resume:
        raise BuildError("--rebuild and --resume cannot be used together")

    build_profile, bundle_hash, _bundle_status, source_runs = _load_bundle(conn, bundle_id)
    required = BUILD_PROFILES[build_profile]
    missing = sorted(required - set(source_runs.keys()))
    if missing:
        raise BuildError(
            f"Bundle {bundle_id} missing required sources: {', '.join(missing)}"
        )
    for source_name in required:
        run_ids = source_runs.get(source_name, ())
        if source_name == "ppd":
            if len(run_ids) == 0:
                raise BuildError("Bundle must include at least one ppd ingest run")
        else:
            if len(run_ids) != 1:
                raise BuildError(
                    f"Bundle source {source_name} must include exactly one ingest run"
                )

    if resume:
        resumable = _latest_resumable_run(conn, bundle_id)
        if resumable is None:
            raise BuildError(f"No resumable run found for bundle {bundle_id}")
        build_run_id, dataset_version = resumable
        completed_passes = _load_completed_passes(conn, build_run_id)
    else:
        dataset_version = _dataset_version_from_bundle_hash(bundle_hash)
        build_run_id = _create_build_run(conn, bundle_id, dataset_version)
        completed_passes = set()
        if rebuild:
            _clear_run_outputs(conn, build_run_id)
    conn.commit()

    try:
        for pass_name in PASS_ORDER:
            if pass_name in completed_passes:
                continue

            _set_build_run_pass(conn, build_run_id, pass_name)

            handler = _pass_handler(pass_name)
            if pass_name in {"0a_raw_ingest", "0b_stage_normalisation"}:
                row_count_summary = handler(conn, build_run_id, source_runs)
            elif pass_name == "8_finalisation":
                row_count_summary = handler(conn, build_run_id, dataset_version)
            else:
                row_count_summary = handler(conn, build_run_id)

            _mark_pass_checkpoint(conn, build_run_id, pass_name, row_count_summary)
            conn.commit()

        _mark_build_built(conn, bundle_id, build_run_id)
        conn.commit()
        return BuildRunResult(
            build_run_id=build_run_id,
            status="built",
            dataset_version=dataset_version,
            message="Build completed successfully",
        )
    except Exception as exc:
        conn.rollback()
        try:
            _mark_build_failed(conn, build_run_id, pass_name, str(exc))
            conn.commit()
        except Exception:
            conn.rollback()
        raise


def _canonical_hash_query(
    conn: psycopg.Connection,
    query_sql: sql.SQL,
    params: tuple[Any, ...] = (),
) -> tuple[int, str]:
    digest = hashlib.sha256()
    row_count = 0

    cursor_name = f"canon_{uuid.uuid4().hex[:12]}"
    with conn.cursor(name=cursor_name) as cur:
        cur.execute(query_sql, params)
        for row in cur:
            row_count += 1
            normalized = []
            for value in row:
                if isinstance(value, Decimal):
                    normalized.append(str(value))
                else:
                    normalized.append(value)
            digest.update(
                json.dumps(normalized, separators=(",", ":"), ensure_ascii=True, default=str).encode("utf-8")
            )
            digest.update(b"\n")

    return row_count, digest.hexdigest()


def verify_build(conn: psycopg.Connection, build_run_id: str) -> VerifyResult:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT dataset_version, status
            FROM meta.build_run
            WHERE build_run_id = %s
            """,
            (build_run_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise BuildError(f"Build run not found: {build_run_id}")
        dataset_version, status = row
        if status not in {"built", "published"}:
            raise BuildError(f"Build run {build_run_id} must be built before verify (status={status})")

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT postcode, SUM(probability)::numeric(10,4) AS prob_sum
            FROM derived.postcode_streets_final
            WHERE produced_build_run_id = %s
            GROUP BY postcode
            HAVING SUM(probability)::numeric(10,4) <> 1.0000
            LIMIT 1
            """,
            (build_run_id,),
        )
        bad = cur.fetchone()
        if bad is not None:
            raise BuildError(
                f"Probability sum check failed for postcode={bad[0]} sum={bad[1]}"
            )

    suffix = _safe_version_suffix(dataset_version)
    street_table = f"api.postcode_street_lookup__{suffix}"
    lookup_table = f"api.postcode_lookup__{suffix}"

    specs = [
        (
            "derived_postcode_streets_final",
            sql.SQL(
                """
                SELECT postcode, street_name, usrn, confidence, frequency_score, probability
                FROM derived.postcode_streets_final
                WHERE produced_build_run_id = %s
                ORDER BY postcode COLLATE "C" ASC, street_name COLLATE "C" ASC, usrn ASC NULLS LAST
                """
            ),
            (build_run_id,),
        ),
        (
            "api_postcode_street_lookup",
            sql.SQL(
                """
                SELECT postcode, street_name, usrn, confidence, frequency_score, probability, dataset_version
                FROM api.{}
                ORDER BY postcode COLLATE "C" ASC, street_name COLLATE "C" ASC, usrn ASC NULLS LAST
                """
            ).format(sql.Identifier(f"postcode_street_lookup__{suffix}")),
            (),
        ),
        (
            "api_postcode_lookup",
            sql.SQL(
                """
                SELECT postcode, status, country_iso2, country_iso3, subdivision_code,
                       post_town, locality, lat, lon, easting, northing,
                       street_enrichment_available, multi_street, streets_json::text,
                       sources::text, dataset_version
                FROM api.{}
                ORDER BY postcode COLLATE "C" ASC
                """
            ).format(sql.Identifier(f"postcode_lookup__{suffix}")),
            (),
        ),
    ]

    object_hashes: dict[str, str] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT to_regclass(%s), to_regclass(%s)
            """,
            (street_table, lookup_table),
        )
        street_regclass, lookup_regclass = cur.fetchone()
        if street_regclass is None or lookup_regclass is None:
            raise BuildError(
                f"API projection tables not found for dataset_version={dataset_version}; expected {street_table} and {lookup_table}"
            )

    with conn.cursor() as cur:
        cur.execute("DELETE FROM meta.canonical_hash WHERE build_run_id = %s", (build_run_id,))

    for object_name, query_sql, params in specs:
        row_count, sha256_digest = _canonical_hash_query(conn, query_sql, params)
        object_hashes[object_name] = sha256_digest
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO meta.canonical_hash (
                    build_run_id,
                    object_name,
                    projection,
                    row_count,
                    sha256,
                    computed_at_utc
                ) VALUES (%s, %s, %s, %s, %s, now())
                """,
                (
                    build_run_id,
                    object_name,
                    Jsonb({"ordering": "deterministic"}),
                    row_count,
                    sha256_digest,
                ),
            )

    return VerifyResult(build_run_id=build_run_id, status="verified", object_hashes=object_hashes)


def publish_build(conn: psycopg.Connection, build_run_id: str, actor: str) -> PublishResult:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT bundle_id, dataset_version, status
            FROM meta.build_run
            WHERE build_run_id = %s
            FOR UPDATE
            """,
            (build_run_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise BuildError(f"Build run not found: {build_run_id}")
        bundle_id, dataset_version, status = row
        if status not in {"built", "published"}:
            raise BuildError(f"Build run {build_run_id} must be built before publish (status={status})")

    suffix = _safe_version_suffix(dataset_version)
    lookup_table_name = f"postcode_lookup__{suffix}"
    street_lookup_table_name = f"postcode_street_lookup__{suffix}"

    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s), to_regclass(%s)", (
            f"api.{lookup_table_name}",
            f"api.{street_lookup_table_name}",
        ))
        lookup_regclass, street_regclass = cur.fetchone()
        if lookup_regclass is None or street_regclass is None:
            raise BuildError(
                "Cannot publish: versioned api tables are missing for dataset_version="
                f"{dataset_version}"
            )

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE OR REPLACE VIEW api.postcode_lookup AS SELECT * FROM api.{}").format(
                sql.Identifier(lookup_table_name)
            )
        )
        cur.execute(
            sql.SQL(
                "CREATE OR REPLACE VIEW api.postcode_street_lookup AS SELECT * FROM api.{}"
            ).format(sql.Identifier(street_lookup_table_name))
        )

        cur.execute("SELECT txid_current()")
        publish_txid = int(cur.fetchone()[0])

        cur.execute(
            """
            INSERT INTO meta.dataset_publication (
                dataset_version,
                build_run_id,
                published_at_utc,
                published_by,
                lookup_table_name,
                street_lookup_table_name,
                publish_txid
            ) VALUES (%s, %s, now(), %s, %s, %s, %s)
            ON CONFLICT (dataset_version)
            DO UPDATE SET
                build_run_id = EXCLUDED.build_run_id,
                published_at_utc = EXCLUDED.published_at_utc,
                published_by = EXCLUDED.published_by,
                lookup_table_name = EXCLUDED.lookup_table_name,
                street_lookup_table_name = EXCLUDED.street_lookup_table_name,
                publish_txid = EXCLUDED.publish_txid
            """,
            (
                dataset_version,
                build_run_id,
                actor,
                f"api.{lookup_table_name}",
                f"api.{street_lookup_table_name}",
                publish_txid,
            ),
        )

        cur.execute(
            """
            UPDATE meta.build_run
            SET status = 'published',
                current_pass = 'published',
                finished_at_utc = COALESCE(finished_at_utc, now())
            WHERE build_run_id = %s
            """,
            (build_run_id,),
        )

        cur.execute(
            """
            UPDATE meta.build_bundle
            SET status = 'published'
            WHERE bundle_id = %s
            """,
            (bundle_id,),
        )

    return PublishResult(build_run_id=build_run_id, dataset_version=dataset_version, status="published")
