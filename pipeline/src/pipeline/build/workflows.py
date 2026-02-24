"""Build bundle, pass execution, verification, and publish workflows for Pipeline V3."""

from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from pipeline.config import (
    frequency_weights_config_path,
    open_names_type_families_config_path,
    source_schema_config_path,
)
from pipeline.manifest import BUILD_PROFILES, BuildBundleManifest
from pipeline.util.normalise import (
    postcode_display,
    postcode_norm,
    street_casefold,
    text_or_none,
    uri_fragment_or_terminal,
    uri_terminal_segment,
)


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
    "os_open_lids": "raw.os_open_lids_row",
    "nsul": "raw.nsul_row",
    "osni_gazetteer": "raw.osni_gazetteer_row",
    "dfi_highway": "raw.dfi_highway_row",
    "ppd": "raw.ppd_row",
}

CANDIDATE_TYPES = (
    "names_postcode_feature",
    "open_lids_toid_usrn",
    "uprn_usrn",
    "spatial_os_open_roads",
    "osni_gazetteer_direct",
    "spatial_dfi_highway",
    "ppd_parse_matched",
    "ppd_parse_unmatched",
)

ROAD_NUMBER_PATTERN = r"^[ABM][[:space:]]*[0-9]{1,4}([[:space:]]*\([[:space:]]*M[[:space:]]*\))?$"
PASS5_SPATIAL_RADIUS_M = 150.0


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


def _open_names_family_config() -> dict[str, Any]:
    return _load_json_config(open_names_type_families_config_path())


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
    unexpected = sorted(set(manifest.source_runs.keys()) - required_sources)
    if unexpected:
        raise BuildError(
            "Bundle manifest has sources outside profile "
            f"{manifest.build_profile}: {', '.join(unexpected)}"
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
    unexpected = sorted(set(source_runs.keys()) - required)
    if unexpected:
        raise BuildError(
            f"Bundle {bundle_id} has sources outside profile {build_profile}: {', '.join(unexpected)}"
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


def _build_has_source(conn: psycopg.Connection, build_run_id: str, source_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM meta.build_run AS br
            JOIN meta.build_bundle_source AS bbs
              ON bbs.bundle_id = br.bundle_id
            WHERE br.build_run_id = %s
              AND bbs.source_name = %s
            LIMIT 1
            """,
            (build_run_id, source_name),
        )
        return cur.fetchone() is not None


def _env_flag_enabled(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


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


RAW_FETCH_BATCH_SIZE = 5000
STAGE_INSERT_BATCH_SIZE = 5000


def _iter_validated_raw_rows(
    conn: psycopg.Connection,
    *,
    source_name: str,
    raw_table: str,
    ingest_run_id: str,
    field_map: dict[str, str],
    required_fields: tuple[str, ...],
):
    schema_name, table_name = raw_table.split(".", 1)
    cursor_name = f"stage_raw_{table_name}_{uuid.uuid4().hex[:8]}"
    with conn.cursor(name=cursor_name) as cur:
        cur.itersize = RAW_FETCH_BATCH_SIZE
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
        first = cur.fetchone()
        if first is None:
            raise BuildError(f"Raw source is empty for {source_name}; cannot stage-normalise")

        first_row = first[0]
        _assert_required_mapped_fields_present(
            conn=conn,
            source_name=source_name,
            raw_table=raw_table,
            ingest_run_id=ingest_run_id,
            field_map=field_map,
            required_fields=required_fields,
        )
        yield first_row

        while True:
            chunk = cur.fetchmany(RAW_FETCH_BATCH_SIZE)
            if not chunk:
                break
            for row in chunk:
                yield row[0]


def _iter_validated_raw_rows_with_rownum(
    conn: psycopg.Connection,
    *,
    source_name: str,
    raw_table: str,
    ingest_run_id: str,
    field_map: dict[str, str],
    required_fields: tuple[str, ...],
):
    schema_name, table_name = raw_table.split(".", 1)
    cursor_name = f"stage_raw_num_{table_name}_{uuid.uuid4().hex[:8]}"
    with conn.cursor(name=cursor_name) as cur:
        cur.itersize = RAW_FETCH_BATCH_SIZE
        cur.execute(
            sql.SQL(
                """
                SELECT source_row_num, payload_jsonb
                FROM {}.{}
                WHERE ingest_run_id = %s
                ORDER BY source_row_num ASC
                """
            ).format(sql.Identifier(schema_name), sql.Identifier(table_name)),
            (ingest_run_id,),
        )
        first = cur.fetchone()
        if first is None:
            raise BuildError(f"Raw source is empty for {source_name}; cannot stage-normalise")

        _assert_required_mapped_fields_present(
            conn=conn,
            source_name=source_name,
            raw_table=raw_table,
            ingest_run_id=ingest_run_id,
            field_map=field_map,
            required_fields=required_fields,
        )
        yield int(first[0]), first[1]

        while True:
            chunk = cur.fetchmany(RAW_FETCH_BATCH_SIZE)
            if not chunk:
                break
            for row in chunk:
                yield int(row[0]), row[1]


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
    conn: psycopg.Connection,
    source_name: str,
    raw_table: str,
    ingest_run_id: str,
    field_map: dict[str, str],
    required_fields: tuple[str, ...],
) -> None:
    if len(required_fields) == 0:
        return

    schema_name, table_name = raw_table.split(".", 1)
    candidate_sets: list[tuple[str, ...]] = []
    select_clauses: list[sql.SQL] = []
    params: list[Any] = []

    for key in required_fields:
        candidates = _field_name_candidates(field_map, key)
        candidate_sets.append(candidates)
        conditions = sql.SQL(" OR ").join(sql.SQL("payload_jsonb ? %s") for _ in candidates)
        select_clauses.append(sql.SQL("COALESCE(BOOL_OR({}), FALSE)").format(conditions))
        params.extend(candidates)

    with conn.cursor() as cur:
        query = sql.SQL(
            """
            SELECT {}
            FROM {}.{}
            WHERE ingest_run_id = %s
            """
        ).format(
            sql.SQL(", ").join(select_clauses),
            sql.Identifier(schema_name),
            sql.Identifier(table_name),
        )
        cur.execute(query, (*params, ingest_run_id))
        row = cur.fetchone()

    if row is None:
        row = tuple(False for _ in candidate_sets)

    missing = []
    for index, candidates in enumerate(candidate_sets):
        if not bool(row[index]):
            missing.append("/".join(candidates))

    if missing:
        raise BuildError(
            f"Schema mapping unresolved for {source_name}; missing mapped fields in raw rows: "
            + ", ".join(sorted(missing))
        )


def _field_name_candidates(field_map: dict[str, str], logical_key: str) -> tuple[str, ...]:
    names: list[str] = []
    mapped = field_map.get(logical_key)
    if mapped:
        names.append(mapped)
    names.append(logical_key)
    legacy_aliases = {
        "id_1": ("identifier_1", "left_id"),
        "id_2": ("identifier_2", "right_id"),
        "identifier_1": ("id_1", "left_id"),
        "identifier_2": ("id_2", "right_id"),
        "left_id": ("id_1", "identifier_1"),
        "right_id": ("id_2", "identifier_2"),
    }
    aliases = legacy_aliases.get(logical_key, ())
    names.extend(aliases)

    expanded: list[str] = []
    for name in names:
        expanded.append(name)
        expanded.append(name.lower())
        expanded.append(name.upper())

    deduped: list[str] = []
    seen: set[str] = set()
    for name in expanded:
        if name not in seen:
            deduped.append(name)
            seen.add(name)
    return tuple(deduped)


def _field_value(row: dict[str, Any], field_map: dict[str, str], logical_key: str) -> Any:
    for candidate in _field_name_candidates(field_map, logical_key):
        if candidate in row:
            return row.get(candidate)
    return None


def _postcode_district_norm(value: str | None) -> str | None:
    text = (value or "").strip()
    if text == "":
        return None
    normalized = re.sub(r"[^A-Za-z0-9]", "", text.upper())
    return normalized or None


def _geom_point_wkt(x_value: Any, y_value: Any) -> str | None:
    try:
        x = int(float(x_value)) if x_value not in (None, "") else None
        y = int(float(y_value)) if y_value not in (None, "") else None
    except Exception:
        return None
    if x is None or y is None:
        return None
    return f"POINT({x} {y})"


def _open_names_family_rules() -> dict[str, tuple[str, str]]:
    payload = _open_names_family_config()
    default_family_raw = payload.get("default_type_family")
    type_map_raw = payload.get("type_to_family")
    families_raw = payload.get("families")
    if not isinstance(default_family_raw, str) or not default_family_raw.strip():
        raise BuildError("open_names_type_families missing non-empty default_type_family")
    if not isinstance(type_map_raw, dict):
        raise BuildError("open_names_type_families missing object key type_to_family")
    if not isinstance(families_raw, dict):
        raise BuildError("open_names_type_families missing object key families")

    family_rules: dict[str, tuple[str, str]] = {}
    for family_name, family_cfg in families_raw.items():
        if not isinstance(family_name, str) or not isinstance(family_cfg, dict):
            raise BuildError("open_names_type_families families entries must be objects")
        table_name = family_cfg.get("table")
        linkage_policy = family_cfg.get("linkage_policy")
        if not isinstance(table_name, str) or not table_name.startswith("stage.open_names_"):
            raise BuildError(
                f"open_names_type_families invalid table for family={family_name}: {table_name}"
            )
        if linkage_policy not in {"eligible", "context_only", "excluded"}:
            raise BuildError(
                f"open_names_type_families invalid linkage_policy for family={family_name}: {linkage_policy}"
            )
        family_rules[family_name] = (table_name, linkage_policy)

    if default_family_raw not in family_rules:
        raise BuildError(
            "open_names_type_families default_type_family not found in families: "
            f"{default_family_raw}"
        )

    resolved: dict[str, tuple[str, str]] = {}
    for type_name_raw, family_name_raw in type_map_raw.items():
        if not isinstance(type_name_raw, str) or not isinstance(family_name_raw, str):
            raise BuildError("open_names_type_families type_to_family must be string:string")
        family_name = family_name_raw.strip()
        if family_name not in family_rules:
            raise BuildError(
                f"open_names_type_families type_to_family references unknown family: {family_name}"
            )
        resolved[type_name_raw.strip().lower()] = family_rules[family_name]

    resolved["__default__"] = family_rules[default_family_raw]
    return resolved


def _open_names_family_rule(
    type_value: str | None,
    family_rules: dict[str, tuple[str, str]],
) -> tuple[str, str]:
    key = (type_value or "").strip().lower()
    if key in family_rules:
        return family_rules[key]
    return family_rules["__default__"]


def _is_open_names_road_local_type(local_type: str) -> bool:
    return "road" in local_type or "transport" in local_type


def _validated_raw_sample_row(
    conn: psycopg.Connection,
    *,
    source_name: str,
    raw_table: str,
    ingest_run_id: str,
    field_map: dict[str, str],
    required_fields: tuple[str, ...],
) -> dict[str, Any]:
    schema_name, table_name = raw_table.split(".", 1)
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                SELECT payload_jsonb
                FROM {}.{}
                WHERE ingest_run_id = %s
                ORDER BY source_row_num ASC
                LIMIT 1
                """
            ).format(sql.Identifier(schema_name), sql.Identifier(table_name)),
            (ingest_run_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise BuildError(f"Raw source is empty for {source_name}; cannot stage-normalise")

    sample_row = row[0]
    _assert_required_mapped_fields_present(
        conn=conn,
        source_name=source_name,
        raw_table=raw_table,
        ingest_run_id=ingest_run_id,
        field_map=field_map,
        required_fields=required_fields,
    )
    return sample_row


def _json_text_from_candidates(payload_expr: sql.SQL, candidates: tuple[str, ...]) -> sql.SQL:
    if len(candidates) == 0:
        return sql.SQL("NULL")
    lookups = [
        sql.SQL("{} ->> {}").format(payload_expr, sql.Literal(candidate))
        for candidate in candidates
    ]
    return sql.SQL("COALESCE({})").format(sql.SQL(", ").join(lookups))


def _json_text_for_field(payload_expr: sql.SQL, field_map: dict[str, str], logical_key: str) -> sql.SQL:
    return _json_text_from_candidates(payload_expr, _field_name_candidates(field_map, logical_key))


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


def _flush_stage_batch(
    conn: psycopg.Connection,
    query: sql.SQL,
    payload: list[tuple[Any, ...]],
) -> int:
    if not payload:
        return 0
    inserted = _schema_insert_rows(conn, query, payload)
    payload.clear()
    return inserted


STAGE_TABLES = (
    "stage.open_names_other",
    "stage.open_names_hydrography",
    "stage.open_names_landform",
    "stage.open_names_landcover",
    "stage.open_names_populated_place",
    "stage.open_names_transport_network",
    "stage.ppd_parsed_address",
    "stage.dfi_road_segment",
    "stage.osni_street_point",
    "stage.nsul_uprn_postcode",
    "stage.open_lids_uprn_usrn",
    "stage.open_lids_toid_usrn",
    "stage.open_lids_pair",
    "stage.uprn_point",
    "stage.open_roads_segment",
    "stage.open_names_postcode_feature",
    "stage.open_names_road_feature",
    "stage.streets_usrn_input",
    "stage.onspd_postcode",
)


def _analyze_relations(conn: psycopg.Connection, relations: tuple[str, ...]) -> None:
    with conn.cursor() as cur:
        for relation in relations:
            schema_name, table_name = relation.split(".", 1)
            cur.execute(
                sql.SQL("ANALYZE {}.{}").format(
                    sql.Identifier(schema_name),
                    sql.Identifier(table_name),
                )
            )


def _assert_no_other_started_build(conn: psycopg.Connection, build_run_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT build_run_id::text, current_pass
            FROM meta.build_run
            WHERE status = 'started'
              AND build_run_id <> %s
            ORDER BY started_at_utc ASC
            LIMIT 1
            """,
            (build_run_id,),
        )
        row = cur.fetchone()

    if row is not None:
        other_run_id, current_pass = row
        raise BuildError(
            "Stage truncate is unsafe while another build is in status=started; "
            f"other_build_run_id={other_run_id} other_current_pass={current_pass}"
        )


def _stage_cleanup(conn: psycopg.Connection, build_run_id: str) -> None:
    _assert_no_other_started_build(conn, build_run_id)
    table_identifiers = []
    for table in STAGE_TABLES:
        schema_name, table_name = table.split(".", 1)
        table_identifiers.append(
            sql.SQL("{}.{}").format(
                sql.Identifier(schema_name),
                sql.Identifier(table_name),
            )
        )

    with conn.cursor() as cur:
        # Stage tables are transient build artifacts; truncation keeps runtime stable
        # across rebuilds by preventing historical-row/index accumulation.
        cur.execute(
            sql.SQL("TRUNCATE TABLE {}").format(sql.SQL(", ").join(table_identifiers))
        )


def _pass_0a_raw_ingest(
    conn: psycopg.Connection,
    build_run_id: str,
    source_runs: dict[str, tuple[str, ...]],
) -> dict[str, int]:
    del build_run_id  # Pass 0a validates bundle/run metadata only.
    counts: dict[str, int] = {}
    with conn.cursor() as cur:
        for source_name, run_ids in sorted(source_runs.items()):
            total_row_count = 0
            for ingest_run_id in run_ids:
                cur.execute(
                    """
                    SELECT source_name, record_count
                    FROM meta.ingest_run
                    WHERE run_id = %s
                    """,
                    (ingest_run_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise BuildError(
                        f"Pass 0a failed: ingest run missing in metadata source={source_name} run={ingest_run_id}"
                    )
                row_source_name, record_count = row
                if row_source_name != source_name:
                    raise BuildError(
                        "Pass 0a failed: ingest run/source mismatch "
                        f"bundle_source={source_name} run_source={row_source_name} run={ingest_run_id}"
                    )
                row_count = int(record_count or 0)
                if row_count <= 0:
                    raise BuildError(
                        "Pass 0a failed: source has no recorded rows for "
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


def _onspd_country_mapping(value: str | None) -> tuple[str, str, str | None]:
    code = (value or "").strip().upper()
    mapping = {
        "E92000001": ("GB", "GBR", "GB-ENG"),
        "S92000003": ("GB", "GBR", "GB-SCT"),
        "W92000004": ("GB", "GBR", "GB-WLS"),
        "N92000002": ("GB", "GBR", "GB-NIR"),
    }
    if code in mapping:
        return mapping[code]
    if code in {"GB", "GBR"}:
        return "GB", "GBR", None
    return "GB", "GBR", None


def _normalise_onspd_status(value: str | None) -> str:
    raw = (value or "").strip()
    if raw == "":
        return "active"
    lowered = raw.lower()
    if lowered in {"active", "terminated"}:
        return lowered
    return "terminated"


def _populate_stage_onspd(
    conn: psycopg.Connection,
    build_run_id: str,
    ingest_run_id: str,
    field_map: dict[str, str],
    required_fields: tuple[str, ...],
) -> int:
    insert_sql = sql.SQL(
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
            street_enrichment_available,
            onspd_run_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
    )

    payload: list[tuple[Any, ...]] = []
    inserted = 0
    for row in _iter_validated_raw_rows(
        conn,
        source_name="onspd",
        raw_table="raw.onspd_row",
        ingest_run_id=ingest_run_id,
        field_map=field_map,
        required_fields=required_fields,
    ):
        postcode_raw = _field_value(row, field_map, "postcode")
        postcode_n = postcode_norm(str(postcode_raw) if postcode_raw is not None else None)
        postcode_d = postcode_display(str(postcode_raw) if postcode_raw is not None else None)
        if postcode_n is None or postcode_d is None:
            continue

        status_key = field_map.get("status")
        status = _normalise_onspd_status(
            str(row.get(status_key)) if status_key and row.get(status_key) is not None else None
        )

        country_key = field_map.get("subdivision_code") or field_map.get("country_iso2")
        mapped_country_value = (
            str(row.get(country_key)) if country_key and row.get(country_key) is not None else None
        )
        country_iso2, country_iso3, subdivision_code = _onspd_country_mapping(mapped_country_value)

        lat_raw = _field_value(row, field_map, "lat")
        lon_raw = _field_value(row, field_map, "lon")
        easting_raw = _field_value(row, field_map, "easting")
        northing_raw = _field_value(row, field_map, "northing")

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
                _country_enrichment_available(country_iso2, subdivision_code),
                ingest_run_id,
            )
        )
        if len(payload) >= STAGE_INSERT_BATCH_SIZE:
            inserted += _flush_stage_batch(conn, insert_sql, payload)

    inserted += _flush_stage_batch(conn, insert_sql, payload)
    return inserted


def _populate_stage_usrn(
    conn: psycopg.Connection,
    build_run_id: str,
    ingest_run_id: str,
    field_map: dict[str, str],
    required_fields: tuple[str, ...],
) -> int:
    insert_sql = sql.SQL(
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
    )

    payload: list[tuple[Any, ...]] = []
    inserted = 0
    for row in _iter_validated_raw_rows(
        conn,
        source_name="os_open_usrn",
        raw_table="raw.os_open_usrn_row",
        ingest_run_id=ingest_run_id,
        field_map=field_map,
        required_fields=required_fields,
    ):
        usrn_raw = _field_value(row, field_map, "usrn")
        name_raw = _field_value(row, field_map, "street_name")
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

        street_class_raw = _field_value(row, field_map, "street_class")
        street_status_raw = _field_value(row, field_map, "street_status")

        payload.append(
            (
                build_run_id,
                usrn,
                street_name,
                folded,
                str(street_class_raw).strip() if street_class_raw not in (None, "") else None,
                str(street_status_raw).strip() if street_status_raw not in (None, "") else None,
                ingest_run_id,
            )
        )
        if len(payload) >= STAGE_INSERT_BATCH_SIZE:
            inserted += _flush_stage_batch(conn, insert_sql, payload)

    inserted += _flush_stage_batch(conn, insert_sql, payload)
    return inserted


def _populate_stage_open_names(
    conn: psycopg.Connection,
    build_run_id: str,
    ingest_run_id: str,
    field_map: dict[str, str],
    required_fields: tuple[str, ...],
) -> tuple[int, int, int, dict[str, int]]:
    road_insert_sql = sql.SQL(
        """
        INSERT INTO stage.open_names_road_feature (
            build_run_id,
            feature_id,
            toid,
            related_toid,
            feature_toid,
            postcode_norm,
            postcode_district_norm,
            street_name_raw,
            street_name_casefolded,
            geom_bng,
            ingest_run_id
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            CASE
                WHEN %s IS NULL THEN NULL
                ELSE ST_GeomFromText(%s, 27700)
            END,
            %s
        )
        ON CONFLICT (build_run_id, feature_id)
        DO UPDATE SET
            toid = EXCLUDED.toid,
            related_toid = EXCLUDED.related_toid,
            feature_toid = EXCLUDED.feature_toid,
            postcode_norm = EXCLUDED.postcode_norm,
            postcode_district_norm = EXCLUDED.postcode_district_norm,
            street_name_raw = EXCLUDED.street_name_raw,
            street_name_casefolded = EXCLUDED.street_name_casefolded,
            geom_bng = EXCLUDED.geom_bng,
            ingest_run_id = EXCLUDED.ingest_run_id
        """
    )

    postcode_insert_sql = sql.SQL(
        """
        INSERT INTO stage.open_names_postcode_feature (
            build_run_id,
            source_row_num,
            feature_id,
            postcode_norm,
            postcode_display,
            populated_place,
            place_type,
            place_toid,
            district_borough,
            district_borough_type,
            district_borough_toid,
            county_unitary,
            county_unitary_type,
            county_unitary_toid,
            region,
            region_toid,
            country,
            geometry_x,
            geometry_y,
            ingest_run_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (build_run_id, source_row_num)
        DO UPDATE SET
            feature_id = EXCLUDED.feature_id,
            postcode_norm = EXCLUDED.postcode_norm,
            postcode_display = EXCLUDED.postcode_display,
            populated_place = EXCLUDED.populated_place,
            place_type = EXCLUDED.place_type,
            place_toid = EXCLUDED.place_toid,
            district_borough = EXCLUDED.district_borough,
            district_borough_type = EXCLUDED.district_borough_type,
            district_borough_toid = EXCLUDED.district_borough_toid,
            county_unitary = EXCLUDED.county_unitary,
            county_unitary_type = EXCLUDED.county_unitary_type,
            county_unitary_toid = EXCLUDED.county_unitary_toid,
            region = EXCLUDED.region,
            region_toid = EXCLUDED.region_toid,
            country = EXCLUDED.country,
            geometry_x = EXCLUDED.geometry_x,
            geometry_y = EXCLUDED.geometry_y,
            ingest_run_id = EXCLUDED.ingest_run_id
        """
    )

    road_payload: list[tuple[Any, ...]] = []
    postcode_payload: list[tuple[Any, ...]] = []
    family_rules = _open_names_family_rules()
    family_tables = sorted(
        {
            table_name
            for key, (table_name, _linkage_policy) in family_rules.items()
            if key != "__default__"
        }
    )
    feature_insert_sql: dict[str, sql.SQL] = {}
    feature_payloads: dict[str, list[tuple[Any, ...]]] = {}
    feature_inserted: dict[str, int] = {}
    for table_name in family_tables:
        schema_name, raw_table_name = table_name.split(".", 1)
        feature_insert_sql[table_name] = sql.SQL(
            """
            INSERT INTO {}.{} (
                build_run_id,
                source_row_num,
                feature_id,
                related_toid,
                name1,
                name2,
                type,
                local_type,
                postcode_district_norm,
                populated_place,
                district_borough,
                county_unitary,
                region,
                country,
                geom_bng,
                linkage_policy,
                ingest_run_id
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                CASE
                    WHEN %s IS NULL THEN NULL
                    ELSE ST_GeomFromText(%s, 27700)
                END,
                %s,
                %s
            )
            ON CONFLICT (build_run_id, source_row_num)
            DO UPDATE SET
                feature_id = EXCLUDED.feature_id,
                related_toid = EXCLUDED.related_toid,
                name1 = EXCLUDED.name1,
                name2 = EXCLUDED.name2,
                type = EXCLUDED.type,
                local_type = EXCLUDED.local_type,
                postcode_district_norm = EXCLUDED.postcode_district_norm,
                populated_place = EXCLUDED.populated_place,
                district_borough = EXCLUDED.district_borough,
                county_unitary = EXCLUDED.county_unitary,
                region = EXCLUDED.region,
                country = EXCLUDED.country,
                geom_bng = EXCLUDED.geom_bng,
                linkage_policy = EXCLUDED.linkage_policy,
                ingest_run_id = EXCLUDED.ingest_run_id
            """
        ).format(sql.Identifier(schema_name), sql.Identifier(raw_table_name))
        feature_payloads[table_name] = []
        feature_inserted[table_name] = 0

    road_inserted = 0
    postcode_inserted = 0

    for source_row_num, row in _iter_validated_raw_rows_with_rownum(
        conn,
        source_name="os_open_names",
        raw_table="raw.os_open_names_row",
        ingest_run_id=ingest_run_id,
        field_map=field_map,
        required_fields=required_fields,
    ):
        feature_id_raw = _field_value(row, field_map, "feature_id")
        name1_raw = _field_value(row, field_map, "street_name")
        if feature_id_raw in (None, "") or name1_raw in (None, ""):
            continue

        local_type_raw = _field_value(row, field_map, "local_type")
        local_type = str(local_type_raw).strip().lower() if local_type_raw not in (None, "") else ""

        geometry_x_raw = _field_value(row, field_map, "geometry_x")
        geometry_y_raw = _field_value(row, field_map, "geometry_y")
        geom_wkt = _geom_point_wkt(geometry_x_raw, geometry_y_raw)

        if local_type == "postcode":
            postcode_d = postcode_display(str(name1_raw))
            postcode_n = postcode_norm(str(name1_raw))
            if postcode_d is not None and postcode_n is not None:
                populated_place_raw = _field_value(row, field_map, "populated_place")
                populated_place_type_raw = _field_value(row, field_map, "populated_place_type")
                populated_place_uri_raw = _field_value(row, field_map, "populated_place_uri")
                district_borough_raw = _field_value(row, field_map, "district_borough")
                district_borough_type_raw = _field_value(row, field_map, "district_borough_type")
                district_borough_uri_raw = _field_value(row, field_map, "district_borough_uri")
                county_unitary_raw = _field_value(row, field_map, "county_unitary")
                county_unitary_type_raw = _field_value(row, field_map, "county_unitary_type")
                county_unitary_uri_raw = _field_value(row, field_map, "county_unitary_uri")
                region_raw = _field_value(row, field_map, "region")
                region_uri_raw = _field_value(row, field_map, "region_uri")
                country_raw = _field_value(row, field_map, "country")
                try:
                    geometry_x = int(float(geometry_x_raw)) if geometry_x_raw not in (None, "") else None
                except Exception:
                    geometry_x = None
                try:
                    geometry_y = int(float(geometry_y_raw)) if geometry_y_raw not in (None, "") else None
                except Exception:
                    geometry_y = None

                postcode_payload.append(
                    (
                        build_run_id,
                        source_row_num,
                        str(feature_id_raw).strip(),
                        postcode_n,
                        postcode_d,
                        text_or_none(populated_place_raw),
                        uri_fragment_or_terminal(
                            str(populated_place_type_raw) if populated_place_type_raw is not None else None
                        ),
                        uri_terminal_segment(
                            str(populated_place_uri_raw) if populated_place_uri_raw is not None else None
                        ),
                        text_or_none(district_borough_raw),
                        uri_terminal_segment(
                            str(district_borough_type_raw) if district_borough_type_raw is not None else None
                        ),
                        uri_terminal_segment(
                            str(district_borough_uri_raw) if district_borough_uri_raw is not None else None
                        ),
                        text_or_none(county_unitary_raw),
                        uri_terminal_segment(
                            str(county_unitary_type_raw) if county_unitary_type_raw is not None else None
                        ),
                        uri_terminal_segment(
                            str(county_unitary_uri_raw) if county_unitary_uri_raw is not None else None
                        ),
                        text_or_none(region_raw),
                        uri_terminal_segment(str(region_uri_raw) if region_uri_raw is not None else None),
                        text_or_none(country_raw),
                        geometry_x,
                        geometry_y,
                        ingest_run_id,
                    )
                )
                if len(postcode_payload) >= STAGE_INSERT_BATCH_SIZE:
                    postcode_inserted += _flush_stage_batch(conn, postcode_insert_sql, postcode_payload)
            continue

        if _is_open_names_road_local_type(local_type):
            folded = street_casefold(str(name1_raw))
            if folded is None:
                continue

            postcode_raw = _field_value(row, field_map, "postcode")
            toid_raw = _field_value(row, field_map, "toid")
            postcode_district_raw = _field_value(row, field_map, "postcode_district")
            postcode_n = postcode_norm(str(postcode_raw) if postcode_raw is not None else None)
            related_toid = text_or_none(str(toid_raw) if toid_raw is not None else None)

            road_payload.append(
                (
                    build_run_id,
                    str(feature_id_raw).strip(),
                    related_toid,
                    related_toid,
                    str(feature_id_raw).strip(),
                    postcode_n,
                    _postcode_district_norm(
                        str(postcode_district_raw) if postcode_district_raw is not None else None
                    ),
                    str(name1_raw).strip(),
                    folded,
                    geom_wkt,
                    geom_wkt,
                    ingest_run_id,
                )
            )
            if len(road_payload) >= STAGE_INSERT_BATCH_SIZE:
                road_inserted += _flush_stage_batch(conn, road_insert_sql, road_payload)
            continue

        type_raw = _field_value(row, field_map, "type")
        name2_raw = _field_value(row, field_map, "street_name_alt")
        postcode_district_raw = _field_value(row, field_map, "postcode_district")
        populated_place_raw = _field_value(row, field_map, "populated_place")
        district_borough_raw = _field_value(row, field_map, "district_borough")
        county_unitary_raw = _field_value(row, field_map, "county_unitary")
        region_raw = _field_value(row, field_map, "region")
        country_raw = _field_value(row, field_map, "country")
        toid_raw = _field_value(row, field_map, "toid")

        type_text = text_or_none(str(type_raw) if type_raw is not None else None) or "other"
        family_table, linkage_policy = _open_names_family_rule(type_text, family_rules)
        feature_payloads[family_table].append(
            (
                build_run_id,
                source_row_num,
                str(feature_id_raw).strip(),
                text_or_none(str(toid_raw) if toid_raw is not None else None),
                str(name1_raw).strip(),
                text_or_none(str(name2_raw) if name2_raw is not None else None),
                type_text,
                str(local_type_raw).strip() if local_type_raw not in (None, "") else "",
                _postcode_district_norm(
                    str(postcode_district_raw) if postcode_district_raw is not None else None
                ),
                text_or_none(str(populated_place_raw) if populated_place_raw is not None else None),
                text_or_none(str(district_borough_raw) if district_borough_raw is not None else None),
                text_or_none(str(county_unitary_raw) if county_unitary_raw is not None else None),
                text_or_none(str(region_raw) if region_raw is not None else None),
                text_or_none(str(country_raw) if country_raw is not None else None),
                geom_wkt,
                geom_wkt,
                linkage_policy,
                ingest_run_id,
            )
        )
        if len(feature_payloads[family_table]) >= STAGE_INSERT_BATCH_SIZE:
            feature_inserted[family_table] += _flush_stage_batch(
                conn,
                feature_insert_sql[family_table],
                feature_payloads[family_table],
            )

    road_inserted += _flush_stage_batch(conn, road_insert_sql, road_payload)
    postcode_inserted += _flush_stage_batch(conn, postcode_insert_sql, postcode_payload)
    for table_name in family_tables:
        feature_inserted[table_name] += _flush_stage_batch(
            conn,
            feature_insert_sql[table_name],
            feature_payloads[table_name],
        )

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)::bigint
            FROM (
                SELECT postcode_norm
                FROM stage.open_names_postcode_feature
                WHERE build_run_id = %s
                GROUP BY postcode_norm
                HAVING COUNT(*) > 1
            ) AS d
            """,
            (build_run_id,),
        )
        duplicate_postcode_keys = int(cur.fetchone()[0] or 0)

    family_counts = {
        table_name.removeprefix("stage.open_names_"): int(feature_inserted[table_name])
        for table_name in family_tables
    }
    return road_inserted, postcode_inserted, duplicate_postcode_keys, family_counts


def _populate_stage_open_roads(
    conn: psycopg.Connection,
    build_run_id: str,
    ingest_run_id: str,
    field_map: dict[str, str],
    required_fields: tuple[str, ...],
) -> int:
    insert_sql = sql.SQL(
        """
        INSERT INTO stage.open_roads_segment (
            build_run_id,
            segment_id,
            road_id,
            postcode_norm,
            usrn,
            road_name,
            road_name_casefolded,
            geom_bng,
            ingest_run_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, NULL, %s)
        ON CONFLICT (build_run_id, segment_id)
        DO UPDATE SET
            road_id = EXCLUDED.road_id,
            postcode_norm = EXCLUDED.postcode_norm,
            usrn = EXCLUDED.usrn,
            road_name = EXCLUDED.road_name,
            road_name_casefolded = EXCLUDED.road_name_casefolded,
            geom_bng = EXCLUDED.geom_bng,
            ingest_run_id = EXCLUDED.ingest_run_id
        """
    )

    payload: list[tuple[Any, ...]] = []
    inserted = 0
    for row in _iter_validated_raw_rows(
        conn,
        source_name="os_open_roads",
        raw_table="raw.os_open_roads_row",
        ingest_run_id=ingest_run_id,
        field_map=field_map,
        required_fields=required_fields,
    ):
        segment_id_raw = _field_value(row, field_map, "segment_id")
        road_name_raw = _field_value(row, field_map, "road_name")
        if segment_id_raw in (None, "") or road_name_raw in (None, ""):
            continue

        folded = street_casefold(str(road_name_raw))
        if folded is None:
            continue

        postcode_raw = _field_value(row, field_map, "postcode")
        postcode_n = postcode_norm(str(postcode_raw) if postcode_raw not in (None, "") else None)

        usrn_raw = _field_value(row, field_map, "usrn")
        try:
            usrn = int(usrn_raw) if usrn_raw not in (None, "") else None
        except Exception:
            usrn = None

        road_id_raw = _field_value(row, field_map, "road_id")

        payload.append(
            (
                build_run_id,
                str(segment_id_raw).strip(),
                str(road_id_raw).strip() if road_id_raw not in (None, "") else None,
                postcode_n,
                usrn,
                str(road_name_raw).strip(),
                folded,
                ingest_run_id,
            )
        )
        if len(payload) >= STAGE_INSERT_BATCH_SIZE:
            inserted += _flush_stage_batch(conn, insert_sql, payload)

    inserted += _flush_stage_batch(conn, insert_sql, payload)

    payload_expr = sql.SQL("r.payload_jsonb")
    segment_expr = _json_text_for_field(payload_expr, field_map, "segment_id")
    geometry_expr = _json_text_for_field(payload_expr, field_map, "geometry")

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                WITH extracted AS (
                    SELECT
                        r.source_row_num,
                        btrim({segment_expr}) AS segment_id_text,
                        btrim({geometry_expr}) AS geometry_hex
                    FROM raw.os_open_roads_row AS r
                    WHERE r.ingest_run_id = %s
                ),
                dedup AS (
                    SELECT DISTINCT ON (segment_id_text)
                        segment_id_text,
                        geometry_hex
                    FROM extracted
                    WHERE segment_id_text IS NOT NULL
                      AND segment_id_text <> ''
                    ORDER BY segment_id_text ASC, source_row_num DESC
                ),
                validated AS (
                    SELECT
                        d.segment_id_text,
                        d.geometry_hex,
                        decode(d.geometry_hex, 'hex') AS geom_blob
                    FROM dedup AS d
                    WHERE d.geometry_hex IS NOT NULL
                      AND d.geometry_hex <> ''
                      AND d.geometry_hex ~ '^[0-9A-Fa-f]+$'
                      AND mod(length(d.geometry_hex), 2) = 0
                ),
                header AS (
                    SELECT
                        v.segment_id_text,
                        v.geom_blob,
                        get_byte(v.geom_blob, 3) AS flags,
                        (
                            get_byte(v.geom_blob, 4)
                            + (get_byte(v.geom_blob, 5) * 256)
                            + (get_byte(v.geom_blob, 6) * 65536)
                            + (get_byte(v.geom_blob, 7) * 16777216)
                        ) AS srid
                    FROM validated AS v
                ),
                decoded AS (
                    SELECT
                        h.segment_id_text,
                        ST_SetSRID(
                            ST_GeomFromWKB(
                                substring(
                                    h.geom_blob
                                    FROM (
                                        8 + CASE ((h.flags >> 1) & 7)
                                            WHEN 0 THEN 0
                                            WHEN 1 THEN 32
                                            WHEN 2 THEN 48
                                            WHEN 3 THEN 48
                                            WHEN 4 THEN 64
                                            ELSE 0
                                        END
                                    ) + 1
                                )
                            ),
                            h.srid
                        ) AS geom_bng
                    FROM header AS h
                    WHERE h.srid = 27700
                )
                UPDATE stage.open_roads_segment AS s
                SET geom_bng = d.geom_bng
                FROM decoded AS d
                WHERE s.build_run_id = %s
                  AND s.segment_id = d.segment_id_text
                """
            ).format(segment_expr=segment_expr, geometry_expr=geometry_expr),
            (ingest_run_id, build_run_id),
        )

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                WITH extracted AS (
                    SELECT
                        btrim({geometry_expr}) AS geometry_hex
                    FROM raw.os_open_roads_row AS r
                    WHERE r.ingest_run_id = %s
                ),
                validated AS (
                    SELECT
                        decode(geometry_hex, 'hex') AS geom_blob
                    FROM extracted
                    WHERE geometry_hex IS NOT NULL
                      AND geometry_hex <> ''
                      AND geometry_hex ~ '^[0-9A-Fa-f]+$'
                      AND mod(length(geometry_hex), 2) = 0
                ),
                srids AS (
                    SELECT
                        (
                            get_byte(geom_blob, 4)
                            + (get_byte(geom_blob, 5) * 256)
                            + (get_byte(geom_blob, 6) * 65536)
                            + (get_byte(geom_blob, 7) * 16777216)
                        ) AS srid
                    FROM validated
                )
                SELECT COUNT(*)::bigint
                FROM srids
                WHERE srid <> 27700
                """
            ).format(geometry_expr=geometry_expr),
            (ingest_run_id,),
        )
        non_bng = int(cur.fetchone()[0] or 0)
        if non_bng > 0:
            raise BuildError(
                "Open Roads geometry decode failed: non-BNG SRID detected "
                f"for source run {ingest_run_id} rows={non_bng}"
            )

    return inserted


def _populate_stage_open_uprn(
    conn: psycopg.Connection,
    build_run_id: str,
    ingest_run_id: str,
    field_map: dict[str, str],
    required_fields: tuple[str, ...],
) -> int:
    _validated_raw_sample_row(
        conn,
        source_name="os_open_uprn",
        raw_table="raw.os_open_uprn_row",
        ingest_run_id=ingest_run_id,
        field_map=field_map,
        required_fields=required_fields,
    )

    payload_expr = sql.SQL("r.payload_jsonb")
    uprn_expr = _json_text_for_field(payload_expr, field_map, "uprn")
    postcode_expr = _json_text_for_field(payload_expr, field_map, "postcode")

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                WITH extracted AS (
                    SELECT
                        r.source_row_num,
                        btrim({uprn_expr}) AS uprn_text,
                        btrim({postcode_expr}) AS postcode_text
                    FROM raw.os_open_uprn_row AS r
                    WHERE r.ingest_run_id = %s
                ),
                filtered AS (
                    SELECT
                        source_row_num,
                        uprn_text::bigint AS uprn,
                        NULLIF(
                            upper(regexp_replace(COALESCE(postcode_text, ''), '[^A-Za-z0-9]', '', 'g')),
                            ''
                        ) AS postcode_norm
                    FROM extracted
                    WHERE uprn_text IS NOT NULL
                      AND uprn_text <> ''
                      AND uprn_text ~ '^[0-9]+$'
                ),
                deduped AS (
                    SELECT DISTINCT ON (uprn)
                        uprn,
                        postcode_norm
                    FROM filtered
                    ORDER BY uprn ASC, source_row_num DESC
                )
                INSERT INTO stage.uprn_point (
                    build_run_id,
                    uprn,
                    postcode_norm,
                    ingest_run_id
                )
                SELECT
                    %s,
                    d.uprn,
                    d.postcode_norm,
                    %s
                FROM deduped AS d
                ON CONFLICT (build_run_id, uprn)
                DO UPDATE SET
                    postcode_norm = EXCLUDED.postcode_norm,
                    ingest_run_id = EXCLUDED.ingest_run_id
                """
            ).format(uprn_expr=uprn_expr, postcode_expr=postcode_expr),
            (ingest_run_id, build_run_id, ingest_run_id),
        )
        return int(cur.rowcount)


def _populate_stage_open_lids(
    conn: psycopg.Connection,
    build_run_id: str,
    ingest_run_id: str,
    field_map: dict[str, str],
    required_fields: tuple[str, ...],
) -> tuple[int, int, int]:
    _validated_raw_sample_row(
        conn,
        source_name="os_open_lids",
        raw_table="raw.os_open_lids_row",
        ingest_run_id=ingest_run_id,
        field_map=field_map,
        required_fields=required_fields,
    )

    payload_expr = sql.SQL("r.payload_jsonb")
    id_1_expr = _json_text_for_field(payload_expr, field_map, "id_1")
    id_2_expr = _json_text_for_field(payload_expr, field_map, "id_2")
    relation_expr = _json_text_for_field(payload_expr, field_map, "relation_type")

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                WITH extracted AS (
                    SELECT
                        btrim({id_1_expr}) AS left_id,
                        btrim({id_2_expr}) AS right_id,
                        lower(btrim(COALESCE({relation_expr}, ''))) AS relation_hint
                    FROM raw.os_open_lids_row AS r
                    WHERE r.ingest_run_id = %s
                ),
                prepared AS (
                    SELECT
                        left_id,
                        right_id,
                        relation_hint,
                        (left_id IS NOT NULL AND left_id <> '') AS left_present,
                        (right_id IS NOT NULL AND right_id <> '') AS right_present,
                        (lower(COALESCE(left_id, '')) LIKE 'osgb%%') AS left_is_toid,
                        (lower(COALESCE(right_id, '')) LIKE 'osgb%%') AS right_is_toid,
                        (COALESCE(left_id, '') ~ '^[0-9]+$') AS left_is_digits,
                        (COALESCE(right_id, '') ~ '^[0-9]+$') AS right_is_digits
                    FROM extracted
                ),
                resolved AS MATERIALIZED (
                    SELECT
                        CASE
                            WHEN relation_hint IN ('toid_usrn', 'toid->usrn', 'toid_usrn_link') THEN 'toid_usrn'
                            WHEN relation_hint IN ('uprn_usrn', 'uprn->usrn', 'uprn_usrn_link') THEN 'uprn_usrn'
                            WHEN left_is_toid AND right_is_digits THEN 'toid_usrn'
                            WHEN right_is_toid AND left_is_digits THEN 'toid_usrn'
                            WHEN left_is_digits AND right_is_digits THEN 'uprn_usrn'
                            ELSE NULL
                        END AS relation_type,
                        CASE
                            WHEN relation_hint IN ('toid_usrn', 'toid->usrn', 'toid_usrn_link') THEN left_id
                            WHEN relation_hint IN ('uprn_usrn', 'uprn->usrn', 'uprn_usrn_link') THEN left_id
                            WHEN left_is_toid AND right_is_digits THEN left_id
                            WHEN right_is_toid AND left_is_digits THEN right_id
                            WHEN left_is_digits AND right_is_digits AND length(right_id) > 8 AND length(left_id) <= 8 THEN right_id
                            ELSE left_id
                        END AS id_1,
                        CASE
                            WHEN relation_hint IN ('toid_usrn', 'toid->usrn', 'toid_usrn_link') THEN right_id
                            WHEN relation_hint IN ('uprn_usrn', 'uprn->usrn', 'uprn_usrn_link') THEN right_id
                            WHEN left_is_toid AND right_is_digits THEN right_id
                            WHEN right_is_toid AND left_is_digits THEN left_id
                            WHEN left_is_digits AND right_is_digits AND length(right_id) > 8 AND length(left_id) <= 8 THEN left_id
                            ELSE right_id
                        END AS id_2
                    FROM prepared
                    WHERE left_present AND right_present
                ),
                ins_toid AS (
                    INSERT INTO stage.open_lids_toid_usrn (
                        build_run_id,
                        toid,
                        usrn,
                        ingest_run_id
                    )
                    SELECT
                        %s,
                        resolved.id_1,
                        resolved.id_2::bigint,
                        %s
                    FROM resolved
                    WHERE resolved.relation_type = 'toid_usrn'
                      AND resolved.id_2 ~ '^[0-9]+$'
                    ON CONFLICT (build_run_id, toid, usrn)
                    DO NOTHING
                    RETURNING 1
                ),
                ins_uprn AS (
                    INSERT INTO stage.open_lids_uprn_usrn (
                        build_run_id,
                        uprn,
                        usrn,
                        ingest_run_id
                    )
                    SELECT
                        %s,
                        resolved.id_1::bigint,
                        resolved.id_2::bigint,
                        %s
                    FROM resolved
                    WHERE resolved.relation_type = 'uprn_usrn'
                      AND resolved.id_1 ~ '^[0-9]+$'
                      AND resolved.id_2 ~ '^[0-9]+$'
                    ON CONFLICT (build_run_id, uprn, usrn)
                    DO NOTHING
                    RETURNING 1
                )
                SELECT
                    (SELECT COUNT(*)::bigint FROM ins_toid) AS toid_count,
                    (SELECT COUNT(*)::bigint FROM ins_uprn) AS uprn_count,
                    (
                        SELECT COUNT(*)::bigint
                        FROM resolved
                        WHERE
                            (
                                resolved.relation_type = 'toid_usrn'
                                AND resolved.id_2 ~ '^[0-9]+$'
                            )
                            OR (
                                resolved.relation_type = 'uprn_usrn'
                                AND resolved.id_1 ~ '^[0-9]+$'
                                AND resolved.id_2 ~ '^[0-9]+$'
                            )
                    ) AS relation_count
                """
            ).format(
                id_1_expr=id_1_expr,
                id_2_expr=id_2_expr,
                relation_expr=relation_expr,
            ),
            (
                ingest_run_id,
                build_run_id,
                ingest_run_id,
                build_run_id,
                ingest_run_id,
            ),
        )
        row = cur.fetchone()
        if row is None:
            return 0, 0, 0
        toid_count = int(row[0])
        uprn_count = int(row[1])
        pair_count = int(row[2])

    return toid_count, uprn_count, pair_count


def _populate_stage_nsul(
    conn: psycopg.Connection,
    build_run_id: str,
    ingest_run_id: str,
    field_map: dict[str, str],
    required_fields: tuple[str, ...],
) -> int:
    _validated_raw_sample_row(
        conn,
        source_name="nsul",
        raw_table="raw.nsul_row",
        ingest_run_id=ingest_run_id,
        field_map=field_map,
        required_fields=required_fields,
    )

    payload_expr = sql.SQL("r.payload_jsonb")
    uprn_expr = _json_text_for_field(payload_expr, field_map, "uprn")
    postcode_expr = _json_text_for_field(payload_expr, field_map, "postcode")

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                """
                WITH extracted AS (
                    SELECT
                        btrim({uprn_expr}) AS uprn_text,
                        btrim({postcode_expr}) AS postcode_text
                    FROM raw.nsul_row AS r
                    WHERE r.ingest_run_id = %s
                ),
                normalized AS (
                    SELECT DISTINCT
                        uprn_text::bigint AS uprn,
                        NULLIF(
                            upper(regexp_replace(COALESCE(postcode_text, ''), '[^A-Za-z0-9]', '', 'g')),
                            ''
                        ) AS postcode_norm
                    FROM extracted
                    WHERE uprn_text IS NOT NULL
                      AND uprn_text <> ''
                      AND uprn_text ~ '^[0-9]+$'
                )
                INSERT INTO stage.nsul_uprn_postcode (
                    build_run_id,
                    uprn,
                    postcode_norm,
                    ingest_run_id
                )
                SELECT
                    %s,
                    n.uprn,
                    n.postcode_norm,
                    %s
                FROM normalized AS n
                WHERE n.postcode_norm IS NOT NULL
                ON CONFLICT (build_run_id, uprn, postcode_norm)
                DO NOTHING
                """
            ).format(uprn_expr=uprn_expr, postcode_expr=postcode_expr),
            (ingest_run_id, build_run_id, ingest_run_id),
        )
        return int(cur.rowcount)


def _populate_stage_osni(
    conn: psycopg.Connection,
    build_run_id: str,
    ingest_run_id: str,
    field_map: dict[str, str],
    required_fields: tuple[str, ...],
) -> int:
    insert_sql = sql.SQL(
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
    )

    payload: list[tuple[Any, ...]] = []
    inserted = 0
    postcode_key = field_map.get("postcode")
    for row in _iter_validated_raw_rows(
        conn,
        source_name="osni_gazetteer",
        raw_table="raw.osni_gazetteer_row",
        ingest_run_id=ingest_run_id,
        field_map=field_map,
        required_fields=required_fields,
    ):
        feature_id_raw = _field_value(row, field_map, "feature_id")
        street_raw = _field_value(row, field_map, "street_name")
        if feature_id_raw in (None, "") or street_raw in (None, ""):
            continue

        folded = street_casefold(str(street_raw))
        if folded is None:
            continue

        postcode_n = postcode_norm(str(row.get(postcode_key)) if postcode_key and row.get(postcode_key) not in (None, "") else None)
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
        if len(payload) >= STAGE_INSERT_BATCH_SIZE:
            inserted += _flush_stage_batch(conn, insert_sql, payload)

    inserted += _flush_stage_batch(conn, insert_sql, payload)
    return inserted


def _populate_stage_dfi(
    conn: psycopg.Connection,
    build_run_id: str,
    ingest_run_id: str,
    field_map: dict[str, str],
    required_fields: tuple[str, ...],
) -> int:
    insert_sql = sql.SQL(
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
    )

    payload: list[tuple[Any, ...]] = []
    inserted = 0
    postcode_key = field_map.get("postcode")
    for row in _iter_validated_raw_rows(
        conn,
        source_name="dfi_highway",
        raw_table="raw.dfi_highway_row",
        ingest_run_id=ingest_run_id,
        field_map=field_map,
        required_fields=required_fields,
    ):
        segment_id_raw = _field_value(row, field_map, "segment_id")
        street_raw = _field_value(row, field_map, "street_name")
        if segment_id_raw in (None, "") or street_raw in (None, ""):
            continue

        folded = street_casefold(str(street_raw))
        if folded is None:
            continue
        postcode_n = postcode_norm(str(row.get(postcode_key)) if postcode_key and row.get(postcode_key) not in (None, "") else None)

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
        if len(payload) >= STAGE_INSERT_BATCH_SIZE:
            inserted += _flush_stage_batch(conn, insert_sql, payload)

    inserted += _flush_stage_batch(conn, insert_sql, payload)
    return inserted


def _populate_stage_ppd(
    conn: psycopg.Connection,
    build_run_id: str,
    ingest_run_id: str,
    field_map: dict[str, str],
    required_fields: tuple[str, ...],
) -> int:
    insert_sql = sql.SQL(
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
    )

    payload: list[tuple[Any, ...]] = []
    inserted = 0
    for row in _iter_validated_raw_rows(
        conn,
        source_name="ppd",
        raw_table="raw.ppd_row",
        ingest_run_id=ingest_run_id,
        field_map=field_map,
        required_fields=required_fields,
    ):
        row_hash_raw = _field_value(row, field_map, "row_hash")
        postcode_raw = _field_value(row, field_map, "postcode")
        street_raw = _field_value(row, field_map, "street")
        house_number_raw = _field_value(row, field_map, "house_number")

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
        if len(payload) >= STAGE_INSERT_BATCH_SIZE:
            inserted += _flush_stage_batch(conn, insert_sql, payload)

    inserted += _flush_stage_batch(conn, insert_sql, payload)
    return inserted


def _pass_0b_stage_normalisation(
    conn: psycopg.Connection,
    build_run_id: str,
    source_runs: dict[str, tuple[str, ...]],
) -> dict[str, int]:
    with conn.cursor() as cur:
        # Pass 0b executes large sort/dedupe operations on raw snapshots.
        # Raising work_mem here avoids repeated temp-file spill on default settings.
        cur.execute("SET LOCAL work_mem = '256MB'")

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
        road_count, postcode_count, duplicate_postcode_keys, family_counts = _populate_stage_open_names(
            conn, build_run_id, ingest_run_id, field_map, required_fields
        )
        counts["stage.open_names_road_feature"] = road_count
        counts["stage.open_names_postcode_feature"] = postcode_count
        counts["qa.open_names_postcode_duplicate_keys"] = duplicate_postcode_keys
        for family_name in sorted(family_counts.keys()):
            counts[f"stage.open_names_{family_name}"] = int(family_counts[family_name])
            counts[f"qa.open_names_feature_family_row_counts.{family_name}"] = int(
                family_counts[family_name]
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

    if "os_open_lids" in source_runs:
        field_map, required_fields = _mapped_fields_for_source(schema_config, "os_open_lids")
        ingest_run_id = _single_source_run(source_runs, "os_open_lids")
        toid_count, uprn_count, relation_count = _populate_stage_open_lids(
            conn, build_run_id, ingest_run_id, field_map, required_fields
        )
        counts["stage.open_lids_toid_usrn"] = toid_count
        counts["stage.open_lids_uprn_usrn"] = uprn_count
        counts["stage.open_lids_relation_count"] = relation_count

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

    _analyze_relations(conn, STAGE_TABLES)
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
            WITH ranked_open_names_postcode AS (
                SELECT
                    postcode_norm,
                    NULLIF(btrim(populated_place), '') AS place,
                    NULLIF(btrim(place_type), '') AS place_type,
                    NULLIF(btrim(place_toid), '') AS place_toid,
                    NULLIF(btrim(region), '') AS region_name,
                    NULLIF(btrim(region_toid), '') AS region_toid,
                    NULLIF(btrim(county_unitary), '') AS county_unitary_name,
                    NULLIF(btrim(county_unitary_toid), '') AS county_unitary_toid,
                    NULLIF(btrim(county_unitary_type), '') AS county_unitary_type,
                    NULLIF(btrim(district_borough), '') AS district_borough_name,
                    NULLIF(btrim(district_borough_toid), '') AS district_borough_toid,
                    NULLIF(btrim(district_borough_type), '') AS district_borough_type,
                    source_row_num,
                    ROW_NUMBER() OVER (
                        PARTITION BY postcode_norm
                        ORDER BY source_row_num ASC
                    ) AS rn
                FROM stage.open_names_postcode_feature
                WHERE build_run_id = %s
            ),
            selected_open_names_postcode AS (
                SELECT
                    postcode_norm,
                    place,
                    place_type,
                    place_toid,
                    region_name,
                    region_toid,
                    county_unitary_name,
                    county_unitary_toid,
                    county_unitary_type,
                    district_borough_name,
                    district_borough_toid,
                    district_borough_type
                FROM ranked_open_names_postcode
                WHERE rn = 1
            )
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
                place,
                place_type,
                place_toid,
                region_name,
                region_toid,
                county_unitary_name,
                county_unitary_toid,
                county_unitary_type,
                district_borough_name,
                district_borough_toid,
                district_borough_type,
                street_enrichment_available,
                onspd_run_id
            )
            SELECT
                sp.build_run_id,
                sp.postcode_display,
                sp.status,
                sp.lat,
                sp.lon,
                sp.easting,
                sp.northing,
                sp.country_iso2,
                sp.country_iso3,
                sp.subdivision_code,
                sop.place,
                sop.place_type,
                sop.place_toid,
                sop.region_name,
                sop.region_toid,
                sop.county_unitary_name,
                sop.county_unitary_toid,
                sop.county_unitary_type,
                sop.district_borough_name,
                sop.district_borough_toid,
                sop.district_borough_type,
                sp.street_enrichment_available,
                sp.onspd_run_id
            FROM stage.onspd_postcode AS sp
            LEFT JOIN selected_open_names_postcode AS sop
              ON sop.postcode_norm = sp.postcode_norm
            WHERE sp.build_run_id = %s
            ORDER BY sp.postcode_norm COLLATE "C" ASC
            """,
            (build_run_id, build_run_id),
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
                p.produced_build_run_id,
                p.postcode,
                jsonb_build_object(
                    'postcode_norm', sp.postcode_norm,
                    'country_iso2', sp.country_iso2,
                    'country_iso3', sp.country_iso3,
                    'subdivision_code', sp.subdivision_code,
                    'place', p.place,
                    'place_type', p.place_type,
                    'place_toid', p.place_toid,
                    'region_name', p.region_name,
                    'region_toid', p.region_toid,
                    'county_unitary_name', p.county_unitary_name,
                    'county_unitary_toid', p.county_unitary_toid,
                    'county_unitary_type', p.county_unitary_type,
                    'district_borough_name', p.district_borough_name,
                    'district_borough_toid', p.district_borough_toid,
                    'district_borough_type', p.district_borough_type,
                    'status', sp.status
                ),
                p.onspd_run_id
            FROM core.postcodes AS p
            JOIN stage.onspd_postcode AS sp
              ON sp.build_run_id = p.produced_build_run_id
             AND sp.postcode_display = p.postcode
            WHERE p.produced_build_run_id = %s
            ORDER BY sp.postcode_norm COLLATE "C" ASC
            """,
            (build_run_id,),
        )
        inserted_meta = cur.rowcount

        cur.execute(
            """
            WITH totals AS (
                SELECT COUNT(*)::bigint AS postcode_total
                FROM core.postcodes
                WHERE produced_build_run_id = %s
            ),
            coverage AS (
                SELECT COUNT(*)::bigint AS place_populated_count
                FROM core.postcodes
                WHERE produced_build_run_id = %s
                  AND place IS NOT NULL
            ),
            open_names_stats AS (
                SELECT
                    COUNT(*)::bigint AS open_names_postcode_rows,
                    COUNT(DISTINCT postcode_norm)::bigint AS open_names_postcode_distinct
                FROM stage.open_names_postcode_feature
                WHERE build_run_id = %s
            )
            SELECT
                totals.postcode_total,
                coverage.place_populated_count,
                open_names_stats.open_names_postcode_rows,
                open_names_stats.open_names_postcode_distinct
            FROM totals, coverage, open_names_stats
            """,
            (build_run_id, build_run_id, build_run_id),
        )
        postcode_total, place_populated_count, open_names_postcode_rows, open_names_postcode_distinct = cur.fetchone()

    _analyze_relations(conn, ("core.postcodes", "core.postcodes_meta"))

    return {
        "core.postcodes": int(inserted_postcodes),
        "core.postcodes_meta": int(inserted_meta),
        "qa.open_names_postcode_rows": int(open_names_postcode_rows),
        "qa.open_names_postcode_distinct": int(open_names_postcode_distinct),
        "qa.postcode_place_populated_count": int(place_populated_count),
        "qa.postcode_place_missing_count": int(postcode_total - place_populated_count),
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
                %(build_run_id)s,
                s.usrn,
                s.street_name,
                s.street_name_casefolded,
                s.street_class,
                s.street_status,
                s.usrn_run_id
            FROM stage.streets_usrn_input AS s
            WHERE s.build_run_id = %(build_run_id)s
            ORDER BY s.usrn ASC
            """,
            {"build_run_id": build_run_id},
        )
        inserted_direct = int(cur.rowcount)

        cur.execute(
            """
            CREATE TEMP TABLE tmp_toid_name_counts
            ON COMMIT DROP AS
            SELECT
                src.toid,
                src.street_name,
                src.street_name_casefolded,
                src.source_priority,
                src.name_quality_rank,
                src.name_quality_class,
                COUNT(*)::bigint AS feature_count
            FROM (
                SELECT
                    COALESCE(n.related_toid, n.feature_toid, n.toid) AS toid,
                    n.street_name_raw AS street_name,
                    n.street_name_casefolded,
                    1::smallint AS source_priority,
                    CASE
                        WHEN regexp_replace(upper(btrim(COALESCE(n.street_name_raw, ''))), '[[:space:]]+', ' ', 'g') = '' THEN 1::smallint
                        WHEN regexp_replace(upper(btrim(COALESCE(n.street_name_raw, ''))), '[[:space:]]+', ' ', 'g') ~ %(road_number_pattern)s THEN 2::smallint
                        WHEN regexp_replace(upper(btrim(COALESCE(n.street_name_raw, ''))), '[[:space:]]+', ' ', 'g') ~ '[A-Z]' THEN 0::smallint
                        ELSE 1::smallint
                    END AS name_quality_rank,
                    CASE
                        WHEN regexp_replace(upper(btrim(COALESCE(n.street_name_raw, ''))), '[[:space:]]+', ' ', 'g') = '' THEN 'unknown'
                        WHEN regexp_replace(upper(btrim(COALESCE(n.street_name_raw, ''))), '[[:space:]]+', ' ', 'g') ~ %(road_number_pattern)s THEN 'road_number'
                        WHEN regexp_replace(upper(btrim(COALESCE(n.street_name_raw, ''))), '[[:space:]]+', ' ', 'g') ~ '[A-Z]' THEN 'postal_plausible'
                        ELSE 'unknown'
                    END AS name_quality_class
                FROM stage.open_names_road_feature AS n
                WHERE n.build_run_id = %(build_run_id)s
                  AND COALESCE(n.related_toid, n.feature_toid, n.toid) IS NOT NULL

                UNION ALL

                SELECT
                    r.road_id AS toid,
                    r.road_name AS street_name,
                    r.road_name_casefolded,
                    2::smallint AS source_priority,
                    CASE
                        WHEN regexp_replace(upper(btrim(COALESCE(r.road_name, ''))), '[[:space:]]+', ' ', 'g') = '' THEN 1::smallint
                        WHEN regexp_replace(upper(btrim(COALESCE(r.road_name, ''))), '[[:space:]]+', ' ', 'g') ~ %(road_number_pattern)s THEN 2::smallint
                        WHEN regexp_replace(upper(btrim(COALESCE(r.road_name, ''))), '[[:space:]]+', ' ', 'g') ~ '[A-Z]' THEN 0::smallint
                        ELSE 1::smallint
                    END AS name_quality_rank,
                    CASE
                        WHEN regexp_replace(upper(btrim(COALESCE(r.road_name, ''))), '[[:space:]]+', ' ', 'g') = '' THEN 'unknown'
                        WHEN regexp_replace(upper(btrim(COALESCE(r.road_name, ''))), '[[:space:]]+', ' ', 'g') ~ %(road_number_pattern)s THEN 'road_number'
                        WHEN regexp_replace(upper(btrim(COALESCE(r.road_name, ''))), '[[:space:]]+', ' ', 'g') ~ '[A-Z]' THEN 'postal_plausible'
                        ELSE 'unknown'
                    END AS name_quality_class
                FROM stage.open_roads_segment AS r
                WHERE r.build_run_id = %(build_run_id)s
                  AND r.road_id IS NOT NULL
            ) AS src
            GROUP BY
                src.toid,
                src.street_name,
                src.street_name_casefolded,
                src.source_priority,
                src.name_quality_rank,
                src.name_quality_class
            """,
            {
                "build_run_id": build_run_id,
                "road_number_pattern": ROAD_NUMBER_PATTERN,
            },
        )
        cur.execute(
            """
            CREATE INDEX idx_tmp_toid_name_counts_toid
                ON tmp_toid_name_counts (toid)
            """
        )
        cur.execute(
            """
            CREATE TEMP TABLE tmp_inferred_name_counts
            ON COMMIT DROP AS
            SELECT
                lids.usrn,
                n.street_name,
                n.street_name_casefolded,
                SUM(n.feature_count)::bigint AS evidence_count,
                MIN(n.source_priority)::smallint AS source_priority,
                MIN(n.name_quality_rank)::smallint AS name_quality_rank,
                (ARRAY_AGG(lids.ingest_run_id ORDER BY lids.ingest_run_id::text ASC))[1] AS usrn_run_id
            FROM tmp_toid_name_counts AS n
            JOIN stage.open_lids_toid_usrn AS lids
              ON lids.build_run_id = %(build_run_id)s
             AND lids.toid = n.toid
            GROUP BY lids.usrn, n.street_name, n.street_name_casefolded
            """,
            {"build_run_id": build_run_id},
        )
        cur.execute(
            """
            CREATE INDEX idx_tmp_inferred_name_counts_usrn
                ON tmp_inferred_name_counts (
                    usrn,
                    evidence_count DESC,
                    name_quality_rank ASC,
                    source_priority ASC,
                    street_name_casefolded,
                    street_name
                )
            """
        )
        cur.execute(
            """
            WITH prepared AS (
                SELECT
                    usrn,
                    street_name,
                    street_name_casefolded,
                    evidence_count,
                    source_priority,
                    name_quality_rank,
                    usrn_run_id,
                    BOOL_OR(name_quality_rank = 0) OVER (PARTITION BY usrn) AS has_postal_plausible
                FROM tmp_inferred_name_counts
            ),
            filtered AS (
                SELECT
                    usrn,
                    street_name,
                    street_name_casefolded,
                    evidence_count,
                    source_priority,
                    name_quality_rank,
                    usrn_run_id
                FROM prepared
                WHERE NOT (has_postal_plausible AND name_quality_rank = 2)
            ),
            inferred_usrn AS (
                SELECT
                    usrn,
                    street_name,
                    street_name_casefolded,
                    NULL::text AS street_class,
                    NULL::text AS street_status,
                    usrn_run_id
                FROM (
                    SELECT
                        usrn,
                        street_name,
                        street_name_casefolded,
                        usrn_run_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY usrn
                            ORDER BY evidence_count DESC,
                                     name_quality_rank ASC,
                                     source_priority ASC,
                                     street_name_casefolded COLLATE "C" ASC,
                                     street_name COLLATE "C" ASC
                        ) AS rn
                    FROM filtered
                ) AS ranked
                WHERE rn = 1
            )
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
                %(build_run_id)s,
                inferred.usrn,
                inferred.street_name,
                inferred.street_name_casefolded,
                inferred.street_class,
                inferred.street_status,
                inferred.usrn_run_id
            FROM inferred_usrn AS inferred
            WHERE NOT EXISTS (
                SELECT 1
                FROM core.streets_usrn AS direct
                WHERE direct.produced_build_run_id = %(build_run_id)s
                  AND direct.usrn = inferred.usrn
            )
            ORDER BY inferred.usrn ASC
            """,
            {"build_run_id": build_run_id},
        )
        inserted_inferred = int(cur.rowcount)

    _analyze_relations(conn, ("core.streets_usrn",))
    return {"core.streets_usrn": inserted_direct + inserted_inferred}


def _pass_3_open_names_candidates(conn: psycopg.Connection, build_run_id: str) -> dict[str, int]:
    schema_config = _schema_config()
    _mapped_fields_for_source(schema_config, "os_open_names")
    has_open_lids = _build_has_source(conn, build_run_id, "os_open_lids")
    if has_open_lids:
        _mapped_fields_for_source(schema_config, "os_open_lids")

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)::bigint
            FROM stage.open_names_road_feature
            WHERE build_run_id = %s
              AND postcode_norm IS NOT NULL
            """,
            (build_run_id,),
        )
        stage_open_names_rows = int(cur.fetchone()[0])
        cur.execute(
            """
            SELECT COUNT(*)::bigint
            FROM core.postcodes
            WHERE produced_build_run_id = %s
            """,
            (build_run_id,),
        )
        core_postcode_rows = int(cur.fetchone()[0])

    if stage_open_names_rows == 0:
        return {
            "derived.postcode_street_candidates_base": 0,
            "derived.postcode_street_candidates_promoted": 0,
            "derived.postcode_street_candidate_lineage": 0,
            "qa.pass3_base_input_rows": 0,
            "qa.pass3_skipped_no_open_names_rows": 1,
        }

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
                n.resolved_street_name_raw,
                n.resolved_street_name_casefolded,
                NULL,
                'names_postcode_feature',
                'medium',
                'open_names:feature:' || n.resolved_feature_id,
                'os_open_names',
                n.resolved_ingest_run_id,
                jsonb_build_object(
                    'feature_id', n.resolved_feature_id,
                    'toid', n.resolved_toid,
                    'related_toid', n.related_toid,
                    'feature_toid', n.feature_toid
                )
            FROM (
                SELECT
                    feature_id AS resolved_feature_id,
                    COALESCE(related_toid, feature_toid, toid) AS resolved_toid,
                    related_toid,
                    feature_toid,
                    street_name_raw AS resolved_street_name_raw,
                    street_name_casefolded AS resolved_street_name_casefolded,
                    postcode_norm,
                    ingest_run_id AS resolved_ingest_run_id
                FROM stage.open_names_road_feature
                WHERE build_run_id = %s
            ) AS n
            JOIN core.postcodes AS p
              ON p.produced_build_run_id = %s
             AND replace(p.postcode, ' ', '') = n.postcode_norm
            ORDER BY n.resolved_feature_id COLLATE "C" ASC
            """,
            (build_run_id, build_run_id, build_run_id),
        )
        base_inserted = int(cur.rowcount)

    if stage_open_names_rows > 0 and core_postcode_rows > 0 and base_inserted == 0:
        raise BuildError(
            "Pass 3 produced zero base Open Names candidates despite staged input rows; "
            "check postcode key alignment between stage.open_names_road_feature and core.postcodes"
        )

    promotions_inserted = 0
    lineage_inserted = 0

    if has_open_lids:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TEMP TABLE tmp_pass3_promotions
                ON COMMIT DROP AS
                SELECT
                    ROW_NUMBER() OVER (
                        ORDER BY parent.candidate_id ASC, lids.usrn ASC
                    )::bigint AS promotion_rank,
                    parent.candidate_id AS parent_candidate_id,
                    parent.postcode,
                    parent.street_name_raw,
                    parent.street_name_canonical,
                    parent.evidence_json ->> 'toid' AS toid,
                    lids.usrn,
                    lids.ingest_run_id
                FROM derived.postcode_street_candidates AS parent
                JOIN stage.open_lids_toid_usrn AS lids
                  ON lids.build_run_id = parent.produced_build_run_id
                 AND lids.toid = parent.evidence_json ->> 'toid'
                WHERE parent.produced_build_run_id = %s
                  AND parent.candidate_type = 'names_postcode_feature'
                  AND parent.evidence_json ->> 'toid' IS NOT NULL
                """,
                (build_run_id,),
            )
            cur.execute("SELECT COUNT(*)::bigint FROM tmp_pass3_promotions")
            promotions_inserted = int(cur.fetchone()[0])

            if promotions_inserted > 0:
                cur.execute(
                    """
                    WITH inserted AS (
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
                            p.street_name_raw,
                            p.street_name_canonical,
                            p.usrn,
                            'open_lids_toid_usrn',
                            'high',
                            'open_lids:toid_usrn:' || p.toid,
                            'os_open_lids',
                            p.ingest_run_id,
                            jsonb_build_object('toid', p.toid, 'usrn', p.usrn)
                        FROM tmp_pass3_promotions AS p
                        ORDER BY p.promotion_rank ASC
                        RETURNING candidate_id
                    ),
                    ranked_inserted AS (
                        SELECT
                            candidate_id,
                            ROW_NUMBER() OVER (ORDER BY candidate_id ASC)::bigint AS promotion_rank
                        FROM inserted
                    )
                    INSERT INTO derived.postcode_street_candidate_lineage (
                        parent_candidate_id,
                        child_candidate_id,
                        relation_type,
                        produced_build_run_id
                    )
                    SELECT
                        p.parent_candidate_id,
                        i.candidate_id,
                        'promotion_toid_usrn',
                        %s
                    FROM tmp_pass3_promotions AS p
                    JOIN ranked_inserted AS i
                      ON i.promotion_rank = p.promotion_rank
                    ON CONFLICT DO NOTHING
                    """,
                    (build_run_id, build_run_id),
                )
                lineage_inserted = int(cur.rowcount)

    return {
        "derived.postcode_street_candidates_base": int(base_inserted),
        "derived.postcode_street_candidates_promoted": int(promotions_inserted),
        "derived.postcode_street_candidate_lineage": int(lineage_inserted),
        "qa.pass3_base_input_rows": int(stage_open_names_rows),
        "qa.pass3_skipped_no_open_names_rows": 0,
    }


def _pass_4_uprn_reinforcement(conn: psycopg.Connection, build_run_id: str) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute("SET LOCAL work_mem = '256MB'")
        cur.execute(
            """
            SELECT bbs.ingest_run_id
            FROM meta.build_run AS br
            JOIN meta.build_bundle_source AS bbs
              ON bbs.bundle_id = br.bundle_id
            WHERE br.build_run_id = %s
              AND bbs.source_name = 'os_open_lids'
            ORDER BY bbs.ingest_run_id::text ASC
            LIMIT 1
            """,
            (build_run_id,),
        )
        run_row = cur.fetchone()
        if run_row is None or run_row[0] is None:
            raise BuildError(
                "Pass 4 failed: missing os_open_lids ingest run for build bundle "
                f"build_run_id={build_run_id}"
            )
        open_lids_ingest_run_id = run_row[0]

        cur.execute(
            """
            WITH aggregate_pairs AS (
                SELECT
                    nsul.postcode_norm,
                    lids.usrn,
                    COUNT(*)::bigint AS uprn_count
                FROM stage.nsul_uprn_postcode AS nsul
                JOIN stage.open_lids_uprn_usrn AS lids
                  ON lids.build_run_id = nsul.build_run_id
                 AND lids.uprn = nsul.uprn
                WHERE nsul.build_run_id = %s
                GROUP BY nsul.postcode_norm, lids.usrn
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
                'open_lids:uprn_usrn:' || a.uprn_count::text || '_uprns',
                'os_open_lids',
                %s,
                jsonb_build_object('uprn_count', a.uprn_count)
            FROM aggregate_pairs AS a
            JOIN stage.onspd_postcode AS sp
              ON sp.build_run_id = %s
             AND sp.postcode_norm = a.postcode_norm
            JOIN core.postcodes AS p
              ON p.produced_build_run_id = %s
             AND p.postcode = sp.postcode_display
            JOIN core.streets_usrn AS s
              ON s.produced_build_run_id = %s
             AND s.usrn = a.usrn
            ORDER BY p.postcode COLLATE "C" ASC, a.usrn ASC
            """,
            (
                build_run_id,
                build_run_id,
                open_lids_ingest_run_id,
                build_run_id,
                build_run_id,
                build_run_id,
            ),
        )
        inserted = cur.rowcount

    return {"derived.postcode_street_candidates_uprn_usrn": int(inserted)}


def _pass_5_gb_spatial_fallback(conn: psycopg.Connection, build_run_id: str) -> dict[str, int]:
    schema_config = _schema_config()
    _mapped_fields_for_source(schema_config, "os_open_roads")

    with conn.cursor() as cur:
        cur.execute("SET LOCAL work_mem = '256MB'")
        cur.execute(
            """
            CREATE TEMP TABLE tmp_pass5_candidates
            ON COMMIT DROP AS
            WITH gb_postcodes_without_high AS (
                SELECT
                    p.postcode,
                    replace(p.postcode, ' ', '') AS postcode_norm,
                    p.easting,
                    p.northing
                FROM core.postcodes AS p
                WHERE p.produced_build_run_id = %(build_run_id)s
                  AND p.country_iso2 = 'GB'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM derived.postcode_street_candidates AS c
                      WHERE c.produced_build_run_id = p.produced_build_run_id
                        AND c.postcode = p.postcode
                        AND c.confidence = 'high'
                  )
            ),
            postcode_points AS (
                SELECT
                    g.postcode,
                    g.postcode_norm,
                    ST_SetSRID(
                        ST_MakePoint(g.easting::double precision, g.northing::double precision),
                        27700
                    ) AS geom_bng
                FROM gb_postcodes_without_high AS g
                WHERE g.easting IS NOT NULL
                  AND g.northing IS NOT NULL
            ),
            spatial_candidates AS (
                SELECT
                    p.postcode,
                    p.postcode_norm,
                    r.segment_id,
                    r.usrn,
                    r.road_name,
                    r.road_name_casefolded,
                    r.ingest_run_id,
                    ST_Distance(p.geom_bng, r.geom_bng)::double precision AS distance_m,
                    true AS is_spatial
                FROM postcode_points AS p
                JOIN stage.open_roads_segment AS r
                  ON r.build_run_id = %(build_run_id)s
                 AND r.geom_bng IS NOT NULL
                 AND ST_DWithin(p.geom_bng, r.geom_bng, %(radius_m)s)
            ),
            gb_without_spatial AS (
                SELECT g.postcode, g.postcode_norm
                FROM gb_postcodes_without_high AS g
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM spatial_candidates AS s
                    WHERE s.postcode = g.postcode
                )
            ),
            postcode_fallback_candidates AS (
                SELECT
                    g.postcode,
                    g.postcode_norm,
                    r.segment_id,
                    r.usrn,
                    r.road_name,
                    r.road_name_casefolded,
                    r.ingest_run_id,
                    NULL::double precision AS distance_m,
                    false AS is_spatial
                FROM gb_without_spatial AS g
                JOIN stage.open_roads_segment AS r
                  ON r.build_run_id = %(build_run_id)s
                 AND r.postcode_norm = g.postcode_norm
            ),
            combined_candidates AS (
                SELECT * FROM spatial_candidates
                UNION ALL
                SELECT * FROM postcode_fallback_candidates
            )
            SELECT
                c.postcode,
                c.postcode_norm,
                c.segment_id,
                c.usrn,
                c.road_name,
                c.road_name_casefolded,
                c.ingest_run_id,
                c.distance_m,
                c.is_spatial,
                CASE
                    WHEN regexp_replace(upper(btrim(COALESCE(c.road_name, ''))), '[[:space:]]+', ' ', 'g') = '' THEN 'unknown'
                    WHEN regexp_replace(upper(btrim(COALESCE(c.road_name, ''))), '[[:space:]]+', ' ', 'g') ~ %(road_number_pattern)s THEN 'road_number'
                    WHEN regexp_replace(upper(btrim(COALESCE(c.road_name, ''))), '[[:space:]]+', ' ', 'g') ~ '[A-Z]' THEN 'postal_plausible'
                    ELSE 'unknown'
                END AS name_quality_class,
                CASE
                    WHEN regexp_replace(upper(btrim(COALESCE(c.road_name, ''))), '[[:space:]]+', ' ', 'g') = '' THEN 'blank_or_null'
                    WHEN regexp_replace(upper(btrim(COALESCE(c.road_name, ''))), '[[:space:]]+', ' ', 'g') ~ %(road_number_pattern)s THEN 'matched_uk_road_number_pattern'
                    WHEN regexp_replace(upper(btrim(COALESCE(c.road_name, ''))), '[[:space:]]+', ' ', 'g') ~ '[A-Z]' THEN 'not_road_number_pattern'
                    ELSE 'not_road_number_pattern'
                END AS name_quality_reason,
                CASE
                    WHEN regexp_replace(upper(btrim(COALESCE(c.road_name, ''))), '[[:space:]]+', ' ', 'g') = '' THEN 1::smallint
                    WHEN regexp_replace(upper(btrim(COALESCE(c.road_name, ''))), '[[:space:]]+', ' ', 'g') ~ %(road_number_pattern)s THEN 2::smallint
                    WHEN regexp_replace(upper(btrim(COALESCE(c.road_name, ''))), '[[:space:]]+', ' ', 'g') ~ '[A-Z]' THEN 0::smallint
                    ELSE 1::smallint
                END AS name_quality_rank,
                0::smallint AS ppd_match_score,
                NULL::text AS ppd_matched_street,
                'none'::text AS ppd_match_type
            FROM combined_candidates AS c
            """,
            {
                "build_run_id": build_run_id,
                "radius_m": PASS5_SPATIAL_RADIUS_M,
                "road_number_pattern": ROAD_NUMBER_PATTERN,
            },
        )
        cur.execute(
            """
            CREATE INDEX idx_tmp_pass5_candidates_postcode
                ON tmp_pass5_candidates (postcode)
            """
        )
        cur.execute(
            """
            CREATE INDEX idx_tmp_pass5_candidates_postcode_norm
                ON tmp_pass5_candidates (postcode_norm)
            """
        )

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*)::bigint FILTER (WHERE name_quality_class = 'postal_plausible'),
                COUNT(*)::bigint FILTER (WHERE name_quality_class = 'road_number')
            FROM tmp_pass5_candidates
            """
        )
        candidate_postal_plausible, candidate_road_number = (int(value or 0) for value in cur.fetchone())

    ppd_enabled = (
        _env_flag_enabled("PIPELINE_PASS5_ENABLE_PPD_TIE_BREAK", True)
        and _build_has_source(conn, build_run_id, "ppd")
    )
    if ppd_enabled:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*)::bigint
                FROM stage.ppd_parsed_address
                WHERE build_run_id = %s
                """,
                (build_run_id,),
            )
            ppd_enabled = int(cur.fetchone()[0] or 0) > 0

    if ppd_enabled:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TEMP TABLE tmp_pass5_ppd_tokens
                ON COMMIT DROP AS
                SELECT DISTINCT
                    postcode_norm,
                    street_token_raw,
                    street_token_casefolded
                FROM stage.ppd_parsed_address
                WHERE build_run_id = %s
                """,
                (build_run_id,),
            )
            cur.execute(
                """
                CREATE INDEX idx_tmp_pass5_ppd_tokens_postcode
                    ON tmp_pass5_ppd_tokens (postcode_norm, street_token_casefolded)
                """
            )
            cur.execute(
                """
                WITH scored AS (
                    SELECT
                        c.postcode,
                        c.segment_id,
                        CASE
                            WHEN p.street_token_casefolded = c.road_name_casefolded THEN 2::smallint
                            WHEN p.street_token_casefolded IS NOT NULL
                                 AND (
                                     p.street_token_casefolded LIKE c.road_name_casefolded || ' %%'
                                     OR c.road_name_casefolded LIKE p.street_token_casefolded || ' %%'
                                     OR position(c.road_name_casefolded in p.street_token_casefolded) > 0
                                     OR position(p.street_token_casefolded in c.road_name_casefolded) > 0
                                 ) THEN 1::smallint
                            ELSE 0::smallint
                        END AS match_score,
                        CASE
                            WHEN p.street_token_casefolded = c.road_name_casefolded THEN 'exact'
                            WHEN p.street_token_casefolded IS NOT NULL
                                 AND (
                                     p.street_token_casefolded LIKE c.road_name_casefolded || ' %%'
                                     OR c.road_name_casefolded LIKE p.street_token_casefolded || ' %%'
                                     OR position(c.road_name_casefolded in p.street_token_casefolded) > 0
                                     OR position(p.street_token_casefolded in c.road_name_casefolded) > 0
                                 ) THEN 'partial'
                            ELSE 'none'
                        END AS match_type,
                        p.street_token_raw AS matched_street,
                        ROW_NUMBER() OVER (
                            PARTITION BY c.postcode, c.segment_id
                            ORDER BY
                                CASE
                                    WHEN p.street_token_casefolded = c.road_name_casefolded THEN 2::smallint
                                    WHEN p.street_token_casefolded IS NOT NULL
                                         AND (
                                             p.street_token_casefolded LIKE c.road_name_casefolded || ' %%'
                                             OR c.road_name_casefolded LIKE p.street_token_casefolded || ' %%'
                                             OR position(c.road_name_casefolded in p.street_token_casefolded) > 0
                                             OR position(p.street_token_casefolded in c.road_name_casefolded) > 0
                                         ) THEN 1::smallint
                                    ELSE 0::smallint
                                END DESC,
                                length(COALESCE(p.street_token_casefolded, '')) DESC,
                                COALESCE(p.street_token_casefolded, '') COLLATE "C" ASC
                        ) AS rn
                    FROM tmp_pass5_candidates AS c
                    LEFT JOIN tmp_pass5_ppd_tokens AS p
                      ON p.postcode_norm = c.postcode_norm
                    WHERE c.is_spatial
                )
                UPDATE tmp_pass5_candidates AS c
                SET
                    ppd_match_score = s.match_score,
                    ppd_match_type = s.match_type,
                    ppd_matched_street = CASE WHEN s.match_score > 0 THEN s.matched_street ELSE NULL END
                FROM scored AS s
                WHERE s.rn = 1
                  AND c.postcode = s.postcode
                  AND c.segment_id = s.segment_id
                """
            )

    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TEMP TABLE tmp_pass5_ranked
            ON COMMIT DROP AS
            WITH policy AS (
                SELECT
                    c.*,
                    BOOL_OR(c.name_quality_class = 'postal_plausible') OVER (
                        PARTITION BY c.postcode
                    ) AS has_postal_plausible
                FROM tmp_pass5_candidates AS c
            ),
            eligible AS (
                SELECT
                    p.*
                FROM policy AS p
                WHERE NOT (
                    p.has_postal_plausible
                    AND p.name_quality_class = 'road_number'
                )
            )
            SELECT
                e.*,
                ROW_NUMBER() OVER (
                    PARTITION BY e.postcode
                    ORDER BY
                        e.name_quality_rank ASC,
                        e.ppd_match_score DESC,
                        e.distance_m ASC NULLS LAST,
                        e.segment_id COLLATE "C" ASC
                ) AS rn
            FROM eligible AS e
            """
        )
        cur.execute(
            """
            CREATE INDEX idx_tmp_pass5_ranked_postcode_rn
                ON tmp_pass5_ranked (postcode, rn)
            """
        )
        cur.execute(
            """
            SELECT COUNT(DISTINCT postcode)::bigint
            FROM tmp_pass5_ranked
            """
        )
        postcodes_with_candidates = int(cur.fetchone()[0] or 0)

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
                rs.postcode,
                rs.road_name,
                rs.road_name_casefolded,
                rs.usrn,
                'spatial_os_open_roads',
                'low',
                'spatial:os_open_roads:' || rs.segment_id || ':fallback',
                'os_open_roads',
                rs.ingest_run_id,
                jsonb_build_object(
                    'segment_id', rs.segment_id,
                    'distance_m', rs.distance_m,
                    'name_quality_class', rs.name_quality_class,
                    'name_quality_reason', rs.name_quality_reason,
                    'fallback_policy', 'postal_name_preferred',
                    'ppd_match_score', rs.ppd_match_score,
                    'ppd_matched_street', rs.ppd_matched_street,
                    'tie_break_basis', %s,
                    'road_number_only_output', (rs.name_quality_class = 'road_number')
                )
            FROM tmp_pass5_ranked AS rs
            WHERE rs.rn = 1
            ORDER BY rs.postcode COLLATE "C" ASC
            """,
            (
                build_run_id,
                "postal_name_then_ppd_then_distance"
                if ppd_enabled
                else "postal_name_then_distance",
            ),
        )
        inserted = int(cur.rowcount)

    if postcodes_with_candidates > 0 and inserted == 0:
        raise BuildError(
            "Pass 5 produced zero fallback winners despite available candidates; "
            "check pass-5 ranking and candidate filtering logic"
        )

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*)::bigint FILTER (WHERE rn = 1 AND name_quality_class = 'road_number'),
                COUNT(*)::bigint FILTER (WHERE rn = 1 AND ppd_match_type = 'exact'),
                COUNT(*)::bigint FILTER (WHERE rn = 1 AND ppd_match_type = 'partial'),
                COUNT(*)::bigint FILTER (WHERE rn = 1 AND ppd_match_type = 'none')
            FROM tmp_pass5_ranked
            """
        )
        (
            road_number_only_wins,
            ppd_match_exact_count,
            ppd_match_partial_count,
            ppd_match_none_count,
        ) = (int(value or 0) for value in cur.fetchone())

        cur.execute(
            """
            SELECT COUNT(*)::bigint
            FROM (
                SELECT winner.postcode
                FROM tmp_pass5_ranked AS winner
                WHERE winner.rn = 1
                  AND EXISTS (
                      SELECT 1
                      FROM tmp_pass5_ranked AS peer
                      WHERE peer.postcode = winner.postcode
                        AND peer.rn > 1
                        AND peer.name_quality_rank = winner.name_quality_rank
                  )
                  AND winner.ppd_match_score > COALESCE((
                      SELECT MAX(peer.ppd_match_score)
                      FROM tmp_pass5_ranked AS peer
                      WHERE peer.postcode = winner.postcode
                        AND peer.rn > 1
                        AND peer.name_quality_rank = winner.name_quality_rank
                  ), -1)
            ) AS t
            """
        )
        ppd_tie_break_applied_count = int(cur.fetchone()[0] or 0)

    if not ppd_enabled:
        ppd_match_exact_count = 0
        ppd_match_partial_count = 0
        ppd_match_none_count = 0
        ppd_tie_break_applied_count = 0

    return {
        "derived.postcode_street_candidates_spatial_os_open_roads": int(inserted),
        "pass5_candidates_postal_plausible": int(candidate_postal_plausible),
        "pass5_candidates_road_number": int(candidate_road_number),
        "pass5_road_number_only_wins": int(road_number_only_wins),
        "pass5_ppd_tie_break_enabled": 1 if ppd_enabled else 0,
        "pass5_ppd_tie_break_applied_count": int(ppd_tie_break_applied_count),
        "pass5_ppd_match_exact_count": int(ppd_match_exact_count),
        "pass5_ppd_match_partial_count": int(ppd_match_partial_count),
        "pass5_ppd_match_none_count": int(ppd_match_none_count),
    }


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
            CREATE INDEX IF NOT EXISTS idx_tmp_weighted_candidates_street
                ON tmp_weighted_candidates (postcode, canonical_street_name, candidate_id)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tmp_weighted_candidates_source
                ON tmp_weighted_candidates (
                    postcode,
                    canonical_street_name,
                    source_name,
                    ingest_run_id,
                    candidate_type
                )
            """
        )

        cur.execute("DROP TABLE IF EXISTS pg_temp.tmp_final_scored")
        cur.execute(
            """
            CREATE TEMP TABLE tmp_final_scored AS
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
                ROUND(weighted_score::numeric, 4) AS frequency_score,
                CASE conf_rank
                    WHEN 3 THEN 'high'
                    WHEN 2 THEN 'medium'
                    WHEN 1 THEN 'low'
                    ELSE 'none'
                END AS confidence,
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
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tmp_final_scored_street
                ON tmp_final_scored (postcode, canonical_street_name)
            """
        )

        cur.execute("DROP TABLE IF EXISTS pg_temp.tmp_final_inserted")
        cur.execute(
            """
            CREATE TEMP TABLE tmp_final_inserted (
                final_id bigint PRIMARY KEY,
                postcode text NOT NULL,
                canonical_street_name text NOT NULL
            ) ON COMMIT DROP
            """
        )
        cur.execute(
            """
            WITH inserted AS (
                INSERT INTO derived.postcode_streets_final (
                    produced_build_run_id,
                    postcode,
                    street_name,
                    usrn,
                    confidence,
                    frequency_score,
                    probability
                )
                SELECT
                    %s,
                    fs.postcode,
                    fs.canonical_street_name,
                    fs.usrn,
                    fs.confidence,
                    fs.frequency_score,
                    fs.final_probability
                FROM tmp_final_scored AS fs
                ORDER BY fs.postcode COLLATE "C" ASC, fs.rn ASC
                RETURNING final_id, postcode, street_name
            )
            INSERT INTO tmp_final_inserted (final_id, postcode, canonical_street_name)
            SELECT final_id, postcode, street_name
            FROM inserted
            """,
            (build_run_id,),
        )
        inserted_final = int(cur.rowcount)

        cur.execute(
            """
            INSERT INTO derived.postcode_streets_final_candidate (
                final_id,
                candidate_id,
                produced_build_run_id,
                link_rank
            )
            SELECT
                fi.final_id,
                wc.candidate_id,
                %s,
                ROW_NUMBER() OVER (
                    PARTITION BY fi.final_id
                    ORDER BY wc.candidate_id ASC
                ) AS link_rank
            FROM tmp_final_inserted AS fi
            JOIN tmp_weighted_candidates AS wc
              ON wc.postcode = fi.postcode
             AND wc.canonical_street_name = fi.canonical_street_name
            ORDER BY fi.final_id ASC, wc.candidate_id ASC
            """,
            (build_run_id,),
        )
        inserted_final_candidate = int(cur.rowcount)

        cur.execute(
            """
            INSERT INTO derived.postcode_streets_final_source (
                final_id,
                source_name,
                ingest_run_id,
                candidate_type,
                contribution_weight,
                produced_build_run_id
            )
            SELECT
                fi.final_id,
                wc.source_name,
                wc.ingest_run_id,
                wc.candidate_type,
                ROUND(SUM(wc.weight)::numeric, 4) AS contribution_weight,
                %s
            FROM tmp_final_inserted AS fi
            JOIN tmp_weighted_candidates AS wc
              ON wc.postcode = fi.postcode
             AND wc.canonical_street_name = fi.canonical_street_name
            GROUP BY
                fi.final_id,
                wc.source_name,
                wc.ingest_run_id,
                wc.candidate_type
            ORDER BY
                fi.final_id ASC,
                wc.source_name COLLATE "C" ASC,
                wc.ingest_run_id::text ASC,
                wc.candidate_type COLLATE "C" ASC
            """,
            (build_run_id,),
        )
        inserted_final_source = int(cur.rowcount)

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
                    p.place,
                    p.place_type,
                    p.place_toid,
                    p.region_name,
                    p.region_toid,
                    p.county_unitary_name,
                    p.county_unitary_toid,
                    p.county_unitary_type,
                    p.district_borough_name,
                    p.district_borough_toid,
                    p.district_borough_type,
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
    unexpected = sorted(set(source_runs.keys()) - required)
    if unexpected:
        raise BuildError(
            f"Bundle {bundle_id} has sources outside profile {build_profile}: {', '.join(unexpected)}"
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

    with conn.cursor() as cur:
        # Stage tables are rebuildable; disabling per-transaction fsync waits reduces
        # pass runtime without changing deterministic outputs.
        cur.execute("SET synchronous_commit TO off")

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
                       place, place_type, place_toid,
                       region_name, region_toid,
                       county_unitary_name, county_unitary_toid, county_unitary_type,
                       district_borough_name, district_borough_toid, district_borough_type,
                       lat, lon, easting, northing,
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
