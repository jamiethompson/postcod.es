"""Source ingest workflows for Pipeline V3."""

from __future__ import annotations

import csv
import json
import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from pipeline.manifest import SourceFileManifest, SourceIngestManifest
from pipeline.util.hashing import sha256_file


class IngestError(RuntimeError):
    """Raised when source ingest fails."""


@dataclass(frozen=True)
class IngestResult:
    source_name: str
    run_id: str
    status: str
    files_loaded: int
    rows_loaded: int


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


CSV_INSERT_BATCH_SIZE = 5_000


def _file_set_hash(files: tuple[SourceFileManifest, ...]) -> str:
    payload = [
        {
            "file_role": file.file_role,
            "path": str(file.file_path),
            "sha256": file.sha256,
            "size_bytes": file.size_bytes,
            "format": file.format,
            "layer_name": file.layer_name,
        }
        for file in sorted(files, key=lambda item: (item.file_role, str(item.file_path), item.layer_name))
    ]
    encoded = json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    import hashlib

    return hashlib.sha256(encoded).hexdigest()


def _iter_rows_from_csv(path: Path) -> Iterator[dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise IngestError(f"CSV file is missing header row: {path}")
        for row in reader:
            yield {str(key): value for key, value in row.items()}


def _iter_rows_from_geojson(path: Path) -> Iterator[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise IngestError(f"GeoJSON root must be object: {path}")

    features = payload.get("features")
    if not isinstance(features, list):
        raise IngestError(f"GeoJSON features missing or invalid: {path}")

    for feature in features:
        if not isinstance(feature, dict):
            continue
        props = feature.get("properties")
        row: dict[str, Any] = {}
        if isinstance(props, dict):
            row.update({str(key): value for key, value in props.items()})
        geometry = feature.get("geometry")
        row["__geometry"] = geometry
        yield row


def _iter_rows_from_json(path: Path) -> Iterator[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                yield {str(key): value for key, value in item.items()}
        return
    if isinstance(payload, dict):
        yield {str(key): value for key, value in payload.items()}
        return
    raise IngestError(f"Unsupported JSON payload shape: {path}")


def _iter_rows_from_gpkg(path: Path, layer_name: str) -> Iterator[dict[str, Any]]:
    if not layer_name:
        raise IngestError(f"GeoPackage manifest must set layer_name: {path}")

    quoted_layer = '"' + layer_name.replace('"', '""') + '"'
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        cur = conn.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type IN ('table', 'view')
              AND name = ?
            LIMIT 1
            """,
            (layer_name,),
        )
        if cur.fetchone() is None:
            raise IngestError(
                f"GeoPackage layer '{layer_name}' not found in {path}"
            )

        row_cur = conn.execute(f"SELECT * FROM {quoted_layer}")
        col_names = [desc[0] for desc in row_cur.description]
        for values in row_cur:
            row: dict[str, Any] = {}
            for index, column_name in enumerate(col_names):
                value = values[index]
                if isinstance(value, (bytes, bytearray, memoryview)):
                    # Keep raw binary columns JSON-safe while preserving source bytes.
                    value = bytes(value).hex()
                row[str(column_name)] = value
            yield row
    finally:
        conn.close()


def _iter_rows(file_manifest: SourceFileManifest) -> Iterator[dict[str, Any]]:
    file_format = file_manifest.format.lower()
    if file_format == "csv":
        return _iter_rows_from_csv(file_manifest.file_path)
    if file_format in {"geojson", "json"}:
        return _iter_rows_from_geojson(file_manifest.file_path)
    if file_format == "json_array":
        return _iter_rows_from_json(file_manifest.file_path)
    if file_format in {"gpkg", "geopackage"}:
        return _iter_rows_from_gpkg(file_manifest.file_path, file_manifest.layer_name)
    raise IngestError(f"Unsupported file format '{file_manifest.format}' for {file_manifest.file_path}")


def _table_ident(qualified_table: str) -> tuple[sql.Identifier, sql.Identifier]:
    schema_name, table_name = qualified_table.split(".", 1)
    return sql.Identifier(schema_name), sql.Identifier(table_name)


def _insert_raw_rows(
    conn: psycopg.Connection,
    qualified_table: str,
    ingest_run_id: str,
    rows: Iterable[dict[str, Any]],
) -> int:
    schema_ident, table_ident = _table_ident(qualified_table)
    insert_sql = sql.SQL(
        """
        INSERT INTO {}.{} (
            ingest_run_id,
            source_row_num,
            payload_jsonb
        ) VALUES (%s, %s, %s)
        """
    ).format(schema_ident, table_ident)

    total_loaded = 0
    batch: list[tuple[str, int, Jsonb]] = []
    with conn.cursor() as cur:
        for row_num, row in enumerate(rows, start=1):
            batch.append((ingest_run_id, row_num, Jsonb(row)))
            if len(batch) >= CSV_INSERT_BATCH_SIZE:
                cur.executemany(insert_sql, batch)
                total_loaded += len(batch)
                batch.clear()
        if batch:
            cur.executemany(insert_sql, batch)
            total_loaded += len(batch)
            batch.clear()

    return total_loaded


def _analyze_raw_table(conn: psycopg.Connection, qualified_table: str) -> None:
    schema_ident, table_ident = _table_ident(qualified_table)
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("ANALYZE {}.{}").format(schema_ident, table_ident),
        )


def _existing_ingest_run(
    conn: psycopg.Connection,
    source_name: str,
    source_version: str,
    file_set_sha256: str,
) -> str | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id
            FROM meta.ingest_run
            WHERE source_name = %s
              AND source_version = %s
              AND file_set_sha256 = %s
            """,
            (source_name, source_version, file_set_sha256),
        )
        row = cur.fetchone()
    return str(row[0]) if row is not None else None


def ingest_source(conn: psycopg.Connection, manifest: SourceIngestManifest) -> IngestResult:
    file_set_sha256 = _file_set_hash(manifest.files)
    existing = _existing_ingest_run(
        conn,
        source_name=manifest.source_name,
        source_version=manifest.source_version,
        file_set_sha256=file_set_sha256,
    )
    if existing is not None:
        return IngestResult(
            source_name=manifest.source_name,
            run_id=existing,
            status="noop",
            files_loaded=0,
            rows_loaded=0,
        )

    raw_table = RAW_TABLE_BY_SOURCE[manifest.source_name]

    run_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO meta.ingest_run (
                run_id,
                source_name,
                source_version,
                retrieved_at_utc,
                source_url,
                processing_git_sha,
                record_count,
                notes,
                file_set_sha256
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                manifest.source_name,
                manifest.source_version,
                manifest.retrieved_at_utc,
                manifest.source_url,
                manifest.processing_git_sha,
                0,
                manifest.notes,
                file_set_sha256,
            ),
        )

    total_rows = 0
    for file_manifest in manifest.files:
        actual_sha = sha256_file(file_manifest.file_path)
        if actual_sha != file_manifest.sha256:
            raise IngestError(
                "SHA256 mismatch for source file: "
                f"path={file_manifest.file_path} expected={file_manifest.sha256} actual={actual_sha}"
            )

        actual_size = file_manifest.file_path.stat().st_size
        if actual_size != file_manifest.size_bytes:
            raise IngestError(
                f"size_bytes mismatch for {file_manifest.file_path}: "
                f"expected={file_manifest.size_bytes} actual={actual_size}"
            )

        rows = _iter_rows(file_manifest)
        loaded_rows = _insert_raw_rows(conn, raw_table, run_id, rows)

        if file_manifest.row_count_expected is not None and loaded_rows != file_manifest.row_count_expected:
            raise IngestError(
                f"row_count_expected mismatch for {file_manifest.file_path}: "
                f"expected={file_manifest.row_count_expected} loaded={loaded_rows}"
            )

        total_rows += loaded_rows

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO meta.ingest_run_file (
                    ingest_run_id,
                    file_role,
                    filename,
                    layer_name,
                    sha256,
                    size_bytes,
                    row_count,
                    format
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    file_manifest.file_role,
                    str(file_manifest.file_path),
                    file_manifest.layer_name,
                    actual_sha,
                    actual_size,
                    loaded_rows,
                    file_manifest.format,
                ),
            )

    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE meta.ingest_run
            SET record_count = %s
            WHERE run_id = %s
            """,
            (total_rows, run_id),
        )

    _analyze_raw_table(conn, raw_table)

    return IngestResult(
        source_name=manifest.source_name,
        run_id=run_id,
        status="ingested",
        files_loaded=len(manifest.files),
        rows_loaded=total_rows,
    )
