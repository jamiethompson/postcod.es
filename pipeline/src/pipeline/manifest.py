"""Manifest parsing and validation for Pipeline V3."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID


class ManifestError(ValueError):
    """Raised when a manifest file is invalid."""


SOURCE_NAMES = {
    "onspd",
    "os_open_usrn",
    "os_open_names",
    "os_open_roads",
    "os_open_uprn",
    "os_open_lids",
    "nsul",
    "osni_gazetteer",
    "dfi_highway",
    "ppd",
}

BUILD_PROFILES = {
    "gb_core": {
        "onspd",
        "os_open_usrn",
        "os_open_names",
        "os_open_roads",
        "os_open_uprn",
        "os_open_lids",
        "nsul",
    },
    "gb_core_ppd": {
        "onspd",
        "os_open_usrn",
        "os_open_names",
        "os_open_roads",
        "os_open_uprn",
        "os_open_lids",
        "nsul",
        "ppd",
    },
    "core_ni": {
        "onspd",
        "os_open_usrn",
        "os_open_names",
        "os_open_roads",
        "os_open_uprn",
        "os_open_lids",
        "nsul",
        "osni_gazetteer",
        "dfi_highway",
    },
}

SHA256_RE = re.compile(r"^[0-9a-fA-F]{64}$")
GIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$")


@dataclass(frozen=True)
class SourceFileManifest:
    file_role: str
    file_path: Path
    sha256: str
    size_bytes: int
    format: str
    layer_name: str
    row_count_expected: int | None


@dataclass(frozen=True)
class SourceIngestManifest:
    source_name: str
    source_version: str
    retrieved_at_utc: datetime
    source_url: str | None
    processing_git_sha: str
    notes: str | None
    files: tuple[SourceFileManifest, ...]
    raw: dict[str, Any]


@dataclass(frozen=True)
class BuildBundleManifest:
    build_profile: str
    source_runs: dict[str, tuple[str, ...]]
    raw: dict[str, Any]


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ManifestError(f"Invalid JSON manifest: {path}") from exc
    if not isinstance(payload, dict):
        raise ManifestError(f"Manifest root must be an object: {path}")
    return payload


def _require_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ManifestError(f"Manifest field '{key}' must be a non-empty string")
    return value.strip()


def _parse_optional_string(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ManifestError(f"Manifest field '{key}' must be a string when present")
    text = value.strip()
    return text or None


def _parse_utc_datetime(value: str, field_name: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ManifestError(f"Manifest field '{field_name}' must be ISO8601 datetime") from exc
    if parsed.tzinfo is None:
        raise ManifestError(f"Manifest field '{field_name}' must include timezone")
    return parsed.astimezone(timezone.utc)


def _parse_file_entry(entry: dict[str, Any]) -> SourceFileManifest:
    if not isinstance(entry, dict):
        raise ManifestError("Each files[] entry must be an object")

    file_role = _require_string(entry, "file_role")
    file_path_value = _require_string(entry, "file_path")
    file_path = Path(file_path_value).expanduser().resolve()
    if not file_path.exists() or not file_path.is_file():
        raise ManifestError(f"Manifest file_path does not exist: {file_path}")

    sha256 = _require_string(entry, "sha256")
    if SHA256_RE.match(sha256) is None:
        raise ManifestError("files[].sha256 must be 64 hex chars")

    size_bytes = entry.get("size_bytes")
    if not isinstance(size_bytes, int) or size_bytes < 0:
        raise ManifestError("files[].size_bytes must be an integer >= 0")

    format_value = _require_string(entry, "format")
    layer_name = _parse_optional_string(entry, "layer_name") or ""

    row_count_expected_raw = entry.get("row_count_expected")
    row_count_expected: int | None
    if row_count_expected_raw is None:
        row_count_expected = None
    else:
        if not isinstance(row_count_expected_raw, int) or row_count_expected_raw < 0:
            raise ManifestError("files[].row_count_expected must be integer >= 0 when present")
        row_count_expected = row_count_expected_raw

    return SourceFileManifest(
        file_role=file_role,
        file_path=file_path,
        sha256=sha256.lower(),
        size_bytes=size_bytes,
        format=format_value,
        layer_name=layer_name,
        row_count_expected=row_count_expected,
    )


def load_source_manifest(path: Path) -> SourceIngestManifest:
    payload = _load_json(path)

    source_name = _require_string(payload, "source_name")
    if source_name not in SOURCE_NAMES:
        raise ManifestError(f"Invalid source_name '{source_name}'")

    source_version = _require_string(payload, "source_version")
    retrieved_raw = _require_string(payload, "retrieved_at_utc")
    retrieved_at_utc = _parse_utc_datetime(retrieved_raw, "retrieved_at_utc")

    source_url = _parse_optional_string(payload, "source_url")
    processing_git_sha = _require_string(payload, "processing_git_sha")
    if GIT_SHA_RE.match(processing_git_sha) is None:
        raise ManifestError("processing_git_sha must be 40 lowercase hex chars")

    notes = _parse_optional_string(payload, "notes")

    files_raw = payload.get("files")
    if not isinstance(files_raw, list) or not files_raw:
        raise ManifestError("Manifest files must be a non-empty array")

    files = tuple(_parse_file_entry(entry) for entry in files_raw)

    return SourceIngestManifest(
        source_name=source_name,
        source_version=source_version,
        retrieved_at_utc=retrieved_at_utc,
        source_url=source_url,
        processing_git_sha=processing_git_sha,
        notes=notes,
        files=files,
        raw=payload,
    )


def load_bundle_manifest(path: Path) -> BuildBundleManifest:
    payload = _load_json(path)

    build_profile = _require_string(payload, "build_profile")
    if build_profile not in BUILD_PROFILES:
        raise ManifestError(f"Invalid build_profile '{build_profile}'")

    source_runs_raw = payload.get("source_runs")
    if not isinstance(source_runs_raw, dict):
        raise ManifestError("Bundle manifest source_runs must be an object")

    source_runs: dict[str, tuple[str, ...]] = {}
    for source_name, run_ids_raw in source_runs_raw.items():
        if source_name not in SOURCE_NAMES:
            raise ManifestError(f"Unknown source in source_runs: {source_name}")
        run_ids: tuple[str, ...]
        if isinstance(run_ids_raw, str):
            run_ids = (run_ids_raw,)
        elif isinstance(run_ids_raw, list):
            if not run_ids_raw:
                raise ManifestError(f"source_runs[{source_name}] list must not be empty")
            normalized: list[str] = []
            for item in run_ids_raw:
                if not isinstance(item, str):
                    raise ManifestError(
                        f"source_runs[{source_name}] values must be UUID strings"
                    )
                normalized.append(item)
            run_ids = tuple(normalized)
        else:
            raise ManifestError(
                f"source_runs[{source_name}] must be a UUID string or non-empty UUID array"
            )

        parsed_ids: list[str] = []
        for run_id in run_ids:
            try:
                UUID(run_id)
            except ValueError as exc:
                raise ManifestError(
                    f"Invalid ingest run UUID for {source_name}: {run_id}"
                ) from exc
            parsed_ids.append(run_id)
        source_runs[source_name] = tuple(parsed_ids)

    required = BUILD_PROFILES[build_profile]
    missing = sorted(required - set(source_runs.keys()))
    if missing:
        raise ManifestError(
            "Bundle manifest missing required sources for profile "
            f"{build_profile}: {', '.join(missing)}"
        )

    for source_name in required:
        if len(source_runs.get(source_name, ())) == 0:
            raise ManifestError(
                f"Bundle manifest source_runs[{source_name}] must include at least one ingest run id"
            )

    return BuildBundleManifest(build_profile=build_profile, source_runs=source_runs, raw=payload)
