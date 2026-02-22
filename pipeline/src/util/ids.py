"""ID generators: deterministic human-readable bundle IDs and UUID run IDs."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Final
from uuid import uuid4

from .id_words import ADJECTIVES_64, BUNDLE_NOUNS_256

_CLEAN_RE: Final[re.Pattern[str]] = re.compile(r"[^a-z0-9_]")


def _require_non_empty(value: str, *, field_name: str) -> str:
    if value is None or value.strip() == "":
        raise ValueError(f"{field_name} must be non-empty and non-whitespace")
    return value


def _to_utc_aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _hash_parts(seed: str, nouns: tuple[str, ...]) -> tuple[str, str, str]:
    if not seed:
        raise ValueError("seed must not be empty")

    digest = hashlib.sha256(seed.encode("utf-8")).digest()
    digest_hex = hashlib.sha256(seed.encode("utf-8")).hexdigest()

    adjective = ADJECTIVES_64[digest[0] % len(ADJECTIVES_64)]
    noun = nouns[digest[1] % len(nouns)]
    hash6 = digest_hex[:6]
    return adjective, noun, hash6


def _clean_identifier(value: str, *, max_len: int | None = None) -> str:
    cleaned = _CLEAN_RE.sub("", value.lower())
    if max_len is not None:
        return cleaned[:max_len]
    return cleaned


def _bundle_seed(onsud_release_id: str, open_uprn_release_id: str, open_roads_release_id: str) -> str:
    # Canonical JSON encoding avoids delimiter-collision ambiguity.
    return json.dumps(
        [onsud_release_id, open_uprn_release_id, open_roads_release_id],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def generate_bundle_id(
    onsud_release_id: str,
    open_uprn_release_id: str,
    open_roads_release_id: str,
    *,
    created_at: datetime,
) -> str:
    """Generate deterministic bundle ID.

    Format:
        v<yyyymm>_<adjective>_<bundle_noun>_<hash6>
    """

    onsud = _require_non_empty(onsud_release_id, field_name="onsud_release_id")
    open_uprn = _require_non_empty(open_uprn_release_id, field_name="open_uprn_release_id")
    open_roads = _require_non_empty(open_roads_release_id, field_name="open_roads_release_id")
    created_at_utc = _to_utc_aware(created_at, field_name="created_at")

    seed = _bundle_seed(onsud, open_uprn, open_roads)
    adjective, noun, hash6 = _hash_parts(seed, BUNDLE_NOUNS_256)
    yyyymm = created_at_utc.strftime("%Y%m")
    identifier = f"v{yyyymm}_{adjective}_{noun}_{hash6}"

    # PostgreSQL identifier max length is 63 bytes.
    return _clean_identifier(identifier, max_len=63)


def generate_ingest_run_id() -> str:
    """Generate ingest run ID as UUIDv4."""

    return str(uuid4())


def generate_build_run_id() -> str:
    """Generate build run ID as UUIDv4."""

    return str(uuid4())


__all__ = [
    "generate_build_run_id",
    "generate_bundle_id",
    "generate_ingest_run_id",
]
