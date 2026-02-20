"""Canonicalisation helpers for Pipeline V3."""

from __future__ import annotations

import json
import re
import unicodedata
from functools import lru_cache
from pathlib import Path

from pipeline.config import normalisation_config_path


def _load_json_config(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


@lru_cache(maxsize=1)
def _alias_map() -> dict[str, str]:
    config = _load_json_config(normalisation_config_path())
    raw_alias = config.get("alias_map", {})
    if not isinstance(raw_alias, dict):
        return {}
    output: dict[str, str] = {}
    for key, value in raw_alias.items():
        if isinstance(key, str) and isinstance(value, str):
            output[key.upper()] = value.upper()
    return output


@lru_cache(maxsize=1)
def _strip_punctuation() -> str:
    config = _load_json_config(normalisation_config_path())
    value = config.get("strip_punctuation", ".,'-")
    if not isinstance(value, str):
        return ".,'-"
    return value


def postcode_norm(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = re.sub(r"[^A-Za-z0-9]", "", value).upper()
    if not cleaned:
        return None
    return cleaned


def postcode_display(value: str | None) -> str | None:
    normalized = postcode_norm(value)
    if normalized is None:
        return None
    if len(normalized) <= 3:
        return normalized
    return f"{normalized[:-3]} {normalized[-3:]}"


def street_casefold(value: str | None) -> str | None:
    if value is None:
        return None

    text = unicodedata.normalize("NFKC", value).strip().upper()
    text = re.sub(r"\s+", " ", text)
    strip_chars = _strip_punctuation()
    if strip_chars:
        text = text.translate(str.maketrans("", "", strip_chars))
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return None

    alias_map = _alias_map()
    tokens = [alias_map.get(token, token) for token in text.split(" ")]
    canonical = " ".join(tokens).strip()
    return canonical or None

