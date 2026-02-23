"""Runtime configuration for Pipeline V3."""

from __future__ import annotations

import os
from pathlib import Path

PROBABILITY_SCALE = 4
PROBABILITY_NUMERIC_TYPE = "numeric(6,4)"


def default_dsn() -> str:
    return os.getenv("PIPELINE_DSN", "dbname=postcodes_v3")


def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def migrations_dir() -> Path:
    return repo_root() / "pipeline" / "sql" / "migrations"


def source_schema_config_path() -> Path:
    return repo_root() / "pipeline" / "config" / "source_schema.yaml"


def frequency_weights_config_path() -> Path:
    return repo_root() / "pipeline" / "config" / "frequency_weights.yaml"


def normalisation_config_path() -> Path:
    return repo_root() / "pipeline" / "config" / "normalisation.yaml"
