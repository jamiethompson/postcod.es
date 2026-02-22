"""Simple SQL migration runner for pipeline schemas."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List


@dataclass(frozen=True)
class Migration:
    version: str
    path: Path


def discover_migrations(migrations_dir: Path) -> List[Migration]:
    """Return sorted migration files based on numeric filename prefix."""

    migrations: List[Migration] = []
    for path in sorted(migrations_dir.glob("*.sql")):
        version = path.stem.split("_", 1)[0]
        migrations.append(Migration(version=version, path=path))
    return migrations


def _read_sql(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def apply_migrations(dsn: str, migrations_dir: Path) -> int:
    """Apply unapplied migrations in filename order.

    Requires psycopg at runtime, but keeps import optional for environments
    where only static checks are needed.
    """

    try:
        import psycopg  # type: ignore
    except ImportError as exc:  # pragma: no cover - import-path safety
        raise RuntimeError("psycopg is required to run migrations") from exc

    migrations = discover_migrations(migrations_dir)
    applied_count = 0

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS meta")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS meta.schema_migration (
                    version text PRIMARY KEY,
                    applied_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            cur.execute("SELECT version FROM meta.schema_migration")
            applied_versions = {row[0] for row in cur.fetchall()}

            for migration in migrations:
                if migration.version in applied_versions:
                    continue
                cur.execute(_read_sql(migration.path))
                cur.execute(
                    "INSERT INTO meta.schema_migration (version) VALUES (%s)",
                    (migration.version,),
                )
                applied_count += 1

    return applied_count
