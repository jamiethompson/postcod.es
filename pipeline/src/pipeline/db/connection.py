"""Database connection helpers."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import psycopg


@contextmanager
def connect(dsn: str) -> Iterator[psycopg.Connection]:
    conn = psycopg.connect(dsn)
    try:
        yield conn
    finally:
        conn.close()
