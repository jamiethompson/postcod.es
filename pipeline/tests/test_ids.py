from __future__ import annotations

import re
from datetime import datetime

from util.id_words import ADJECTIVES_64, BUNDLE_NOUNS_256
from util.ids import generate_build_run_id, generate_bundle_id, generate_ingest_run_id


def test_bundle_lexicon_sizes() -> None:
    assert len(ADJECTIVES_64) == 64
    assert len(BUNDLE_NOUNS_256) == 256


def test_bundle_id_is_deterministic_and_pg_safe() -> None:
    created_at = datetime(2026, 2, 21, 8, 45, 0)

    bundle_id_1 = generate_bundle_id(
        "onsud-2026q1",
        "open-uprn-2026q1",
        "open-roads-2026q1",
        created_at=created_at,
    )
    bundle_id_2 = generate_bundle_id(
        "onsud-2026q1",
        "open-uprn-2026q1",
        "open-roads-2026q1",
        created_at=created_at,
    )

    assert bundle_id_1 == bundle_id_2
    assert len(bundle_id_1) <= 63
    assert re.fullmatch(r"v\d{6}_[a-z0-9_]+_[a-z0-9_]+_[a-f0-9]{6}", bundle_id_1)


def test_ingest_and_build_run_ids_are_uuids() -> None:
    ingest_run_id_1 = generate_ingest_run_id()
    ingest_run_id_2 = generate_ingest_run_id()
    build_run_id_1 = generate_build_run_id()
    build_run_id_2 = generate_build_run_id()

    uuid_pattern = r"[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}"

    assert re.fullmatch(uuid_pattern, ingest_run_id_1)
    assert re.fullmatch(uuid_pattern, ingest_run_id_2)
    assert re.fullmatch(uuid_pattern, build_run_id_1)
    assert re.fullmatch(uuid_pattern, build_run_id_2)

    assert ingest_run_id_1 != ingest_run_id_2
    assert build_run_id_1 != build_run_id_2
