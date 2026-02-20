"""CLI entrypoint for Pipeline V3 lifecycle commands."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pipeline.build.workflows import (
    BuildError,
    create_build_bundle,
    publish_build,
    run_build,
    verify_build,
)
from pipeline.config import default_dsn, migrations_dir
from pipeline.db.connection import connect
from pipeline.db.migrations import apply_migrations
from pipeline.ingest.workflows import IngestError, ingest_source
from pipeline.manifest import ManifestError, load_bundle_manifest, load_source_manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pipeline")
    parser.add_argument("--dsn", default=default_dsn(), help="PostgreSQL DSN")

    subparsers = parser.add_subparsers(dest="command", required=True)

    db_parser = subparsers.add_parser("db", help="Database operations")
    db_subparsers = db_parser.add_subparsers(dest="db_command", required=True)
    db_subparsers.add_parser("migrate", help="Apply SQL migrations")

    ingest_parser = subparsers.add_parser("ingest", help="Source ingest operations")
    ingest_subparsers = ingest_parser.add_subparsers(dest="ingest_command", required=True)
    source_parser = ingest_subparsers.add_parser("source", help="Ingest a source manifest")
    source_parser.add_argument("--manifest", required=True, type=Path)

    bundle_parser = subparsers.add_parser("bundle", help="Bundle lifecycle")
    bundle_subparsers = bundle_parser.add_subparsers(dest="bundle_command", required=True)
    bundle_create_parser = bundle_subparsers.add_parser("create", help="Create build bundle")
    bundle_create_parser.add_argument("--manifest", required=True, type=Path)

    build_parser = subparsers.add_parser("build", help="Build lifecycle")
    build_subparsers = build_parser.add_subparsers(dest="build_command", required=True)

    build_run_parser = build_subparsers.add_parser("run", help="Run build passes")
    build_run_parser.add_argument("--bundle-id", required=True)
    build_run_parser.add_argument("--rebuild", action="store_true")
    build_run_parser.add_argument("--resume", action="store_true")

    build_verify_parser = build_subparsers.add_parser("verify", help="Verify build outputs")
    build_verify_parser.add_argument("--build-run-id", required=True)

    build_publish_parser = build_subparsers.add_parser("publish", help="Publish verified build")
    build_publish_parser.add_argument("--build-run-id", required=True)
    build_publish_parser.add_argument("--actor", required=True)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "db" and args.db_command == "migrate":
            applied = apply_migrations(args.dsn, migrations_dir())
            print(json.dumps({"status": "ok", "migrations_applied": applied}))
            return 0

        if args.command == "ingest" and args.ingest_command == "source":
            manifest = load_source_manifest(args.manifest)
            with connect(args.dsn) as conn:
                result = ingest_source(conn, manifest)
                conn.commit()
            print(
                json.dumps(
                    {
                        "status": result.status,
                        "source_name": result.source_name,
                        "ingest_run_id": result.run_id,
                        "files_loaded": result.files_loaded,
                        "rows_loaded": result.rows_loaded,
                    }
                )
            )
            return 0

        if args.command == "bundle" and args.bundle_command == "create":
            manifest = load_bundle_manifest(args.manifest)
            with connect(args.dsn) as conn:
                result = create_build_bundle(conn, manifest)
                conn.commit()
            print(
                json.dumps(
                    {
                        "status": result.status,
                        "bundle_id": result.bundle_id,
                        "bundle_hash": result.bundle_hash,
                    }
                )
            )
            return 0

        if args.command == "build" and args.build_command == "run":
            with connect(args.dsn) as conn:
                result = run_build(
                    conn,
                    bundle_id=args.bundle_id,
                    rebuild=args.rebuild,
                    resume=args.resume,
                )
                conn.commit()
            print(
                json.dumps(
                    {
                        "status": result.status,
                        "build_run_id": result.build_run_id,
                        "dataset_version": result.dataset_version,
                        "message": result.message,
                    }
                )
            )
            return 0

        if args.command == "build" and args.build_command == "verify":
            with connect(args.dsn) as conn:
                result = verify_build(conn, build_run_id=args.build_run_id)
                conn.commit()
            print(
                json.dumps(
                    {
                        "status": result.status,
                        "build_run_id": result.build_run_id,
                        "object_hashes": result.object_hashes,
                    }
                )
            )
            return 0

        if args.command == "build" and args.build_command == "publish":
            with connect(args.dsn) as conn:
                result = publish_build(
                    conn,
                    build_run_id=args.build_run_id,
                    actor=args.actor,
                )
                conn.commit()
            print(
                json.dumps(
                    {
                        "status": result.status,
                        "build_run_id": result.build_run_id,
                        "dataset_version": result.dataset_version,
                    }
                )
            )
            return 0

        parser.print_help(sys.stderr)
        return 2
    except (ManifestError, IngestError, BuildError, RuntimeError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
