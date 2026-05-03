"""CLI for the Awksion data pipeline.

Examples:
    python -m awksion init                  # create tables
    python -m awksion migrate                # load existing CSVs into the DB
    python -m awksion run db1                # rebuild venue database
    python -m awksion run db2                # rebuild artist database
    python -m awksion export xlsx            # write awksion_handoff.xlsx
    python -m awksion stats                  # row counts per table
"""
from __future__ import annotations

import argparse
import logging
import sys

from awksion.db import init_db, get_session, Venue, Artist, ScrapeRun

log = logging.getLogger(__name__)


def cmd_init(args) -> int:
    init_db()
    print("Initialized database tables.")
    return 0


def cmd_migrate(args) -> int:
    from awksion.pipelines import migrate_csvs
    result = migrate_csvs.run()
    print(f"Migrated: {result}")
    return 0


def cmd_run(args) -> int:
    if args.target == "db1":
        from awksion.pipelines import build_db1
        result = build_db1.run(dry_run=args.dry_run)
    elif args.target == "db2":
        from awksion.pipelines import build_db2
        result = build_db2.run(dry_run=args.dry_run)
    else:
        print(f"Unknown target: {args.target}", file=sys.stderr)
        return 2
    print(f"Done: {result}")
    return 0


def cmd_export(args) -> int:
    if args.format == "xlsx":
        from awksion.exporters import to_xlsx
        path = to_xlsx.export()
        print(f"Wrote: {path}")
        return 0
    print(f"Unknown export format: {args.format}", file=sys.stderr)
    return 2


def cmd_stats(args) -> int:
    init_db()
    with get_session() as s:
        venues = s.query(Venue).count()
        ca_venues = s.query(Venue).filter(Venue.country == "CA").count()
        us_venues = s.query(Venue).filter(Venue.country == "US").count()
        flagged = s.query(Venue).filter(Venue.ticketing_affiliated.is_(True)).count()
        artists = s.query(Artist).count()
        runs = s.query(ScrapeRun).count()

        print("=" * 50)
        print("AWKSION DB STATS")
        print("=" * 50)
        print(f"  Venues total:            {venues}")
        print(f"    Canada:                {ca_venues}")
        print(f"    US:                    {us_venues}")
        print(f"    Ticketing-affiliated:  {flagged}")
        print(f"  Artists total:           {artists}")
        print(f"  Scrape runs logged:      {runs}")
        print()

        # Tier breakdown
        from sqlalchemy import func
        rows = s.query(Artist.tier, func.count(Artist.id)).group_by(Artist.tier).all()
        if rows:
            print("  Artists by tier:")
            for tier, count in rows:
                print(f"    {tier or '(none)':20s} {count}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="awksion", description="Awksion data pipeline CLI")
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="Create DB tables").set_defaults(func=cmd_init)
    sub.add_parser("migrate", help="Load existing CSVs into the DB").set_defaults(func=cmd_migrate)

    p_run = sub.add_parser("run", help="Run a pipeline")
    p_run.add_argument("target", choices=["db1", "db2"])
    p_run.add_argument("--dry-run", action="store_true")
    p_run.set_defaults(func=cmd_run)

    p_export = sub.add_parser("export", help="Export the DB to a file")
    p_export.add_argument("format", choices=["xlsx"])
    p_export.set_defaults(func=cmd_export)

    sub.add_parser("stats", help="Print row counts").set_defaults(func=cmd_stats)
    return p


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
