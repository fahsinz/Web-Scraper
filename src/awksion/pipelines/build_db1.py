"""DB1 pipeline orchestrator.

Steps:
1. Migrate any new CSV rows from `data/indieonthemove_with_capacity.csv` (idempotent).
2. Apply the ticketing-affiliation filter (domain + HTML + capacity heuristic).
3. (Optional, time-permitting) website regex enrichment for owner/year/email on top venues.

Each step is idempotent — re-running will only update flags / add new rows.
"""
from __future__ import annotations

import logging

from awksion.db import init_db
from awksion.pipelines import migrate_csvs

log = logging.getLogger(__name__)


def run(dry_run: bool = False, fetch_html: bool = True) -> dict:
    init_db()

    if dry_run:
        log.info("DRY RUN — printing intended steps only")
        return {"dry_run": True, "steps": ["migrate_csvs", "ticketing_filter"]}

    log.info("[1/3] Migrating CSV rows into DB...")
    migration = migrate_csvs.run()
    log.info("       Added: %s", migration)

    log.info("[2/3] Applying ticketing-affiliation filter (fetch_html=%s)...", fetch_html)
    from awksion.enrichers import ticketing_filter
    tf = ticketing_filter.run(fetch_html=fetch_html)
    log.info("       %s", tf)

    log.info("[3/3] Website regex enrichment (top venues)...")
    try:
        from awksion.enrichers import website_regex
        wr = website_regex.run(top_n=200)
        log.info("       %s", wr)
    except ImportError:
        log.info("       skipped (website_regex not yet implemented)")
        wr = {"skipped": True}

    return {"migration": migration, "ticketing_filter": tf, "website_regex": wr}
