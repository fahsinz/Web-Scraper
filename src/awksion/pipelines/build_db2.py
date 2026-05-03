"""DB2 pipeline orchestrator: emerging musicians by tier + geography.

Steps:
1. Last.fm geo.getTopArtists for Canada + 5 US cities → add new candidate artists
2. Spotify follower enrichment (search + batch follower lookup)
3. Tier bucketing (10k-25k, 25k-50k, 50k-75k, 75k-100k)
"""
from __future__ import annotations

import logging

from awksion.db import init_db

log = logging.getLogger(__name__)


def run(dry_run: bool = False) -> dict:
    init_db()

    if dry_run:
        log.info("DRY RUN — printing intended steps only")
        return {"dry_run": True, "steps": ["lastfm_geo", "spotify_followers", "artist_tiers"]}

    from awksion.scrapers import lastfm_geo
    from awksion.enrichers import spotify_followers, artist_tiers

    log.info("[1/3] Last.fm geo.getTopArtists...")
    geo = lastfm_geo.run()
    log.info("       %s", geo)

    log.info("[2/3] Spotify follower enrichment...")
    sp = spotify_followers.run(only_missing=True)
    log.info("       %s", sp)

    log.info("[3/3] Artist tier bucketing...")
    tiers = artist_tiers.run()
    log.info("       %s", tiers)

    return {"lastfm_geo": geo, "spotify_followers": sp, "artist_tiers": tiers}
