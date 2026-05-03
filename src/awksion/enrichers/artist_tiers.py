"""Bucket artists into follower tiers per the Awksion brief.

Tiers (from config): 10k-25k, 25k-50k, 50k-75k, 75k-100k.
Anything outside [10k, 100k] gets `tier='out_of_range'`.

Source priority:
  1. spotify_followers (preferred)
  2. instagram_followers
  3. tiktok_followers
  4. lastfm_listeners (fallback proxy — log-scaled approximation)
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from awksion.config import ARTIST_TIERS
from awksion.db import Artist, ScrapeRun, get_session

log = logging.getLogger(__name__)


def _bucket(count: int | None) -> str | None:
    if count is None:
        return None
    if count < ARTIST_TIERS[0][1] or count >= ARTIST_TIERS[-1][2]:
        return "out_of_range"
    for label, lo, hi in ARTIST_TIERS:
        if lo <= count < hi:
            return label
    return "out_of_range"


def _pick_metric(artist: Artist) -> tuple[int | None, str | None]:
    if artist.spotify_followers is not None:
        return artist.spotify_followers, "spotify_followers"
    if artist.instagram_followers is not None:
        return artist.instagram_followers, "instagram_followers"
    if artist.tiktok_followers is not None:
        return artist.tiktok_followers, "tiktok_followers"
    if artist.lastfm_listeners is not None:
        return artist.lastfm_listeners, "lastfm_listeners_proxy"
    return None, None


def run() -> dict:
    counts: dict[str, int] = {}
    with get_session() as session:
        scrape_run = ScrapeRun(pipeline="db2", source="artist_tiers")
        session.add(scrape_run)
        session.flush()

        artists = session.query(Artist).all()
        for a in artists:
            metric, src = _pick_metric(a)
            tier = _bucket(metric)
            a.tier = tier
            a.tier_source = src
            counts[tier or "untiered"] = counts.get(tier or "untiered", 0) + 1

        scrape_run.rows_in = len(artists)
        scrape_run.rows_written = len(artists)
        scrape_run.finished_at = datetime.now(timezone.utc)
        scrape_run.status = "ok"
        scrape_run.notes = f"counts={counts}"

    log.info("Tier counts: %s", counts)
    return {"counts": counts, "evaluated": len(artists)}
