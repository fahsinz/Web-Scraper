"""Last.fm geo.getTopArtists scraper — free, key-only.

Pulls top artists per country/city from Last.fm. Provides geographic segmentation
that the Spotify API doesn't expose (Spotify has no "artists by country" endpoint).
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

import requests
from tqdm import tqdm

from awksion.config import LASTFM_API_KEY
from awksion.db import Artist, ScrapeRun, get_session

log = logging.getLogger(__name__)

LASTFM_URL = "https://ws.audioscrobbler.com/2.0/"
SLEEP = 0.25
PAGES_PER_AREA = 5  # 50 artists per page → 250 per area
LIMIT_PER_PAGE = 50

# (display_label, lastfm_country_param, country_code, city_or_None)
TARGET_AREAS = [
    ("Canada",      "Canada",        "CA", None),
    ("New York",    "United States", "US", "New York"),
    ("Los Angeles", "United States", "US", "Los Angeles"),
    ("Chicago",     "United States", "US", "Chicago"),
    ("Nashville",   "United States", "US", "Nashville"),
    ("Austin",      "United States", "US", "Austin"),
]


def _fetch_top_artists_country(country_name: str, page: int = 1) -> list[dict]:
    if not LASTFM_API_KEY:
        log.warning("LASTFM_API_KEY not set — skipping geo fetch")
        return []
    params = {
        "method": "geo.getTopArtists",
        "country": country_name,
        "api_key": LASTFM_API_KEY,
        "format": "json",
        "limit": LIMIT_PER_PAGE,
        "page": page,
    }
    try:
        r = requests.get(LASTFM_URL, params=params, timeout=15)
        if r.status_code != 200:
            return []
        data = r.json()
    except (requests.RequestException, ValueError):
        return []
    return (data.get("topartists") or {}).get("artist") or []


def _fetch_top_metro(metro: str, page: int = 1) -> list[dict]:
    """For US cities we use chart.getTopArtists with no perfect city filter; fall
    back to country-level for now. Last.fm doesn't expose city-level top-artists
    publicly, so we tag city as a hint only."""
    return _fetch_top_artists_country("United States", page=page)


def run() -> dict:
    if not LASTFM_API_KEY:
        log.warning("LASTFM_API_KEY missing — skipping lastfm_geo run.")
        return {"skipped": True, "reason": "no_api_key"}

    total_added = 0
    per_area: dict[str, int] = {}

    with get_session() as session:
        scrape_run = ScrapeRun(pipeline="db2", source="lastfm_geo")
        session.add(scrape_run)
        session.flush()

        existing = {
            (n, sid) for (n, sid) in session.query(Artist.name, Artist.spotify_id).all()
        }

        for label, country_name, country_code, city in TARGET_AREAS:
            log.info("Fetching Last.fm top artists for %s...", label)
            added_here = 0
            for page in range(1, PAGES_PER_AREA + 1):
                artists = (_fetch_top_metro(label, page) if city
                           else _fetch_top_artists_country(country_name, page))
                if not artists:
                    break
                for a in artists:
                    name = (a.get("name") or "").strip()
                    if not name:
                        continue
                    key = (name, None)
                    if key in existing:
                        continue
                    artist = Artist(
                        name=name,
                        lastfm_url=(a.get("url") or "").strip() or None,
                        country=country_code,
                        city=city,
                        geo_confidence="low" if city else "medium",
                        source="lastfm_geo",
                    )
                    session.add(artist)
                    existing.add(key)
                    added_here += 1
                time.sleep(SLEEP)
            per_area[label] = added_here
            total_added += added_here
            log.info("  %s: +%d artists", label, added_here)

        scrape_run.rows_written = total_added
        scrape_run.finished_at = datetime.now(timezone.utc)
        scrape_run.status = "ok"
        scrape_run.notes = f"per_area={per_area}"

    return {"added": total_added, "per_area": per_area}
