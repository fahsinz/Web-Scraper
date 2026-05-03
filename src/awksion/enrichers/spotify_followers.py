"""Spotify follower count enricher.

Uses spotipy's `artists()` batch endpoint (50 IDs per call) to look up follower
counts for every artist in the DB. The Web API still exposes
`followers.total` and `genres` on the artist object; what changed in Nov 2024
was the recommendation/related-artists endpoints, not these.

For artists without a `spotify_id`, we run a single search to find one first.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from difflib import SequenceMatcher

from tqdm import tqdm

from awksion.config import SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET
from awksion.db import Artist, ScrapeRun, get_session

log = logging.getLogger(__name__)

NAME_MATCH_THRESHOLD = 0.6
SLEEP_PER_BATCH = 0.3
SLEEP_PER_SEARCH = 0.2

_sp_client = None


def _get_spotify():
    global _sp_client
    if _sp_client is not None:
        return _sp_client
    if not SPOTIFY_CLIENT_ID or not SPOTIFY_CLIENT_SECRET:
        log.warning("Spotify credentials not set — skipping follower enrichment")
        return None
    try:
        import spotipy
        from spotipy.oauth2 import SpotifyClientCredentials
        _sp_client = spotipy.Spotify(
            auth_manager=SpotifyClientCredentials(
                client_id=SPOTIFY_CLIENT_ID,
                client_secret=SPOTIFY_CLIENT_SECRET,
            ),
            requests_timeout=15,
            retries=3,
        )
    except Exception as e:
        log.warning("Could not init Spotify client: %s", e)
        return None
    return _sp_client


def _name_sim(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()


def _search_one(sp, name: str) -> dict | None:
    try:
        res = sp.search(q=f"artist:{name}", type="artist", limit=1)
    except Exception:
        return None
    items = (res.get("artists") or {}).get("items") or []
    if not items:
        return None
    top = items[0]
    if _name_sim(name, top.get("name", "")) < NAME_MATCH_THRESHOLD:
        return None
    return top


def run(only_missing: bool = True) -> dict:
    sp = _get_spotify()
    if sp is None:
        return {"skipped": True, "reason": "no_credentials"}

    with get_session() as session:
        scrape_run = ScrapeRun(pipeline="db2", source="spotify_followers")
        session.add(scrape_run)
        session.flush()

        q = session.query(Artist)
        if only_missing:
            q = q.filter(Artist.spotify_followers.is_(None))
        artists = q.all()
        log.info("Enriching %d artists with Spotify followers...", len(artists))

        # Phase 1: search for spotify_id where missing
        for a in tqdm(artists, desc="Spotify search"):
            if a.spotify_id:
                continue
            top = _search_one(sp, a.name)
            time.sleep(SLEEP_PER_SEARCH)
            if not top:
                continue
            a.spotify_id = top.get("id")
            a.spotify_url = (top.get("external_urls") or {}).get("spotify")
            a.spotify_name = top.get("name")
            if top.get("followers"):
                a.spotify_followers = (top.get("followers") or {}).get("total")
            if top.get("genres"):
                a.genres = ", ".join(top["genres"][:8]) or None

        session.flush()

        # Phase 2: batch refresh follower counts for artists missing it
        targets = [a for a in artists if a.spotify_id and a.spotify_followers is None]
        log.info("Batch fetching followers for %d artists with spotify_id...", len(targets))

        updated = 0
        for i in tqdm(range(0, len(targets), 50), desc="Spotify batch"):
            chunk = targets[i:i + 50]
            ids = [a.spotify_id for a in chunk]
            try:
                resp = sp.artists(ids)
            except Exception as e:
                log.debug("  batch failed: %s", e)
                time.sleep(2)
                continue
            by_id = {x.get("id"): x for x in (resp.get("artists") or []) if x}
            for a in chunk:
                obj = by_id.get(a.spotify_id)
                if not obj:
                    continue
                a.spotify_followers = (obj.get("followers") or {}).get("total")
                if obj.get("genres") and not a.genres:
                    a.genres = ", ".join(obj["genres"][:8]) or None
                updated += 1
            time.sleep(SLEEP_PER_BATCH)

        scrape_run.rows_in = len(artists)
        scrape_run.rows_written = updated
        scrape_run.finished_at = datetime.now(timezone.utc)
        scrape_run.status = "ok"
        scrape_run.notes = f"updated={updated}"

    return {"considered": len(artists), "updated": updated}
