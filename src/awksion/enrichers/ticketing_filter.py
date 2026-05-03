"""Flag venues affiliated with major ticketing agencies.

Three signals (any one trips the flag):
1. Domain blocklist on `website` (Ticketmaster, Live Nation, AXS, etc.)
2. Homepage HTML regex for those brand names / "powered by"
3. Capacity heuristic: capacity_high > 2000 is "likely major room"

Sets `ticketing_affiliated` and `ticketing_evidence` on every Venue row.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from tqdm import tqdm

from awksion.config import TICKETING_BRAND_PATTERNS, TICKETING_DOMAIN_BLOCKLIST
from awksion.db import ScrapeRun, Venue, get_session

log = logging.getLogger(__name__)

REQUEST_TIMEOUT = 8
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)
LARGE_VENUE_THRESHOLD = 2000  # capacity_high above this → flagged as "likely major room"

_brand_re = re.compile("|".join(TICKETING_BRAND_PATTERNS), re.IGNORECASE)


def _check_domain(website: str | None) -> str | None:
    if not website:
        return None
    try:
        host = urlparse(website).hostname or ""
    except Exception:
        return None
    host = host.lower().lstrip("www.")
    for blocked in TICKETING_DOMAIN_BLOCKLIST:
        if blocked in host:
            return f"domain:{blocked}"
    return None


def _check_homepage_html(website: str | None) -> str | None:
    if not website or not website.startswith(("http://", "https://")):
        return None
    try:
        resp = requests.get(
            website,
            headers={"User-Agent": USER_AGENT},
            timeout=REQUEST_TIMEOUT,
            allow_redirects=True,
        )
        if resp.status_code != 200:
            return None
        html = resp.text or ""
    except requests.RequestException:
        return None

    # Check redirects landed on a ticketing domain
    final_host = (urlparse(resp.url).hostname or "").lower().lstrip("www.")
    for blocked in TICKETING_DOMAIN_BLOCKLIST:
        if blocked in final_host:
            return f"redirect:{blocked}"

    m = _brand_re.search(html)
    if m:
        snippet = m.group(0)[:60]
        return f"html:{snippet.lower()}"

    if re.search(r"<iframe[^>]*src\s*=\s*[\"'][^\"']*(?:ticketmaster|livenation|axs|etix|seetickets|ticketweb)\.", html, re.IGNORECASE):
        return "iframe:ticketing"

    return None


def _check_capacity_heuristic(capacity_high: int | None) -> str | None:
    if capacity_high is None:
        return None
    if capacity_high > LARGE_VENUE_THRESHOLD:
        return f"large_room:{capacity_high}"
    return None


def evaluate_venue(venue: Venue, fetch_html: bool = True) -> tuple[bool, str | None]:
    """Return (affiliated, evidence_string)."""
    ev = _check_domain(venue.website)
    if ev:
        return True, ev

    if fetch_html:
        ev = _check_homepage_html(venue.website)
        if ev:
            return True, ev

    ev = _check_capacity_heuristic(venue.capacity_high)
    if ev:
        return True, ev

    return False, None


def run(fetch_html: bool = True, limit: int | None = None,
        only_unflagged: bool = False) -> dict:
    """Apply ticketing filter to every venue.

    If `only_unflagged=True`, skip venues already marked True (preserves prior
    HTML-based detections when re-running with `fetch_html=False`).
    """
    with get_session() as session:
        scrape_run = ScrapeRun(pipeline="db1", source="ticketing_filter")
        session.add(scrape_run)
        session.flush()

        q = session.query(Venue)
        if only_unflagged:
            q = q.filter(Venue.ticketing_affiliated.is_(False))
        if limit:
            q = q.limit(limit)
        venues = q.all()
        log.info("Evaluating %d venues against ticketing filter...", len(venues))

        flagged = 0
        for venue in tqdm(venues, desc="Ticketing filter"):
            affiliated, evidence = evaluate_venue(venue, fetch_html=fetch_html)
            # Preserve True flags when running a weaker check
            if affiliated or not only_unflagged:
                venue.ticketing_affiliated = affiliated
                venue.ticketing_evidence = evidence
            if affiliated:
                flagged += 1

        scrape_run.rows_in = len(venues)
        scrape_run.rows_written = flagged
        scrape_run.finished_at = datetime.now(timezone.utc)
        scrape_run.status = "ok"
        scrape_run.notes = f"flagged={flagged}/{len(venues)}; fetch_html={fetch_html}"

    log.info("  Flagged %d/%d venues as ticketing-affiliated", flagged, len(venues))
    return {"evaluated": len(venues), "flagged": flagged}
