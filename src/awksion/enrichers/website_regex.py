"""Best-effort website regex enrichment.

For the top-N highest-capacity venues with a website, fetches homepage + /about +
/contact pages and regex-extracts:
  - email (mailto:)
  - year established
  - owner name (heuristic, low-confidence)

Marked as best-effort per the Awksion brief — sets `owner_confidence` to
'low'/'medium' and never blocks delivery.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

import requests
from sqlalchemy import desc
from tqdm import tqdm

from awksion.db import ScrapeRun, Venue, get_session

log = logging.getLogger(__name__)

REQUEST_TIMEOUT = 8
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)

EMAIL_RE = re.compile(r"\b[\w.+-]+@[\w-]+\.[\w.-]+\b", re.IGNORECASE)
YEAR_RE = re.compile(
    r"(?:established|est\.?|since|founded|opened)\s+(?:in\s+)?(\d{4})",
    re.IGNORECASE,
)
OWNER_RES = [
    re.compile(r"(?:owner|owned by|proprietor)[:\s]+([A-Z][A-Za-z'-]+(?:\s+[A-Z][A-Za-z'-]+){0,3})"),
    re.compile(r"founded\s+by\s+([A-Z][A-Za-z'-]+(?:\s+[A-Z][A-Za-z'-]+){0,3})", re.IGNORECASE),
]

PATHS_TO_TRY = ["", "/about", "/about-us", "/contact", "/contact-us"]


def _fetch(url: str) -> str | None:
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT},
                         timeout=REQUEST_TIMEOUT, allow_redirects=True)
        if r.status_code != 200:
            return None
        return r.text
    except requests.RequestException:
        return None


def _scrape_one(website: str) -> dict:
    parsed = urlparse(website)
    if not parsed.scheme:
        website = "http://" + website
    base = website.rstrip("/")

    findings = {"email": None, "year_established": None,
                "owner": None, "owner_confidence": None}

    for path in PATHS_TO_TRY:
        url = urljoin(base + "/", path.lstrip("/"))
        html = _fetch(url)
        if not html:
            continue

        if not findings["email"]:
            m = EMAIL_RE.search(html)
            if m:
                addr = m.group(0)
                # Filter junk and image extensions
                if not re.search(r"\.(png|jpg|jpeg|gif|svg|webp)$", addr, re.IGNORECASE):
                    findings["email"] = addr

        if not findings["year_established"]:
            m = YEAR_RE.search(html)
            if m:
                yr = int(m.group(1))
                if 1700 <= yr <= datetime.now().year:
                    findings["year_established"] = yr

        if not findings["owner"]:
            for owner_re in OWNER_RES:
                m = owner_re.search(html)
                if m:
                    findings["owner"] = m.group(1).strip()
                    findings["owner_confidence"] = "low"
                    break

        # Stop early if we got everything
        if all(findings[k] for k in ("email", "year_established", "owner")):
            break

    return findings


def run(top_n: int = 200) -> dict:
    with get_session() as session:
        scrape_run = ScrapeRun(pipeline="db1", source="website_regex")
        session.add(scrape_run)
        session.flush()

        # Order by capacity_high desc, with NULLS LAST
        from sqlalchemy import nulls_last
        q = (session.query(Venue)
             .filter(Venue.website.isnot(None))
             .filter(Venue.website != "")
             .filter(Venue.website != "N/A")
             .order_by(nulls_last(desc(Venue.capacity_high)))
             .limit(top_n))
        venues = q.all()
        log.info("Scraping websites for top %d venues...", len(venues))

        emails = years = owners = 0
        for v in tqdm(venues, desc="Website regex"):
            findings = _scrape_one(v.website)
            if findings["email"] and not v.email:
                v.email = findings["email"]
                emails += 1
            if findings["year_established"] and not v.year_established:
                v.year_established = findings["year_established"]
                years += 1
            if findings["owner"] and not v.owner:
                v.owner = findings["owner"]
                v.owner_confidence = findings["owner_confidence"]
                owners += 1

        scrape_run.rows_in = len(venues)
        scrape_run.rows_written = emails + years + owners
        scrape_run.finished_at = datetime.now(timezone.utc)
        scrape_run.status = "ok"
        scrape_run.notes = f"emails={emails} years={years} owners={owners}"

    log.info("  Found: emails=%d years=%d owners=%d", emails, years, owners)
    return {"venues_scanned": len(venues), "emails_found": emails,
            "years_found": years, "owners_found": owners}
