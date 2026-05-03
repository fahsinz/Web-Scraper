"""OSM Overpass venue scraper — free, no API key, fully unattended.

Queries OpenStreetMap for music/entertainment venues in our 5 target US cities,
using the Overpass interpreter. Used as the unattended path for DB1 since the
IndieOnTheMove flow requires manual Cloudflare auth.

Tag filter:
  amenity ∈ {bar, pub, nightclub, theatre, music_venue}
  leisure ∈ {concert_hall}

Output rows go into the `venues` table with source='osm'.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests
from tqdm import tqdm

from awksion.config import DATA_DIR
from awksion.db import ScrapeRun, SourceRecord, Venue, get_session

log = logging.getLogger(__name__)

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OVERPASS_BACKUP = "https://overpass.kumi.systems/api/interpreter"
TIMEOUT = 60

CACHE_DIR = DATA_DIR / "osm_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

USER_AGENT = "AwksionDataPipeline/0.1 (student-project)"

# (city_label, region, country, [bbox: south,west,north,east])
# Bounding boxes are loose; Overpass area names work better when available.
TARGET_AREAS = [
    ("New York",   "NY", "US", "New York City, New York, USA"),
    ("Los Angeles","CA", "US", "Los Angeles, California, USA"),
    ("Chicago",    "IL", "US", "Chicago, Illinois, USA"),
    ("Nashville",  "TN", "US", "Nashville, Tennessee, USA"),
    ("Austin",     "TX", "US", "Austin, Texas, USA"),
]

VENUE_TAG_FILTER = """
  node["amenity"~"^(bar|pub|nightclub|theatre|music_venue)$"](area.searchArea);
  way["amenity"~"^(bar|pub|nightclub|theatre|music_venue)$"](area.searchArea);
  node["leisure"="concert_hall"](area.searchArea);
  way["leisure"="concert_hall"](area.searchArea);
"""


def _build_query(area_name: str) -> str:
    return f"""
    [out:json][timeout:{TIMEOUT}];
    area[name="{area_name.split(',')[0]}"]->.searchArea;
    (
      {VENUE_TAG_FILTER}
    );
    out center tags;
    """


def _fetch_with_retry(query: str, retries: int = 2) -> dict | None:
    last_err = None
    for attempt, url in enumerate([OVERPASS_URL, OVERPASS_BACKUP][: retries + 1]):
        try:
            resp = requests.post(
                url,
                data={"data": query},
                headers={"User-Agent": USER_AGENT},
                timeout=TIMEOUT + 30,
            )
            if resp.status_code == 200:
                return resp.json()
            last_err = f"HTTP {resp.status_code}"
        except (requests.RequestException, json.JSONDecodeError) as e:
            last_err = str(e)
        time.sleep(3)
    log.warning("Overpass fetch failed after retries: %s", last_err)
    return None


def _parse_element(el: dict, default_region: str, default_country: str) -> dict | None:
    tags = el.get("tags", {})
    name = (tags.get("name") or "").strip()
    if not name:
        return None

    if el.get("type") == "node":
        lat, lon = el.get("lat"), el.get("lon")
    else:
        c = el.get("center") or {}
        lat, lon = c.get("lat"), c.get("lon")

    addr_parts = []
    if tags.get("addr:housenumber"):
        addr_parts.append(tags["addr:housenumber"])
    if tags.get("addr:street"):
        addr_parts.append(tags["addr:street"])
    address = " ".join(addr_parts) or None

    venue_type_map = {
        "bar": "bar", "pub": "bar", "nightclub": "club",
        "theatre": "theatre", "music_venue": "venue",
    }
    venue_type = venue_type_map.get(tags.get("amenity") or "")
    if tags.get("leisure") == "concert_hall":
        venue_type = "theatre"

    natural_key = f"osm:{el.get('type')}/{el.get('id')}"

    return {
        "name": name,
        "address": address,
        "city": tags.get("addr:city") or tags.get("is_in:city") or None,
        "region": tags.get("addr:state") or default_region,
        "country": default_country,
        "zip_code": tags.get("addr:postcode"),
        "lat": lat,
        "lon": lon,
        "phone": tags.get("phone") or tags.get("contact:phone"),
        "email": tags.get("email") or tags.get("contact:email"),
        "website": tags.get("website") or tags.get("contact:website"),
        "facebook": tags.get("contact:facebook"),
        "instagram": tags.get("contact:instagram"),
        "venue_type": venue_type,
        "categories": tags.get("amenity") or tags.get("leisure"),
        "cuisine": tags.get("cuisine"),
        "hours": tags.get("opening_hours"),
        "natural_key": natural_key,
        "raw_tags": tags,
    }


def fetch_city(area_name: str, region: str, country: str) -> list[dict]:
    cache_path = CACHE_DIR / f"osm_{area_name.split(',')[0].replace(' ', '_')}.json"
    if cache_path.exists():
        log.info("  Using cached %s", cache_path.name)
        data = json.loads(cache_path.read_text(encoding="utf-8"))
    else:
        query = _build_query(area_name)
        log.info("  Querying Overpass for %s...", area_name)
        data = _fetch_with_retry(query)
        if not data:
            return []
        cache_path.write_text(json.dumps(data), encoding="utf-8")

    elements = data.get("elements", [])
    parsed = []
    for el in elements:
        row = _parse_element(el, region, country)
        if row:
            parsed.append(row)
    log.info("  Parsed %d venues from %s", len(parsed), area_name)
    return parsed


def _normalize_website(w: str | None) -> str | None:
    if not w:
        return None
    w = w.strip()
    if not w.startswith(("http://", "https://")):
        w = "http://" + w
    try:
        urlparse(w)
        return w
    except Exception:
        return None


def run(dry_run: bool = False) -> dict:
    """Fetch & insert OSM venues for all target US cities. Idempotent on natural_key."""
    total_fetched = 0
    total_added = 0
    per_city: dict[str, int] = {}

    with get_session() as session:
        scrape_run = ScrapeRun(pipeline="db1", source="osm_overpass")
        session.add(scrape_run)
        session.flush()

        # Pre-load existing OSM keys to dedup
        existing_keys = {
            k for (k,) in session.query(SourceRecord.natural_key)
            .filter(SourceRecord.source == "osm").all()
        }

        for city, region, country, area_name in TARGET_AREAS:
            try:
                rows = fetch_city(area_name, region, country)
            except Exception as e:
                log.warning("  Skipping %s — %s", city, e)
                continue
            total_fetched += len(rows)
            added_here = 0

            for row in rows:
                key = row["natural_key"]
                if key in existing_keys:
                    continue

                if dry_run:
                    added_here += 1
                    continue

                provenance = SourceRecord(
                    run_id=scrape_run.id,
                    source="osm",
                    entity_type="venue",
                    natural_key=key,
                    payload=row.get("raw_tags"),
                )
                session.add(provenance)

                website = _normalize_website(row.get("website"))
                venue = Venue(
                    name=row["name"],
                    address=row.get("address"),
                    city=row.get("city") or city,
                    region=region,
                    country=country,
                    zip_code=row.get("zip_code"),
                    lat=row.get("lat"),
                    lon=row.get("lon"),
                    phone=row.get("phone"),
                    email=row.get("email"),
                    website=website,
                    facebook=row.get("facebook"),
                    instagram=row.get("instagram"),
                    venue_type=row.get("venue_type"),
                    categories=row.get("categories"),
                    cuisine=row.get("cuisine"),
                    hours=row.get("hours"),
                    source="osm",
                    source_query=f"overpass:{area_name}",
                    scraped_at=datetime.now(timezone.utc),
                )
                session.add(venue)
                existing_keys.add(key)
                added_here += 1

            per_city[city] = added_here
            total_added += added_here

        scrape_run.rows_in = total_fetched
        scrape_run.rows_written = total_added
        scrape_run.finished_at = datetime.now(timezone.utc)
        scrape_run.status = "ok"
        scrape_run.notes = f"per_city={per_city}"

    log.info("OSM run complete — fetched=%d added=%d per_city=%s",
             total_fetched, total_added, per_city)
    return {"fetched": total_fetched, "added": total_added, "per_city": per_city}
