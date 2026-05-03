"""One-time migration: load existing CSVs into the SQLite database.

Sources:
- data/indieonthemove_with_capacity.csv  → venues table
- data/artist_enriched.csv               → artists table
"""
from __future__ import annotations

import logging
import math
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from awksion.config import DATA_DIR
from awksion.db import Artist, ScrapeRun, Venue, get_session, init_db

log = logging.getLogger(__name__)


def _coerce_int(val) -> int | None:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    try:
        return int(float(val))
    except (TypeError, ValueError):
        return None


def _coerce_float(val) -> float | None:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _str_or_none(val) -> str | None:
    if val is None:
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    s = str(val).strip()
    return s or None


_CA_CODES = {"AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT"}
_CA_NAMES = {
    "ontario", "quebec", "british columbia", "alberta", "manitoba",
    "nova scotia", "new brunswick", "saskatchewan", "newfoundland and labrador",
    "prince edward island", "yukon", "northwest territories", "nunavut",
}


def _country_from_region(region: str | None) -> str | None:
    if not region:
        return None
    r = region.strip()
    if r.upper() in _CA_CODES:
        return "CA"
    if r.lower() in _CA_NAMES:
        return "CA"
    return "US"


def migrate_venues(csv_path: Path | None = None) -> int:
    csv_path = csv_path or (DATA_DIR / "indieonthemove_with_capacity.csv")
    if not csv_path.exists():
        log.warning("Venue CSV not found at %s — skipping", csv_path)
        return 0

    df = pd.read_csv(csv_path)
    log.info("Loaded %d venue rows from %s", len(df), csv_path.name)

    with get_session() as session:
        run = ScrapeRun(pipeline="db1", source="csv_migration", rows_in=len(df))
        session.add(run)
        session.flush()

        existing_urls = {
            url for (url,) in session.query(Venue.profile_url)
            .filter(Venue.profile_url.isnot(None)).all()
        }

        written = 0
        for _, row in df.iterrows():
            profile_url = _str_or_none(row.get("Normalized_Profile_URL")) \
                or _str_or_none(row.get("Profile_URL"))
            if profile_url and profile_url in existing_urls:
                continue

            region = _str_or_none(row.get("State/Province"))
            country = _country_from_region(region)

            scraped_at = None
            sa_raw = _str_or_none(row.get("Scraped_At"))
            if sa_raw:
                try:
                    scraped_at = datetime.fromisoformat(sa_raw.replace("Z", "+00:00"))
                except ValueError:
                    scraped_at = None

            venue = Venue(
                name=_str_or_none(row.get("Name")) or "Unknown",
                profile_url=profile_url,
                address=_str_or_none(row.get("Address")),
                city=_str_or_none(row.get("City")),
                region=region,
                country=country,
                zip_code=_str_or_none(row.get("Zip_Code")),
                lat=_coerce_float(row.get("lat")),
                lon=_coerce_float(row.get("lon")),
                phone=_str_or_none(row.get("Phone")),
                website=_str_or_none(row.get("Website")),
                facebook=_str_or_none(row.get("Facebook")),
                instagram=_str_or_none(row.get("Instagram")),
                venue_type=_str_or_none(row.get("venue_type")),
                categories=_str_or_none(row.get("Categories")),
                genres=_str_or_none(row.get("Genres")),
                age_restriction=_str_or_none(row.get("Age_Restriction")),
                rating=_coerce_float(row.get("Rating")),
                description=_str_or_none(row.get("Description")),
                booking_info=_str_or_none(row.get("Booking_Info")),
                upcoming_events=_str_or_none(row.get("Upcoming_Events")),
                capacity_known=_coerce_int(row.get("Capacity")),
                capacity_estimated=_coerce_int(row.get("Estimated_Capacity")),
                capacity_low=_coerce_int(row.get("Estimated_Capacity_Low")),
                capacity_high=_coerce_int(row.get("Estimated_Capacity_High")),
                estimation_method=_str_or_none(row.get("Estimation_Method")),
                confidence=_str_or_none(row.get("Confidence")),
                building_area_sqft=_coerce_float(row.get("Building_Area_SqFt")),
                multi_tenant=bool(row.get("Multi_Tenant_Flag")) if pd.notna(row.get("Multi_Tenant_Flag")) else None,
                source="indieonthemove",
                source_query=_str_or_none(row.get("Source_Province_Search")),
                scraped_at=scraped_at,
            )
            session.add(venue)
            written += 1

        run.rows_written = written
        run.finished_at = datetime.now(timezone.utc)
        run.status = "ok"

    log.info("  Wrote %d new venues", written)
    return written


def migrate_artists(csv_path: Path | None = None) -> int:
    csv_path = csv_path or (DATA_DIR / "artist_enriched.csv")
    if not csv_path.exists():
        log.warning("Artist CSV not found at %s — skipping", csv_path)
        return 0

    df = pd.read_csv(csv_path)
    log.info("Loaded %d artist rows from %s", len(df), csv_path.name)

    with get_session() as session:
        run = ScrapeRun(pipeline="db2", source="csv_migration", rows_in=len(df))
        session.add(run)
        session.flush()

        existing = {
            (name, sid) for (name, sid) in session.query(Artist.name, Artist.spotify_id).all()
        }

        seen_in_batch: set[tuple[str, str | None]] = set()
        written = 0
        for _, row in df.iterrows():
            name = _str_or_none(row.get("Scraped_Artist")) or _str_or_none(row.get("Spotify_Name"))
            if not name:
                continue
            spotify_url = _str_or_none(row.get("Spotify_URL"))
            spotify_id = None
            if spotify_url and "/artist/" in spotify_url:
                spotify_id = spotify_url.rsplit("/", 1)[-1].split("?")[0]

            key = (name, spotify_id)
            if key in existing or key in seen_in_batch:
                continue
            seen_in_batch.add(key)

            venue_city = _str_or_none(row.get("Venue_City"))

            artist = Artist(
                name=name,
                spotify_id=spotify_id,
                spotify_url=spotify_url,
                spotify_name=_str_or_none(row.get("Spotify_Name")),
                lastfm_listeners=_coerce_int(row.get("Lastfm_Listeners")),
                lastfm_playcount=_coerce_int(row.get("Lastfm_Playcount")),
                lastfm_tags=_str_or_none(row.get("Lastfm_Tags")),
                source_venue_name=_str_or_none(row.get("Venue_Name")),
                city=venue_city,
                country="CA",  # existing data was Canadian venues
                geo_confidence="low",
                source="csv_migration",
            )
            session.add(artist)
            written += 1

        run.rows_written = written
        run.finished_at = datetime.now(timezone.utc)
        run.status = "ok"

    log.info("  Wrote %d new artists", written)
    return written


def run() -> dict:
    init_db()
    venues = migrate_venues()
    artists = migrate_artists()
    return {"venues_added": venues, "artists_added": artists}
