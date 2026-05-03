"""Export the awksion DB to a single XLSX hand-off file.

Produces `data/awksion_handoff.xlsx` with three sheets:
  - "Venues (DB1)"         — every Venue row with all enrichment columns
  - "Artists (DB2)"        — every Artist row with tier + geography
  - "Run Log"              — every ScrapeRun for provenance/debugging
"""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import pandas as pd

from awksion.config import DATA_DIR
from awksion.db import Artist, ScrapeRun, Venue, get_session

log = logging.getLogger(__name__)

OUTPUT_PATH = DATA_DIR / "awksion_handoff.xlsx"


def _venues_df(session) -> pd.DataFrame:
    cols = [
        "id", "name", "country", "region", "city", "address", "zip_code",
        "lat", "lon",
        "phone", "email", "website", "facebook", "instagram",
        "venue_type", "categories", "genres", "cuisine",
        "hours", "age_restriction", "rating",
        "capacity_known", "capacity_estimated", "capacity_low", "capacity_high",
        "estimation_method", "confidence", "building_area_sqft", "multi_tenant",
        "owner", "owner_confidence", "year_established",
        "number_of_locations", "performer_payment_info", "past_performers",
        "ticketing_affiliated", "ticketing_evidence",
        "description", "booking_info", "upcoming_events",
        "source", "source_query", "scraped_at", "updated_at",
    ]
    rows = session.query(Venue).all()
    return pd.DataFrame([{c: getattr(r, c, None) for c in cols} for r in rows])


def _artists_df(session) -> pd.DataFrame:
    cols = [
        "id", "name", "tier", "tier_source",
        "country", "city", "geo_confidence",
        "spotify_followers", "instagram_followers", "tiktok_followers",
        "lastfm_listeners", "lastfm_playcount",
        "spotify_id", "spotify_url", "spotify_name",
        "lastfm_url", "instagram_handle", "tiktok_handle",
        "genres", "lastfm_tags",
        "source_venue_name", "source", "updated_at",
    ]
    rows = session.query(Artist).all()
    return pd.DataFrame([{c: getattr(r, c, None) for c in cols} for r in rows])


def _runs_df(session) -> pd.DataFrame:
    cols = ["id", "pipeline", "source", "started_at", "finished_at",
            "rows_in", "rows_written", "status", "error", "notes"]
    rows = session.query(ScrapeRun).order_by(ScrapeRun.id.desc()).all()
    return pd.DataFrame([{c: getattr(r, c, None) for c in cols} for r in rows])


def export(output_path: Path | None = None) -> Path:
    output_path = output_path or OUTPUT_PATH
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with get_session() as session:
        venues_df = _venues_df(session)
        artists_df = _artists_df(session)
        runs_df = _runs_df(session)

    # Strip timezone so openpyxl is happy
    for df in (venues_df, artists_df, runs_df):
        for c in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[c]):
                try:
                    df[c] = df[c].dt.tz_localize(None)
                except (TypeError, AttributeError):
                    pass

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        venues_df.to_excel(writer, sheet_name="Venues (DB1)", index=False)
        artists_df.to_excel(writer, sheet_name="Artists (DB2)", index=False)
        runs_df.to_excel(writer, sheet_name="Run Log", index=False)

        # Add filters to data sheets
        for sheet_name in ("Venues (DB1)", "Artists (DB2)"):
            ws = writer.sheets[sheet_name]
            ws.auto_filter.ref = ws.dimensions
            ws.freeze_panes = "A2"

    log.info("Wrote %s — venues=%d artists=%d runs=%d",
             output_path, len(venues_df), len(artists_df), len(runs_df))
    return output_path
