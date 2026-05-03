"""SQLAlchemy models for the Awksion data pipeline.

Schema is intentionally flat. CSVs map almost 1:1 to columns; provenance is
tracked via SourceRecord (raw payload per scrape) and ScrapeRun (per invocation).
"""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    Boolean, DateTime, Float, ForeignKey, Integer, String, Text, JSON,
    UniqueConstraint, Index,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class Venue(Base):
    __tablename__ = "venues"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # Identity
    name: Mapped[str] = mapped_column(String(255), index=True)
    profile_url: Mapped[str | None] = mapped_column(String(500), nullable=True, unique=True)

    # Location
    address: Mapped[str | None] = mapped_column(String(500), nullable=True)
    city: Mapped[str | None] = mapped_column(String(120), nullable=True, index=True)
    region: Mapped[str | None] = mapped_column(String(120), nullable=True, index=True)  # province or state
    country: Mapped[str | None] = mapped_column(String(2), nullable=True, index=True)   # 'CA' or 'US'
    zip_code: Mapped[str | None] = mapped_column(String(20), nullable=True)
    lat: Mapped[float | None] = mapped_column(Float, nullable=True)
    lon: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Contact
    phone: Mapped[str | None] = mapped_column(String(50), nullable=True)
    email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    website: Mapped[str | None] = mapped_column(String(500), nullable=True)
    facebook: Mapped[str | None] = mapped_column(String(500), nullable=True)
    instagram: Mapped[str | None] = mapped_column(String(500), nullable=True)

    # Classification
    venue_type: Mapped[str | None] = mapped_column(String(50), nullable=True, index=True)
    categories: Mapped[str | None] = mapped_column(Text, nullable=True)
    genres: Mapped[str | None] = mapped_column(Text, nullable=True)
    cuisine: Mapped[str | None] = mapped_column(String(200), nullable=True)

    # Operations
    hours: Mapped[str | None] = mapped_column(Text, nullable=True)
    age_restriction: Mapped[str | None] = mapped_column(String(50), nullable=True)
    rating: Mapped[float | None] = mapped_column(Float, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    booking_info: Mapped[str | None] = mapped_column(Text, nullable=True)
    upcoming_events: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Capacity (from estimate_capacity.py)
    capacity_known: Mapped[int | None] = mapped_column(Integer, nullable=True)
    capacity_estimated: Mapped[int | None] = mapped_column(Integer, nullable=True)
    capacity_low: Mapped[int | None] = mapped_column(Integer, nullable=True)
    capacity_high: Mapped[int | None] = mapped_column(Integer, nullable=True)
    estimation_method: Mapped[str | None] = mapped_column(String(50), nullable=True)
    confidence: Mapped[str | None] = mapped_column(String(20), nullable=True)
    building_area_sqft: Mapped[float | None] = mapped_column(Float, nullable=True)
    multi_tenant: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    # Best-effort enrichment fields (per plan)
    owner: Mapped[str | None] = mapped_column(String(255), nullable=True)
    owner_confidence: Mapped[str | None] = mapped_column(String(20), nullable=True)
    year_established: Mapped[int | None] = mapped_column(Integer, nullable=True)
    number_of_locations: Mapped[int | None] = mapped_column(Integer, nullable=True)
    performer_payment_info: Mapped[str | None] = mapped_column(Text, nullable=True)
    past_performers: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Ticketing-affiliation filter
    ticketing_affiliated: Mapped[bool] = mapped_column(Boolean, default=False)
    ticketing_evidence: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # Provenance
    source: Mapped[str] = mapped_column(String(50), index=True)  # 'indieonthemove', 'osm', 'website_scrape'
    source_query: Mapped[str | None] = mapped_column(String(255), nullable=True)
    scraped_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, onupdate=_utcnow)

    __table_args__ = (
        Index("ix_venues_country_region", "country", "region"),
        Index("ix_venues_ticketing", "ticketing_affiliated"),
    )


class Artist(Base):
    __tablename__ = "artists"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # Identity
    name: Mapped[str] = mapped_column(String(255), index=True)
    spotify_id: Mapped[str | None] = mapped_column(String(50), nullable=True, index=True)
    spotify_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    spotify_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    lastfm_url: Mapped[str | None] = mapped_column(String(500), nullable=True)
    instagram_handle: Mapped[str | None] = mapped_column(String(120), nullable=True)
    tiktok_handle: Mapped[str | None] = mapped_column(String(120), nullable=True)

    # Tier metrics
    spotify_followers: Mapped[int | None] = mapped_column(Integer, nullable=True)
    lastfm_listeners: Mapped[int | None] = mapped_column(Integer, nullable=True)
    lastfm_playcount: Mapped[int | None] = mapped_column(Integer, nullable=True)
    instagram_followers: Mapped[int | None] = mapped_column(Integer, nullable=True)
    tiktok_followers: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Bucket: '10k-25k', '25k-50k', '50k-75k', '75k-100k', or 'out_of_range'
    tier: Mapped[str | None] = mapped_column(String(20), nullable=True, index=True)
    tier_source: Mapped[str | None] = mapped_column(String(50), nullable=True)  # 'spotify_followers' etc

    # Geography
    country: Mapped[str | None] = mapped_column(String(2), nullable=True, index=True)
    city: Mapped[str | None] = mapped_column(String(120), nullable=True, index=True)
    geo_confidence: Mapped[str | None] = mapped_column(String(20), nullable=True)

    # Classification
    genres: Mapped[str | None] = mapped_column(Text, nullable=True)
    lastfm_tags: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Linkage to a venue (optional — set when sourced from venue events)
    source_venue_id: Mapped[int | None] = mapped_column(ForeignKey("venues.id"), nullable=True)
    source_venue_name: Mapped[str | None] = mapped_column(String(255), nullable=True)

    source: Mapped[str] = mapped_column(String(50), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow, onupdate=_utcnow)

    __table_args__ = (
        UniqueConstraint("name", "spotify_id", name="uq_artist_name_spotify"),
    )


class ScrapeRun(Base):
    __tablename__ = "scrape_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    pipeline: Mapped[str] = mapped_column(String(50), index=True)  # 'db1' or 'db2'
    source: Mapped[str] = mapped_column(String(50))               # 'indieonthemove', 'lastfm_geo', etc
    started_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    rows_in: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rows_written: Mapped[int | None] = mapped_column(Integer, nullable=True)
    status: Mapped[str] = mapped_column(String(20), default="running")  # running/ok/failed
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)


class SourceRecord(Base):
    """Raw scraped payload, one row per source record. Used for provenance / re-derivation."""
    __tablename__ = "source_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int | None] = mapped_column(ForeignKey("scrape_runs.id"), nullable=True)
    source: Mapped[str] = mapped_column(String(50), index=True)
    entity_type: Mapped[str] = mapped_column(String(20), index=True)  # 'venue' or 'artist'
    natural_key: Mapped[str | None] = mapped_column(String(500), nullable=True, index=True)
    payload: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=_utcnow)
