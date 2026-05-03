"""Central configuration: paths, target geographies, blocklists."""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data"
DB_PATH = DATA_DIR / "awksion.db"
DB_URL = f"sqlite:///{DB_PATH}"

load_dotenv(REPO_ROOT / ".env")

LASTFM_API_KEY = os.getenv("LASTFM_API_KEY")
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")

# Geographic scope (decided in plan)
CANADIAN_PROVINCES = [
    "Ontario", "Quebec", "British Columbia", "Alberta",
    "Manitoba", "Nova Scotia", "New Brunswick",
]

US_STATES = ["New York", "California", "Illinois", "Tennessee", "Texas"]

# 5 target US cities to post-filter to
TARGET_US_CITIES = {
    "New York": ["New York", "Brooklyn", "Manhattan", "Queens", "Bronx"],
    "California": ["Los Angeles", "Hollywood", "West Hollywood", "Santa Monica"],
    "Illinois": ["Chicago"],
    "Tennessee": ["Nashville"],
    "Texas": ["Austin"],
}

# Major ticketing-agency domains (for DB1 filter)
TICKETING_DOMAIN_BLOCKLIST = [
    "ticketmaster.com", "ticketmaster.ca",
    "livenation.com", "livenation.ca",
    "axs.com",
    "seetickets.us", "seetickets.com",
    "etix.com",
    "ticketweb.com",
    "stubhub.com",
]

TICKETING_BRAND_PATTERNS = [
    r"\bticketmaster\b", r"\blive\s*nation\b", r"\baxs\b",
    r"\bsee\s*tickets\b", r"\betix\b", r"\bticketweb\b",
]

# DB2 follower tier buckets (lower bound, upper bound)
ARTIST_TIERS = [
    ("10k-25k", 10_000, 25_000),
    ("25k-50k", 25_000, 50_000),
    ("50k-75k", 50_000, 75_000),
    ("75k-100k", 75_000, 100_000),
]
