"""
spotify_enricher.py

Reads indieonthemove_venues.csv, looks up each upcoming artist on Spotify
(for profile URL verification) and Last.fm (for listeners, playcount, genre tags),
then validates whether the artist's tags match the venue's listed genres.

Setup:
    1. Create a .env file with:
           SPOTIFY_CLIENT_ID=your_id_here
           SPOTIFY_CLIENT_SECRET=your_secret_here
           LASTFM_API_KEY=your_key_here
    2. pip install -r requirements.txt
    3. python enrichers/spotify_enricher.py
"""

import csv
import os
import time
import requests
from pathlib import Path
from difflib import SequenceMatcher
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

# ─── PATHS ───────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
load_dotenv(ROOT / ".env")

# ─── CONFIG ──────────────────────────────────────────────────
INPUT_FILE  = ROOT / "data" / "indieonthemove_venues.csv"
OUTPUT_FILE = ROOT / "data" / "artist_enriched.csv"

NAME_MATCH_THRESHOLD = 0.6  # 0.0–1.0; lower = more lenient name matching
SPOTIFY_SLEEP = 0.2         # seconds between Spotify calls
LASTFM_SLEEP  = 0.25        # seconds between Last.fm calls

LASTFM_API_KEY = os.getenv("LASTFM_API_KEY")
LASTFM_BASE    = "https://ws.audioscrobbler.com/2.0/"

# ─── SETUP ───────────────────────────────────────────────────
sp = spotipy.Spotify(
    auth_manager=SpotifyClientCredentials(
        client_id=os.getenv("SPOTIFY_CLIENT_ID"),
        client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
    )
)


# ─── HELPERS ─────────────────────────────────────────────────
def name_similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()


def search_spotify(name: str) -> dict | None:
    """
    Confirms an artist exists on Spotify and returns their profile URL.
    Only returns a result if the name similarity passes the threshold.
    """
    try:
        results = sp.search(q=f"artist:{name}", type="artist", limit=1)
        items = results["artists"]["items"]
        if not items:
            return None

        top = items[0]
        similarity = name_similarity(name, top["name"])
        if similarity < NAME_MATCH_THRESHOLD:
            return None

        return {
            "spotify_name": top["name"],
            "spotify_url":  top["external_urls"]["spotify"],
            "name_similarity": round(similarity, 2),
        }
    except Exception as e:
        print(f"      Spotify error for '{name}': {e}")
        return None
    finally:
        time.sleep(SPOTIFY_SLEEP)


def search_lastfm(name: str) -> dict | None:
    """
    Fetches artist info from Last.fm: listeners, playcount, and genre tags.
    Returns None if the artist is not found.
    """
    try:
        resp = requests.get(LASTFM_BASE, params={
            "method":  "artist.getinfo",
            "artist":  name,
            "api_key": LASTFM_API_KEY,
            "format":  "json",
        }, timeout=10)
        data = resp.json()

        if "error" in data or "artist" not in data:
            return None

        artist = data["artist"]
        stats  = artist.get("stats", {})
        tags   = [t["name"] for t in artist.get("tags", {}).get("tag", [])]

        return {
            "lastfm_listeners": int(stats.get("listeners", 0)),
            "lastfm_playcount": int(stats.get("playcount", 0)),
            "lastfm_tags":      ", ".join(tags) if tags else "N/A",
        }
    except Exception as e:
        print(f"      Last.fm error for '{name}': {e}")
        return None
    finally:
        time.sleep(LASTFM_SLEEP)


def normalize_genres(genre_str: str) -> set[str]:
    if not genre_str or genre_str.strip().lower() in ("n/a", "all genres", ""):
        return set()
    return {g.strip().lower() for g in genre_str.split(",")}


def genre_overlap(venue_genres: set, artist_tags: str) -> tuple[int, list[str]]:
    if not venue_genres or artist_tags == "N/A":
        return 0, []
    tag_blob = artist_tags.lower()
    matches = [vg for vg in venue_genres if vg in tag_blob]
    return len(matches), matches


# ─── MAIN ────────────────────────────────────────────────────
def main():
    with open(INPUT_FILE, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    print(f"Loaded {len(rows)} venues from {INPUT_FILE}\n")

    output_rows = []
    artist_cache: dict[str, dict] = {}  # avoid duplicate API calls

    for venue_idx, venue in enumerate(rows, 1):
        venue_name   = venue["Name"]
        venue_genres = normalize_genres(venue["Genres"])
        events_raw   = venue.get("Upcoming_Events", "").strip()

        if not events_raw or events_raw == "N/A":
            print(f"[{venue_idx}/{len(rows)}] {venue_name} — no upcoming events, skipping")
            continue

        artists = [a.strip() for a in events_raw.split(";") if a.strip()]
        print(f"[{venue_idx}/{len(rows)}] {venue_name} ({len(artists)} artists)")

        for artist_name in artists:
            if artist_name not in artist_cache:
                print(f"      Searching: '{artist_name}'")
                spotify = search_spotify(artist_name)
                lastfm  = search_lastfm(artist_name)
                artist_cache[artist_name] = {"spotify": spotify, "lastfm": lastfm}

            cached  = artist_cache[artist_name]
            spotify = cached["spotify"]
            lastfm  = cached["lastfm"]

            spotify_match = "Found" if spotify else "No match"
            lastfm_match  = "Found" if lastfm  else "No match"

            tags = lastfm["lastfm_tags"] if lastfm else "N/A"
            overlap_count, matched = genre_overlap(venue_genres, tags)

            if not venue_genres:
                validation = "venue genres unspecified"
            elif overlap_count > 0:
                validation = "confirmed"
            elif lastfm:
                validation = "genre mismatch"
            else:
                validation = "unverified"

            output_rows.append({
                "Venue_Name":          venue_name,
                "Venue_City":          venue["City"],
                "Venue_Genres":        venue["Genres"],
                "Scraped_Artist":      artist_name,
                "Spotify_Match":       spotify_match,
                "Spotify_Name":        spotify["spotify_name"] if spotify else "N/A",
                "Spotify_URL":         spotify["spotify_url"]  if spotify else "N/A",
                "Name_Similarity":     spotify["name_similarity"] if spotify else "N/A",
                "Lastfm_Match":        lastfm_match,
                "Lastfm_Listeners":    lastfm["lastfm_listeners"] if lastfm else "N/A",
                "Lastfm_Playcount":    lastfm["lastfm_playcount"] if lastfm else "N/A",
                "Lastfm_Tags":         tags,
                "Genre_Overlap_Count": overlap_count if lastfm else "N/A",
                "Matched_Genres":      ", ".join(matched) if matched else "none",
                "Validation":          validation,
            })

            listeners = f"{lastfm['lastfm_listeners']:,}" if lastfm else "N/A"
            icon = "✅" if validation == "confirmed" else ("⚠️ " if validation == "genre mismatch" else "➖")
            print(f"         {icon} {artist_name} | listeners={listeners} | tags={tags[:50]} | {validation}")

    fieldnames = [
        "Venue_Name", "Venue_City", "Venue_Genres",
        "Scraped_Artist",
        "Spotify_Match", "Spotify_Name", "Spotify_URL", "Name_Similarity",
        "Lastfm_Match", "Lastfm_Listeners", "Lastfm_Playcount", "Lastfm_Tags",
        "Genre_Overlap_Count", "Matched_Genres", "Validation",
    ]
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)

    total     = len(output_rows)
    confirmed = sum(1 for r in output_rows if r["Validation"] == "confirmed")
    mismatch  = sum(1 for r in output_rows if r["Validation"] == "genre mismatch")
    no_match  = sum(1 for r in output_rows if r["Spotify_Match"] == "No match")

    print(f"\n{'='*55}")
    print(f"Results saved to {OUTPUT_FILE}")
    print(f"  Total artist/venue pairs : {total}")
    print(f"  Confirmed (genre match)  : {confirmed}")
    print(f"  Genre mismatch           : {mismatch}")
    print(f"  No Spotify match found   : {no_match}")
    print(f"{'='*55}")


if __name__ == "__main__":
    main()
