# Artist Enricher — Overview

## What it does

`enrichers/spotify_enricher.py` reads the scraped venue list (`data/indieonthemove_venues.csv`), looks up each upcoming artist across two APIs, and outputs an enriched CSV (`data/artist_enriched.csv`) with real listener data and genre tags for each artist/venue pair.

---

## Why two APIs?

### Spotify
Spotify's Web API was originally used for everything (genres, popularity, followers, profile URL). However, in **November 2024 Spotify restricted their API** for apps in development mode — the artist endpoint now only returns `name`, `id`, `images`, and `external_urls`. Popularity, followers, and genres are no longer accessible without applying for Extended Quota Mode.

**What Spotify is still used for:**
- Confirming the artist exists on the platform
- Getting their Spotify profile URL (useful for booking outreach)
- Name similarity matching (to avoid wrong artist matches)

### Last.fm
Last.fm is a free music tracking platform with a public API that has no such restrictions. It provides the data Spotify can no longer give us.

**What Last.fm is used for:**
- `Listeners` — unique monthly listeners (good proxy for artist reach)
- `Playcount` — total cumulative plays
- `Tags` — crowd-sourced genre tags (e.g. "indie rock", "pop punk", "toronto")

---

## How the enricher works

1. Loads all venues from `indieonthemove_venues.csv`
2. For each venue with upcoming events, splits the `Upcoming_Events` field (semicolon-separated) into individual artist names
3. For each artist:
   - Searches **Spotify** for a name match (uses `SequenceMatcher` to filter out false positives below a 0.6 similarity threshold)
   - Searches **Last.fm** for listener stats and genre tags
   - Results are cached so the same artist isn't looked up twice
4. Compares Last.fm tags against the venue's listed genres to produce a `Validation` label
5. Writes everything to `data/artist_enriched.csv`

---

## Output columns

| Column | Source | Description |
|---|---|---|
| `Venue_Name` | Scraped | Name of the venue |
| `Venue_City` | Scraped | City the venue is in |
| `Venue_Genres` | Scraped | Genres listed on IndieOnTheMove |
| `Scraped_Artist` | Scraped | Artist name as scraped |
| `Spotify_Match` | Spotify | Found / No match |
| `Spotify_Name` | Spotify | Artist name as it appears on Spotify |
| `Spotify_URL` | Spotify | Direct link to Spotify artist profile |
| `Name_Similarity` | Spotify | 0.0–1.0 score of how close the name matched |
| `Lastfm_Match` | Last.fm | Found / No match |
| `Lastfm_Listeners` | Last.fm | Unique listener count |
| `Lastfm_Playcount` | Last.fm | Total play count |
| `Lastfm_Tags` | Last.fm | Comma-separated genre tags |
| `Genre_Overlap_Count` | Derived | How many venue genres appear in artist tags |
| `Matched_Genres` | Derived | Which specific genres matched |
| `Validation` | Derived | confirmed / genre mismatch / venue genres unspecified / unverified |

---

## Validation labels

| Label | Meaning |
|---|---|
| `confirmed` | Artist tags overlap with the venue's listed genres |
| `genre mismatch` | Artist found on Last.fm but tags don't match venue genres |
| `venue genres unspecified` | Venue lists "All Genres" or N/A — no comparison possible |
| `unverified` | Artist not found on Last.fm at all |

---

## Setup

Add the following to your `.env` file:

```
SPOTIFY_CLIENT_ID=your_id
SPOTIFY_CLIENT_SECRET=your_secret
LASTFM_API_KEY=your_key
LASTFM_SHARED_SECRET=your_secret
```

Run with:

```bash
python enrichers/spotify_enricher.py
```

Last.fm API key: free, instant signup at https://www.last.fm/api/account/create
