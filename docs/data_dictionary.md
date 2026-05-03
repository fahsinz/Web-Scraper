# Awksion Data Dictionary

This document describes every column in the `data/awksion_handoff.xlsx` deliverable.
Sources are tagged where the data ultimately came from.

---

## Sheet 1: Venues (DB1)

| Column | Type | Description | Source |
|---|---|---|---|
| `id` | int | Internal database row ID. | DB |
| `name` | string | Venue name. | IOTM, OSM |
| `country` | string | `'CA'` or `'US'`. Derived from region. | derived |
| `region` | string | Province (CA: ON/QC/BC/AB/etc.) or state (US: NY/CA/IL/TN/TX). | IOTM, OSM |
| `city` | string | City name. | IOTM, OSM |
| `address` | string | Street address. | IOTM, OSM |
| `zip_code` | string | Postal/ZIP code. | IOTM, OSM |
| `lat`, `lon` | float | Latitude/longitude (WGS84). | IOTM (Nominatim cache), OSM |
| `phone`, `email` | string | Contact info. | IOTM, OSM, website regex |
| `website` | string | Primary venue website. | IOTM, OSM |
| `facebook`, `instagram` | string | Social links. | IOTM, OSM |
| `venue_type` | string | One of: `bar`, `club`, `restaurant`, `theatre`, `gallery`, `festival`, `venue`. | classifier |
| `categories` | string | Raw category tags from source. | IOTM, OSM |
| `genres` | string | Music genres venue books. | IOTM |
| `cuisine` | string | Cuisine type (restaurants only). | OSM |
| `hours` | string | `opening_hours` in OSM format or free-text. | OSM, IOTM |
| `age_restriction` | string | e.g. `'18+'`, `'21+'`. | IOTM |
| `rating` | float | Site-rated score (IOTM only). | IOTM |
| `capacity_known` | int | Capacity directly reported by venue. | IOTM |
| `capacity_estimated` | int | ML-predicted capacity. | `estimate_capacity.py` |
| `capacity_low`, `capacity_high` | int | P10/P90 prediction intervals. | `estimate_capacity.py` |
| `estimation_method` | string | `'known'`, `'model'`, `'heuristic_fallback'`, `'province_type_median'`, `'description_hint'`. | `estimate_capacity.py` |
| `confidence` | string | Heuristic confidence label. | `estimate_capacity.py` |
| `building_area_sqft` | float | Building footprint area (sq ft) from OSM building polygon. | `estimate_capacity.py` |
| `multi_tenant` | bool | True if the building footprint suggests multiple tenants share the space. | `estimate_capacity.py` |
| `owner` | string | Best-effort owner name from website regex. | website regex |
| `owner_confidence` | string | `'low'` / `'medium'` (always low-confidence; manual verification needed). | website regex |
| `year_established` | int | Year extracted from "Established YYYY" / "Since YYYY" text on website. | website regex |
| `number_of_locations` | int | Currently always NULL; field reserved for future enrichment. | future |
| `performer_payment_info` | string | Currently always NULL; field reserved. | future |
| `past_performers` | string | Partial; from upcoming-events scrape. | IOTM |
| `ticketing_affiliated` | bool | **Filter flag.** True if venue uses Ticketmaster / Live Nation / AXS / SeeTickets / Etix. | ticketing_filter |
| `ticketing_evidence` | string | Why it was flagged (e.g. `'domain:ticketmaster.com'`, `'iframe:ticketing'`). | ticketing_filter |
| `description` | string | Long venue description text. | IOTM |
| `booking_info` | string | How to book the venue. | IOTM |
| `upcoming_events` | string | Semicolon-separated list of upcoming event titles. | IOTM |
| `source` | string | `'indieonthemove'` or `'osm'` (or `'website_scrape'`). | DB |
| `source_query` | string | Query/region the row was discovered under. | DB |
| `scraped_at` | datetime | When the row was scraped from source. | DB |
| `updated_at` | datetime | Last DB update. | DB |

---

## Sheet 2: Artists (DB2)

| Column | Type | Description | Source |
|---|---|---|---|
| `id` | int | Internal database row ID. | DB |
| `name` | string | Artist name (preferring scraped form). | IOTM events, Last.fm geo |
| `tier` | string | One of `'10k-25k'`, `'25k-50k'`, `'50k-75k'`, `'75k-100k'`, `'out_of_range'`. | tier bucketing |
| `tier_source` | string | Which metric was used (e.g. `'spotify_followers'`, `'lastfm_listeners_proxy'`). | tier bucketing |
| `country`, `city` | string | Geographic tag. `country` is reliable; `city` is best-effort. | Last.fm geo, venue link |
| `geo_confidence` | string | `'low'` / `'medium'`. | derived |
| `spotify_followers` | int | Spotify follower count. | Spotify Web API |
| `instagram_followers` | int | Reserved for future Instagram enrichment (Apify/RapidAPI). | future |
| `tiktok_followers` | int | Reserved for future TikTok enrichment. | future |
| `lastfm_listeners` | int | Last.fm listener count. | Last.fm |
| `lastfm_playcount` | int | Last.fm cumulative playcount. | Last.fm |
| `spotify_id` | string | Spotify artist ID. | Spotify |
| `spotify_url` | string | Spotify artist page URL. | Spotify |
| `spotify_name` | string | Canonical Spotify artist name. | Spotify |
| `lastfm_url` | string | Last.fm artist page URL. | Last.fm |
| `genres` | string | Comma-separated Spotify genres. | Spotify |
| `lastfm_tags` | string | Comma-separated Last.fm tags. | Last.fm |
| `source_venue_name` | string | If artist was discovered via a venue's upcoming events, the venue's name. | IOTM |
| `source` | string | `'csv_migration'`, `'lastfm_geo'`, `'spotify_search'`. | DB |
| `updated_at` | datetime | Last DB update. | DB |

---

## Sheet 3: Run Log

Per-pipeline-run audit trail. Useful for debugging what data was added when.

| Column | Description |
|---|---|
| `id` | Run ID. |
| `pipeline` | `'db1'` or `'db2'`. |
| `source` | Which scraper/enricher (e.g. `'osm_overpass'`, `'spotify_followers'`). |
| `started_at`, `finished_at` | Wall-clock timestamps. |
| `rows_in`, `rows_written` | How many rows were considered vs persisted. |
| `status` | `'running'` / `'ok'` / `'failed'`. |
| `error`, `notes` | Free-text. |

---

## Known limitations / caveats

- **Owner / year_established / performer_payment**: best-effort only. Many venues will have `NULL` here. Where present, manual verification is recommended.
- **Instagram / TikTok followers**: not populated in v1. Columns exist for future enrichment via Apify or RapidAPI.
- **Geographic precision for artists**: Last.fm only exposes country-level top-artists publicly; per-city tags are heuristic.
- **`ticketing_affiliated`**: low false-positive rate (domain blocklist + brand regex). Use as a filter, not a hard exclusion — verify edge cases.
- **`capacity_estimated`**: ML model trained on 91 venues with known capacity. Cross-validation R²=0.080 / MAPE=78.6%. Treat as order-of-magnitude.

---

## Refresh / regeneration

```
python -m awksion run db1     # rebuild venue database (CSV migrate + ticketing + websites)
python -m awksion run db2     # rebuild artist database (Last.fm geo + Spotify + tiers)
python -m awksion export xlsx # regenerate handoff XLSX
python -m awksion stats       # quick row counts
```

To add a new venue scraper, drop it under `src/awksion/scrapers/` and call it
from `src/awksion/pipelines/build_db1.py`. Same pattern for artist sources.
