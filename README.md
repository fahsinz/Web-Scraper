# Awksion Data Pipeline

Two databases for the Awksion project:
- **DB1** — restaurant/venue data across Canada and 5 US cities, with ML-estimated capacity, ticketing-affiliation flagging, and best-effort owner/year/email enrichment.
- **DB2** — emerging musicians bucketed into follower tiers (10k-25k, 25k-50k, 50k-75k, 75k-100k) with geographic segmentation.

The output is a single SQLite database at `data/awksion.db` and an XLSX hand-off at `data/awksion_handoff.xlsx`.

## Setup

```bash
# Install dependencies
uv sync                        # or: pip install -r requirements.txt

# Configure API keys (only needed for DB2 enrichment)
cp .env.example .env
# Edit .env to add LASTFM_API_KEY, SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET
```

## CLI

The pipeline is fully driven by a single CLI (`python -m awksion <cmd>`).

```bash
python -m awksion init           # create DB tables
python -m awksion migrate        # load existing CSVs into the DB
python -m awksion run db1        # build venue database (DB1)
python -m awksion run db2        # build artist database (DB2)
python -m awksion export xlsx    # write data/awksion_handoff.xlsx
python -m awksion stats          # row counts per table
```

The `run db1` and `run db2` commands are idempotent — re-running won't double-insert.

## Pipeline steps

### DB1 — Venues
1. **CSV migration** — load existing `data/indieonthemove_with_capacity.csv` (351 Canadian venues with ML capacity).
2. **Ticketing filter** — flag venues affiliated with major ticketing agencies (Ticketmaster, Live Nation, AXS, etc.) via domain blocklist + homepage HTML regex + capacity heuristic.
3. **Website regex enrichment** — best-effort scrape of top-200 highest-capacity venue homepages for owner / year-established / contact email.
4. (Optional) **OSM Overpass** — `python -c "from awksion.scrapers import osm_venues; osm_venues.run()"` populates US venues from OpenStreetMap (free, unattended).
5. (Optional) **IndieOnTheMove US** — `cd scrapers && python indieonthemove_scraper.py` runs the Selenium scraper for Canadian provinces + 5 US states. **Requires manual Cloudflare login.** Then re-run `python -m awksion migrate`.

### DB2 — Artists
1. **Last.fm geo.getTopArtists** — fetch top artists for Canada + 5 US cities. (Free; needs `LASTFM_API_KEY`.)
2. **Spotify follower enrichment** — search + batch follower lookup for every artist. (Needs Spotify creds.)
3. **Tier bucketing** — assign each artist to a tier based on follower count.

## Project layout

```
src/awksion/
  config.py                # paths, target geographies, blocklists
  cli.py                   # `python -m awksion ...`
  db/models.py             # SQLAlchemy: Venue, Artist, ScrapeRun, SourceRecord
  db/session.py            # WAL-mode SQLite engine
  pipelines/
    migrate_csvs.py        # CSV → DB
    build_db1.py           # DB1 orchestrator
    build_db2.py           # DB2 orchestrator
  scrapers/
    osm_venues.py          # OpenStreetMap Overpass venue fetcher
    lastfm_geo.py          # Last.fm geo.getTopArtists
  enrichers/
    ticketing_filter.py    # major-ticketing affiliation flag
    website_regex.py       # best-effort owner/year/email scrape
    spotify_followers.py   # Spotify follower count enrichment
    artist_tiers.py        # bucket artists into 10-100k tiers
  exporters/
    to_xlsx.py             # 3-sheet hand-off file

scrapers/                  # legacy scripts kept working
  indieonthemove_scraper.py  # Canadian + US states (Selenium, manual Cloudflare)
  estimate_capacity.py       # ML capacity estimator (HistGradientBoosting + OSM)
  ...

data/
  awksion.db                          # SQLite output
  awksion_handoff.xlsx                # final deliverable
  indieonthemove_venues.csv           # raw IOTM scrape (input)
  indieonthemove_with_capacity.csv    # IOTM + ML capacity (input)
  artist_enriched.csv                 # raw artist enrichment (input)

docs/
  data_dictionary.md       # column-by-column documentation
```

## Scheduled refresh (future)

The CLI design makes scheduling trivial. Examples:

**Windows Task Scheduler** (weekly):
```
Action: python -m awksion run db1
Trigger: weekly
```

**GitHub Actions** (`.github/workflows/refresh.yml`):
```yaml
on:
  schedule:
    - cron: '0 6 * * 1'   # every Monday 6am UTC
jobs:
  refresh:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install -r requirements.txt
      - run: python -m awksion run db1 && python -m awksion run db2
      - run: python -m awksion export xlsx
      - uses: actions/upload-artifact@v4
        with:
          path: data/awksion_handoff.xlsx
```

## Known limitations

- IOTM US scrape requires manual Cloudflare login. OSM Overpass is the unattended alternative.
- Spotify follower counts: still works via the Web API artists endpoint; the Nov 2024 restrictions affected related-artists / popularity, not followers.
- Instagram / TikTok follower columns exist in the schema but are populated separately (deferred to a paid Apify/RapidAPI snapshot enrichment step in v2).
- Owner / year_established / performer_payment are populated best-effort only and need manual verification before use.
- Capacity model has cross-validation R²=0.080 / MAPE=78.6% — treat as order-of-magnitude estimates with the prediction intervals as sanity bounds.

See `docs/data_dictionary.md` for full column descriptions.
