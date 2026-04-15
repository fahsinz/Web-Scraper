# Scrapers — Overview

There are three scrapers, each targeting a different data source. Together they build a picture of Toronto's live music venue landscape.

---

## 1. IndieOnTheMove Scraper
**File:** `scrapers/indieonthemove_scraper.py`
**Output:** `data/indieonthemove_venues.csv`

### What it scrapes
[IndieOnTheMove](https://www.indieonthemove.com) is a dedicated music industry directory. It's the richest data source — venues have detailed profiles including genres, capacity, booking info, age restrictions, and upcoming events.

### How it works
IndieOnTheMove uses a **Vue.js single-page app**, which means standard URL parameters are ignored. The scraper has to physically interact with the page like a real user:

1. Launches a **stealth Chrome browser** (`undetected-chromedriver`) to avoid bot detection
2. Opens the login page and **pauses for you to log in manually** (solves Cloudflare challenge, then you sign in)
3. Navigates to the venue search form, selects **Country → Province/State** from the dropdowns, and clicks Search
4. Paginates through all result pages collecting venue profile URLs
5. Visits each venue profile and extracts data using CSS selectors and regex

### Key fields collected

| Field | Notes |
|---|---|
| Name | Venue name |
| City / State | From Open Graph meta tags |
| Address / Phone | From card body |
| Website | First external link (skips social media) |
| Categories | e.g. Music Venue, Bar, Open Mic |
| Genres | e.g. Indie, Rock, Hip-Hop |
| Capacity | Parsed from "Capacity: 348" text |
| Age Restriction | e.g. 19+ |
| Booking Info | Full text, or "Requires Premium" if paywalled |
| Facebook / Instagram | Social links |
| Upcoming Events | Semicolon-separated list of artist names |
| Profile URL | Link back to IndieOnTheMove |

### Config (top of file)
```python
TARGET_COUNTRY = "Canada"
TARGET_STATE   = "Ontario"
TEST_LIMIT     = 500       # max venues per run
```

### Requirements
- Paid IndieOnTheMove account (for full venue profiles)
- Chrome installed (matched to `version_main=146` in driver setup)

### Run
```bash
python scrapers/indieonthemove_scraper.py
```
The browser will open — log in manually when prompted, then let it run.

---

## 2. ThisWeek.to Scraper
**File:** `scrapers/thisweek_to_scraper.py`
**Output:** `data/thisweekto_indie_venues.csv`

### What it scrapes
[ThisWeek.to](https://thisweek.to) is a Toronto-focused events/directory site. It's a simpler, lighter scrape — good for cross-referencing venue names and getting basic contact info.

### How it works
Simple **requests + BeautifulSoup** scrape (no browser needed):

1. Fetches the live music directory page (`/directory.html`)
2. Finds all "map link" anchor tags — each one marks a venue entry
3. Walks up the DOM to the parent container and extracts text fields
4. Uses regex for phone numbers

### Key fields collected

| Field | Notes |
|---|---|
| Name | Venue name |
| Address | First text node after name |
| Phone | Regex `(XXX) XXX-XXXX` |
| Hours | "Events Only" or "Check Website" |
| Website | Link tagged as "website" in the row |

### Run
```bash
python scrapers/thisweek_to_scraper.py
```

---

## 3. Yellow Pages Scraper
**File:** `scrapers/yellow_pages_scraper.py`
**Output:** `data/yellow_pages_music_leads.csv`

### What it scrapes
[YellowPages.ca](https://www.yellowpages.ca) for Toronto listings matching `"live music restaurants"`. This catches venues that might not be in the music-specific directories — bars and restaurants that host live music.

### How it works
Paginated **requests + BeautifulSoup** scrape:

1. Iterates through paginated search result pages (up to 20 pages)
2. Finds all `.listing__content__wrapper` divs on each page
3. Extracts name, address, phone, and category from each listing
4. Deduplicates by `name + phone` to handle pagination overlap
5. Stops automatically when a page returns no new results

### Key fields collected

| Field | Notes |
|---|---|
| Name | Business name |
| Address | Full street address |
| Phone | Cleaned phone number |
| Cuisine | Category tag (defaults to "Live Music / Restaurant") |

### Run
```bash
python scrapers/yellow_pages_scraper.py
```

---

## Data flow summary

```
IndieOnTheMove  ──► indieonthemove_venues.csv  ──► enrichers/spotify_enricher.py
                                                         │
ThisWeek.to     ──► thisweekto_indie_venues.csv          ▼
                                                    artist_enriched.csv
Yellow Pages    ──► yellow_pages_music_leads.csv         │
                                                         ▼
                                                  venue_genre_profiles.csv
```

IndieOnTheMove is the primary source feeding the enrichment pipeline. ThisWeek.to and Yellow Pages are supplementary lead sources.
