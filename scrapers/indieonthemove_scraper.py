import csv
import re
import time
import os
import random
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    NoSuchWindowException,
    WebDriverException,
    StaleElementReferenceException,
)
import undetected_chromedriver as uc

# ─── CONFIG ──────────────────────────────────────────────────
OUTPUT_FILE = "../data/indieonthemove_venues.csv"
ERROR_LOG_FILE = "../data/scraper_errors.log"

TARGET_COUNTRY = "Canada"

# Add or remove provinces here. Each one runs a full search-collect-scrape cycle.
TARGET_PROVINCES = [
    "Ontario",
    "Quebec",
    "British Columbia",
    "Alberta",
    "Manitoba",
    "Nova Scotia",
    "New Brunswick",
]

# US expansion (per Awksion brief simplification: 5 cities). Set RUN_US=True to enable.
RUN_US = True
US_TARGET_STATES = [
    "New York",
    "California",
    "Illinois",
    "Tennessee",
    "Texas",
]
# Post-filter venues to these cities per state. Empty list = keep all venues in that state.
US_TARGET_CITIES = {
    "New York":   ["New York", "Brooklyn", "Manhattan", "Queens", "Bronx", "Staten Island"],
    "California": ["Los Angeles", "Hollywood", "West Hollywood", "Santa Monica", "Long Beach", "Pasadena"],
    "Illinois":   ["Chicago"],
    "Tennessee":  ["Nashville"],
    "Texas":      ["Austin"],
}

TEST_LIMIT = 0                       # 0 = no per-province limit on NEW venues
MAX_PAGES_PER_PROVINCE = 50

# Driver cycling thresholds — protects against Selenium memory bloat on long runs
DRIVER_RESTART_EVERY_N_PAGES = 100
DRIVER_RESTART_EVERY_SECONDS = 3600

# Timeouts
PAGE_LOAD_TIMEOUT = 30
WAIT_TIMEOUT = 20

# Cloudflare login is manual, so headless is off by default
HEADLESS = False


# ─── SMALL UTILS ────────────────────────────────────────────
def rand_sleep(a=1.5, b=3.5):
    time.sleep(random.uniform(a, b))


def normalize_url(url):
    if not url:
        return ""
    u = url.strip().split("?")[0].split("#")[0].rstrip("/")
    return u.lower()


def log_error(province, url, error_type, message):
    parent = os.path.dirname(ERROR_LOG_FILE) or "."
    os.makedirs(parent, exist_ok=True)
    with open(ERROR_LOG_FILE, "a", encoding="utf-8") as f:
        ts = datetime.now().isoformat(timespec="seconds")
        msg = (message or "").replace("\n", " ").replace("\r", " ")[:300]
        f.write(f"{ts}\t{province}\t{url}\t{error_type}\t{msg}\n")


# ─── DRIVER SETUP ───────────────────────────────────────────
def init_driver():
    print("🕵️‍♂️ Launching stealth browser...")
    options = uc.ChromeOptions()
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-notifications")
    options.add_argument("--start-maximized")
    if HEADLESS:
        options.add_argument("--headless=new")
    driver = uc.Chrome(options=options, version_main=146)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    return driver


def browser_is_alive(driver):
    try:
        _ = driver.current_url
        return True
    except (NoSuchWindowException, WebDriverException):
        return False


def manual_login_pause(driver):
    print("🔐 Navigating to login page...")
    driver.get("https://www.indieonthemove.com/login")
    print("\n" + "=" * 60)
    print("🛑 ACTION REQUIRED")
    print("1. Solve the Cloudflare challenge in the Chrome window.")
    print("2. Log into your IndieOnTheMove account.")
    print("3. Wait until the dashboard loads fully.")
    print("=" * 60 + "\n")
    input("👉 Press ENTER here ONLY AFTER you are fully logged in... ")
    if not browser_is_alive(driver):
        raise SystemExit("❌ Browser died during login.")
    print("✅ Browser alive. Resuming...\n")


# ─── DRIVER STATE (FOR CYCLING) ─────────────────────────────
class DriverState:
    """Tracks the live driver, its age, and pages-since-restart for cycling."""

    def __init__(self):
        self.driver = None
        self.started_at = 0.0
        self.pages_since_restart = 0

    def start(self):
        self.driver = init_driver()
        self.started_at = time.time()
        self.pages_since_restart = 0
        manual_login_pause(self.driver)

    def quit(self):
        try:
            if self.driver:
                self.driver.quit()
        except Exception:
            pass
        self.driver = None

    def restart(self):
        print("\n♻️ Cycling driver to free memory & reset state...")
        self.quit()
        self.start()

    def maybe_cycle(self):
        elapsed = time.time() - self.started_at
        if (self.pages_since_restart >= DRIVER_RESTART_EVERY_N_PAGES
                or elapsed >= DRIVER_RESTART_EVERY_SECONDS):
            print(
                f"\n♻️ Cycle threshold reached "
                f"({self.pages_since_restart} pages / {elapsed:.0f}s)"
            )
            self.restart()

    def increment_page(self):
        self.pages_since_restart += 1


# ─── RETRY HELPERS ──────────────────────────────────────────
def get_with_retries(driver, url, retries=2):
    """Navigate to url with up to `retries` retries on Timeout / WebDriver errors."""
    last_err = None
    for attempt in range(retries + 1):
        try:
            driver.get(url)
            WebDriverWait(driver, WAIT_TIMEOUT).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            return True
        except TimeoutException as e:
            last_err = e
            print(f"   ⏰ Page-load timeout (attempt {attempt + 1}/{retries + 1})")
            try:
                driver.execute_script("window.stop();")
            except Exception:
                pass
            rand_sleep(2, 4)
        except (StaleElementReferenceException, WebDriverException) as e:
            last_err = e
            print(f"   ⚠️ {type(e).__name__} (attempt {attempt + 1}/{retries + 1})")
            rand_sleep(2, 4)
    if last_err:
        raise last_err
    return False


# ─── SEARCH FORM (Vue.js SPA) ───────────────────────────────
def _find_country_select(driver):
    selects = driver.find_elements(By.CSS_SELECTOR, "form select.form-control")
    for sel in selects:
        if "All Countries" in sel.text:
            return sel
    return None


def _find_state_select(driver):
    selects = driver.find_elements(By.CSS_SELECTOR, "form select.form-control")
    for sel in selects:
        opts = sel.find_elements(By.TAG_NAME, "option")
        texts = [o.text for o in opts]
        if "All States" in texts and len(opts) > 1:
            return sel
    return None


def search_venues_via_form(driver, country, state):
    """The venue search is a Vue.js SPA — URL params are ignored, must use the form."""
    print(f"📍 Loading /venues for {country} → {state}...")
    try:
        driver.get("https://www.indieonthemove.com/venues")
    except TimeoutException:
        try:
            driver.execute_script("window.stop();")
        except Exception:
            pass

    try:
        WebDriverWait(driver, WAIT_TIMEOUT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "form select.form-control"))
        )
    except TimeoutException:
        print("   ❌ Search form never appeared.")
        return False

    rand_sleep(1, 2)

    # Country
    try:
        country_sel = _find_country_select(driver)
        if not country_sel:
            print("   ❌ Country dropdown not found.")
            return False
        Select(country_sel).select_by_visible_text(country)
        print(f"   ✅ Country: {country}")
    except Exception as e:
        print(f"   ❌ Country select failed: {e}")
        return False

    # Wait for state dropdown to populate via AJAX
    try:
        WebDriverWait(driver, WAIT_TIMEOUT).until(
            lambda d: _find_state_select(d) is not None
        )
    except TimeoutException:
        print("   ⚠️ State dropdown didn't populate in time.")
        return False

    if state:
        try:
            state_sel = _find_state_select(driver)
            Select(state_sel).select_by_visible_text(state)
            print(f"   ✅ State: {state}")
        except Exception as e:
            print(f"   ❌ State select failed for '{state}': {e}")
            return False

    # Click Search
    try:
        btn = WebDriverWait(driver, WAIT_TIMEOUT).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'Search')]"))
        )
        btn.click()
    except Exception as e:
        try:
            for b in driver.find_elements(By.TAG_NAME, "button"):
                if "Search" in b.text:
                    b.click()
                    break
        except Exception:
            print(f"   ❌ Could not click Search: {e}")
            return False

    # Wait for results table OR empty-state text
    try:
        WebDriverWait(driver, WAIT_TIMEOUT).until(
            lambda d: (
                len(d.find_elements(By.CSS_SELECTOR, "tbody.bg-white tr")) > 0
                or "No venues found" in (d.find_element(By.TAG_NAME, "body").text or "")
            )
        )
    except TimeoutException:
        print("   ⏰ Results never appeared.")
        return False

    rows = driver.find_elements(By.CSS_SELECTOR, "tbody.bg-white tr")
    if rows:
        print(f"   ✅ {len(rows)} venues on first results page")
        return True
    print("   ⚠️ Search returned 'No venues found'.")
    return False


# ─── COLLECT LINKS ──────────────────────────────────────────
def collect_links_from_table(driver):
    links = []
    try:
        tbody = driver.find_element(By.CSS_SELECTOR, "tbody.bg-white")
        rows = tbody.find_elements(By.TAG_NAME, "tr")
        for row in rows:
            try:
                for a in row.find_elements(By.TAG_NAME, "a"):
                    href = a.get_attribute("href") or ""
                    if "/venues/" in href and "indieonthemove.com" in href:
                        slug = href.split("/venues/")[-1].split("?")[0].split("#")[0]
                        if slug and slug not in ("", "search", "view"):
                            clean = href.split("/venues/")[0] + "/venues/" + slug
                            links.append(clean)
            except StaleElementReferenceException:
                continue
    except Exception as e:
        print(f"   ⚠️ Error reading table: {e}")
    return list(dict.fromkeys(links))


def get_active_page_num(driver):
    try:
        pagination = driver.find_element(By.CSS_SELECTOR, "nav[aria-label='pagination']")
        for item in pagination.find_elements(By.CSS_SELECTOR, "li.page-item"):
            if "active" in (item.get_attribute("class") or ""):
                return (item.text or "").strip()
    except Exception:
        pass
    return None


def click_next_page(driver):
    current = get_active_page_num(driver)
    try:
        pagination = driver.find_element(By.CSS_SELECTOR, "nav[aria-label='pagination']")
        items = pagination.find_elements(By.CSS_SELECTOR, "li.page-item")
        active_seen = False
        for item in items:
            if active_seen:
                cls = item.get_attribute("class") or ""
                if "disabled" in cls:
                    return False
                try:
                    item.find_element(By.TAG_NAME, "a").click()
                except Exception:
                    return False
                # Wait for active page number to actually change
                try:
                    WebDriverWait(driver, WAIT_TIMEOUT).until(
                        lambda d: get_active_page_num(d) not in (current, None)
                    )
                except TimeoutException:
                    return False
                rand_sleep(1.5, 3.0)
                return True
            if "active" in (item.get_attribute("class") or ""):
                active_seen = True
        return False
    except Exception:
        return False


def get_all_venue_links(driver, max_pages):
    """Walk pagination collecting venue profile links. Stops on repeated active page."""
    all_links = []
    seen_pages = set()
    for page_num in range(1, max_pages + 1):
        active = get_active_page_num(driver)
        if active and active in seen_pages:
            print(f"   🛑 Already visited active page {active}. Stopping.")
            break
        if active:
            seen_pages.add(active)

        print(f"\n📄 Reading results page {page_num} (active={active})...")
        page_links = collect_links_from_table(driver)
        if not page_links:
            print("   🛑 No links on this page. Stopping pagination.")
            break
        print(f"   {len(page_links)} venue links")
        all_links.extend(page_links)

        if page_num < max_pages:
            if not click_next_page(driver):
                print("   🛑 No more pages.")
                break

    all_links = list(dict.fromkeys(all_links))
    print(f"\n🔗 {len(all_links)} unique venue links across {len(seen_pages)} page(s)")
    return all_links


# ─── EXTRACT VENUE DATA ─────────────────────────────────────
def extract_venue_data(driver, url, source_province):
    """
    Selectors mapped from debug HTML:
      Name         → h4.card-title
      City/State   → meta og:locality / og:region
      Zip          → meta zipcode
      Address      → p.mb-0 in card-body
      Phone        → regex (XXX) XXX-XXXX
      Website      → external <a> in div.col.col-md-8
      Categories   → span.category.badge-secondary
      Genres       → span.genre.badge-primary
      Capacity     → regex after "Capacity:"
      Age          → regex after "Age:"
      Rating       → hidden p.rating-text
      Description  → card with "Description" header
      Booking Info → card with "Booking Info" header
    """
    get_with_retries(driver, url, retries=2)

    try:
        WebDriverWait(driver, WAIT_TIMEOUT).until(
            EC.any_of(
                EC.presence_of_element_located((By.CSS_SELECTOR, "h4.card-title")),
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.card-body")),
            )
        )
    except TimeoutException:
        pass  # extraction below will return N/A for missing fields

    def safe_meta(name):
        try:
            return driver.find_element(
                By.CSS_SELECTOR, f'meta[name="{name}"]'
            ).get_attribute("content").strip()
        except Exception:
            return "N/A"

    def safe_meta_prop(prop):
        try:
            return driver.find_element(
                By.CSS_SELECTOR, f'meta[property="{prop}"]'
            ).get_attribute("content").strip()
        except Exception:
            return "N/A"

    # 1. NAME
    name = "N/A"
    try:
        el = driver.find_element(By.CSS_SELECTOR, "h4.card-title")
        name = re.sub(r'\s*Edit\s*$', '', el.text.split("\n")[0].strip())
    except Exception:
        og = safe_meta_prop("og:title")
        if og != "N/A":
            name = og.split(" - ")[0].split(",")[0].strip()

    # 2. LOCATION
    city = safe_meta("og:locality")
    state_prov = safe_meta("og:region")
    zip_code = safe_meta("zipcode")

    # 3. ADDRESS
    address = "N/A"
    try:
        els = driver.find_elements(By.CSS_SELECTOR, "div.card-body p.mb-0")
        if els:
            address = ", ".join(e.text.strip() for e in els if e.text.strip())
    except Exception:
        pass

    body_text = ""
    try:
        body_text = driver.find_element(By.CSS_SELECTOR, "div.card-body").text
    except Exception:
        pass

    # 4. PHONE
    phone = "N/A"
    m = re.search(r'\(\d{3}\)\s*\d{3}[-.]?\d{4}', body_text)
    if m:
        phone = m.group(0)

    # 5. WEBSITE
    website = "N/A"
    SKIP = ["indieonthemove", "facebook.com", "instagram.com", "x.com",
            "twitter.com", "youtube.com", "disqus.com", "bandsintown.com",
            "yelp.com", "google.com", "tiktok.com", "spotify.com"]
    try:
        col = driver.find_element(By.CSS_SELECTOR, "div.col.col-md-8")
        for link in col.find_elements(By.TAG_NAME, "a"):
            href = link.get_attribute("href") or ""
            if href.startswith("http") and not any(s in href for s in SKIP):
                website = href
                break
    except Exception:
        pass

    # 6. CATEGORIES
    categories = "N/A"
    try:
        els = driver.find_elements(By.CSS_SELECTOR, "span.category.badge-secondary")
        if els:
            categories = ", ".join(e.text.strip() for e in els if e.text.strip())
    except Exception:
        pass

    # 7. GENRES
    genres = "N/A"
    try:
        els = driver.find_elements(By.CSS_SELECTOR, "span.genre.badge-primary")
        if els:
            genres = ", ".join(e.text.strip() for e in els if e.text.strip())
    except Exception:
        pass

    # 8. CAPACITY
    capacity = "N/A"
    m = re.search(r'Capacity:\s*(\d[\d,]*)', body_text)
    if m:
        capacity = m.group(1)

    # 9. AGE
    age = "N/A"
    m = re.search(r'Age:\s*(\d+\+?)', body_text)
    if m:
        age = m.group(1)

    # 10. RATING
    rating = "N/A"
    try:
        els = driver.find_elements(By.CSS_SELECTOR, "p.rating-text")
        if els:
            raw = els[0].get_attribute("textContent").strip()
            if raw:
                rating = raw
    except Exception:
        pass

    # 11. DESCRIPTION
    description = "N/A"
    try:
        for card in driver.find_elements(By.CSS_SELECTOR, "div.card"):
            try:
                hdr = card.find_element(By.CSS_SELECTOR, "h4.card-header")
                if "Description" in hdr.text:
                    description = card.find_element(
                        By.CSS_SELECTOR, "div.card-body"
                    ).text.strip()
                    break
            except Exception:
                continue
    except Exception:
        pass

    # 12. BOOKING INFO
    booking = "N/A"
    try:
        for card in driver.find_elements(By.CSS_SELECTOR, "div.card"):
            try:
                hdr = card.find_element(By.CSS_SELECTOR, "h4.card-header")
                if "Booking Info" in hdr.text:
                    txt = card.text
                    if "Upgrade" in txt or "Premium" in txt:
                        booking = "Requires Premium"
                    else:
                        booking = card.find_element(
                            By.CSS_SELECTOR, "div.card-body"
                        ).text.strip()
                    break
            except Exception:
                continue
    except Exception:
        pass

    # 13. SOCIAL
    facebook = instagram = "N/A"
    try:
        col = driver.find_element(By.CSS_SELECTOR, "div.col.col-md-8")
        for link in col.find_elements(By.TAG_NAME, "a"):
            href = link.get_attribute("href") or ""
            if "facebook.com" in href and facebook == "N/A":
                facebook = href
            if "instagram.com" in href and instagram == "N/A":
                instagram = href
    except Exception:
        pass

    # 14. UPCOMING EVENTS
    events = "N/A"
    try:
        els = driver.find_elements(By.CSS_SELECTOR, "h5.calendar-event-title")
        if els:
            events = "; ".join(e.text.strip() for e in els if e.text.strip())
    except Exception:
        pass

    rand_sleep(1.5, 3.5)

    return {
        "Name": name, "City": city, "State/Province": state_prov,
        "Zip_Code": zip_code, "Address": address, "Phone": phone,
        "Website": website, "Categories": categories, "Genres": genres,
        "Capacity": capacity, "Age_Restriction": age, "Rating": rating,
        "Description": description, "Booking_Info": booking,
        "Facebook": facebook, "Instagram": instagram,
        "Upcoming_Events": events, "Profile_URL": url,
        "Normalized_Profile_URL": normalize_url(url),
        "Source_Province_Search": source_province,
        "Scraped_At": datetime.now().isoformat(timespec="seconds"),
    }


# ─── CSV (APPEND-SAFE) ──────────────────────────────────────
FIELDNAMES = [
    "Name", "City", "State/Province", "Zip_Code", "Address",
    "Phone", "Website", "Categories", "Genres", "Capacity",
    "Age_Restriction", "Rating", "Description", "Booking_Info",
    "Facebook", "Instagram", "Upcoming_Events", "Profile_URL",
    "Normalized_Profile_URL", "Source_Province_Search", "Scraped_At",
]


def ensure_csv_ready(filename):
    """Create CSV with header if missing/empty; migrate header if older schema."""
    parent = os.path.dirname(filename) or "."
    os.makedirs(parent, exist_ok=True)

    if not os.path.exists(filename) or os.path.getsize(filename) == 0:
        with open(filename, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=FIELDNAMES).writeheader()
        print(f"📄 Created fresh CSV: {filename}")
        return

    with open(filename, "r", encoding="utf-8") as f:
        try:
            existing = next(csv.reader(f))
        except StopIteration:
            existing = []

    if set(existing) >= set(FIELDNAMES):
        return  # already has all our columns (extra columns OK to ignore on append)

    print(f"🔄 Migrating CSV header: adding {sorted(set(FIELDNAMES) - set(existing))}")
    with open(filename, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            if not r.get("Normalized_Profile_URL"):
                r["Normalized_Profile_URL"] = normalize_url(r.get("Profile_URL", ""))
            writer.writerow(r)
    print("   ✅ Migration complete")


def load_seen_urls(filename):
    """Load normalized URLs from existing CSV into a set for fast dedup."""
    seen = set()
    if not os.path.exists(filename):
        return seen
    try:
        with open(filename, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                norm = (row.get("Normalized_Profile_URL") or "").strip()
                if not norm:
                    norm = normalize_url(row.get("Profile_URL", ""))
                if norm:
                    seen.add(norm)
    except Exception as e:
        print(f"⚠️ Error reading existing CSV: {e}")
    print(f"📂 Loaded {len(seen)} already-scraped URLs from {filename}")
    return seen


def append_row_to_csv(row, filename):
    """Append a single row to the CSV and flush so a crash doesn't lose it."""
    with open(filename, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writerow(row)
        f.flush()


# ─── PER-REGION SCRAPE LOOP (extracted so we can reuse for CA + US) ──
def _scrape_one_region(state, country, region, city_filter, seen_urls):
    """Scrape one (country, region) pair. city_filter: list of city names to keep,
    or empty/None to keep everything. Returns (appended, skipped, errors)."""
    appended = 0
    skipped = 0
    errors = 0

    try:
        ok = search_venues_via_form(state.driver, country, region)
    except Exception as e:
        log_error(region, "", type(e).__name__, str(e))
        ok = False

    if not ok:
        print(f"   ⚠️ No results for {region}, skipping.")
        log_error(region, "", "SearchFailed", "search_venues_via_form returned False")
        rand_sleep(2, 4)
        return appended, skipped, errors

    try:
        urls = get_all_venue_links(state.driver, MAX_PAGES_PER_PROVINCE)
    except Exception as e:
        log_error(region, "", type(e).__name__, str(e))
        urls = []

    new_urls = []
    for u in urls:
        norm = normalize_url(u)
        if norm in seen_urls:
            skipped += 1
        else:
            new_urls.append(u)

    print(f"⏭️ {skipped} duplicates skipped | 🆕 {len(new_urls)} new")

    if TEST_LIMIT > 0:
        new_urls = new_urls[:TEST_LIMIT]
        print(f"   🎯 Capped to TEST_LIMIT={TEST_LIMIT}")

    cf_lower = [c.lower() for c in (city_filter or [])]

    for i, url in enumerate(new_urls, 1):
        norm = normalize_url(url)
        if norm in seen_urls:
            continue

        state.maybe_cycle()
        if not browser_is_alive(state.driver):
            print("❌ Browser died unexpectedly. Restarting...")
            state.restart()

        print(f"[{region} {i}/{len(new_urls)}] ", end="", flush=True)
        try:
            data = extract_venue_data(state.driver, url, region)
            state.increment_page()
        except (TimeoutException, WebDriverException, StaleElementReferenceException) as e:
            log_error(region, url, type(e).__name__, str(e))
            errors += 1
            print(f"❌ {type(e).__name__}: {str(e)[:80]}")
            continue
        except Exception as e:
            log_error(region, url, type(e).__name__, str(e))
            errors += 1
            print(f"❌ {type(e).__name__}: {str(e)[:80]}")
            continue

        if not data:
            log_error(region, url, "NoData", "extract_venue_data returned None")
            errors += 1
            print("⚠️  No data extracted")
            continue

        # Apply city filter (US scope-narrowing per Awksion brief)
        if cf_lower:
            city_l = (data.get("City") or "").strip().lower()
            if not any(target in city_l or city_l in target for target in cf_lower):
                seen_urls.add(norm)  # don't re-fetch even if we skip
                print(f"⏭️  filtered out (city='{data.get('City')}' not in target list)")
                continue

        append_row_to_csv(data, OUTPUT_FILE)
        seen_urls.add(norm)
        appended += 1
        print(
            f"✅ {data['Name']} | {data['City']}, {data['State/Province']} | "
            f"Cap: {data['Capacity']} | Ph: {data['Phone']}"
        )

    return appended, skipped, errors


# ─── MAIN ────────────────────────────────────────────────────
def main():
    ensure_csv_ready(OUTPUT_FILE)
    seen_urls = load_seen_urls(OUTPUT_FILE)

    print(f"🌐 Canada — provinces ({len(TARGET_PROVINCES)}): {', '.join(TARGET_PROVINCES)}")
    if RUN_US:
        print(f"🌐 USA — states ({len(US_TARGET_STATES)}): {', '.join(US_TARGET_STATES)}")
        for st, cities in US_TARGET_CITIES.items():
            print(f"     {st}: filter to {cities}")
    print(
        f"♻️ Driver cycles every {DRIVER_RESTART_EVERY_N_PAGES} pages "
        f"or {DRIVER_RESTART_EVERY_SECONDS}s\n"
    )

    state = DriverState()
    state.start()

    total_appended = 0
    total_skipped = 0
    total_errors = 0

    # Build the (country, region, city_filter) work list
    work = [("Canada", p, []) for p in TARGET_PROVINCES]
    if RUN_US:
        work += [("United States", st, US_TARGET_CITIES.get(st, [])) for st in US_TARGET_STATES]

    try:
        for country, region, city_filter in work:
            print(f"\n{'='*60}\n🏛️  {country} → {region}\n{'='*60}")
            appended, skipped, errors = _scrape_one_region(
                state, country, region, city_filter, seen_urls
            )
            total_appended += appended
            total_skipped += skipped
            total_errors += errors
            print(
                f"\n📊 {region}: 🆕 {appended} appended | "
                f"⏭️ {skipped} skipped | ❌ {errors} errors"
            )
            rand_sleep(3, 6)

    except KeyboardInterrupt:
        print("\n⚠️ Interrupted by user. Progress is already saved (append-mode).")

    finally:
        print(
            f"\n📈 RUN SUMMARY: 🆕 {total_appended} appended | "
            f"⏭️ {total_skipped} skipped | ❌ {total_errors} errors"
        )
        print(f"🗂️ CSV:    {OUTPUT_FILE}")
        print(f"🪵 Errors: {ERROR_LOG_FILE}")
        state.quit()
        os._exit(0)


if __name__ == "__main__":
    main()
