import csv
import re
import time
import os
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
OUTPUT_FILE = "indieonthemove_venues.csv"
TEST_LIMIT = 500           # venues per run (raise once confirmed working)
SLEEP_BETWEEN = 3         # seconds between page loads
PAGE_LOAD_TIMEOUT = 30
MAX_PAGES = 50             # max result pages to paginate through

# Search filters — the scraper will interact with the form dropdowns
TARGET_COUNTRY = "Canada"       # or "United States"
TARGET_STATE = "Ontario"        # leave "" to get all states in that country


# ─── DRIVER SETUP ───────────────────────────────────────────
def init_driver():
    print("🕵️‍♂️ Launching stealth browser...")
    options = uc.ChromeOptions()
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--no-first-run")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")
    driver = uc.Chrome(options=options, version_main=146)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    return driver


def browser_is_alive(driver):
    try:
        _ = driver.current_url
        return True
    except (NoSuchWindowException, WebDriverException):
        return False


# ─── MANUAL LOGIN ────────────────────────────────────────────
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


# ─── INTERACT WITH SEARCH FORM (Vue.js SPA) ─────────────────
def search_venues_via_form(driver, country=TARGET_COUNTRY, state=TARGET_STATE):
    """
    THE KEY FIX: The venue search is a Vue.js SPA. URL parameters
    are IGNORED. We must interact with the actual form dropdowns:

    1. Navigate to /venues
    2. Select country from the 1st <select> (enables the state dropdown)
    3. Wait for state dropdown to populate via AJAX
    4. Select state from the 2nd <select>
    5. Click the "Search" button
    6. Wait for table rows to appear
    """
    print("📍 Navigating to Venues directory...")
    driver.get("https://www.indieonthemove.com/venues")
    time.sleep(5)  # let Vue.js mount and render

    # ── Step 1: Find and select COUNTRY ──
    print(f"   🌍 Selecting country: {country}")
    try:
        # The form has multiple <select> elements. The country one
        # has options "All Countries", "United States", "Canada"
        selects = driver.find_elements(By.CSS_SELECTOR, "form select.form-control")
        country_select = None
        for sel in selects:
            options_text = sel.text
            if "All Countries" in options_text:
                country_select = sel
                break

        if not country_select:
            print("   ❌ Could not find country dropdown!")
            return False

        # Use Selenium's Select helper to pick by visible text
        select_obj = Select(country_select)
        select_obj.select_by_visible_text(country)
        print(f"   ✅ Country set to: {country}")

    except Exception as e:
        print(f"   ❌ Failed to select country: {e}")
        return False

    # ── Step 2: Wait for STATE dropdown to enable and populate ──
    print(f"   🏛️ Waiting for state dropdown to load...")
    time.sleep(3)  # give Vue.js time to fetch states via API

    if state:
        try:
            # Re-find selects (Vue may have re-rendered the DOM)
            selects = driver.find_elements(By.CSS_SELECTOR, "form select.form-control")
            state_select = None
            for sel in selects:
                # The state dropdown is the one that WAS disabled
                # and now should have options beyond "All States"
                options = sel.find_elements(By.TAG_NAME, "option")
                option_texts = [o.text for o in options]
                if "All States" in option_texts and len(options) > 1:
                    state_select = sel
                    break

            if not state_select:
                # Maybe it's still loading — wait more
                time.sleep(3)
                selects = driver.find_elements(By.CSS_SELECTOR, "form select.form-control")
                for sel in selects:
                    options = sel.find_elements(By.TAG_NAME, "option")
                    option_texts = [o.text for o in options]
                    if "All States" in option_texts and len(options) > 1:
                        state_select = sel
                        break

            if state_select:
                select_obj = Select(state_select)
                select_obj.select_by_visible_text(state)
                print(f"   ✅ State set to: {state}")
            else:
                print(f"   ⚠️ State dropdown didn't populate. Searching all states.")

        except Exception as e:
            print(f"   ⚠️ Could not select state: {e}")

    # ── Step 3: Click the SEARCH button ──
    print("   🔍 Clicking Search...")
    try:
        search_btn = driver.find_element(
            By.XPATH, "//button[contains(text(),'Search')]"
        )
        search_btn.click()
    except Exception:
        # Fallback: try any button with "Search"
        try:
            btns = driver.find_elements(By.TAG_NAME, "button")
            for btn in btns:
                if "Search" in btn.text:
                    btn.click()
                    break
        except Exception as e:
            print(f"   ❌ Could not click Search: {e}")
            return False

    # ── Step 4: Wait for results to load ──
    print("   ⏳ Waiting for results...")
    time.sleep(5)

    # Check if results appeared
    try:
        tbody = driver.find_element(By.CSS_SELECTOR, "tbody.bg-white")
        rows = tbody.find_elements(By.TAG_NAME, "tr")
        if rows:
            print(f"   ✅ Found {len(rows)} venues in results!")
            return True
        else:
            # Check for "No venues found"
            page_text = driver.find_element(By.TAG_NAME, "body").text
            if "No venues found" in page_text:
                print("   ⚠️ Search returned 'No venues found'.")
                print("   💡 Try a different state or remove the state filter.")
            return False
    except Exception:
        return False


# ─── COLLECT VENUE LINKS FROM RESULTS TABLE ──────────────────
def collect_links_from_table(driver):
    """
    After search results appear, extract venue profile URLs
    from the results table. Each row has a link to the venue.
    """
    links = []
    try:
        tbody = driver.find_element(By.CSS_SELECTOR, "tbody.bg-white")
        rows = tbody.find_elements(By.TAG_NAME, "tr")
        for row in rows:
            try:
                anchors = row.find_elements(By.TAG_NAME, "a")
                for a in anchors:
                    href = a.get_attribute("href") or ""
                    if "/venues/" in href and "indieonthemove.com" in href:
                        slug = href.split("/venues/")[-1].split("?")[0].split("#")[0]
                        if slug and slug not in ["", "search", "view"]:
                            clean = href.split("/venues/")[0] + "/venues/" + slug
                            links.append(clean)
            except StaleElementReferenceException:
                continue
    except Exception as e:
        print(f"   ⚠️ Error reading table: {e}")

    return list(dict.fromkeys(links))  # deduplicate


def click_next_page(driver):
    """Click the next pagination link. Returns True if successful."""
    try:
        pagination = driver.find_element(By.CSS_SELECTOR, "nav[aria-label='pagination']")
        page_items = pagination.find_elements(By.CSS_SELECTOR, "li.page-item")

        # Find the currently active page
        active_found = False
        for item in page_items:
            if active_found:
                # This is the page AFTER the active one
                link = item.find_element(By.TAG_NAME, "a")
                link.click()
                time.sleep(4)
                return True
            if "active" in item.get_attribute("class"):
                active_found = True

        return False
    except Exception:
        return False


def get_all_venue_links(driver, max_pages=MAX_PAGES):
    """Collect venue links across multiple result pages."""
    all_links = []

    for page_num in range(1, max_pages + 1):
        print(f"\n📄 Collecting links from results page {page_num}...")
        page_links = collect_links_from_table(driver)

        if not page_links:
            print(f"   🛑 No links on page {page_num}. Stopping.")
            break

        print(f"   Found {len(page_links)} venues")
        all_links.extend(page_links)

        # Try to go to next page
        if page_num < max_pages:
            if not click_next_page(driver):
                print("   🛑 No more pages.")
                break

    all_links = list(dict.fromkeys(all_links))
    print(f"\n🔗 Total unique venue links: {len(all_links)}")
    return all_links


# ─── EXTRACT DATA FROM ONE VENUE PROFILE ─────────────────────
def extract_venue_data(driver, url):
    """
    Extract venue data using selectors mapped from debug HTML:
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
    try:
        driver.get(url)
    except TimeoutException:
        print("⚠️ Timed out. Skipping.")
        return None
    except (NoSuchWindowException, WebDriverException):
        print("❌ Browser died.")
        return None

    time.sleep(3)

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

    # 2. LOCATION (meta tags — reliable)
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

    # Get card-body text for regex
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

    time.sleep(SLEEP_BETWEEN)

    return {
        "Name": name, "City": city, "State/Province": state_prov,
        "Zip_Code": zip_code, "Address": address, "Phone": phone,
        "Website": website, "Categories": categories, "Genres": genres,
        "Capacity": capacity, "Age_Restriction": age, "Rating": rating,
        "Description": description, "Booking_Info": booking,
        "Facebook": facebook, "Instagram": instagram,
        "Upcoming_Events": events, "Profile_URL": url,
    }


# ─── SAVE TO CSV ────────────────────────────────────────────
FIELDNAMES = [
    "Name", "City", "State/Province", "Zip_Code", "Address",
    "Phone", "Website", "Categories", "Genres", "Capacity",
    "Age_Restriction", "Rating", "Description", "Booking_Info",
    "Facebook", "Instagram", "Upcoming_Events", "Profile_URL",
]


def save_to_csv(data, filename):
    if not data:
        print("⚠️ No data to save.")
        return
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(data)
    print(f"\n📁 Saved {len(data)} venues to {filename}")


# ─── MAIN ────────────────────────────────────────────────────
def main():
    driver = init_driver()
    scraped_data = []

    try:
        # 1. Login
        manual_login_pause(driver)

        # 2. Use the search form to find venues
        search_ok = search_venues_via_form(
            driver, country=TARGET_COUNTRY, state=TARGET_STATE
        )

        if not search_ok:
            # Save page for debugging
            with open("debug_listing_page.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            print("\n⚠️ Search returned no results.")
            print("   Saved debug_listing_page.html for inspection.")
            print("\n💡 TROUBLESHOOTING:")
            print("   - Try setting TARGET_STATE = '' to search all states")
            print("   - Try TARGET_COUNTRY = 'United States' instead")
            print("   - The state name must exactly match the dropdown text")
            return

        # 3. Collect venue links from result pages
        urls = get_all_venue_links(driver, max_pages=MAX_PAGES)

        if not urls:
            print("⚠️ No venue links found in results table.")
            with open("debug_listing_page.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            return

        urls = urls[:TEST_LIMIT]
        print(f"\n🎯 Scraping {len(urls)} venues (TEST_LIMIT={TEST_LIMIT})...\n")

        # 4. Scrape each venue profile
        for i, url in enumerate(urls, 1):
            print(f"[{i}/{len(urls)}] ", end="")
            if not browser_is_alive(driver):
                print("❌ Browser died. Saving progress...")
                break
            data = extract_venue_data(driver, url)
            if data:
                scraped_data.append(data)
                print(
                    f"   ✅ {data['Name']} | "
                    f"{data['City']}, {data['State/Province']} | "
                    f"Cap: {data['Capacity']} | Ph: {data['Phone']}"
                )
            else:
                print("   ⚠️ Failed")

        # 5. Save
        save_to_csv(scraped_data, OUTPUT_FILE)

    except KeyboardInterrupt:
        print("\n⚠️ Interrupted. Saving progress...")
        save_to_csv(scraped_data, OUTPUT_FILE)

    finally:
        print("🧹 Closing browser...")
        try:
            driver.quit()
        except Exception:
            pass
        os._exit(0)


if __name__ == "__main__":
    main()