import requests
from bs4 import BeautifulSoup
import csv
import time

def scrape_yellow_pages_paginated():
    base_url = "https://www.yellowpages.ca/search/si/{}/live+music+restaurants/Toronto+ON"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    } 
    
    venues = []
    seen_venues = set() # To track duplicates using a unique identifier (Name + Phone)
    page = 1
    max_pages = 20 # Safety limit to prevent infinite loops

    print("🚀 Starting Yellow Pages Pagination Scraper...")

    while page <= max_pages:
        url = base_url.format(page)
        print(f"\n🔗 Scraping Page {page}: {url}")
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status() # Raise an exception for bad status codes
        except requests.RequestException as e:
            print(f"❌ Error fetching page {page}: {e}")
            break

        soup = BeautifulSoup(response.text, 'html.parser')
        listings = soup.find_all('div', class_='listing__content__wrapper')
        
        # If no listings are found on the page, we've reached the end
        if not listings:
            print("🛑 No more listings found. Ending pagination.")
            break
            
        new_venues_count = 0

        for item in listings:
            # 1. Name
            name_tag = item.find('a', class_='listing__name--link')
            name = name_tag.text.strip() if name_tag else "N/A"
            
            # 2. Address
            address_tag = item.find('span', class_='listing__address--full')
            address = address_tag.text.strip() if address_tag else "N/A"

            # 3. Phone (Cleaned format)
            phone_tag = item.find('li', class_='mlr__item--phone')
            if phone_tag:
                phone = phone_tag.text.replace("Phone Number", "").strip()
            else:
                phone = "N/A"

            # 4. Cuisine/Category
            cuisine_tag = item.find('div', class_='listing__relevant_categories')
            cuisine = cuisine_tag.text.strip() if cuisine_tag else "Live Music / Restaurant"
            
            # Create a unique identifier to check for duplicates
            unique_id = f"{name}_{phone}"
            
            if unique_id not in seen_venues:
                seen_venues.add(unique_id)
                venues.append({
                    "Name": name,
                    "Address": address,
                    "Phone": phone,
                    "Cuisine": cuisine
                })
                new_venues_count += 1
                print(f"✅ Extracted: {name}")
            else:
                print(f"⚠️ Skipped duplicate: {name}")

        # If a page loaded but all items were duplicates, it might be looping the last page
        if new_venues_count == 0:
            print("🛑 Only duplicates found on this page. Stopping to prevent infinite loop.")
            break

        page += 1
        
        # Polite rate limiting
        print("⏳ Sleeping for 2 seconds to respect server limits...")
        time.sleep(2)

    # Save to CSV
    filename = 'yellow_pages_music_leads.csv'
    if venues:
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=["Name", "Address", "Phone", "Cuisine"])
            writer.writeheader()
            writer.writerows(venues)
        print(f"\n📁 Successfully saved {len(venues)} unique venues to {filename}")
    else:
        print("\n⚠️ No venues were scraped. CSV was not created.")

if __name__ == "__main__":
    scrape_yellow_pages_paginated()