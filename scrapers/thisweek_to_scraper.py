import requests
from bs4 import BeautifulSoup
import csv
import re

def scrape_thisweek_clean():
    # Targeted URL for Toronto's Live Music Directory
    url = "https://thisweek.to/directory.html"
    headers = {"User-Agent": "Mozilla/5.0"} 
    
    print(f"🔗 Accessing: {url}")
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    venues = []
    
    # SMART TRAVERSAL: Find every 'a' (link) tag that contains "map link"
    map_links = soup.find_all('a', string=re.compile(r'map link', re.IGNORECASE))
    
    print(f"Found {len(map_links)} potential venues. Extracting clean data...\n")
    
    for link in map_links:
        parent = link.find_parent(['tr', 'div', 'li']) 
        
        if not parent:
            continue
            
        texts = list(parent.stripped_strings)
        
        # We need at least a name and an address to make a valid row
        if len(texts) < 2: 
            continue
            
        # 1. Name 
        name = texts[0].replace(" page", "").strip() 
        
        # 2. Address (On this site, the address is always the text right after the name)
        address = texts[1].strip()
        
        full_text = " | ".join(texts)
        
        # 3. Phone Number
        phone_match = re.search(r'\(\d{3}\)\s\d{3}-\d{4}', full_text)
        phone = phone_match.group(0) if phone_match else "N/A"
        
        # 4. Hours/Events 
        hours = "Events Only" if "Events Only" in full_text else "Check Website"
        
        # 5. Website Link
        website_tag = parent.find('a', string=re.compile(r'website', re.IGNORECASE))
        website = website_tag['href'] if website_tag and website_tag.has_attr('href') else "N/A"
        
        venues.append({
            "Name": name,
            "Address": address,
            "Phone": phone,
            "Hours": hours,
            "Website": website
        })
        print(f"✅ Cleaned: {name} | {address}")

    # Save to CSV
    filename = '../data/thisweekto_indie_venues.csv'
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        # Notice how Address perfectly replaces Raw_Data here
        writer = csv.DictWriter(f, fieldnames=["Name", "Address", "Phone", "Hours", "Website"])
        writer.writeheader()
        writer.writerows(venues)

    print(f"\n📁 Clean file saved: {filename}")

if __name__ == "__main__":
    scrape_thisweek_clean()