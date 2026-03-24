import requests
from bs4 import BeautifulSoup
import csv

def scrape_yellow_pages():
    # Targeted URL for Toronto Restaurants
    url = "https://www.yellowpages.ca/search/si/1/restaurants/Toronto+ON"
    headers = {"User-Agent": "Mozilla/5.0"} 
    
    print(f"🔗 Accessing: {url}")
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    venues = []
    listings = soup.find_all('div', class_='listing__content__wrapper')
    
    for item in listings:
        # 1. Name
        name_tag = item.find('a', class_='listing__name--link')
        name = name_tag.text.strip() if name_tag else "N/A"
        
        # 2. Address
        address_tag = item.find('span', class_='listing__address--full')
        address = address_tag.text.strip() if address_tag else "N/A"

        # 3. Phone (New!)
        phone_tag = item.find('li', class_='mlr__item--phone')
        phone = phone_tag.text.strip() if phone_tag else "N/A"

        # 4. Cuisine/Category (New!)
        cuisine_tag = item.find('div', class_='listing__relevant_categories')
        cuisine = cuisine_tag.text.strip() if cuisine_tag else "Restaurant"
        
        venues.append({
            "Name": name,
            "Address": address,
            "Phone": phone,
            "Cuisine": cuisine
        })
        print(f"✅ Extracted: {name}")

    # Save to CSV
    filename = 'toronto_restaurant_leads.csv'
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["Name", "Address", "Phone", "Cuisine"])
        writer.writeheader()
        writer.writerows(venues)

    print(f"\n📁 File saved: {filename}")

if __name__ == "__main__":
    scrape_yellow_pages()