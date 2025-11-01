import requests
import gspread
import time
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

# Google Sheet setup
creds = Credentials.from_service_account_file("creds.json")
client = gspread.authorize(creds)
sheet = client.open("Stock Data").sheet1  # Change your sheet name

# List of stock URLs to scrape (replace with yours)
urls = [
    "https://www.tradingview.com/symbols/NSE-TCS/",
    "https://www.tradingview.com/symbols/NSE-INFY/",
]

def get_field(soup, label):
    """Extract numeric field value from label"""
    tag = soup.find("div", string=lambda t: t and label in t)
    if tag:
        parent = tag.find_parent("div", class_="row-hQx6xNJo")
        if parent:
            value = parent.find("span", class_="value-DHeKxVBO")
            if value:
                return value.text.strip()
    return None

def scrape_stock(url):
    """Scrape required fields and wait until valid"""
    for attempt in range(10):  # retry up to 10 times
        print(f"Scraping attempt {attempt+1} for {url}")
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
        soup = BeautifulSoup(response.text, "html.parser")

        data = {
            "Volume": get_field(soup, "Volume"),
            "Market Cap": get_field(soup, "Market Cap"),
            "P/E Ratio": get_field(soup, "P/E Ratio"),
            "Dividend Yield": get_field(soup, "Dividend Yield"),
        }

        # Check if all values are valid
        if all(data.values()):
            print(f"✅ Data fetched for {url}: {data}")
            return data
        else:
            print("⚠️ Missing fields, retrying in 5s...")
            time.sleep(5)

    print(f"❌ Failed to fetch valid data for {url}")
    return None

# Main loop
for url in urls:
    result = scrape_stock(url)
    if result:
        sheet.append_row([url, result["Volume"], result["Market Cap"], result["P/E Ratio"], result["Dividend Yield"]])
        print(f"✅ Appended data for {url}")
    else:
        sheet.append_row([url, "N/A", "N/A", "N/A", "N/A"])
