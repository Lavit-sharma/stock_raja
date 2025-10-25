# run_scraper.py
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import gspread
from datetime import date
import json
import os
import time

print("="*70)
print("TradingView Scraper - Cloud Mode")
print("="*70)

# Google Sheets Authentication
print("\n[1/3] Connecting to Google Sheets...")
credentials_json = os.environ.get('GOOGLE_CREDENTIALS')
if not credentials_json:
    print("ERROR: GOOGLE_CREDENTIALS not found")
    exit(1)

credentials = json.loads(credentials_json)
gc = gspread.service_account_from_dict(credentials)

sheet_main = gc.open('Stock List').worksheet('Sheet1')
try:
    sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')
except gspread.WorksheetNotFound:
    sh = gc.open('Tradingview Data Reel Experimental May')
    sheet_data = sh.add_worksheet(title='Sheet5', rows=1000, cols=26)

company_list = sheet_main.col_values(5)
name_list = sheet_main.col_values(1)
current_date = date.today().strftime("%m/%d/%Y")

print(f"✓ Loaded {len(company_list)} companies")

# Chrome Setup (Headless)
print("\n[2/3] Initializing browser...")
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--window-size=1920,1080")

driver = webdriver.Chrome(options=chrome_options)
print("✓ Browser ready")

# Scraping Function
def scrape_tradingview(url):
    try:
        driver.get(url)
        time.sleep(12)
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # Method 1
        nodes = soup.find_all("div", class_="valueValue-l31H9iuA")
        if nodes:
            values = [el.get_text().strip() for el in nodes if el.get_text(strip=True)]
            return [v.replace('−', '-').replace('∅', 'None') for v in values]
        
        # Method 2
        nodes = soup.find_all("div", attrs={"data-name": True})
        if nodes:
            values = [el.get_text().strip() for el in nodes if el.get_text(strip=True)]
            return [v.replace('−', '-').replace('∅', 'None') for v in values]
        
        return []
    except Exception as e:
        print(f"   Error: {str(e)}")
        return []

# Main Loop
print("\n[3/3] Scraping data...")
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '100'))
start_idx = int(os.environ.get('START_INDEX', '1'))
end_idx = min(len(company_list), start_idx + BATCH_SIZE)

success = 0
for i in range(start_idx, end_idx):
    name = name_list[i] if i < len(name_list) else "Unknown"
    url = company_list[i]
    
    print(f"[{i}/{end_idx-1}] {name}", end=" ")
    vals = scrape_tradingview(url)
    
    if vals:
        sheet_data.append_row([name, current_date] + vals, table_range='A1')
        print(f"✓ {len(vals)} values")
        success += 1
    else:
        print("✗ No data")
    
    time.sleep(2)

driver.quit()
print(f"\n{'='*70}")
print(f"COMPLETED: {success}/{end_idx-start_idx} successful")
print("="*70)
