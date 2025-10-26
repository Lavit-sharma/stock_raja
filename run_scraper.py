from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import gspread
from datetime import date
import json
import os
import time

print("="*70)
print("Parallel Stock Scraper (Batched)")
print("="*70)

# Google Sheets
print("\n[1/3] Connecting to Sheets...")
creds = json.loads(os.environ.get('GOOGLE_CREDENTIALS', '{}'))
gc = gspread.service_account_from_dict(creds)

sheet_main = gc.open('Stock List').worksheet('Sheet1')
try:
    sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')
except:
    sh = gc.open('Tradingview Data Reel Experimental May')
    sheet_data = sh.add_worksheet(title='Sheet5', rows=1000, cols=26)

companies = sheet_main.col_values(5)
names = sheet_main.col_values(1)
today = date.today().strftime("%m/%d/%Y")
print(f"OK - {len(companies)} companies")

# Chrome
print("\n[2/3] Starting browser...")
opts = Options()
opts.add_argument("--headless=new")
opts.add_argument("--no-sandbox")
opts.add_argument("--disable-dev-shm-usage")
opts.add_argument("--disable-blink-features=AutomationControlled")
driver = webdriver.Chrome(options=opts)
print("OK")

# Scraper
def scrape(url):
    try:
        driver.get(url)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((
                By.CSS_SELECTOR,
                "div.valueValue-l31H9iuA, div[data-name]"
            ))
        )
        time.sleep(2)
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        nodes = soup.find_all("div", class_="valueValue-l31H9iuA")
        if not nodes:
            nodes = soup.find_all("div", attrs={"data-name": True})
        
        values = [n.get_text().strip() for n in nodes if n.get_text(strip=True)]
        cleaned = []
        for v in values:
            v = v.replace('−', '-').replace('∅', 'None').strip()
            if v and v not in cleaned:
                cleaned.append(v)
        return cleaned
    except:
        return []

# Batched append (KEY FIX!)
buffer = []
BATCH_SIZE_APPEND = 50

def flush_buffer():
    if not buffer:
        return True
    
    for retry in range(3):
        try:
            rng = 'Sheet5!A1'
            params = {
                'valueInputOption': 'USER_ENTERED',
                'insertDataOption': 'INSERT_ROWS'
            }
            body = {'values': buffer}
            sheet_data.spreadsheet.values_append(rng, params, body)
            buffer.clear()
            return True
        except gspread.exceptions.APIError as e:
            if retry < 2:
                wait = (retry + 1) * 5
                print(f" [Retry {retry+1}, wait {wait}s]", end="")
                time.sleep(wait)
            else:
                print(f" [FAILED after 3 retries]", end="")
                return False
    return False

# Main loop
print("\n[3/3] Scraping...")
batch = int(os.environ.get('BATCH_SIZE', '400'))
start = int(os.environ.get('START_INDEX', '1'))
end = min(len(companies), start + batch)

success = 0
failed = 0

for i in range(start, end):
    name = names[i] if i < len(names) else "Unknown"
    url = companies[i]
    
    print(f"[{i}] {name[:15]:15}", end=" ")
    
    vals = scrape(url)
    
    if vals:
        buffer.append([name, today] + vals)
        print(f"✓ ({len(vals)})", end="")
        success += 1
        
        if len(buffer) >= BATCH_SIZE_APPEND:
            print(" [PUSH]", end="")
            flush_buffer()
            time.sleep(1)
    else:
        print("✗", end="")
        failed += 1
    
    print()
    time.sleep(0.5)

# Final flush
print("\nFlushing remaining...")
flush_buffer()

driver.quit()

print(f"\n{'='*70}")
print(f"COMPLETE: {success} success, {failed} failed")
print(f"Rate: {success/(success+failed)*100:.1f}%")
print("="*70)
