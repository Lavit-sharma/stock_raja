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
print("Parallel Stock Scraper (Update Mode)")
print("="*70)

# Google Sheets
print("\n[1/4] Connecting to Sheets...")
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

# Get existing data to find row positions
print("\n[2/4] Loading existing data...")
existing_data = sheet_data.get_all_values()
existing_names = [row[0] if row else "" for row in existing_data]
print(f"OK - {len(existing_data)} existing rows")

# Chrome
print("\n[3/4] Starting browser...")
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

# Batch updates
updates = []
BATCH_SIZE_UPDATE = 50

def flush_updates():
    if not updates:
        return True
    
    for retry in range(3):
        try:
            sheet_data.batch_update(updates)
            updates.clear()
            return True
        except Exception as e:
            if retry < 2:
                wait = (retry + 1) * 5
                print(f" [Retry {retry+1}, wait {wait}s]", end="")
                time.sleep(wait)
            else:
                print(f" [FAILED: {str(e)[:30]}]", end="")
                return False
    return False

# Main loop
print("\n[4/4] Scraping and updating...")
batch = int(os.environ.get('BATCH_SIZE', '200'))
start = int(os.environ.get('START_INDEX', '1'))
end = min(len(companies), start + batch)

success = 0
failed = 0
new_entries = 0

for i in range(start, end):
    name = names[i] if i < len(names) else "Unknown"
    url = companies[i]
    
    print(f"[{i}] {name[:15]:15}", end=" ")
    
    vals = scrape(url)
    
    if vals:
        # Find existing row for this company
        try:
            row_num = existing_names.index(name) + 1  # Sheet rows are 1-indexed
            # Update existing row
            range_name = f'Sheet5!A{row_num}:Z{row_num}'
            updates.append({
                'range': range_name,
                'values': [[name, today] + vals]
            })
            print(f"✓ UPDATE ({len(vals)})", end="")
        except ValueError:
            # Company not found, append new
            updates.append({
                'range': f'Sheet5!A{len(existing_data) + new_entries + 1}',
                'values': [[name, today] + vals]
            })
            new_entries += 1
            print(f"✓ NEW ({len(vals)})", end="")
        
        success += 1
        
        if len(updates) >= BATCH_SIZE_UPDATE:
            print(" [PUSH]", end="")
            flush_updates()
            time.sleep(1)
    else:
        print("✗", end="")
        failed += 1
    
    print()
    time.sleep(0.5)

# Final flush
print("\nFlushing remaining...")
flush_updates()

driver.quit()

print(f"\n{'='*70}")
print(f"COMPLETE: {success} success, {failed} failed")
print(f"Updated: {success - new_entries}, New: {new_entries}")
print(f"Rate: {success/(success+failed)*100:.1f}%")
print("="*70)
