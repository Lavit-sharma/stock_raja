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
print("Parallel Stock Scraper (Append Mode, CI=Local parity)")
print("="*70)

[1/3] Connect to Google Sheets
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

[2/3] Start Chrome (match local viewport)
print("\n[2/3] Starting browser...")
opts = Options()
opts.add_argument("--headless=new")
opts.add_argument("--no-sandbox")
opts.add_argument("--disable-dev-shm-usage")
opts.add_argument("--disable-blink-features=AutomationControlled")
opts.add_argument("--window-size=1920,1080") # ensure same rendering as local​
opts.add_experimental_option("excludeSwitches", ["enable-automation"])
opts.add_experimental_option('useAutomationExtension', False)

driver = webdriver.Chrome(options=opts)
try:
driver.set_window_size(1920, 1080) # enforce for headless variants​
except Exception:
pass
print("OK")

Unified robust scrape (same logic as local)
def scrape(url, retry_count=0, max_retries=1):
try:
driver.get(url)
# Prefer stable attributes first; then class​
WebDriverWait(driver, 20).until(
EC.any_of(
EC.visibility_of_element_located((By.CSS_SELECTOR, "div[data-name]")),
EC.visibility_of_element_located((By.CSS_SELECTOR, "div.valueValue-l31H9iuA"))
)
)
time.sleep(1.0) # hydration

text
    def parse_once():
        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = []

        # A) Attribute-first (most stable)[3]
        nodes = soup.select("div[data-name]")
        if nodes:
            values = [n.get_text(strip=True) for n in nodes if n.get_text(strip=True)]

        # B) Hashed class fallback (TradingView value block)
        if not values:
            nodes = soup.find_all("div", class_="valueValue-l31H9iuA")
            if nodes:
                values = [n.get_text(strip=True) for n in nodes if n.get_text(strip=True)]

        # C) Generic value/rating classes (broader)
        if not values:
            sections = soup.find_all(
                ["span", "div"],
                class_=lambda x: x and ("value" in str(x).lower() or "rating" in str(x).lower())
            )
            values = [el.get_text(strip=True)
                      for el in sections
                      if el.get_text(strip=True) and len(el.get_text(strip=True)) < 50]

        # D) Widget containers fallback (last resort, trimmed)
        if not values:
            containers = soup.find_all("div", class_=lambda x: x and "widget" in str(x).lower())
            all_text = []
            for c in containers[:5]:
                text = c.get_text(strip=True, separator="|").split("|")
                all_text.extend([t.strip() for t in text if t.strip() and len(t.strip()) < 30])
            values = all_text[:20]

        # Clean + dedupe while preserving order
        cleaned = []
        seen = set()
        for v in values:
            v = v.replace('−', '-').replace('∅', 'None').strip()
            if v and v not in seen:
                seen.add(v)
                cleaned.append(v)
        return cleaned

    cleaned = parse_once()
    # If partial/empty, refresh once (mimic local robustness)
    if len(cleaned) < 10 and retry_count < max_retries:
        time.sleep(2.5)
        driver.refresh()
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-name], div.valueValue-l31H9iuA"))
        )
        time.sleep(0.8)
        return scrape(url, retry_count + 1, max_retries)
    return cleaned
except:
    return []
Batched append to keep under write quota (~60/min/user)​
buffer = []
BATCH_SIZE_APPEND = 50

def flush_buffer():
if not buffer:
return True
for retry in range(3):
try:
rng = 'Sheet5!A1' # values.append appends at end​
params = {
'valueInputOption': 'USER_ENTERED',
'insertDataOption': 'INSERT_ROWS'
}
body = {'values': buffer}
sheet_data.spreadsheet.values_append(rng, params, body)
buffer.clear()
return True
except gspread.exceptions.APIError:
wait = (retry + 1) * 5
print(f" [Append retry {retry+1}, wait {wait}s]", end="")
time.sleep(wait)
except Exception:
wait = (retry + 1) * 5
print(f" [Error retry {retry+1}, wait {wait}s]", end="")
time.sleep(wait)
print(" [FAILED APPEND]")
return False

[3/3] Scrape and append
print("\n[3/3] Scraping and appending...")
batch = int(os.environ.get('BATCH_SIZE', '200')) # use 200 when running 10 parallel jobs
start = int(os.environ.get('START_INDEX', '1'))
end = min(len(companies), start + batch)

success = 0
failed = 0

for i in range(start, end):
name = names[i] if i < len(names) else "Unknown"
url = companies[i]
print(f"[{i}] {name[:20]:20}", end=" ")

text
vals = scrape(url)

if vals:
    buffer.append([name, today] + vals)
    print(f"✓ ({len(vals)})", end="")
    success += 1
    if len(buffer) >= BATCH_SIZE_APPEND:
        print(" [PUSH]", end="")
        flush_buffer()
        time.sleep(1.0)
else:
    print("✗", end="")
    failed += 1

print()
time.sleep(0.5)
print("\nFlushing remaining...")
flush_buffer()
driver.quit()

print(f"\n{'='*70}")
print(f"COMPLETE: {success} success, {failed} failed")
print(f"Rate: {success/(success+failed)*100:.1f}%")
print("="*70)
