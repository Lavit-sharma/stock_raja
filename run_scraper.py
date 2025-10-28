from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import gspread
from google.api_core.exceptions import ServiceUnavailable
from datetime import date
import json
import os
import time

print("="*70)
print("Parallel Stock Scraper (Logged-In, 10x200, Batched Appends)")
print("="*70)

# -------------------------
# [1/3] Connect to Google Sheets
# -------------------------
print("\n[1/3] Connecting to Sheets...")

creds = json.loads(os.environ.get('GOOGLE_CREDENTIALS', '{}'))  # secret string
gc = gspread.service_account_from_dict(creds)

MAX_RETRIES = 5
RETRY_DELAY = 5  # seconds

for attempt in range(1, MAX_RETRIES + 1):
    try:
        sheet_main = gc.open('Stock List').worksheet('Sheet1')
        try:
            sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')
        except:
            sh = gc.open('Tradingview Data Reel Experimental May')
            sheet_data = sh.add_worksheet(title='Sheet5', rows=1000, cols=26)
        print(f"Connected to Google Sheets on attempt {attempt}")
        break
    except (gspread.exceptions.APIError, ServiceUnavailable) as e:
        print(f"Attempt {attempt} failed: {e}")
        if attempt == MAX_RETRIES:
            raise SystemExit("Failed to connect to Google Sheets after multiple attempts")
        time.sleep(RETRY_DELAY)

companies = sheet_main.col_values(5)  # URLs
names = sheet_main.col_values(1)      # Names
today = date.today().strftime("%m/%d/%Y")
print(f"OK - {len(companies)} companies")

# -------------------------
# [2/3] Start Chrome (headless)
# -------------------------
print("\n[2/3] Starting browser...")
opts = Options()
opts.add_argument("--headless=new")
opts.add_argument("--no-sandbox")
opts.add_argument("--disable-dev-shm-usage")
opts.add_argument("--disable-blink-features=AutomationControlled")
opts.add_argument("--window-size=1920,1080")
opts.add_experimental_option("excludeSwitches", ["enable-automation"])
opts.add_experimental_option('useAutomationExtension', False)
driver = webdriver.Chrome(options=opts)
try:
    driver.set_window_size(1920, 1080)
except Exception:
    pass
print("OK")

# -------------------------
# [Login] Mandatory login via cookies
# -------------------------
print("\n[Login] Applying session cookies...")
driver.get("https://www.tradingview.com")
time.sleep(2)
cookies_json = os.environ.get("COOKIES_JSON", "")
if not cookies_json:
    print("ERROR - COOKIES_JSON secret missing. Cannot proceed without login.")
    driver.quit()
    raise SystemExit(1)

applied = 0
try:
    cookies = json.loads(cookies_json)
    for ck in cookies:
        ck = dict(ck)
        ck.pop('sameSite', None)
        ck.pop('expirationDate', None)
        ck.pop('expiry', None)
        ck.pop('storeId', None)
        try:
            driver.add_cookie(ck)
            applied += 1
        except Exception:
            pass
    driver.refresh()
    time.sleep(3)
    print(f"OK - {applied} cookies applied, session refreshed")
except Exception as e:
    print(f"ERROR - Cookie injection failed: {str(e)[:160]}")
    driver.quit()
    raise SystemExit(1)

# -------------------------
# [3/3] Scrape and parse structured data
# -------------------------
COLUMNS = ["Name", "Date", "Open", "High", "Low", "Close", "Volume", "Change", "ChangePercent"]
buffer = []
BATCH_SIZE_APPEND = 50

def scrape(url, retry_count=0, max_retries=1):
    try:
        driver.get(url)
        WebDriverWait(driver, 20).until(
            EC.any_of(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "div[data-name]")),
                EC.visibility_of_element_located((By.CSS_SELECTOR, "div.valueValue-l31H9iuA"))
            )
        )
        time.sleep(1.0)

        soup = BeautifulSoup(driver.page_source, "html.parser")
        data_dict = {col: None for col in COLUMNS}

        # Try structured data first
        nodes = soup.select("div[data-name]")
        for n in nodes:
            key = n.get("data-name", "").strip()
            val = n.get_text(strip=True).replace('−', '-').replace('∅', 'None')
            if key.lower() == "open": data_dict["Open"] = val
            elif key.lower() == "high": data_dict["High"] = val
            elif key.lower() == "low": data_dict["Low"] = val
            elif key.lower() == "close": data_dict["Close"] = val
            elif key.lower() == "volume": data_dict["Volume"] = val
            elif key.lower() == "change": data_dict["Change"] = val
            elif key.lower() == "change%": data_dict["ChangePercent"] = val

        # fallback: first 6 numeric values
        numeric_vals = [v.get_text(strip=True) for v in soup.find_all("div", class_="valueValue-l31H9iuA") if v.get_text(strip=True)]
        if numeric_vals:
            for idx, col in enumerate(COLUMNS[2:]):  # Open->ChangePercent
                if idx < len(numeric_vals):
                    data_dict[col] = numeric_vals[idx]

        return [data_dict[col] for col in COLUMNS]
    except:
        return [None for _ in COLUMNS]

def flush_buffer():
    if not buffer:
        return True
    for retry in range(3):
        try:
            rng = 'Sheet5!A1'
            params = {'valueInputOption': 'USER_ENTERED', 'insertDataOption': 'INSERT_ROWS'}
            body = {'values': buffer}
            sheet_data.spreadsheet.values_append(rng, params, body)
            buffer.clear()
            return True
        except gspread.exceptions.APIError:
            time.sleep((retry + 1) * 5)
    return False

print("\n[Run] Scraping and appending (logged-in, batched)...")
batch = int(os.environ.get('BATCH_SIZE', '200'))
start = int(os.environ.get('START_INDEX', '1'))
end = min(len(companies), start + batch)

# Insert headers first if empty
try:
    if not sheet_data.get_all_values():
        sheet_data.append_row(COLUMNS)
except Exception:
    pass

success = 0
failed = 0
for i in range(start, end):
    name = names[i] if i < len(names) else "Unknown"
    url = companies[i]
    print(f"[{i}] {name[:20]:20}", end=" ")
    vals = scrape(url)
    if vals:
        vals[0] = name  # Name column
        vals[1] = today # Date column
        buffer.append(vals)
        print(f"✓", end="")
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
if success + failed > 0:
    print(f"Rate: {success/(success+failed)*100:.1f}%")
print("="*70)
