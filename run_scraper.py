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

print("=" * 70)
print("Parallel Stock Scraper (ALWAYS LOGGED-IN via Cookies, Append Mode)")
print("=" * 70)

# [1/4] Connect to Google Sheets
print("\n[1/4] Connecting to Sheets...")
creds = json.loads(os.environ.get('GOOGLE_CREDENTIALS', '{}'))  # Secret string from Actions
gc = gspread.service_account_from_dict(creds)  # Auth via service account
sheet_main = gc.open('Stock List').worksheet('Sheet1')  # Read inputs
try:
    sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')  # Target sheet
except:
    sh = gc.open('Tradingview Data Reel Experimental May')  # File handle
    sheet_data = sh.add_worksheet(title='Sheet5', rows=1000, cols=26)  # Create if absent

companies = sheet_main.col_values(5)  # URLs list
names = sheet_main.col_values(1)  # Names list
today = date.today().strftime("%m/%d/%Y")  # Date stamp
print(f"OK - {len(companies)} companies")  # Diagnostics

# [2/4] Start Chrome with local-like viewport
print("\n[2/4] Starting browser...")
opts = Options()
opts.add_argument("--headless=new")  # Headless for CI
opts.add_argument("--no-sandbox")  # Required on runners
opts.add_argument("--disable-dev-shm-usage")  # Shared mem fix
opts.add_argument("--disable-blink-features=AutomationControlled")  # Reduce detection
opts.add_argument("--window-size=1920,1080")  # Match local rendering
opts.add_experimental_option("excludeSwitches", ["enable-automation"])  # Cleaner
opts.add_experimental_option('useAutomationExtension', False)  # Cleaner
driver = webdriver.Chrome(options=opts)  # Launch Chrome
try:
    driver.set_window_size(1920, 1080)  # Enforce viewport
except Exception:
    pass  # Some versions already honor window-size
print("OK")  # Status

# [3/4] Apply TradingView session cookies (MANDATORY LOGIN)
print("\n[3/4] Applying session cookies...")
driver.get("https://www.tradingview.com")  # Base domain for cookies
time.sleep(2)  # Initial settle
cookies_json = os.environ.get("COOKIES_JSON", "")  # Secret with cookie array
if not cookies_json:
    print("ERROR - COOKIES_JSON secret missing. Cannot proceed without login.")
    driver.quit()
    raise SystemExit(1)

applied = 0  # Count successful cookie injections
try:
    cookies = json.loads(cookies_json)  # List of dicts
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

# Heuristic guard for obvious logout (not strict)
page = driver.page_source
if ("Sign in" in page or "Log in" in page) and applied < 3:
    print("ERROR - Still appears logged out. Update COOKIES_JSON and retry.")
    driver.quit()
    raise SystemExit(1)

# [4/4] Robust scrape with attribute-first fallbacks
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

        def parse_once():
            soup = BeautifulSoup(driver.page_source, "html.parser")
            values = []

            # A) Attribute-first (most stable across layouts)
            nodes = soup.select("div[data-name]")
            if nodes:
                values = [n.get_text(strip=True) for n in nodes if n.get_text(strip=True)]

            # B) Hashed class fallback (TradingView value blocks)
            if not values:
                nodes = soup.find_all("div", class_="valueValue-l31H9iuA")
                if nodes:
                    values = [n.get_text(strip=True) for n in nodes if n.get_text(strip=True)]

            # C) Generic 'value'/'rating' classes (broad fallback)
            if not values:
                sections = soup.find_all(
                    ["span", "div"],
                    class_=lambda x: x and ("value" in str(x).lower() or "rating" in str(x).lower())
                )
                values = [
                    el.get_text(strip=True)
                    for el in sections
                    if el.get_text(strip=True) and len(el.get_text(strip=True)) < 50
                ]

            # D) Widget containers (last resort, trimmed)
            if not values:
                containers = soup.find_all("div", class_=lambda x: x and "widget" in str(x).lower())
                all_text = []
                for c in containers[:5]:
                    text = c.get_text(strip=True, separator="|").split("|")
                    all_text.extend([t.strip() for t in text if t.strip() and len(t.strip()) < 30])
                values = all_text[:20]

            # Clean + de-duplicate while preserving order
            cleaned = []
            seen = set()
            for v in values:
                v = v.replace('−', '-').replace('∅', 'None').strip()
                if v and v not in seen:
                    seen.add(v)
                    cleaned.append(v)
            return cleaned

        cleaned = parse_once()
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

# Batched append to respect Google Sheets limits
buffer = []
BATCH_SIZE_APPEND = 50

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
            wait = (retry + 1) * 5
            print(f" [Append retry {retry+1}, wait {wait}s]", end="")
            time.sleep(wait)
        except Exception:
            wait = (retry + 1) * 5
            print(f" [Error retry {retry+1}, wait {wait}s]", end="")
            time.sleep(wait)
    print(" [FAILED APPEND]")
    return False

print("\n[Run] Scraping and appending (logged-in)...")
batch = int(os.environ.get('BATCH_SIZE', '200'))
start = int(os.environ.get('START_INDEX', '1'))
end = min(len(companies), start + batch)

success = 0
failed = 0
for i in range(start, end):
    name = names[i] if i < len(names) else "Unknown"
    url = companies[i]
    print(f"[{i}] {name[:20]:20}", end=" ")

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

print(f"\n{'=' * 70}")
print(f"COMPLETE: {success} success, {failed} failed")
if success + failed > 0:
    print(f"Rate: {success / (success + failed) * 100:.1f}%")
print("=" * 70)
