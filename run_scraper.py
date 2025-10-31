from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import gspread
from datetime import date
import json, os, time, re

print("="*70)
print("TradingView Stock Scraper (Logged-In, Robust Extract)")
print("="*70)

# -------- CONFIG --------
HEADLESS_MODE = True
PAGE_LOAD_TIMEOUT = 25
WAIT_VISIBLE_TIMEOUT = 10
RATE_LIMIT = 0.8
BATCH_SIZE_APPEND = 100
MAX_RETRIES_SCRAPE = 2
MAX_RETRIES_APPEND = 4

START_INDEX = int(os.getenv("START_INDEX", "1"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))

# -------- SHEETS --------
print("\n[1/6] Connecting to Google Sheets...")
creds_json = os.environ.get('GOOGLE_CREDENTIALS', '')
if not creds_json:
    print("✗ GOOGLE_CREDENTIALS missing"); exit(1)

gc = gspread.service_account_from_dict(json.loads(creds_json))
try:
    sheet_main = gc.open('Stock List').worksheet('Sheet1')
    try:
        sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')
    except gspread.WorksheetNotFound:
        sh = gc.open('Tradingview Data Reel Experimental May')
        sheet_data = sh.add_worksheet(title='Sheet5', rows=1000, cols=26)
except Exception as e:
    print(f"✗ Sheets access error: {e}"); exit(1)

urls = sheet_main.col_values(5)
names = sheet_main.col_values(1)
n = len(urls)
today = date.today().strftime("%m/%d/%Y")
print(f"✓ Loaded {n} companies")

start = max(1, START_INDEX)
end = min(n, start + BATCH_SIZE - 1)
print(f"Processing slice: {start}..{end}")

# -------- BROWSER --------
print("\n[2/6] Starting Chrome...")
chrome_options = Options()
if HEADLESS_MODE:
    chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option('useAutomationExtension', False)

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)
driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
driver.set_window_size(1600, 1000)
wait = WebDriverWait(driver, WAIT_VISIBLE_TIMEOUT)
print("✓ Browser initialized")

# -------- LOGIN VIA COOKIES --------
print("\n[3/6] Loading TradingView session...")
cookies_json = os.environ.get("COOKIES_JSON", "")
if not cookies_json:
    print("✗ COOKIES_JSON missing"); driver.quit(); exit(1)

try:
    driver.get("https://www.tradingview.com")
    time.sleep(1.5)
    for ck in json.loads(cookies_json):
        c = dict(ck)
        c.pop('sameSite', None)
        c.pop('expiry', None)
        try:
            driver.add_cookie(c)
        except Exception:
            pass
    driver.refresh()
    time.sleep(2.0)
    if ("Sign in" in driver.page_source) or ("Log in" in driver.page_source):
        print("⚠ Session looks expired; abort."); driver.quit(); exit(1)
    print("✓ Session loaded")
except Exception as e:
    print(f"✗ Cookie load error: {e}"); driver.quit(); exit(1)

# -------- NORMALIZATION HELPERS --------
THIN_SPACE = u'\u2009'
NARROW_NBSP = u'\u202f'
NBSP = u'\u00a0'
def to_ascii(s: str) -> str:
    return s.replace(THIN_SPACE,'').replace(NARROW_NBSP,'').replace(NBSP,'').strip()

km_re = re.compile(r'^\s*([+\-]?\d+(?:\.\d+)?)\s*([KkMm])\s*$')
num_re = re.compile(r'^\s*[+\-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?\s*$')
pct_re = re.compile(r'^\s*[+\-]?\d+(?:\.\d+)?\s*%\s*$')
chg_re = re.compile(r'^\s*([+\-]?\d+(?:\.\d+)?)\s*\(\s*([+\-]?\d+(?:\.\d+)?)%\s*\)\s*$')

def to_float(s):
    t = to_ascii(s).replace(',', '').replace('−','-')
    try:
        return float(t)
    except:
        return None

def clean_cell(text):
    if not text: 
        return None
    t = to_ascii(text).replace('−','-')
    if t in ('', '-', '—'):
        return None
    m = km_re.match(t)
    if m:
        base = float(m.group(1))
        return base * (1_000_000 if m.group(2).lower()=='m' else 1_000)
    if pct_re.match(t):
        return t  # Sheets will parse USER_ENTERED percentages
    if num_re.match(t):
        return to_float(t)
    return t

def split_change_token(s):
    if not isinstance(s, str): 
        return (None, None)
    m = chg_re.match(to_ascii(s))
    if not m:
        return (None, None)
    return (to_float(m.group(1)), to_float(m.group(2)))

# -------- WAIT HELPERS --------
def wait_core_loaded():
    # Prefer stable containers on symbol pages
    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-name='symbol-header']")))
    except Exception:
        pass
    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "[data-name]")))
    except Exception:
        pass

# -------- FIELD EXTRACTORS --------
def extract_fields(soup):
    """
    Try multiple selector sets to assemble:
    Open, High, Low, Close, Change, Change%, Volume
    If missing, keep None to ensure fixed schema.
    """
    open_v = high_v = low_v = close_v = chg = chg_pct = vol = None

    # Strategy A: scan labeled stat rows common in headers/overviews
    # Look for labels near numeric siblings
    label_map = {'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close', 'volume': 'volume'}
    texts = []
    # consider first ~200 data-name nodes to limit work
    for node in soup.select("[data-name]")[:200]:
        t = node.get_text(" ", strip=True)
        if t:
            texts.append(t)

    # Flatten text tokens, pick plausible numbers and labeled pairs
    # Quick heuristics
    candidates = []
    for t in texts:
        parts = [p for p in re.split(r'\s+', to_ascii(t)) if p]
        for p in parts:
            c = clean_cell(p)
            if c is None:
                continue
            candidates.append((p.lower(), c, t))

    # Assign numbers based on label hints from the same block text
    for raw, cval, full in candidates:
        lower_full = full.lower()
        if open_v is None and 'open' in lower_full and isinstance(cval, (int, float, float)):
            open_v = cval
        if high_v is None and 'high' in lower_full and isinstance(cval, (int, float, float)):
            high_v = cval
        if low_v is None and 'low' in lower_full and isinstance(cval, (int, float, float)):
            low_v = cval
        if close_v is None and ('close' in lower_full or 'prev close' in lower_full) and isinstance(cval, (int, float, float)):
            close_v = cval
        if vol is None and 'vol' in lower_full and isinstance(cval, (int, float, float)):
            vol = cval

    # Strategy B: fallback to known numeric clusters
    if open_v is None or high_v is None or low_v is None or close_v is None or vol is None:
        cluster = []
        for el in soup.select("div.valueValue-l31H9iuA, span, div"):
            txt = el.get_text(strip=True)
            c = clean_cell(txt)
            if c is None:
                continue
            if isinstance(c, (int, float)) or (isinstance(c, str) and pct_re.match(str(c))):
                cluster.append(c)
            if len(cluster) >= 40:
                break
        # Try map first plausible 4 numbers to OHLC if missing
        nums = [x for x in cluster if isinstance(x, (int, float))]
        if open_v is None and len(nums) >= 1:
            open_v = nums[0]
        if high_v is None and len(nums) >= 2:
            high_v = nums[1]
        if low_v is None and len(nums) >= 3:
            low_v = nums[2]
        if close_v is None and len(nums) >= 4:
            close_v = nums[3]
        # Volume as largest number beyond OHLC range
        if vol is None and len(nums) >= 5:
            vol = max(nums[4:])

        # Change% as first percentage token
        pcts = [x for x in cluster if isinstance(x, str) and pct_re.match(x)]
        if pcts and chg_pct is None:
            chg_pct = to_float(pcts[0].replace('%',''))

    # Strategy C: parse combined change token if visible anywhere
    # Search limited selection to avoid noise
    for el in soup.select("span, div")[:200]:
        t = el.get_text(strip=True)
        if not t:
            continue
        a, b = split_change_token(t)
        if a is not None or b is not None:
            if chg is None:
                chg = a
            if chg_pct is None:
                chg_pct = b
            break

    return open_v, high_v, low_v, close_v, chg, chg_pct, vol

# -------- SCRAPE ONE --------
def scrape_symbol(url, retry=0):
    try:
        driver.get(url)
        wait_core_loaded()
        time.sleep(0.8)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        o,h,l,c,chg,chg_pct,vol = extract_fields(soup)
        return o,h,l,c,chg,chg_pct,vol
    except Exception as e:
        if retry < MAX_RETRIES_SCRAPE:
            time.sleep(1.5 * (retry + 1))
            return scrape_symbol(url, retry+1)
        else:
            print(f"✗ Failed {url}: {e}")
            return (None, None, None, None, None, None, None)

# -------- APPEND BUFFER --------
buffer = []

def flush_buffer():
    if not buffer:
        return
    backoff = 2
    for attempt in range(1, MAX_RETRIES_APPEND + 1):
        try:
            sheet_data.spreadsheet.values_append(
                'Sheet5!A1',
                {'valueInputOption':'USER_ENTERED','insertDataOption':'INSERT_ROWS'},
                {'values': buffer}
            )
            buffer.clear()
            return
        except Exception as e:
            if attempt == MAX_RETRIES_APPEND:
                print(f"✗ Append failed after {attempt} attempts: {e}")
                return
            print(f"Append retry {attempt} in {backoff}s due to: {e}")
            time.sleep(backoff)
            backoff = min(30, backoff * 2)

# -------- MAIN --------
print("\n[4/6] Scraping slice...")
success = fail = 0
for i in range(start, end + 1):
    name = names[i] if i < len(names) else "Unknown"
    url = urls[i]
    print(f"[{i}] {name} -> scrape", end=" ")

    o,h,l,c,chg,chg_pct,vol = scrape_symbol(url)

    # Always push a fixed 9-column row: Name, Date, O,H,L,C, Change, Change%, Volume
    row = [name, today, o, h, l, c, chg, chg_pct, vol]
    # Count success if we have at least Close or Volume or 2+ numeric fields
    have_vals = sum(1 for v in [o,h,l,c,chg,chg_pct,vol] if v is not None)
    if have_vals >= 2:
        success += 1
        print("✓")
    else:
        fail += 1
        print("… partial")

    buffer.append(row)
    if len(buffer) >= BATCH_SIZE_APPEND:
        print(" [APPEND]")
        flush_buffer()

    time.sleep(RATE_LIMIT)

print("\n[5/6] Flushing remaining rows...")
flush_buffer()

print("\n[6/6] Closing browser")
driver.quit()

print("\n" + "="*70)
print(f"DONE | Slice {start}-{end} | Success {success} | Fail {fail}")
print("="*70)
