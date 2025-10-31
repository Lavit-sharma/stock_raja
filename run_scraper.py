# run_scraper.py
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
print("TradingView Stock Scraper (Logged-In, Targeted Selectors)")
print("="*70)

# -------- CONFIG --------
HEADLESS_MODE = True
PAGE_LOAD_TIMEOUT = 25
WAIT_VISIBLE_TIMEOUT = 10
FIELD_WAIT_TIMEOUT = 4           # per-field probing
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
        sheet_data = sh.add_worksheet(title='Sheet5', rows=2000, cols=20)
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
        print("✗ Session invalid; refresh cookies"); driver.quit(); exit(1)
    print("✓ Session loaded")
except Exception as e:
    print(f"✗ Cookie load error: {e}"); driver.quit(); exit(1)

# -------- NORMALIZATION --------
THIN_SPACE = u'\u2009'
NARROW_NBSP = u'\u202f'
NBSP = u'\u00a0'
def norm(s: str) -> str:
    return s.replace(THIN_SPACE,'').replace(NARROW_NBSP,'').replace(NBSP,'').strip()

km_re = re.compile(r'^\s*([+\-]?\d+(?:\.\d+)?)\s*([KkMm])\s*$')
num_re = re.compile(r'^\s*[+\-]?\d{1,3}(?:,\d{3})*(?:\.\d+)?\s*$')
pct_re = re.compile(r'^\s*[+\-]?\d+(?:\.\d+)?\s*%\s*$')
chg_re = re.compile(r'^\s*([+\-]?\d+(?:\.\d+)?)\s*\(\s*([+\-]?\d+(?:\.\d+)?)%\s*\)\s*$')

def to_float(s):
    t = norm(s).replace(',', '').replace('−','-')
    try:
        return float(t)
    except:
        return None

def clean_num(text):
    if not text:
        return None
    t = norm(text).replace('−','-')
    if t in ('', '-', '—'):
        return None
    m = km_re.match(t)
    if m:
        base = float(m.group(1))
        return base * (1_000_000 if m.group(2).lower()=='m' else 1_000)
    if pct_re.match(t):
        return t
    if num_re.match(t):
        return to_float(t)
    return None

def split_change_token(s):
    if not isinstance(s, str):
        return (None, None)
    m = chg_re.match(norm(s))
    if not m:
        return (None, None)
    return (to_float(m.group(1)), to_float(m.group(2)))

# -------- WAITS --------
def wait_core_loaded():
    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-name='symbol-header']")))
    except Exception:
        pass
    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "[data-name]")))
    except Exception:
        pass

def wait_any_numeric(timeout=FIELD_WAIT_TIMEOUT):
    w = WebDriverWait(driver, timeout)
    try:
        w.until(lambda d: any(ch.isdigit() for ch in d.page_source[-5000:]))
    except Exception:
        pass

# -------- FIELD EXTRACTORS (multi-path) --------
def extract_ohlc(soup):
    # Strategy 1: labeled rows near data-name containers
    o=h=l=c=None
    blocks = soup.select("[data-name]")[:220]
    for node in blocks:
        text = norm(node.get_text(" ", strip=True)).lower()
        if not text:
            continue
        # Try to find nearby numeric tokens in the same block
        nums = []
        for el in node.select("span,div"):
            val = clean_num(el.get_text(strip=True))
            if val is not None and isinstance(val, (int, float, float)):
                nums.append(val)
        if 'open' in text and o is None and nums:
            o = nums[0]
        if 'high' in text and h is None and nums:
            h = nums[0]
        if 'low' in text and l is None and nums:
            l = nums[0]
        if ('close' in text or 'prev close' in text) and c is None and nums:
            c = nums[0]
    # Strategy 2: generic numeric clusters order-mapped
    if None in (o,h,l,c):
        cluster=[]
        for el in soup.select("div.valueValue-l31H9iuA, span, div"):
            txt = el.get_text(strip=True)
            v = clean_num(txt)
            if isinstance(v, (int, float, float)):
                cluster.append(v)
            if len(cluster)>=40:
                break
        nums = cluster
        if o is None and len(nums)>=1: o = nums[0]
        if h is None and len(nums)>=2: h = nums[1]
        if l is None and len(nums)>=3: l = nums[2]
        if c is None and len(nums)>=4: c = nums[3]
    return o,h,l,c

def extract_change_and_volume(soup):
    chg = chg_pct = vol = None
    # Strategy 1: combined token
    for el in soup.select("span, div")[:200]:
        a,b = split_change_token(el.get_text(strip=True))
        if a is not None or b is not None:
            if chg is None: chg = a
            if chg_pct is None: chg_pct = b
            break
    # Strategy 2: percent token alone
    if chg_pct is None:
        for el in soup.select("span, div")[:200]:
            t = el.get_text(strip=True)
            if pct_re.match(norm(t)):
                chg_pct = to_float(norm(t).replace('%',''))
                break
    # Strategy 3: volume hints
    # Look for 'Vol' label in nearby text and take largest numeric in that block
    for node in soup.select("[data-name]")[:220]:
        text = norm(node.get_text(" ", strip=True)).lower()
        if 'vol' in text or 'volume' in text:
            nums=[]
            for el in node.select("span,div"):
                v = clean_num(el.get_text(strip=True))
                if isinstance(v, (int, float, float)):
                    nums.append(v)
            if nums:
                vol = max(nums)
                break
    # Fallback: pick a large number further down the page which likely is volume
    if vol is None:
        nums=[]
        for el in soup.select("span, div"):
            v = clean_num(el.get_text(strip=True))
            if isinstance(v, (int, float, float)):
                nums.append(v)
            if len(nums)>=80:
                break
        if nums:
            vol = max(nums)
    return chg, chg_pct, vol

# -------- SCRAPE ONE WITH PER-FIELD WAITS --------
def scrape_symbol(url, retry=0):
    try:
        driver.get(url)
        wait_core_loaded()
        wait_any_numeric()
        time.sleep(0.6)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        o,h,l,c = extract_ohlc(soup)
        chg, chg_pct, vol = extract_change_and_volume(soup)
        return o,h,l,c,chg,chg_pct,vol
    except Exception as e:
        if retry < MAX_RETRIES_SCRAPE:
            time.sleep(1.5*(retry+1))
            return scrape_symbol(url, retry+1)
        print(f"✗ Failed {url}: {e}")
        return (None,None,None,None,None,None,None)

# -------- APPEND BUFFER --------
buffer=[]

def flush_buffer():
    if not buffer:
        return
    to_send = list(buffer)
    count = len(to_send)
    backoff = 2
    for attempt in range(1, MAX_RETRIES_APPEND+1):
        try:
            print(f"Appending {count} rows...")
            sheet_data.spreadsheet.values_append(
                'Sheet5!A1',
                {'valueInputOption':'USER_ENTERED','insertDataOption':'INSERT_ROWS'},
                {'values': to_send}
            )
            buffer.clear()
            print("✓ Append OK")
            return
        except Exception as e:
            if attempt == MAX_RETRIES_APPEND:
                print(f"✗ Append failed after {attempt} attempts: {e}")
                return
            print(f"Retry append in {backoff}s due to: {e}")
            time.sleep(backoff)
            backoff = min(30, backoff*2)

# -------- MAIN --------
print("\n[4/6] Scraping slice...")
success = fail = 0
for i in range(start, end+1):
    name = names[i] if i < len(names) else "Unknown"
    url = urls[i]
    if not url:
        print(f"[{i}] {name} -> no URL"); 
        row=[name, today, None,None,None,None,None,None,None]
        buffer.append(row)
        continue

    print(f"[{i}] {name} -> scrape", end=" ")
    o,h,l,c,chg,chg_pct,vol = scrape_symbol(url)
    found = {
        'O': o is not None, 'H': h is not None, 'L': l is not None,
        'C': c is not None, 'chg': chg is not None, '%': chg_pct is not None, 'Vol': vol is not None
    }
    have_vals = sum(found.values())
    if have_vals >= 3:
        success += 1
        print("✓", found)
    else:
        fail += 1
        print("… partial", found)

    row = [name, today, o, h, l, c, chg, chg_pct, vol]
    buffer.append(row)
    if len(buffer) >= BATCH_SIZE_APPEND:
        flush_buffer()
    time.sleep(RATE_LIMIT)

print("\n[5/6] Flushing remaining rows...")
flush_buffer()

print("\n[6/6] Closing browser")
driver.quit()

print("\n" + "="*70)
print(f"DONE | Slice {start}-{end} | Success {success} | Partial {fail}")
print("="*70)
