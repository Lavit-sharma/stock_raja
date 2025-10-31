from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import gspread, os, json, time
from datetime import date

print("="*70)
print("TradingView Scraper — Complete Field Version")
print("="*70)

# ─── CONFIG ───
HEADLESS = True
WAIT_TIMEOUT = 15
MAX_RETRY = 3
RATE_DELAY = 1.5
BATCH_SIZE_APPEND = 100

START = int(os.getenv("START_INDEX", "1"))
SIZE = int(os.getenv("BATCH_SIZE", "200"))

# ─── GOOGLE SHEETS ───
gc = gspread.service_account_from_dict(json.loads(os.getenv("GOOGLE_CREDENTIALS")))
sheet_main = gc.open("Stock List").worksheet("Sheet1")
try:
    sheet_data = gc.open("Tradingview Data Reel Experimental May").worksheet("Sheet5")
except gspread.WorksheetNotFound:
    sheet_data = gc.open("Tradingview Data Reel Experimental May").add_worksheet("Sheet5", rows=2000, cols=20)

urls = sheet_main.col_values(5)
names = sheet_main.col_values(1)
today = date.today().strftime("%m/%d/%Y")

start, end = START, min(len(urls), START + SIZE - 1)
print(f"✓ Loaded {len(urls)} stocks | Processing slice {start}-{end}")

# ─── CHROME ───
opts = Options()
if HEADLESS:
    opts.add_argument("--headless=new")
opts.add_argument("--no-sandbox")
opts.add_argument("--disable-dev-shm-usage")
opts.add_argument("--disable-blink-features=AutomationControlled")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
wait = WebDriverWait(driver, WAIT_TIMEOUT)

# ─── COOKIES LOGIN ───
print("Loading cookies...")
driver.get("https://www.tradingview.com")
cookies = json.loads(os.getenv("COOKIES_JSON", "[]"))
for ck in cookies:
    ck.pop("sameSite", None)
    ck.pop("expiry", None)
    try: driver.add_cookie(ck)
    except: pass
driver.refresh()
time.sleep(2)
if "Sign in" in driver.page_source:
    print("✗ Invalid session — please refresh cookies.")
    driver.quit(); exit(1)
print("✓ Session loaded successfully")

# ─── SCRAPER ───
def scrape(url):
    for attempt in range(MAX_RETRY):
        try:
            driver.get(url)
            # Wait for key info area
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='tv-symbol-price-quote__value']")))
            time.sleep(2)

            soup = BeautifulSoup(driver.page_source, "html.parser")
            text = soup.get_text(" ", strip=True)

            def extract(label):
                import re
                m = re.search(rf"{label}\s*([A-Za-z0-9.,\-+%]+)", text)
                return m.group(1) if m else ""

            data = [
                extract("Last"),          # Last Price
                extract("Prev Close"),
                extract("Open"),
                extract("High"),
                extract("Low"),
                extract("Change"),
                extract("%"),
                extract("Volume"),
                extract("Market Cap"),
                extract("P/E Ratio"),
                extract("Dividend Yield"),
            ]

            # Retry if too few filled
            if len([d for d in data if d]) < 5:
                time.sleep(2)
                continue
            return data
        except Exception:
            time.sleep(2)
    return []

# ─── WRITE LOOP ───
buffer = []
success, fail = 0, 0

for i in range(start, end + 1):
    name, url = names[i], urls[i]
    print(f"[{i}] {name} → ", end="")
    vals = scrape(url)
    if vals:
        buffer.append([name, today] + vals)
        success += 1
        print("✓", vals)
        if len(buffer) >= BATCH_SIZE_APPEND:
            sheet_data.append_rows(buffer, value_input_option="USER_ENTERED")
            buffer.clear()
    else:
        fail += 1
        print("✗ Missing data")
    time.sleep(RATE_DELAY)

if buffer:
    sheet_data.append_rows(buffer, value_input_option="USER_ENTERED")

driver.quit()
print(f"\n✅ Completed | Success: {success} | Failed: {fail}")
