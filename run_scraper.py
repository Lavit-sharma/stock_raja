from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import gspread
from google.api_core.exceptions import ServiceUnavailable
from datetime import date
import json, os, time
from typing import List, Dict, Any

print("="*70)
print("TradingView Stock Scraper (Structured Data Extraction)")
print("="*70)

# -------- CONFIGURATION --------
HEADLESS_MODE = True
PAGE_LOAD_TIMEOUT = 30 
WAIT_VISIBLE_TIMEOUT = 12 
RATE_LIMIT = 1.0 
BATCH_SIZE_APPEND = 100 
MAX_RETRIES_SCRAPE = 3 
MAX_RETRIES_APPEND = 4

START_INDEX = int(os.getenv("START_INDEX", "1"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))

# The required order of columns for the output row (excluding Name and Date)
REQUIRED_COLS = [
    "Last Price", "Prev Close", "Open", "High", "Low", "Change Absolute", 
    "Change Percent", "Volume", "Market Cap", "P/E Ratio", "Dividend Yield"
]
EMPTY_VALUE = "None" # Placeholder for missing data

# -------- GOOGLE SHEETS SETUP (No Change) --------
print("\n[1/5] Connecting to Google Sheets...")
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
    print(f"✗ Sheets access error: {e}"); driver.quit(); exit(1) # Added driver.quit() on Sheets error

company_list = sheet_main.col_values(5)
name_list = sheet_main.col_values(1)
n = len(company_list)
current_date = date.today().strftime("%m/%d/%Y")
print(f"✓ Loaded {n} companies")

start = max(1, START_INDEX)
end = min(n, start + BATCH_SIZE - 1)
print(f"Processing slice: {start}..{end}")

# -------- CHROME BROWSER SETUP (No Change) --------
print("\n[2/5] Starting Chrome browser...")
chrome_options = Options()
if HEADLESS_MODE:
    chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option('useAutomationExtension', False)
chrome_options.add_argument('window-size=1600x1000')

service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)
driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
wait = WebDriverWait(driver, WAIT_VISIBLE_TIMEOUT)
print("✓ Browser initialized")

# -------- SESSION LOGIN VIA COOKIES (No Change) --------
print("\n[3/5] Loading TradingView session via cookies...")
cookies_json = os.environ.get("COOKIES_JSON", "")
if not cookies_json:
    print("✗ COOKIES_JSON missing"); driver.quit(); exit(1)

try:
    driver.get("https://www.tradingview.com")
    time.sleep(1.5)
    cookies = json.loads(cookies_json)
    for ck in cookies:
        c = dict(ck)
        c.pop('sameSite', None)
        c.pop('expiry', None)
        try:
            driver.add_cookie(c)
        except Exception:
            pass
    driver.refresh()
    time.sleep(3.0) 
    if ("Sign in" in driver.page_source) or ("Log in" in driver.page_source) or ("Join free" in driver.page_source):
        print("⚠ Session looks expired; aborting."); driver.quit(); exit(1)
    print("✓ Session loaded")
except Exception as e:
    print(f"✗ Cookie load error: {e}"); driver.quit(); exit(1)

# -------- CORE SCRAPE FUNCTION (Structured Output) --------

def clean_text(text: str) -> str:
    """Cleans up the extracted text."""
    return text.replace('−', '-').replace('∅', EMPTY_VALUE).strip()

def scrape_tradingview_values(url: str, retry: int = 0) -> List[str]:
    """Scrapes structured data and returns a list of values in the defined order."""
    
    # Initialize a dictionary to hold data, with required keys set to default
    data_map: Dict[str, str] = {col: EMPTY_VALUE for col in REQUIRED_COLS}
    
    try:
        driver.get(url)
        
        # 1. WAIT for the main data container
        MAIN_CONTAINER_XPATH = "//div[contains(@class, 'container-t8E0bV4D') or contains(@class, 'keyStatsWrapper-')]"
        try:
            wait.until(EC.presence_of_element_located((By.XPATH, MAIN_CONTAINER_XPATH)))
        except TimeoutException:
            wait.until(EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'wrap-KjO5sO5v')]")))
        
        time.sleep(1.5) 
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # 2. Extract Price and Change (Dynamic Header Data)
        # We assume the first 3 dynamic values are Price, Change Abs, Change Percent
        dynamic_values = soup.find_all("div", class_=lambda x: x and x.startswith("valueValue-"))
        if len(dynamic_values) >= 3:
            data_map["Last Price"] = clean_text(dynamic_values[0].get_text())
            data_map["Change Absolute"] = clean_text(dynamic_values[1].get_text())
            data_map["Change Percent"] = clean_text(dynamic_values[2].get_text())

        # 3. Extract Open/High/Low/Prev Close (Static Table Data)
        # TradingView uses a pattern like 'labelNode-X' for the label and 'valueNode-Y' for the value
        data_nodes = soup.find_all("div", class_=lambda x: x and "container-zK4n8Sj2" in x)
        
        # Look for the 'Open', 'High', 'Low', 'Prev Close' data structure
        for node in data_nodes:
            label_divs = node.find_all("div", class_=lambda x: x and "labelNode-" in x)
            value_divs = node.find_all("div", class_=lambda x: x and "valueNode-" in x)

            for i in range(min(len(label_divs), len(value_divs))):
                label = label_divs[i].get_text(strip=True).replace('.', '')
                value = clean_text(value_divs[i].get_text())
                
                if "Open" in label:
                    data_map["Open"] = value
                elif "High" in label:
                    data_map["High"] = value
                elif "Low" in label:
                    data_map["Low"] = value
                elif "Prev Close" in label:
                    data_map["Prev Close"] = value
                elif "Volume" in label: # Sometimes Volume is here
                    data_map["Volume"] = value

        # 4. Extract Key Stats (Market Cap, P/E, Div Yield)
        key_stats_wrappers = soup.find_all("div", class_=lambda x: x and "keyStatsWrapper-" in x)
        for wrapper in key_stats_wrappers:
             # Look for specific stat data attributes or stable structure
             stat_labels = wrapper.find_all("div", class_=lambda x: x and "statLabel-" in x)
             stat_values = wrapper.find_all("div", class_=lambda x: x and "statValue-" in x)
             
             for i in range(min(len(stat_labels), len(stat_values))):
                 label = stat_labels[i].get_text(strip=True)
                 value = clean_text(stat_values[i].get_text())
                 
                 if "Volume" in label and data_map["Volume"] == EMPTY_VALUE: # Prioritize main volume, use this as fallback
                     data_map["Volume"] = value
                 elif "Market Cap" in label:
                     data_map["Market Cap"] = value
                 elif "P/E Ratio" in label:
                     data_map["P/E Ratio"] = value
                 elif "Dividend Yield" in label:
                     data_map["Dividend Yield"] = value
        
        # 5. Compile final ordered list
        final_list = [data_map[col] for col in REQUIRED_COLS]
        
        # Check if basic price data was retrieved
        if data_map["Last Price"] == EMPTY_VALUE:
             raise Exception("Failed to find Last Price (Core Data Missing)")
             
        return final_list
        
    except (TimeoutException, WebDriverException) as e:
        if retry < MAX_RETRIES_SCRAPE:
            print(f" (Retry {retry + 1})", end="")
            time.sleep(2.0 * (retry + 1))
            return scrape_tradingview_values(url, retry + 1)
        else:
            print(f"✗ Failed scrape {url} after retries: {e.__class__.__name__}")
            return []
    except Exception as e:
        print(f"✗ Failed scrape {url}: {e.__class__.__name__}")
        return []

# -------- BATCHED APPEND WITH BACKOFF (No Change) --------
buffer = []

def flush_buffer():
    if not buffer:
        return
    backoff = 2
    for attempt in range(1, MAX_RETRIES_APPEND + 1):
        try:
            sheet_data.spreadsheet.values_append(
                'Sheet5!A1',
                {'valueInputOption': 'USER_ENTERED', 'insertDataOption': 'INSERT_ROWS'},
                {'values': buffer}
            )
            print(f" [APPEND Successful: {len(buffer)} rows]")
            buffer.clear()
            return
        except Exception as e:
            if attempt == MAX_RETRIES_APPEND:
                print(f"✗ Append failed after {attempt} attempts: {e}")
                return
            print(f"Append retry {attempt} in {backoff}s due to: {e}")
            time.sleep(backoff)
            backoff = min(30, backoff * 2)

# -------- MAIN LOOP (No Change) --------
print("\n[4/5] Starting scraping...")
success, fail = 0, 0
for i in range(start, end + 1):
    if i >= len(company_list):
        break
    
    list_index = i
    
    if list_index >= len(name_list):
         name = "Unknown"
    else:
         name = name_list[list_index]
         
    url = company_list[list_index]
    print(f"[{i}] {name} -> scraping...", end=" ")

    vals = scrape_tradingview_values(url)
    if vals:
        # vals is guaranteed to be in REQUIRED_COLS order or empty
        buffer.append([name, current_date] + vals)
        success += 1
        print(f"✓ {len(vals)} values")
        if len(buffer) >= BATCH_SIZE_APPEND:
            print(" [FLUSHING BATCH...]")
            flush_buffer()
    else:
        print("✗ No values")
        fail += 1

    time.sleep(RATE_LIMIT)

print("\nFlushing remaining rows...")
flush_buffer()
driver.quit()

print("\n" + "="*70)
print(f"SCRAPING COMPLETED | Slice {start}-{end}")
print(f"Success: {success} | Failed: {fail}")
print("="*70)
