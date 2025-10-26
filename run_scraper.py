# run_local.py
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
import gspread
from datetime import date, datetime
import json
import pickle
import time
import os

print("="*70)
print(" TradingView Stock Data Scraper v1.0")
print(" Developed for Stock Analysis Team")
print("="*70)

# -------- Configuration --------
HEADLESS_MODE = False  # Set True for background operation
MAX_RETRIES = 2  # Retry failed scrapes
WAIT_TIME = 10  # Seconds to wait for page load
RATE_LIMIT = 3  # Seconds between requests

# -------- Google Sheets Setup --------
print("\n[1/5] Connecting to Google Sheets...")
SERVICE_ACCOUNT_FILE = "creds.json"
try:
    with open(SERVICE_ACCOUNT_FILE, "r", encoding="utf-8") as f:
        credentials = json.load(f)
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
    
    print(f"✓ Connected successfully")
    print(f"✓ Loaded {len(company_list)} companies from 'Stock List'")
except Exception as e:
    print(f"✗ Error connecting to Google Sheets: {str(e)}")
    exit(1)

# -------- Chrome Browser Setup --------
print("\n[2/5] Initializing Chrome browser...")
chrome_options = Options()
if HEADLESS_MODE:
    chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option('useAutomationExtension', False)

driver = webdriver.Chrome(options=chrome_options)
driver.set_window_size(1920, 1080)
print("✓ Browser initialized")

# -------- Session Login --------
print("\n[3/5] Loading TradingView session...")
try:
    if os.path.exists("tradingview_cookies.pkl"):
        driver.get("https://www.tradingview.com")
        time.sleep(2)
        
        with open("tradingview_cookies.pkl", "rb") as f:
            cookies = pickle.load(f)
            for cookie in cookies:
                cookie.pop('sameSite', None)
                cookie.pop('expiry', None)
                try:
                    driver.add_cookie(cookie)
                except Exception:
                    pass
        
        driver.refresh()
        time.sleep(5)
        
        # Verify login
        if "Sign in" in driver.page_source or "Log in" in driver.page_source:
            print("⚠ WARNING: Session may be expired. Please run save_cookies.py")
            driver.quit()
            exit(1)
        else:
            print("✓ Session loaded successfully")
    else:
        print("✗ Cookie file not found! Please run save_cookies.py first.")
        driver.quit()
        exit(1)
except Exception as e:
    print(f"✗ Error loading session: {str(e)}")
    driver.quit()
    exit(1)

# -------- Scraping Function --------
def scrape_tradingview(company_url, retry_count=0):
    """
    Scrapes TradingView chart data using multiple fallback methods
    Returns: List of extracted values or empty list on failure
    """
    try:
        driver.get(company_url)
        time.sleep(WAIT_TIME)
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = []
        
        # Method 1: Primary selector (valueValue class)
        nodes = soup.find_all("div", class_="valueValue-l31H9iuA")
        if nodes:
            values = [el.get_text().strip() for el in nodes if el.get_text(strip=True)]
        
        # Method 2: Broader data-name attributes
        if not values:
            nodes = soup.find_all("div", attrs={"data-name": True})
            if nodes:
                values = [el.get_text().strip() for el in nodes if el.get_text(strip=True)]
        
        # Method 3: Value/rating classes
        if not values:
            sections = soup.find_all(["span", "div"], 
                                    class_=lambda x: x and ("value" in str(x).lower() or "rating" in str(x).lower()))
            values = [el.get_text().strip() for el in sections 
                     if el.get_text(strip=True) and len(el.get_text().strip()) < 50]
        
        # Method 4: Widget containers fallback
        if not values:
            containers = soup.find_all("div", class_=lambda x: x and "widget" in str(x).lower())
            all_text = []
            for c in containers[:5]:
                text = c.get_text(strip=True, separator="|").split("|")
                all_text.extend([t.strip() for t in text if t.strip() and len(t.strip()) < 30])
            values = all_text[:20]
        
        # Clean and deduplicate
        cleaned_values = []
        for v in values:
            v = v.replace('−', '-').replace('∅', 'None').strip()
            if v and v not in cleaned_values:
                cleaned_values.append(v)
        
        return cleaned_values
        
    except Exception as e:
        if retry_count < MAX_RETRIES:
            print(f"      ⚠ Retry {retry_count + 1}/{MAX_RETRIES}...")
            time.sleep(5)
            return scrape_tradingview(company_url, retry_count + 1)
        else:
            print(f"      ✗ Error: {str(e)}")
            return []

# -------- Main Scraping Loop --------
print("\n[4/5] Starting data extraction...")
print("="*70)

# Configuration: Change these values as needed
start_index = 1
end_index = min(len(company_list), 1101)  # Process up to 1100 companies

success_count = 0
fail_count = 0
failed_companies = []

start_time = datetime.now()
print(f"\n📊 Processing {end_index - start_index} companies")
print(f"⏰ Estimated time: {(end_index - start_index) * (WAIT_TIME + RATE_LIMIT) / 60:.1f} minutes")
print(f"🕐 Started at: {start_time.strftime('%I:%M %p')}\n")

try:
    for i in range(start_index, end_index):
        name = name_list[i] if i < len(name_list) else "Unknown"
        url = company_list[i]
        
        # Progress header
        progress = f"[{i}/{end_index-1}]"
        print(f"\n{progress} {name}")
        print("─" * 70)
        
        vals = scrape_tradingview(url)
        
        if vals and len(vals) > 0:
            try:
                sheet_data.append_row([name, current_date] + vals, table_range='A1')
                print(f"   ✅ SUCCESS | {len(vals)} values | Sample: {vals[:3]}")
                success_count += 1
            except Exception as e:
                print(f"   ✗ SHEET ERROR: {str(e)}")
                failed_companies.append((i, name, "Sheet write failed"))
                fail_count += 1
        else:
            print(f"   ⚠ NO DATA")
            failed_companies.append((i, name, "No values extracted"))
            fail_count += 1
        
        # Rate limiting
        time.sleep(RATE_LIMIT)
        
        # Progress checkpoint every 50 companies
        if i % 50 == 0 and i > start_index:
            elapsed = (datetime.now() - start_time).seconds / 60
            remaining = ((end_index - i) * (WAIT_TIME + RATE_LIMIT)) / 60
            
            print(f"\n{'='*70}")
            print(f"📈 CHECKPOINT - {i} companies processed")
            print(f"   ✅ Success: {success_count} ({success_count/i*100:.1f}%)")
            print(f"   ❌ Failed: {fail_count}")
            print(f"   ⏱ Elapsed: {elapsed:.1f} min | Remaining: ~{remaining:.1f} min")
            print(f"{'='*70}\n")
            time.sleep(5)  # Extra break

except KeyboardInterrupt:
    print(f"\n\n⚠ Process interrupted by user at company {i}")
except Exception as e:
    print(f"\n\n✗ Critical error: {str(e)}")

# -------- Final Report --------
end_time = datetime.now()
duration = (end_time - start_time).seconds / 60

print("\n" + "="*70)
print(" [5/5] SCRAPING COMPLETED")
print("="*70)
print(f"\n📊 FINAL STATISTICS")
print(f"   Total processed: {success_count + fail_count}")
print(f"   ✅ Successful: {success_count}")
print(f"   ❌ Failed: {fail_count}")
print(f"   📈 Success rate: {success_count/(success_count+fail_count)*100:.1f}%")
print(f"   ⏱ Total time: {duration:.1f} minutes")
print(f"   🕐 Started: {start_time.strftime('%I:%M %p')}")
print(f"   🕐 Ended: {end_time.strftime('%I:%M %p')}")

if failed_companies:
    print(f"\n⚠ FAILED COMPANIES ({len(failed_companies)}):")
    for idx, name, reason in failed_companies[:20]:  # Show first 20
        print(f"   [{idx}] {name} - {reason}")
    if len(failed_companies) > 20:
        print(f"   ... and {len(failed_companies) - 20} more")

print(f"\n✓ Data saved to: 'Tradingview Data Reel Experimental May' → 'Sheet5'")
print(f"✓ Total rows added: {success_count}")
print("="*70 + "\n")

driver.quit()
print("🔒 Browser closed. Process complete.\n")
