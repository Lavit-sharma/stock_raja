from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from bs4 import BeautifulSoup
import gspread
from datetime import date
import os
import time
import json
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- CONFIGURATION (Optimized) ---
# Reduced max workers to prevent CPU/RAM throttling on GitHub runner.
MAX_WORKERS = 5
AGRESSIVE_TIMEOUT = 10 # Seconds to wait for key elements.

# ---------------- SHARDING (env-driven) ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", "checkpoint_new_1.txt")
try:
    with open(checkpoint_file, "r") as f:
        last_i = int(f.read())
except (FileNotFoundError, ValueError):
    last_i = 0

# ---------------- SETUP ---------------- #

# Chrome Options
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--remote-debugging-port=9222")
# Optimization: Suppress image/media loading for speed (already present, kept)
chrome_options.add_experimental_option("prefs", {
    "profile.managed_default_content_settings.images": 2,
    "profile.managed_default_content_settings.stylesheet": 2
})

# ---------------- GOOGLE SHEETS AUTH ---------------- #

try:
    gc = gspread.service_account("credentials.json")
except Exception as e:
    print(f"Error loading credentials.json: {e}")
    exit(1)

sheet_main = gc.open('Stock List').worksheet('Sheet1')
sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')

company_list = sheet_main.col_values(5)
name_list = sheet_main.col_values(1)
current_date = date.today().strftime("%m/%d/%Y")

# ---------------- DRIVER INITIALIZATION & LOGIN ---------------- #

def initialize_driver():
    """Initializes a single WebDriver instance and performs login once."""
    try:
        # Optimization: Use ChromeDriverManager which is efficient
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        driver.set_window_size(1920, 1080)

        # LOGIN USING SAVED COOKIES
        if os.path.exists("cookies.json"):
            driver.get("https://www.tradingview.com/")
            with open("cookies.json", "r", encoding="utf-8") as f:
                cookies = json.load(f)
            for cookie in cookies:
                try:
                    # Simplified cookie addition for speed
                    cookie_to_add = {k: cookie[k] for k in ('name', 'value', 'domain', 'path', 'secure', 'httpOnly') if k in cookie}
                    driver.add_cookie(cookie_to_add)
                except Exception:
                    pass
            driver.refresh()
            time.sleep(1) # Reduced sleep after refresh
        # else: print("⚠️ cookies.json not found. Worker proceeding without login.") # Removed print for speed
            
        return driver
    except Exception as e:
        # print(f"Failed to initialize driver: {e}") # Removed print for speed
        return None

# ---------------- SCRAPER FUNCTION (Optimized) ---------------- #
def scrape_tradingview(driver, company_url, name, index):
    try:
        # Load the target URL
        driver.get(company_url)
        
        # Reduced timeout from 15s to 10s for concurrency efficiency (AGRESSIVE_TIMEOUT)
        WebDriverWait(driver, AGRESSIVE_TIMEOUT).until(
            EC.visibility_of_element_located((By.XPATH,
                # Targeting a specific, deep element for robustness and speed
                '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'))
        )

        # Optimization: Using 'lxml' is faster than 'html.parser' (already present, kept)
        soup = BeautifulSoup(driver.page_source, "lxml") 
        values = [
            el.get_text().replace('−', '-').replace('∅', 'None')
            for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        ]
        
        if values:
            # print(f"Scraped {index}: {name}") # Simplified print for speed
            return index, [name, current_date] + values
        else:
            # print(f"Skipping {index}: No data scraped.") # Simplified print for speed
            return index, None

    except (NoSuchElementException, TimeoutException):
        # print(f"Timeout for {index} ({name}).") # Simplified print for speed
        return index, None
    except Exception:
        # print(f"Error for {index} ({name}).") # Simplified print for speed
        return index, None

# ---------------- MAIN EXECUTION (Concurrent Loop) ---------------- #

def run_concurrent_scraper():
    
    # 1. Initialize the pool of reusable drivers
    drivers = [initialize_driver() for _ in range(MAX_WORKERS)]
    drivers = [d for d in drivers if d is not None]
    
    if not drivers:
        print("Failed to initialize any drivers. Exiting.")
        return

    all_rows_to_upload = []
    tasks_to_process = []
    
    # Identify tasks for this specific shard, starting from the last checkpoint
    start_index = last_i + 1
    
    # Only iterate up to 2500, but use the full list size if available
    end_index = min(len(company_list), 2501) 
    
    for i in range(start_index, end_index):
        # Shard filter (Your original logic)
        if i % SHARD_STEP == SHARD_INDEX:
            tasks_to_process.append(i)
            
    if not tasks_to_process:
        print(f"Shard {SHARD_INDEX} has no new tasks to process (start index {start_index}).")
        
    # 2. CONCURRENT EXECUTION
    with ThreadPoolExecutor(max_workers=len(drivers)) as executor:
        futures = {}
        
        for task_count, i in enumerate(tasks_to_process):
            company_url = company_list[i]
            name = name_list[i] if i < len(name_list) else f"Row {i}"
            
            # Select a driver from the pool in a round-robin fashion
            worker_driver = drivers[task_count % len(drivers)] 
            
            # Submit the task to the pool
            future = executor.submit(scrape_tradingview, worker_driver, company_url, name, i)
            futures[future] = i
        
        last_successful_index = last_i
        
        # Process results as they complete
        for future in as_completed(futures):
            i = futures[future]
            try:
                index, result_row = future.result(timeout=AGRESSIVE_TIMEOUT * 2) # Added a check for hanging threads
            except Exception:
                # print(f"Task for index {i} failed to return result.") # Removed print for speed
                index, result_row = i, None
                
            if result_row:
                all_rows_to_upload.append(result_row)
            
            # Update the checkpoint for the highest completed index
            if index > last_successful_index:
                last_successful_index = index
                with open(checkpoint_file, "w") as f:
                    f.write(str(last_successful_index))
                    # print(f"Checkpoint updated to {last_successful_index}") # Removed print for speed


    # 3. CLEANUP
    for driver in drivers:
        driver.quit()

    # 4. BATCH UPLOAD (Already optimized)
    if all_rows_to_upload:
        print(f"\n--- Starting Batch Upload of {len(all_rows_to_upload)} Rows ---")
        try:
            sheet_data.append_rows(all_rows_to_upload, value_input_option='USER_ENTERED')
            print("Batch upload to Google Sheets completed successfully!")
        except Exception as e:
            print(f"Error during gspread batch upload: {e}")
            
    print("\n--- Scraper run finished ---")

if __name__ == '__main__':
    run_concurrent_scraper()
