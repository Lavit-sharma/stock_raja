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
# Import concurrent utilities for speed optimization
from concurrent.futures import ThreadPoolExecutor, as_completed 

# --- CONFIGURATION ---
# Maximum concurrent browser instances (threads). Adjust this based on your runner's resources.
MAX_WORKERS = 8 

# ---------------- SHARDING (env-driven) ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))

# Allow workflow to pass a unique checkpoint filename per shard.
checkpoint_file = os.getenv("CHECKPOINT_FILE", "checkpoint_new_1.txt")
try:
    with open(checkpoint_file, "r") as f:
        last_i = int(f.read())
except (FileNotFoundError, ValueError):
    last_i = 0 # Start from 0 if no checkpoint file exists or content is invalid

# ---------------- SETUP ---------------- #

# Chrome Options
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--remote-debugging-port=9222")
# Optimization: Suppress image/media loading for speed
chrome_options.add_experimental_option("prefs", {
    "profile.managed_default_content_settings.images": 2,
    "profile.managed_default_content_settings.stylesheet": 2
})

# ---------------- GOOGLE SHEETS AUTH ---------------- #

try:
    gc = gspread.service_account("credentials.json")
except Exception as e:
    print(f"Error loading credentials.json: {e}")
    print("Ensure 'credentials.json' exists or has been created by GitHub Actions.")
    exit(1)

sheet_main = gc.open('Stock List').worksheet('Sheet1')
sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')

company_list = sheet_main.col_values(5)
name_list = sheet_main.col_values(1)
current_date = date.today().strftime("%m/%d/%Y")

# ---------------- DRIVER INITIALIZATION & LOGIN (New Function) ---------------- #

def initialize_driver():
    """Initializes a single WebDriver instance and performs login once."""
    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        driver.set_window_size(1920, 1080)

        # LOGIN USING SAVED COOKIES
        if os.path.exists("cookies.json"):
            driver.get("https://www.tradingview.com/")
            with open("cookies.json", "r", encoding="utf-8") as f:
                cookies = json.load(f)
            for cookie in cookies:
                try:
                    cookie_to_add = {k: cookie[k] for k in ('name', 'value', 'domain', 'path') if k in cookie}
                    cookie_to_add['secure'] = cookie.get('secure', False)
                    cookie_to_add['httpOnly'] = cookie.get('httpOnly', False)
                    driver.add_cookie(cookie_to_add)
                except Exception:
                    pass
            driver.refresh()
            time.sleep(2)
        else:
            print("⚠️ cookies.json not found. Worker proceeding without login.")
        
        return driver
    except Exception as e:
        print(f"Failed to initialize driver: {e}")
        return None

# ---------------- SCRAPER FUNCTION (Modified to accept Driver) ---------------- #
# This function is now run by a thread and reuses a driver
def scrape_tradingview(driver, company_url, name, index):
    try:
        # AFTER LOGIN, OPEN THE TARGET URL
        driver.get(company_url)
        # Reduced timeout from 45s to 15s for concurrency efficiency
        WebDriverWait(driver, 15).until(
            EC.visibility_of_element_located((By.XPATH,
                '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'))
        )

        # Optimization: Using 'lxml' is faster than 'html.parser'
        soup = BeautifulSoup(driver.page_source, "lxml") 
        values = [
            el.get_text().replace('−', '-').replace('∅', 'None')
            for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        ]
        
        if values:
            print(f"Successfully scraped {index}: {name}.")
            return index, [name, current_date] + values
        else:
            print(f"Skipping {index} ({name}): No data scraped.")
            return index, None

    except (NoSuchElementException, TimeoutException):
        print(f"Data element not found or timed out for {index} ({name}). Skipping.")
        return index, None
    except Exception as e:
        print(f"An error occurred during scraping for {index} ({name}): {e}")
        return index, None

# ---------------- MAIN EXECUTION (Concurrent Loop) ---------------- #

def run_concurrent_scraper():
    
    # 1. Initialize the pool of reusable drivers
    drivers = [initialize_driver() for _ in range(MAX_WORKERS)]
    drivers = [d for d in drivers if d is not None] # Filter out failed initializations
    
    if not drivers:
        print("Failed to initialize any drivers. Exiting.")
        return

    all_rows_to_upload = []
    tasks_to_process = []
    
    # Identify tasks for this specific shard, starting from the last checkpoint
    start_index = last_i + 1
    
    for i in range(start_index, len(company_list)):
        if i > 2500:
            print("Reached scraping limit (i > 2500). Stopping stock list iteration.")
            break
            
        # Shard filter (Your original logic)
        if i % SHARD_STEP == SHARD_INDEX:
            tasks_to_process.append(i)
            
    if not tasks_to_process:
        print(f"Shard {SHARD_INDEX} has no new tasks to process (starting from index {start_index}).")
        
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
            index, result_row = future.result()
            
            if result_row:
                all_rows_to_upload.append(result_row)
            
            # Update the checkpoint for the highest completed index
            if index > last_successful_index:
                last_successful_index = index
                with open(checkpoint_file, "w") as f:
                    f.write(str(last_successful_index))
                    print(f"Checkpoint updated to {last_successful_index}")


    # 3. CLEANUP
    for driver in drivers:
        driver.quit()

    # 4. BATCH UPLOAD (Optimization: Single API call)
    if all_rows_to_upload:
        print(f"\n--- Starting Batch Upload of {len(all_rows_to_upload)} Rows ---")
        try:
            # gspread batch append_rows (much faster than repeated append_row)
            sheet_data.append_rows(all_rows_to_upload, value_input_option='USER_ENTERED')
            print("Batch upload to Google Sheets completed successfully!")
        except Exception as e:
            print(f"Error during gspread batch upload: {e}")
            
    print("\n--- Scraper run finished ---")

if __name__ == '__main__':
    run_concurrent_scraper()
