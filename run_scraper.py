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

# --- CONFIGURATION ---
# Maximum number of concurrent browser instances (threads).
# Adjust this based on your system's resources (CPU/RAM). 8-16 is a good start.
MAX_WORKERS = 8

# ---------------- SHARDING (env-driven) ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", "checkpoint_new_1.txt")
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 0 # Start from 0 if no checkpoint

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
    # Use context manager to ensure file is closed
    gc = gspread.service_account("credentials.json")
except Exception as e:
    print(f"Error loading credentials.json: {e}")
    exit(1)

sheet_main = gc.open('Stock List').worksheet('Sheet1')
sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')

# Get all required data once
company_list = sheet_main.col_values(5)
name_list = sheet_main.col_values(1)
current_date = date.today().strftime("%m/%d/%Y")
TOTAL_STOCKS = len(company_list)

# ---------------- DRIVER INITIALIZATION & LOGIN ---------------- #

def initialize_driver():
    """Initializes a single WebDriver instance and logs in."""
    try:
        # NOTE: Using ChromeDriverManager().install() is convenient but can be slow. 
        # For production CI/CD, pre-installing the driver is recommended.
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        driver.set_window_size(1920, 1080)
        
        # LOGIN USING SAVED COOKIES
        if os.path.exists("cookies.json"):
            driver.get("https://www.tradingview.com/")
            with open("cookies.json", "r", encoding="utf-8") as f:
                cookies = json.load(f)
            for cookie in cookies:
                try:
                    # Simplified cookie adding logic
                    cookie_to_add = {k: cookie[k] for k in ('name', 'value', 'domain', 'path', 'secure', 'httpOnly') if k in cookie}
                    driver.add_cookie(cookie_to_add)
                except Exception:
                    pass
            driver.refresh()
            time.sleep(2) # Wait for login/refresh to complete
        else:
            print(f"⚠️ Worker {os.getpid()} cookies.json not found. Proceeding without login.")
        
        return driver
    except Exception as e:
        print(f"Failed to initialize driver: {e}")
        return None

# ---------------- SCRAPER FUNCTION (Reusable Driver) ---------------- #

def scrape_tradingview(driver, company_url, name, index):
    """Scrapes data for a single stock using an existing driver instance."""
    if not company_url.startswith("http"):
        # Simple check to skip non-URL entries (like header row or empty cells)
        return None 

    print(f"Scraping {index}: {name} | {company_url}")
    
    try:
        driver.get(company_url)
        # Use a reasonable wait time for pages to load
        WebDriverWait(driver, 15).until(
            EC.visibility_of_element_located((By.XPATH,
                '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'))
        )

        # Optimization: Use 'lxml' for faster parsing if installed, otherwise 'html.parser'
        soup = BeautifulSoup(driver.page_source, "lxml") 
        
        values = [
            el.get_text().replace('−', '-').replace('∅', 'None')
            for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        ]
        
        if values:
            return index, [name, current_date] + values
        else:
            print(f"Skipping {index} ({name}): No data scraped from page source.")
            return index, None

    except TimeoutException:
        print(f"Time out waiting for element on {index} ({name}). Skipping.")
        return index, None
    except NoSuchElementException:
        print(f"Data element not found for {index} ({name}). Skipping.")
        return index, None
    except Exception as e:
        print(f"An error occurred during scraping for {index} ({name}): {e}")
        return index, None


# ---------------- MAIN EXECUTION (Concurrent) ---------------- #

def run_concurrent_scraper():
    
    # 1. Initialize all drivers for the worker pool
    drivers = [initialize_driver() for _ in range(MAX_WORKERS)]
    drivers = [d for d in drivers if d is not None] # Filter out failed initializations
    
    if not drivers:
        print("Failed to initialize any drivers. Exiting.")
        return

    all_rows_to_upload = []
    
    # Determine which tasks belong to this shard, starting from the last checkpoint
    tasks_to_process = []
    
    # Start the loop from index 1 to skip potential header, or check if company_list[0] is the header
    start_index = 1 if last_i == 0 else last_i + 1
    
    for i in range(start_index, TOTAL_STOCKS):
        if i > 2500:
            print(f"Reached scraping limit (i > 2500). Stopping stock list iteration.")
            break
            
        if i % SHARD_STEP == SHARD_INDEX:
            tasks_to_process.append(i)

    print(f"Shard {SHARD_INDEX} will process {len(tasks_to_process)} stocks starting from index {tasks_to_process[0] if tasks_to_process else 'N/A'}.")

    # 2. CONCURRENT EXECUTION
    # Use ThreadPoolExecutor to manage concurrency
    with ThreadPoolExecutor(max_workers=len(drivers)) as executor:
        
        futures = {}
        
        # Submit tasks: assign a driver from the pool in a round-robin fashion
        for task_count, i in enumerate(tasks_to_process):
            company_url = company_list[i]
            name = name_list[i] if i < len(name_list) else f"Row {i}"
            
            # Select a driver using modulo (round-robin)
            worker_driver = drivers[task_count % len(drivers)] 

            # Submit the task to the pool
            future = executor.submit(scrape_tradingview, worker_driver, company_url, name, i)
            futures[future] = i # Store the future with its original index
        
        last_successful_index = last_i
        
        # Process results as they complete
        for future in as_completed(futures):
            i = futures[future]
            index, result_row = future.result()
            
            if result_row:
                all_rows_to_upload.append(result_row)
                print(f"✅ Success on {index}. Rows collected: {len(all_rows_to_upload)}.")
            
            # Update the checkpoint only if the current index is higher
            if index > last_successful_index:
                last_successful_index = index
                with open(checkpoint_file, "w") as f:
                    f.write(str(last_successful_index))
                    print(f"Checkpoint updated to {last_successful_index}")

    # 3. CLEANUP
    for driver in drivers:
        driver.quit()

    # 4. BATCH UPLOAD (The Google Sheets Optimization)
    if all_rows_to_upload:
        print(f"\n--- Starting Batch Upload of {len(all_rows_to_upload)} Rows ---")
        try:
            # gspread batch append_rows (much faster than repeated append_row)
            sheet_data.append_rows(all_rows_to_upload, value_input_option='USER_ENTERED')
            print("Batch upload to Google Sheets completed successfully!")
        except Exception as e:
            print(f"Error during gspread batch upload: {e}")
            
    print("\n--- Scraper run finished ---")
    
# Execute the concurrent logic
if __name__ == '__main__':
    run_concurrent_scraper()
