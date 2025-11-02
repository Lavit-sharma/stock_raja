from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup
import gspread
from datetime import date
import os
import time
import json
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- SETUP ---------------- #

# Chrome Options (Remains the same for headless operation)
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--remote-debugging-port=9222")

# ---------------- GOOGLE SHEETS AUTH ---------------- #

# Loads credentials from 'credentials.json' (created by GitHub Action)
try:
    gc = gspread.service_account("credentials.json")
except Exception as e:
    print(f"Error loading credentials.json: {e}")
    exit(1)

# Sheet objects for data writing (only needed for the final batch write)
sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')

current_date = date.today().strftime("%m/%d/%Y")
checkpoint_file = "checkpoint_new_1.txt"

# 1. Load data from local JSON files (created by the 'prepare_data' job)
try:
    with open("company_list.json", "r") as f:
        company_list = json.load(f)
    with open("name_list.json", "r") as f:
        name_list = json.load(f)
    print(f"Successfully loaded {len(company_list)} items from local files.")
except FileNotFoundError:
    print("ERROR: JSON data files not found. Check if 'prepare_data.py' ran successfully.")
    exit(1)


# 2. Read START and END indices from Environment Variables (set by the YAML job)
# Note: The YAML sets CHUNK_START_INDEX (e.g., 1, 251, 501...)
# And CHUNK_END_INDEX (e.g., 250, 500, 750...)
START_INDEX = int(os.environ.get('CHUNK_START_INDEX', 1))
END_INDEX = int(os.environ.get('CHUNK_END_INDEX', len(company_list) - 1)) # Default to full list end

# Ensure indices are within bounds
MAX_INDEX = len(company_list) - 1
if END_INDEX > MAX_INDEX:
    END_INDEX = MAX_INDEX

print(f"This job will process indices from {START_INDEX} to {END_INDEX} (inclusive).")


# ---------------- SCRAPER FUNCTION (No Change) ---------------- #
def scrape_tradingview(company_url):
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.set_window_size(1920, 1080)
    # ... (Login and Scraping logic remains the same) ...
    try:
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
            print("⚠️ cookies.json not found.")
            pass

        driver.get(company_url)
        WebDriverWait(driver, 45).until(
            EC.visibility_of_element_located((By.XPATH,
                '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'))
        )

        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [
            el.get_text().replace('−', '-').replace('∅', 'None')
            for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        ]
        return values

    except NoSuchElementException:
        print(f"Data element not found for URL: {company_url}")
        return []
    except Exception as e:
        print(f"An error occurred during scraping for {company_url}: {e}")
        return []
    finally:
        driver.quit()


# ---------------- MAIN LOOP (NEW LOGIC) ---------------- #
# Initialize a list to hold all the rows we scrape
data_to_append = [] 

# We will loop using the index range determined by the YAML job
# Note: Python lists are 0-indexed, but Google Sheets/your data logic uses 1-indexed.
# We use the START_INDEX and END_INDEX (which are 1-based) directly as the loop counter 'i'.

for i in range(START_INDEX, END_INDEX + 1):
    
    # 3. Use 0-based indexing for accessing the lists (i - 1)
    url_index = i - 1
    name = name_list[url_index]
    company_url = company_list[url_index]
    
    print(f"Scraping {i}: {name} | {company_url}")

    values = scrape_tradingview(company_url)
    
    if values:
        row = [name, current_date] + values
        data_to_append.append(row) 
        print(f"Successfully scraped data for {name}.")
    else:
        print(f"Skipping {name}: No data scraped.")

    # 4. Checkpoint Update: Saves the last index processed (i).
    # This is less critical now, but good for tracking the job's progress.
    with open(checkpoint_file, "w") as f:
        f.write(str(i))

    time.sleep(1) # Keep the sleep to be gentle on TradingView

# ---------------- BATCH WRITE ---------------- #

if data_to_append:
    print(f"\n✅ Writing {len(data_to_append)} rows to Google Sheet in one batch...")
    # Use append_rows (plural) for the massive bulk upload!
    sheet_data.append_rows(data_to_append, value_input_option='USER_ENTERED')
    print("✅ Batch write completed successfully.")
else:
    print(f"\n⚠️ No new data scraped in chunk {START_INDEX} to {END_INDEX} to write to the sheet.")
