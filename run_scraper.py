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
import os # <-- Needed to read environment variables
import time
import json
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- SETUP ---------------- #

# Chrome Options
chrome_options = Options()
chrome_options.add_argument("--headless=new") # <-- Ensure headless is active for CI/CD
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

sheet_main = gc.open('Stock List').worksheet('Sheet1')
sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')

company_list = sheet_main.col_values(5)
name_list = sheet_main.col_values(1)
current_date = date.today().strftime("%m/%d/%Y")

checkpoint_file = "checkpoint_new_1.txt"

# 1. Read the START index from the checkpoint file (set by the YAML job)
# This will be the start of the current job's chunk (e.g., 1, 251, 501, etc.)
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 1

# 2. Read the END index from the environment variable (set by the YAML job)
# This defines the limit for the current job (e.g., 250, 500, 750, etc.)
CHUNK_END_INDEX = int(os.environ.get('CHUNK_END_INDEX', 2500))


# ---------------- SCRAPER FUNCTION ---------------- #
def scrape_tradingview(company_url):
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.set_window_size(1920, 1080)
    try:
        # ✅ LOGIN USING SAVED COOKIES
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
            print("⚠️ cookies.json not found. The script might fail if login is required.")
            pass

        # ✅ AFTER LOGIN, OPEN THE TARGET URL
        driver.get(company_url)
        WebDriverWait(driver, 45).until(
            EC.visibility_of_element_located((By.XPATH,
                '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'))
        )

        soup = BeautifulSoup(driver.page_source, "html.parser")
        # --- CORE LOGIC REMAINS UNCHANGED ---
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

# ---------------- MAIN LOOP ---------------- #
# Initialize a list to hold all the rows we scrape
data_to_append = [] 

for i, company_url in enumerate(company_list[last_i:], last_i):
    
    # 3. Check if the current index is past the job's assigned chunk end
    if i > CHUNK_END_INDEX: 
        print(f"Reached chunk end limit ({CHUNK_END_INDEX}). Stopping job.")
        break

    name = name_list[i]
    print(f"Scraping {i}: {name} | {company_url}")

    values = scrape_tradingview(company_url)
    
    if values:
        row = [name, current_date] + values
        # 4. Collect the row instead of immediate append
        data_to_append.append(row) 
        print(f"Successfully scraped data for {name}.")
    else:
        print(f"Skipping {name}: No data scraped.")

    # Checkpoint is updated for progress tracking within the job
    with open(checkpoint_file, "w") as f:
        f.write(str(i))

    time.sleep(1)

# ---------------- BATCH WRITE ---------------- #

if data_to_append:
    print(f"\n✅ Writing {len(data_to_append)} rows to Google Sheet in one batch...")
    # 5. Use append_rows (plural) for the massive bulk upload!
    # This prevents hitting single-row API limits.
    sheet_data.append_rows(data_to_append, value_input_option='USER_ENTERED')
    print("✅ Batch write completed successfully.")
else:
    print("\n⚠️ No new data scraped in this chunk to write to the sheet.")
