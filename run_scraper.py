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

# Chrome Options
chrome_options = Options()
# ✅ This line MUST be present and NOT commented out for server environments:
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--remote-debugging-port=9222")

# ---------------- GOOGLE SHEETS AUTH ---------------- #

# ⬇️ UPDATED: Load credentials from a file named 'credentials.json'
# This file will be created by the GitHub Action using a secret.
try:
    gc = gspread.service_account("credentials.json")
except Exception as e:
    # Fallback/local testing: you might keep the hardcoded dictionary locally
    print(f"Error loading credentials.json: {e}")
    print("Ensure 'credentials.json' exists or has been created by GitHub Actions.")
    # For CI, this block should ideally fail, but for a stable script:
    # You would need to add a safe way to handle the missing file here, 
    # but for CI, the file creation step is mandatory.
    exit(1)

sheet_main = gc.open('Stock List').worksheet('Sheet1')
sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')

company_list = sheet_main.col_values(5)
name_list = sheet_main.col_values(1)
current_date = date.today().strftime("%m/%d/%Y")

checkpoint_file = "checkpoint_new_1.txt"
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 1

# ---------------- SCRAPER FUNCTION ---------------- #
def scrape_tradingview(company_url):
    # Ensure the correct driver is installed and initialized
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.set_window_size(1920, 1080)
    try:
        # ✅ LOGIN USING SAVED COOKIES
        # The 'cookies.json' file will be created by the GitHub Action using a secret.
        if os.path.exists("cookies.json"):
            driver.get("https://www.tradingview.com/")
            with open("cookies.json", "r", encoding="utf-8") as f:
                # ⬇️ UPDATED: Using 'json.load(f)' which is correct for file content
                cookies = json.load(f)
            for cookie in cookies:
                try:
                    # Minor improvement: safer cookie addition logic
                    cookie_to_add = {k: cookie[k] for k in ('name', 'value', 'domain', 'path') if k in cookie}
                    cookie_to_add['secure'] = cookie.get('secure', False)
                    cookie_to_add['httpOnly'] = cookie.get('httpOnly', False)
                    driver.add_cookie(cookie_to_add)
                except Exception as e:
                    # print(f"Could not add cookie: {e}") # Optional debug
                    pass
            driver.refresh()
            time.sleep(4)  # was 2s; longer settle helps dynamic widgets stabilize [web:39][web:40]
        else:
            print("⚠️ cookies.json not found. The script might fail if login is required.")
            # Continue without login, which will likely fail on TradingView paywall/data limits
            pass

        # ✅ AFTER LOGIN, OPEN THE TARGET URL
        driver.get(company_url)
        WebDriverWait(driver, 90).until(  # was 45; extend explicit wait to reduce early reads [web:39][web:51][web:53]
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

# ---------------- MAIN LOOP ---------------- #
for i, company_url in enumerate(company_list[last_i:], last_i):
    if i > 2500:
        print("Reached scraping limit (i > 2500). Stopping.")
        break

    name = name_list[i]
    print(f"Scraping {i}: {name} | {company_url}")

    values = scrape_tradingview(company_url)
    if values:
        row = [name, current_date] + values
        sheet_data.append_row(row, table_range='A1')
        print(f"Successfully scraped and saved data for {name}.")
    else:
        print(f"Skipping {name}: No data scraped.")

    with open(checkpoint_file, "w") as f:
        f.write(str(i))

    time.sleep(1)
