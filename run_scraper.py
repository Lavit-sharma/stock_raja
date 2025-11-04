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

# ---------------- BATCH CONTROL ---------------- #
BATCH_SIZE = int(os.getenv("INPUT_BATCH_SIZE", "200"))
MATRIX_BATCH = int(os.getenv("MATRIX_BATCH", "1"))

# Read Google Sheet data
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

# ---------------- BATCH SLICING ---------------- #
START_INDEX = (MATRIX_BATCH - 1) * BATCH_SIZE
END_INDEX = min(START_INDEX + BATCH_SIZE, len(company_list))
print(f"[Batch {MATRIX_BATCH}] START_INDEX={START_INDEX}, END_INDEX={END_INDEX}")

# ---------------- SETUP CHROME ---------------- #
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--remote-debugging-port=9222")

# ---------------- SCRAPER FUNCTION (UNCHANGED) ---------------- #
def scrape_tradingview(company_url):
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.set_window_size(1920, 1080)
    try:
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
            print("⚠️ cookies.json not found. Proceeding without login may limit data.")

        # OPEN TARGET URL
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

# ---------------- MAIN LOOP (BATCH-RESPECTING) ---------------- #
for i in range(START_INDEX, END_INDEX):
    name = name_list[i] if i < len(name_list) else f"Row {i+1}"
    company_url = company_list[i]
    print(f"Scraping {i+1}: {name} | {company_url}")

    values = scrape_tradingview(company_url)
    if values:
        row = [name, current_date] + values
        sheet_data.append_row(row, table_range='A1')
        print(f"✅ Saved data for {name}")
    else:
        print(f"⚠️ Skipping {name}: No data scraped.")

    time.sleep(1)  # keep pause between requests (same as original)
