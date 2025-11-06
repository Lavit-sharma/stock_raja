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
import random  # ✅ added for random delay


# ✅ Add random stagger delay to avoid API quota spikes
delay = random.randint(5, 60)
print(f"⏳ Waiting {delay}s before starting to avoid Google API 429...")
time.sleep(delay)


# ---------------- SHARDING (env-driven) ----------------
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "10"))

START_INDEX = int(os.getenv("START_INDEX", "1"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "0"))

checkpoint_file = os.getenv("CHECKPOINT_FILE", "checkpoint_new_1.txt")
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else START_INDEX


# ---------------- SETUP ----------------
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--remote-debugging-port=9222")


# ---------------- GOOGLE SHEETS AUTH ----------------
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


# ---------------- SCRAPER FUNCTION ----------------
def scrape_tradingview(company_url, driver):
    try:
        if os.path.exists("cookies.json"):
            driver.get("https://www.tradingview.com/")
            with open("cookies.json", "r", encoding="utf-8") as f:
                cookies = json.load(f)
            for cookie in cookies:
                try:
                    cookie_to_add = {
                        k: cookie[k]
                        for k in ('name', 'value', 'domain', 'path')
                        if k in cookie
                    }
                    cookie_to_add['secure'] = cookie.get('secure', False)
                    cookie_to_add['httpOnly'] = cookie.get('httpOnly', False)
                    driver.add_cookie(cookie_to_add)
                except Exception:
                    pass
            driver.refresh()
            time.sleep(2)
        else:
            print("⚠️ cookies.json not found. Proceeding without login may limit data.")

        driver.get(company_url)
        WebDriverWait(driver, 45).until(
            EC.visibility_of_element_located((
                By.XPATH,
                '/html/body/div/div/div/div/div/div/div/div/div/div/div/div/div/div/div/div'
            ))
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


# ---------------- BUFFER & APPEND ----------------
buffer = []
BATCH_SIZE_APPEND = 50


def flush_buffer():
    global buffer
    if not buffer:
        return
    for attempt in range(3):
        try:
            sheet_data.spreadsheet.values_append(
                'Sheet5!A1',
                {'valueInputOption': 'USER_ENTERED', 'insertDataOption': 'INSERT_ROWS'},
                {'values': buffer}
            )
            print(f"[PUSH] {len(buffer)} rows appended")
            buffer = []
            return
        except Exception as e:
            wait = (attempt + 1) * 5
            print(f"Append retry in {wait}s due to: {str(e)}")
            time.sleep(wait)
    print("✗ Failed to append after retries")


# ---------------- MAIN LOOP ----------------
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
driver.set_window_size(1920, 1080)

try:
    processed = 0
    i_start = last_i

    for i, company_url in enumerate(company_list[i_start:], i_start):
        if BATCH_SIZE > 0:
            if processed >= BATCH_SIZE:
                break
        else:
            if i % SHARD_STEP != SHARD_INDEX:
                continue

        if i > 2500:
            print("Reached scraping limit (i > 2500). Stopping.")
            break

        name = name_list[i] if i < len(name_list) else f"Row {i}"
        print(f"Scraping {i}: {name} | {company_url}")

        values = scrape_tradingview(company_url, driver)
        if values:
            row = [name, current_date] + values
            buffer.append(row)
            print(f"Queued {name} (buffer={len(buffer)})")
            if len(buffer) >= BATCH_SIZE_APPEND:
                flush_buffer()
        else:
            print(f"Skipping {name}: No data scraped.")

        with open(checkpoint_file, "w") as f:
            f.write(str(i))

        time.sleep(1)
        processed += 1

finally:
    flush_buffer()
    driver.quit()
    print("Shard complete.")
