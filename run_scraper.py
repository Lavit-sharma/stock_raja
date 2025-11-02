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

# ---------------- SHARDING (env-driven) ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))

# Unique checkpoint per shard passed from workflow
checkpoint_file = os.getenv("CHECKPOINT_FILE", "checkpoint_new_1.txt")
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 1

# ---------------- SETUP ---------------- #
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--remote-debugging-port=9222")

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

# ---------------- SCRAPER FUNCTION ---------------- #
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
            pass

        # AFTER LOGIN, OPEN THE TARGET URL
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

# ---------------- BATCH APPEND HELPERS ---------------- #
# Use gspread's values_append to append multiple rows in one API call (fast path).
def flush_rows_buffer(ws, rows_buffer):
    if not rows_buffer:
        return
    a1_range = f"{ws.title}!A1"
    body = {"values": rows_buffer}
    # RAW preserves your exact strings; switch to USER_ENTERED if you want Sheets to parse numbers/dates.
    ws.values_append(a1_range, params={"valueInputOption": "RAW"}, body=body)
    rows_buffer.clear()

# ---------------- MAIN LOOP (matrix-aware) ---------------- #
rows_buffer = []
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "150"))  # tune 100–300 based on throughput

for i, company_url in enumerate(company_list[last_i:], last_i):
    # Shard filter: only process indices that belong to this shard
    if i % SHARD_STEP != SHARD_INDEX:
        continue

    if i > 2500:
        print("Reached scraping limit (i > 2500). Stopping.")
        break

    name = name_list[i] if i < len(name_list) else f"Row {i}"
    print(f"Scraping {i}: {name} | {company_url}")

    values = scrape_tradingview(company_url)
    if values:
        row = [name, current_date] + values
        rows_buffer.append(row)

        # Flush when buffer reaches BATCH_SIZE
        if len(rows_buffer) >= BATCH_SIZE:
            try:
                flush_rows_buffer(sheet_data, rows_buffer)
                print(f"Flushed {BATCH_SIZE} rows.")
            except Exception as e:
                print(f"Batch flush error: {e}. Retrying once...")
                time.sleep(1.0)
                try:
                    flush_rows_buffer(sheet_data, rows_buffer)
                    print("Flush retry succeeded.")
                except Exception as e2:
                    print(f"Flush retry failed: {e2}. Keeping rows to retry later.")
        else:
            # Light pacing to avoid hammering target site
            time.sleep(0.2)
    else:
        print(f"Skipping {name}: No data scraped.")

    with open(checkpoint_file, "w") as f:
        f.write(str(i))

# Final flush at the end of this shard
if rows_buffer:
    try:
        flush_rows_buffer(sheet_data, rows_buffer)
        print(f"Final flush of {len(rows_buffer)} rows.")
    except Exception as e:
        print(f"Final flush error: {e}. Attempting one retry...")
        time.sleep(1.0)
        try:
            flush_rows_buffer(sheet_data, rows_buffer)
            print("Final flush retry succeeded.")
        except Exception as e2:
            print(f"Final flush retry failed: {e2}. Some rows may not be written.")
