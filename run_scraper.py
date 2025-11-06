from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup
from datetime import date
import pandas as pd
import os
import time
import json
import requests
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- SHARDING (env-driven) ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))

START_INDEX = int(os.getenv("START_INDEX", "1"))
END_INDEX = int(os.getenv("END_INDEX", "2500"))

checkpoint_file = os.getenv("CHECKPOINT_FILE", "checkpoint_new_1.txt")
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else START_INDEX

# ---------------- SETUP ---------------- #
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--remote-debugging-port=9222")

# ---------------- READ STOCK LIST FROM GITHUB EXCEL ---------------- #

# 🔹 Replace this URL with your actual raw Excel URL
EXCEL_URL = "https://raw.githubusercontent.com/<username>/<repo>/main/stock_list.xlsx"

print("📥 Fetching stock list from GitHub...")

try:
    response = requests.get(EXCEL_URL)
    response.raise_for_status()
    with open("stocks.xlsx", "wb") as f:
        f.write(response.content)

    df = pd.read_excel("stocks.xlsx")
    # ✅ Assumes columns: 'Name' and 'URL' (or adjust below)
    name_list = df.iloc[:, 0].fillna("").tolist()
    company_list = df.iloc[:, 4].fillna("").tolist()  # 5th column like earlier Google Sheet

except Exception as e:
    print(f"❌ Error reading Excel from GitHub: {e}")
    exit(1)

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

        driver.get(company_url)
        WebDriverWait(driver, 45).until(
            EC.visibility_of_element_located((By.XPATH,
                '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'))
        )

        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [
            el.get_text().replace('−', '-').replace('∅', '').strip()
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
    if i < START_INDEX or i > END_INDEX:
        continue

    if i % SHARD_STEP != SHARD_INDEX:
        continue

    if i > END_INDEX:
        print("Reached scraping limit for this batch. Stopping.")
        break

    name = name_list[i] if i < len(name_list) else f"Row {i}"
    print(f"Scraping {i}: {name} | {company_url}")

    values = scrape_tradingview(company_url)
    if values:
        row = [name, current_date] + values
        # 💾 Save locally instead of Google Sheet (append to CSV for now)
        with open("output_data.csv", "a", encoding="utf-8") as f:
            f.write(",".join(map(str, row)) + "\n")
        print(f"✅ Successfully scraped and saved data for {name}.")
    else:
        print(f"⚠️ Skipping {name}: No data scraped.")

    with open(checkpoint_file, "w") as f:
        f.write(str(i))

    time.sleep(1)
