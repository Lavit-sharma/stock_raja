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
from google.oauth2.service_account import Credentials
from webdriver_manager.chrome import ChromeDriverManager


# ---------------- SETUP ---------------- #

# Chrome Options for GitHub Actions
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")

driver_service = Service(ChromeDriverManager().install())

# Google Sheets Auth
print("🔐 Authorizing Google Sheets...")
try:
    creds_json = os.getenv("GOOGLE_CREDENTIALS")
    service_account_info = json.loads(creds_json)
    creds = Credentials.from_service_account_info(service_account_info)
    gc = gspread.authorize(creds)
except Exception as e:
    print("❌ Google Sheets auth failed:", e)
    raise

sheet_main = gc.open('Stock List').worksheet('Sheet1')
sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')

# Load values
company_list = sheet_main.col_values(5)
name_list = sheet_main.col_values(1)
current_date = date.today().strftime("%m/%d/%Y")

# Checkpoint
checkpoint_file = "checkpoint_new_1.txt"
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 1


# ---------------- SCRAPER FUNCTION ---------------- #
def scrape_tradingview(company_url):
    driver = webdriver.Chrome(service=driver_service, options=chrome_options)
    driver.set_window_size(1920, 1080)
    try:
        driver.get(company_url)
        WebDriverWait(driver, 30).until(
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
        print(f"⚠️ Data element not found for URL: {company_url}")
        return []

    finally:
        driver.quit()


# ---------------- MAIN LOOP ---------------- #
for i, company_url in enumerate(company_list[last_i:], last_i):
    if i > 1100:
        break

    name = name_list[i]
    print(f"Scraping {i}: {name} | {company_url}")

    values = scrape_tradingview(company_url)
    if values:
        row = [name, current_date] + values
        sheet_data.append_row(row, table_range='A1')

    with open(checkpoint_file, "w") as f:
        f.write(str(i))

    time.sleep(1)  # reduce rate to avoid detection
