from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import gspread
from datetime import date
import os
import time
import json

# ---------------- CONFIG ---------------- #
START_INDEX = int(os.getenv("START_INDEX", "0"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))

# Google Sheets credentials
creds_json = os.environ.get("GOOGLE_CREDENTIALS")
if not creds_json:
    raise Exception("GOOGLE_CREDENTIALS missing in secrets")
creds_dict = json.loads(creds_json)
gc = gspread.service_account_from_dict(creds_dict)

sheet_main = gc.open('Stock List').worksheet('Sheet1')
sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')

company_list = sheet_main.col_values(5)  # URLs
name_list = sheet_main.col_values(1)     # Names
current_date = date.today().strftime("%m/%d/%Y")

end_index = min(START_INDEX + BATCH_SIZE, len(company_list))

# ---------------- CHROME SETUP ---------------- #
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option('useAutomationExtension', False)

driver_service = Service("/usr/bin/chromedriver")  # GitHub Actions path

# ---------------- SCRAPE FUNCTION ---------------- #
def scrape_tradingview(url):
    driver = webdriver.Chrome(service=driver_service, options=chrome_options)
    driver.set_window_size(1920, 1080)
    try:
        driver.get(url)
        WebDriverWait(driver, 20).until(
            EC.presence_of_all_elements_located((By.TAG_NAME, "body"))
        )
        soup = BeautifulSoup(driver.page_source, "html.parser")
        # Grab all value divs
        values = [
            el.get_text(strip=True).replace('−','-').replace('∅','None')
            for el in soup.find_all("div", class_="valueValue-l31H9iuA")
        ]
        # fallback if no values
        if not values:
            nodes = soup.find_all("div", attrs={"data-name": True})
            values = [n.get_text(strip=True) for n in nodes if n.get_text(strip=True)]
        return values
    except Exception as e:
        print(f"Failed scraping {url}: {e}")
        return []
    finally:
        driver.quit()

# ---------------- MAIN LOOP ---------------- #
for i in range(START_INDEX, end_index):
    name = name_list[i] if i < len(name_list) else "Unknown"
    url = company_list[i]

    # Validate URL
    if not url or not url.startswith("http"):
        print(f"Skipping invalid URL for {name}: {url}")
        continue

    print(f"Scraping {i}: {name} | {url}")
    values = scrape_tradingview(url)

    if values:
        row = [name, current_date] + values
        try:
            sheet_data.append_row(row, table_range="A1")
            print(f"✓ Appended {len(values)} values for {name}")
        except Exception as e:
            print(f"Failed to append row for {name}: {e}")
    else:
        print(f"✗ No data for {name}")

    time.sleep(1)  # avoid rapid requests
