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

# ---------------- GOOGLE SHEET SETUP ---------------- #
creds_json = os.environ.get("GOOGLE_CREDENTIALS")
if not creds_json:
    raise Exception("GOOGLE_CREDENTIALS secret missing")

creds_dict = json.loads(creds_json)
gc = gspread.service_account_from_dict(creds_dict)

sheet_main = gc.open('Stock List').worksheet('Sheet1')
sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')

company_list = sheet_main.col_values(5)  # URLs
name_list = sheet_main.col_values(1)     # Names
current_date = date.today().strftime("%m/%d/%Y")

# ---------------- CHROME SETUP ---------------- #
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--window-size=1920,1080")
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

        # Wait until 'Fundamentals' or key stats appear
        WebDriverWait(driver, 30).until(
            EC.presence_of_all_elements_located((By.TAG_NAME, "body"))
        )

        # Wait up to 40s until the fields load completely
        waited = 0
        while waited < 40:
            soup = BeautifulSoup(driver.page_source, "html.parser")
            info = {}
            for label, field in [
                ("Volume", "Volume"),
                ("Market Cap", "Market Cap"),
                ("P/E Ratio", "P/E Ratio"),
                ("Dividend Yield", "Dividend Yield")
            ]:
                el = soup.find("span", string=lambda x: x and field in x)
                if el:
                    val = el.find_next("span")
                    if val:
                        info[field] = val.get_text(strip=True)
            if len(info) == 4:
                return info
            time.sleep(2)
            waited += 2

        print(f"Timeout: Some fields missing for {url}")
        return info

    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return {}

    finally:
        driver.quit()


# ---------------- MAIN LOOP ---------------- #
for i, url in enumerate(company_list):
    name = name_list[i] if i < len(name_list) else "Unknown"

    if not url or not url.startswith("http"):
        print(f"Skipping invalid URL for {name}: {url}")
        continue

    print(f"[{i}] Scraping: {name}")
    data = scrape_tradingview(url)

    if data and len(data) == 4:
        row = [name, current_date, data["Volume"], data["Market Cap"], data["P/E Ratio"], data["Dividend Yield"]]
        try:
            sheet_data.append_row(row, table_range="A1")
            print(f"✓ Added {name}")
        except Exception as e:
            print(f"⚠ Failed to append {name}: {e}")
    else:
        print(f"✗ Incomplete data for {name}: {data}")

    time.sleep(1)  # gentle delay between scrapes
