from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import gspread
from datetime import date
import os
import time
import json

# ---------------- CONFIG ---------------- #
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))  # optional batch size
checkpoint_file = "checkpoint_new_1.txt"

# ---------------- GOOGLE SHEETS ---------------- #
GOOGLE_CREDENTIALS = os.environ.get("GOOGLE_CREDENTIALS", "")
if not GOOGLE_CREDENTIALS:
    print("✗ GOOGLE_CREDENTIALS not found"); exit(1)

gc = gspread.service_account_from_dict(json.loads(GOOGLE_CREDENTIALS))
sheet_main = gc.open('Stock List').worksheet('Sheet1')
sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')

company_list = sheet_main.col_values(5)
name_list = sheet_main.col_values(1)
current_date = date.today().strftime("%m/%d/%Y")

# ---------------- CHECKPOINT ---------------- #
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 0

# ---------------- SELENIUM SETUP ---------------- #
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option("useAutomationExtension", False)

driver_service = Service(ChromeDriverManager().install())

# ---------------- SCRAPER ---------------- #
def scrape_tradingview(url):
    driver = webdriver.Chrome(service=driver_service, options=chrome_options)
    driver.set_window_size(1920, 1080)
    try:
        driver.get(url)
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.valueValue-l31H9iuA"))
        )
        time.sleep(0.8)  # small render wait
        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [
            el.get_text(strip=True).replace('−', '-').replace('∅', 'None')
            for el in soup.find_all("div", class_="valueValue-l31H9iuA")
        ]
        if not values:  # fallback
            nodes = soup.find_all("div", attrs={"data-name": True})
            values = [n.get_text(strip=True) for n in nodes if n.get_text(strip=True)]
        return values
    except (TimeoutException, NoSuchElementException):
        print(f"⚠ Data not found: {url}")
        return []
    finally:
        driver.quit()

# ---------------- MAIN LOOP ---------------- #
for i, url in enumerate(company_list[last_i:], start=last_i):
    if i >= last_i + BATCH_SIZE:
        break
    name = name_list[i] if i < len(name_list) else "Unknown"
    print(f"[{i}] Scraping: {name} | {url}")

    values = scrape_tradingview(url)
    if values:
        row = [name, current_date] + values
        try:
            sheet_data.append_row(row, table_range="A1")
            print(f"✓ {len(values)} values appended")
        except Exception as e:
            print(f"✗ Failed to append row: {e}")
    else:
        print("✗ No values scraped")

    # Update checkpoint
    with open(checkpoint_file, "w") as f:
        f.write(str(i + 1))

    time.sleep(1)  # light throttle
