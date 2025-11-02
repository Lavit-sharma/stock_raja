from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from bs4 import BeautifulSoup
import gspread
from datetime import date
import os
import time
import json
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- GOOGLE SHEET SETUP ---------------- #

credentials = json.loads(os.getenv("GOOGLE_SHEET_CREDS"))
gc = gspread.service_account_from_dict(credentials)
sheet_main = gc.open('Stock List').worksheet('Sheet1')
sheet_data = gc.open('Tradingview Data Reel Experimental Nov 25').worksheet('Sheet5')

company_list = sheet_main.col_values(5)
name_list = sheet_main.col_values(1)
current_date = date.today().strftime("%m/%d/%Y")

checkpoint_file = "checkpoint_new_1.txt"
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 1

# ---------------- SELENIUM CONFIG ---------------- #

chrome_options = Options()
chrome_options.add_argument("--headless=new")  # headless mode for GitHub runner
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")

# ---------------- LOAD COOKIES ---------------- #

def load_cookies(driver, cookies_path="cookies.json"):
    """Load cookies into driver if available."""
    if os.path.exists(cookies_path):
        with open(cookies_path, "r") as f:
            cookies = json.load(f)
        driver.get("https://in.tradingview.com")
        for cookie in cookies:
            if "sameSite" in cookie:
                if cookie["sameSite"] not in ["Strict", "Lax", "None"]:
                    cookie["sameSite"] = "Lax"
            try:
                driver.add_cookie(cookie)
            except Exception:
                pass
        driver.refresh()
        print("✅ Logged in using saved cookies")
    else:
        print("⚠️ No cookies.json found — please save cookies first (using savecookies.py)")


# ---------------- SCRAPER FUNCTION ---------------- #

def scrape_tradingview(company_url):
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.set_window_size(1920, 1080)

    try:
        # Load cookies (login session)
        load_cookies(driver)

        driver.get(company_url)
        print(f"Accessing URL: {company_url}")

        try:
            WebDriverWait(driver, 45).until(
                EC.presence_of_element_located((
                    By.XPATH,
                    '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'
                ))
            )
        except TimeoutException:
            print(f"⚠️ Timeout: {company_url} not fully loaded, retrying once...")
            time.sleep(5)
            driver.refresh()
            try:
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((
                        By.XPATH,
                        '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'
                    ))
                )
            except TimeoutException:
                print(f"❌ Skipping {company_url} (still not loaded)")
                return []

        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [
            el.get_text().replace('−', '-').replace('∅', 'None')
            for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        ]
        return values

    except NoSuchElementException:
        print(f"❌ Data element not found for {company_url}")
        return []

    finally:
        driver.quit()

# ---------------- MAIN LOOP ---------------- #

for i, company_url in enumerate(company_list[last_i:], last_i):
    if i > 2500:
        break

    name = name_list[i]
    print(f"\n--- Scraping {i}: {name} | {company_url} ---")

    values = scrape_tradingview(company_url)
    if values:
        row = [name, current_date] + values
        sheet_data.append_row(row, table_range='A1')

    with open(checkpoint_file, "w") as f:
        f.write(str(i))

    time.sleep(1)
