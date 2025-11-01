import os
import time
import json
from datetime import date

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from bs4 import BeautifulSoup
import gspread
import pickle

# ---------------- SETUP ---------------- #

chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")

# ---------------- GOOGLE SHEETS ---------------- #
try:
    credentials_json = os.environ.get("GSPREAD_CREDENTIALS")
    if not credentials_json:
        raise ValueError("GSPREAD_CREDENTIALS environment variable not set.")
    
    credentials = json.loads(credentials_json)
    gc = gspread.service_account_from_dict(credentials)
    
    sheet_main = gc.open('Stock List').worksheet('Sheet1')
    sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')

except (ValueError, json.JSONDecodeError, gspread.exceptions.APIError) as e:
    print(f"Error setting up Google Sheets: {e}")
    exit(1)

company_list = sheet_main.col_values(5)
name_list = sheet_main.col_values(1)
current_date = date.today().strftime("%m/%d/%Y")

checkpoint_file = "checkpoint_new_1.txt"
try:
    with open(checkpoint_file, "r") as f:
        last_i = int(f.read().strip())
except (FileNotFoundError, ValueError):
    last_i = 1

print(f"Starting from index: {last_i}")

# ---------------- LOGIN ---------------- #
LOGIN_URL = "https://www.tradingview.com/#signin"
COOKIES_FILE = "tradingview_cookies.pkl"

def tradingview_login(driver):
    email = os.environ.get("TV_EMAIL")
    password = os.environ.get("TV_PASSWORD")
    if not email or not password:
        print("TV_EMAIL and TV_PASSWORD environment variables must be set!")
        exit(1)
    
    driver.get(LOGIN_URL)
    WebDriverWait(driver, 20).until(EC.visibility_of_element_located((By.NAME, "username")))
    driver.find_element(By.NAME, "username").send_keys(email)
    driver.find_element(By.NAME, "password").send_keys(password)
    driver.find_element(By.XPATH, "//button[contains(text(),'Sign in')]").click()
    
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.CLASS_NAME, "tv-header__user-menu"))
    )
    print("Login successful")
    
    # Save cookies
    pickle.dump(driver.get_cookies(), open(COOKIES_FILE, "wb"))

def load_cookies(driver):
    if os.path.exists(COOKIES_FILE):
        driver.get("https://www.tradingview.com/")
        cookies = pickle.load(open(COOKIES_FILE, "rb"))
        for cookie in cookies:
            driver.add_cookie(cookie)
        driver.refresh()
        print("Loaded cookies successfully")
        return True
    return False

# ---------------- SCRAPER FUNCTION ---------------- #
def scrape_tradingview(company_url):
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_window_size(1920, 1080)
    
    # Load cookies if available, else login
    if not load_cookies(driver):
        tradingview_login(driver)
    
    TARGET_XPATH = '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'
    
    try:
        print(f"Accessing URL: {company_url}")
        driver.get(company_url)
        
        WebDriverWait(driver, 60).until(  # increased timeout
            EC.visibility_of_element_located((By.XPATH, TARGET_XPATH))
        )
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [
            el.get_text().replace('−', '-').replace('∅', 'None')
            for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        ]
        
        print(f"Successfully scraped {len(values)} values.")
        return values

    except TimeoutException:
        print(f"Timeout: Data element not visible within 60 seconds for URL: {company_url}")
        return []
    except NoSuchElementException:
        print(f"Data element not found (XPath might be wrong) for URL: {company_url}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred for URL {company_url}: {e}")
        return []
    finally:
        driver.quit()

# ---------------- MAIN LOOP ---------------- #
for i, company_url in enumerate(company_list[last_i:], last_i):
    if i > 1100:
        print("Reached max index (1100). Stopping.")
        break
    
    if not company_url or not company_url.startswith('http'):
        print(f"Skipping index {i}: Invalid URL '{company_url}'")
        continue

    name = name_list[i]
    print(f"--- Scraping {i}: {name} | {company_url} ---")

    values = scrape_tradingview(company_url)
    
    if values:
        row = [name, current_date] + values
        try:
            sheet_data.append_row(row, table_range='A1')
            print(f"Successfully appended data for {name} to Google Sheet.")
        except gspread.exceptions.APIError as e:
            print(f"Failed to append row for {name} to Google Sheet: {e}")
    else:
        print(f"No data scraped for {name}. Skipping sheet update.")

    with open(checkpoint_file, "w") as f:
        f.write(str(i + 1))
        
    print(f"Updated checkpoint to {i + 1}")
    time.sleep(1)

print("Scraping finished.")
