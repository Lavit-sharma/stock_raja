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

# ---------------- SETUP ---------------- #

# --- Chrome Options for GitHub Actions Runner (Linux) ---
chrome_options = Options()

# Essential options for running headless on a CI/CD runner
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")
# chrome_options.add_argument("--remote-debugging-port=9222") # Not strictly needed for basic headless

# --- Google Sheets Auth ---
try:
    # Load credentials from environment variable (set securely in GitHub Secrets)
    credentials_json = os.environ.get("GSPREAD_CREDENTIALS")
    if not credentials_json:
        raise ValueError("GSPREAD_CREDENTIALS environment variable not set.")
    
    credentials = json.loads(credentials_json)
    gc = gspread.service_account_from_dict(credentials)
    
    # Open Google Sheets
    sheet_main = gc.open('Stock List').worksheet('Sheet1')
    sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')

except (ValueError, json.JSONDecodeError, gspread.exceptions.APIError) as e:
    print(f"Error setting up Google Sheets: {e}")
    # Exit or handle error gracefully if sheet access is critical
    exit(1)

# Load values
company_list = sheet_main.col_values(5)
name_list = sheet_main.col_values(1)
current_date = date.today().strftime("%m/%d/%Y")

# Checkpoint - Use a path suitable for the runner
checkpoint_file = "checkpoint_new_1.txt"
try:
    with open(checkpoint_file, "r") as f:
        last_i = int(f.read().strip())
except FileNotFoundError:
    last_i = 1 # Start from the second row (index 1) if checkpoint file doesn't exist
except ValueError:
    last_i = 1 # Handle case where file is empty or not a number

print(f"Starting from index: {last_i}")

# ---------------- SCRAPER FUNCTION ---------------- #
def scrape_tradingview(company_url):
    # Driver management is simplified: Service is no longer required with modern Selenium 4+ and Chrome
    driver = webdriver.Chrome(options=chrome_options)
    driver.set_window_size(1920, 1080)
    
    # Define a robust XPATH for the element (check if this specific XPath is still correct)
    # Target: A value element in the TradingView snapshot
    TARGET_XPATH = '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'

    try:
        print(f"Accessing URL: {company_url}")
        driver.get(company_url)
        
        # Wait until the specific element is visible (max 30 seconds)
        WebDriverWait(driver, 30).until(
            EC.visibility_of_element_located((By.XPATH, TARGET_XPATH))
        )
        
        # Get page source and parse with BeautifulSoup
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # Find all value elements
        values = [
            el.get_text().replace('−', '-').replace('∅', 'None')
            for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        ]
        
        print(f"Successfully scraped {len(values)} values.")
        return values

    except TimeoutException:
        print(f"Timeout: Data element not visible within 30 seconds for URL: {company_url}")
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
    
    # Ensure the URL is valid before attempting to scrape
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

    # Update checkpoint
    with open(checkpoint_file, "w") as f:
        f.write(str(i + 1)) # Write the index of the *next* item to be processed
        
    print(f"Updated checkpoint to {i + 1}")

    time.sleep(1) # reduce rate to avoid detection

print("Scraping finished.")
