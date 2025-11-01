import time
import gspread
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from google.oauth2.service_account import Credentials
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# ✅ Correct Google Sheets scopes
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# ✅ Google Sheets setup
creds = Credentials.from_service_account_file("creds.json", scopes=SCOPES)
client = gspread.authorize(creds)
sheet = client.open("Stock Data").sheet1  # Change to your Google Sheet name

# ✅ Selenium setup (headless Chrome)
def create_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(options=chrome_options)
    return driver

# ✅ Function to safely extract text with waiting and retries
def safe_get_text(driver, xpath, timeout=15):
    try:
        element = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.XPATH, xpath))
        )
        return element.text.strip()
    except Exception:
        return "N/A"

# ✅ Scraper function
def scrape_tradingview_data(symbol_url):
    driver = create_driver()
    data = {"Symbol": symbol_url}

    try:
        driver.get(symbol_url)
        time.sleep(3)  # give page time to load JS

        # wait until stats appear
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//div[contains(@class,'container-')]"))
        )

        # scrape stats
        data["Volume"] = safe_get_text(driver, "//div[contains(text(),'Volume')]/following-sibling::div")
        data["Market Cap"] = safe_get_text(driver, "//div[contains(text(),'Market capitalization')]/following-sibling::div")
        data["P/E Ratio"] = safe_get_text(driver, "//div[contains(text(),'P/E ratio')]/following-sibling::div")
        data["Dividend Yield"] = safe_get_text(driver, "//div[contains(text(),'Dividend yield')]/following-sibling::div")

        print(f"[SUCCESS] {symbol_url}: {data}")

    except Exception as e:
        print(f"[ERROR] Failed scraping {symbol_url}: {e}")

    finally:
        driver.quit()

    return data

# ✅ Append data to Google Sheet
def append_to_sheet(data):
    values = [
        data["Symbol"],
        data["Volume"],
        data["Market Cap"],
        data["P/E Ratio"],
        data["Dividend Yield"],
    ]
    sheet.append_row(values)
    print(f"[APPENDED] {values}")

# ✅ Entry point
if __name__ == "__main__":
    print("Starting sequential scraper...")

    # Add your TradingView stock URLs here 👇
    stock_urls = [
        "https://in.tradingview.com/chart/?symbol=NSE%3A20MICRONS",
        "https://in.tradingview.com/chart/?symbol=NSE%3A3PLAND"
    ]

    for url in stock_urls:
        stock_data = scrape_tradingview_data(url)
        append_to_sheet(stock_data)
        time.sleep(5)  # polite delay between requests
