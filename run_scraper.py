from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import gspread
from datetime import date
import json
import os
import time

print("="*70)
print("TradingView Scraper - GitHub Actions")
print("="*70)

# Google Sheets
print("\n[1/4] Google Sheets...")
creds = json.loads(os.environ.get('GOOGLE_CREDENTIALS', '{}'))
gc = gspread.service_account_from_dict(creds)

sheet_main = gc.open('Stock List').worksheet('Sheet1')
try:
    sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')
except:
    sh = gc.open('Tradingview Data Reel Experimental May')
    sheet_data = sh.add_worksheet(title='Sheet5', rows=1000, cols=26)

companies = sheet_main.col_values(5)
names = sheet_main.col_values(1)
today = date.today().strftime("%m/%d/%Y")
print(f"OK - {len(companies)} companies")

# Chrome
print("\n[2/4] Browser...")
opts = Options()
opts.add_argument("--headless=new")
opts.add_argument("--no-sandbox")
opts.add_argument("--disable-dev-shm-usage")
driver = webdriver.Chrome(options=opts)
print("OK")

# Login
print("\n[3/4] Login...")
email = os.environ.get('TRADINGVIEW_EMAIL')
pwd = os.environ.get('TRADINGVIEW_PASSWORD')

if email and pwd:
    try:
        driver.get("https://www.tradingview.com/#signin")
        time.sleep(5)
        
        e = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='username']"))
        )
        e.send_keys(email)
        
        p = driver.find_element(By.CSS_SELECTOR, "input[name='password']")
        p.send_keys(pwd)
        p.send_keys(Keys.RETURN)
        time.sleep(10)
        print("OK - Logged in")
    except:
        print("WARN - Login failed")
else:
    print("SKIP - No credentials")

# Scrape
def scrape(url):
    driver.get(url)
    time.sleep(12)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    nodes = soup.find_all("div", class_="valueValue-l31H9iuA")
    if nodes:
        return [n.get_text().strip() for n in nodes if n.get_text(strip=True)]
    return []

# Run
print("\n[4/4] Scraping...")
batch = int(os.environ.get('BATCH_SIZE', '100'))
start = int(os.environ.get('START_INDEX', '1'))
end = min(len(companies), start + batch)

success = 0
for i in range(start, end):
    name = names[i] if i < len(names) else "Unknown"
    url = companies[i]
    print(f"[{i}] {name}", end=" ")
    
    vals = scrape(url)
    if vals:
        sheet_data.append_row([name, today] + vals, table_range='A1')
        print(f"OK ({len(vals)})")
        success += 1
    else:
        print("SKIP")
    time.sleep(2)

driver.quit()
print(f"\nDone: {success}/{end-start}")

