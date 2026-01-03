import os
import time
import json
import gspread
import requests
import random
from typing import List

# ---------------- CONFIG ---------------- #
# Update these URLs with your actual spreadsheet links
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit#gid=0"

START_INDEX = int(os.getenv("START_INDEX", "0"))
END_INDEX   = int(os.getenv("END_INDEX", "2500"))
BATCH_SIZE  = 5  # Small batches help avoid IP bans on GitHub runners

class NSEDeliveryScraper:
    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://www.nseindia.com/get-quotes/equity?symbol=SBIN',
        }
        self.session.headers.update(self.headers)
        self.cookies = None

    def refresh_session(self):
        """Mandatory: Visit home page to get valid session cookies"""
        try:
            self.session.get("https://www.nseindia.com/", timeout=10)
            self.cookies = self.session.cookies
            print("🔄 Session Refreshed")
        except Exception as e:
            print(f"❌ Session Refresh Failed: {e}")

    def get_delivery_data(self, symbol: str) -> List[str]:
        encoded_sym = symbol.replace('&', '%26')
        url = f"https://www.nseindia.com/api/quote-equity?symbol={encoded_sym}&section=trade_info"
        
        # Result Schema: [Symbol, Traded Qty, Delivery Qty, % Delivery, Time, '', '']
        row = [symbol, 'N/A', 'N/A', 'N/A', 'N/A', '', '']
        
        for attempt in range(2):
            try:
                if not self.cookies: self.refresh_session()
                
                resp = self.session.get(url, timeout=10, cookies=self.cookies)
                
                if resp.status_code == 401:
                    self.refresh_session()
                    continue
                
                if resp.status_code == 200:
                    data = resp.json()
                    # Navigating the NSE JSON structure
                    t_info = data.get('marketDeptOrderBook', {}).get('tradeInfo', {})
                    
                    row[1] = t_info.get('totalTradedVolume', '0')
                    row[2] = t_info.get('deliveryQuantity', '0')
                    row[3] = f"{t_info.get('deliveryToTradedQuantity', '0')}%"
                    row[4] = data.get('metadata', {}).get('lastUpdateTime', 'N/A')
                    
                    print(f"✅ {symbol}: {row[3]} delivery")
                    return row
                
            except Exception as e:
                print(f"⚠️ {symbol} Error: {str(e)[:50]}")
                time.sleep(2)
        
        return row

# ---------------- EXECUTION ---------------- #
def run_scraper():
    # 1. AUTHENTICATION
    try:
        creds_env = os.getenv("GSPREAD_CREDENTIALS")
        if creds_env:
            gs_client = gspread.service_account_from_dict(json.loads(creds_env))
        else:
            gs_client = gspread.service_account(filename="credentials.json")
        
        source_sheet = gs_client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
        dest_sheet = gs_client.open_by_url(NEW_MV2_URL).worksheet("Sheet20")
        
        full_data = source_sheet.get_all_values()[1:] # Skip header
        stocks = full_data[START_INDEX:END_INDEX]
    except Exception as e:
        print(f"❌ Auth/Sheet Error: {e}")
        return

    scraper = NSEDeliveryScraper()
    print(f"🚀 Starting Scrape: {len(stocks)} symbols")

    # 2. BATCH PROCESSING
    for i in range(0, len(stocks), BATCH_SIZE):
        batch = stocks[i:i + BATCH_SIZE]
        batch_results = []
        
        for row in batch:
            symbol = row[0].strip()
            result = scraper.get_delivery_data(symbol)
            batch_results.append(result)
            time.sleep(random.uniform(1.2, 2.5)) # Respectful delay
        
        try:
            dest_sheet.append_rows(batch_results)
            print(f"💾 Batch {i//BATCH_SIZE + 1} saved to Sheet13")
        except Exception as e:
            print(f"❌ Write Error: {e}")
        
        time.sleep(5) # Cooldown between batches

if __name__ == "__main__":
    run_scraper()
