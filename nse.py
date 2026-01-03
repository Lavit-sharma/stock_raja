import os
import time
import json
import gspread
import requests
import random
from typing import List

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit#gid=0"

START_INDEX = int(os.getenv("START_INDEX", "0"))
END_INDEX   = int(os.getenv("END_INDEX", "2500"))
BATCH_SIZE  = 5 

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
        """Must visit home page to initialize cookies for the API"""
        try:
            self.session.get("https://www.nseindia.com/", timeout=15)
            self.cookies = self.session.cookies
            print("🔄 Session Cookies Refreshed")
        except Exception as e:
            print(f"❌ Session Error: {e}")

    def get_popup_data(self, symbol: str) -> List[str]:
        """Scrapes the exact data found in the 'Trade Info' (i) popup"""
        encoded_sym = symbol.replace('&', '%26')
        url = f"https://www.nseindia.com/api/quote-equity?symbol={encoded_sym}&section=trade_info"
        
        # Schema for Google Sheets: [Symbol, Traded Qty, Delivery Qty, % Delivery, Update Time]
        row = [symbol, 'N/A', 'N/A', 'N/A', 'N/A']
        
        for attempt in range(2):
            try:
                if not self.cookies: self.refresh_session()
                
                resp = self.session.get(url, timeout=15, cookies=self.cookies)
                
                if resp.status_code == 401:
                    self.refresh_session()
                    continue
                
                if resp.status_code == 200:
                    data = resp.json()
                    
                    # This is the "i" button/popup data source
                    popup_data = data.get('securityWiseDP', {})
                    
                    if popup_data:
                        row[1] = popup_data.get('quantityTraded', '0')
                        row[2] = popup_data.get('deliveryQuantity', '0')
                        # Rounding the percentage for clarity
                        row[3] = f"{popup_data.get('deliveryToTradedQuantity', 0)}%"
                        row[4] = data.get('metadata', {}).get('lastUpdateTime', 'N/A')
                        
                        print(f"✅ {symbol} | Delivery: {row[3]} | Qty: {row[2]}")
                        return row
                    else:
                        print(f"⚠️ {symbol}: Popup data (securityWiseDP) not found in response")
                
            except Exception as e:
                print(f"⚠️ {symbol} Error: {str(e)[:50]}")
                time.sleep(2)
        
        return row

def run_scraper():
    try:
        creds_env = os.getenv("GSPREAD_CREDENTIALS")
        if creds_env:
            gs_client = gspread.service_account_from_dict(json.loads(creds_env))
        else:
            gs_client = gspread.service_account(filename="credentials.json")
        
        source_sheet = gs_client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
        dest_sheet = gs_client.open_by_url(NEW_MV2_URL).worksheet("Sheet13")
        
        stocks = source_sheet.get_all_values()[1:][START_INDEX:END_INDEX]
    except Exception as e:
        print(f"❌ Startup Error: {e}")
        return

    scraper = NSEDeliveryScraper()
    
    for i in range(0, len(stocks), BATCH_SIZE):
        batch = stocks[i:i + BATCH_SIZE]
        results = []
        
        for row in batch:
            symbol = row[0].strip()
            results.append(scraper.get_popup_data(symbol))
            time.sleep(random.uniform(2, 4)) # Critical delay for NSE
            
        try:
            dest_sheet.append_rows(results)
            print(f"💾 Saved {len(results)} rows.")
        except Exception as e:
            print(f"❌ Write Error: {e}")
        
        time.sleep(10) # Batch cooldown

if __name__ == "__main__":
    run_scraper()
