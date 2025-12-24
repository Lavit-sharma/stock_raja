import os
import time
import json
import gspread
import concurrent.futures
import requests
from bs4 import BeautifulSoup
import random
from typing import List, Tuple
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

START_INDEX = int(os.getenv("START_INDEX", "0"))
END_INDEX   = int(os.getenv("END_INDEX", "2500"))
CHECKPOINT_FILE = "checkpoint.txt"
MAX_WORKERS = 2  # Reduced drastically
BATCH_SIZE = 5   # Tiny batches
RETRY_DELAY = 5  # Seconds between retries

# Rotating user agents
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0'
]

def create_session():
    """Create fresh session with random UA"""
    session = requests.Session()
    ua = random.choice(USER_AGENTS)
    session.headers.update({
        'User-Agent': ua,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    })
    return session

def scrape_sector(symbol: str, retries: int = 3) -> List[str]:
    """Robust scraper with retries + exponential backoff"""
    for attempt in range(retries):
        try:
            session = create_session()
            url = f"https://www.screener.in/company/{symbol.upper()}/"
            
            response = session.get(url, timeout=15)
            
            if response.status_code == 429:  # Too Many Requests
                wait_time = (2 ** attempt) * RETRY_DELAY
                print(f"⏳ 429 Rate limit. Waiting {wait_time}s... ({symbol})")
                time.sleep(wait_time)
                continue
                
            if response.status_code != 200:
                print(f"⚠️ HTTP {response.status_code} ({symbol})")
                return [symbol, 'HTTP_Error', 'HTTP_Error', 'HTTP_Error', 'HTTP_Error', 'HTTP_Error', 'HTTP_Error']
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            result = {'level1': 'N/A', 'level2': 'N/A', 'level3': 'N/A', 'level4': 'N/A', 'sector': 'N/A', 'industry': 'N/A'}
            
            # Company info table
            sector_row = soup.find('td', string='Sector')
            if sector_row:
                result['sector'] = sector_row.find_next_sibling('td').get_text(strip=True)
            
            industry_row = soup.find('td', string='Industry')
            if industry_row:
                result['industry'] = industry_row.find_next_sibling('td').get_text(strip=True)
            
            # Breadcrumb (try multiple selectors)
            for selector in ['ol.breadcrumb', 'div.company-header', 'nav.breadcrumb']:
                breadcrumbs = soup.select_one(selector)
                if breadcrumbs:
                    links = breadcrumbs.find_all('a')
                    path = [link.get_text(strip=True) for link in links if link.get_text(strip=True)]
                    for i, level in enumerate(path[:4]):
                        result[f'level{i+1}'] = level
                    break
            
            # Rate limit + random delay
            time.sleep(random.uniform(3, 6))  # 3-6 seconds between requests
            
            return [
                symbol, result['level1'], result['level2'], result['level3'], result['level4'],
                result['sector'], result['industry']
            ]
            
        except Exception as e:
            error_msg = str(e)[:50]
            if "timeout" in error_msg.lower() or "connection" in error_msg.lower():
                wait_time = (2 ** attempt) * 2
                print(f"⏳ Connection error. Retry {attempt+1}/{retries} in {wait_time}s ({symbol})")
                time.sleep(wait_time)
                continue
            print(f"⚠️ Scrape Error ({symbol}): {error_msg}")
            break
    
    return [symbol, 'Failed', 'Failed', 'Failed', 'Failed', 'Failed', 'Failed']

# ---------------- GOOGLE SHEETS (same as before) ---------------- #
try:
    creds_env = os.getenv("GSPREAD_CREDENTIALS")
    if creds_env:
        gs_client = gspread.service_account_from_dict(json.loads(creds_env))
    else:
        gs_client = gspread.service_account(filename="credentials.json")
        
    source_sheet = gs_client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet   = gs_client.open_by_url(NEW_MV2_URL).worksheet("Sheet13")
    
    full_data = source_sheet.get_all_values()[1:]
    print(f"✅ Connected. Total rows: {len(full_data)}")
except Exception as e:
    print(f"❌ Google Sheets Error: {e}")
    raise

# Resume logic (same)
last_i = START_INDEX
if os.path.exists(CHECKPOINT_FILE):
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            last_i = int(f.read().strip())
    except:
        pass

print(f"🔧 Range: {START_INDEX}-{END_INDEX} | Resume: {last_i} | Workers: {MAX_WORKERS}")

def process_single_row(args: Tuple[int, List[str]]) -> Tuple[List[str], int]:
    idx, row = args
    symbol = row[0].strip()
    result = scrape_sector(symbol)
    print(f"[{idx+1}] {symbol} -> {result[1]} | {result[2]}")
    return result, idx

# ---------------- PROCESSING (slower but stable) ---------------- #
print(f"\n🚀 Starting SAFE scraping from index {last_i}...")

to_process = [(i, row) for i, row in enumerate(full_data) if last_i <= i < END_INDEX]

with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    for batch_start in range(0, len(to_process), BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, len(to_process))
        batch_args = to_process[batch_start:batch_end]
        
        futures = [executor.submit(process_single_row, arg) for arg in batch_args]
        batch_results = []
        
        for future in concurrent.futures.as_completed(futures):
            res, _ = future.result()
            batch_results.append(res)
        
        if batch_results:
            try:
                dest_sheet.append_rows(batch_results)
                current_checkpoint = to_process[batch_end-1][0] + 1
                with open(CHECKPOINT_FILE, "w") as f:
                    f.write(str(current_checkpoint))
                print(f"💾 Saved batch ({len(batch_results)} rows). Next: {current_checkpoint}")
                time.sleep(10)  # 10s break between batches
            except Exception as e:
                print(f"❌ Write Error: {e}")

print(f"\n🎉 DONE! Processed {len(to_process)} symbols.")
