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
END_INDEX   = int(os.getenv("END_INDEX", "10"))  # SMALL for testing
CHECKPOINT_FILE = "checkpoint.txt"
MAX_WORKERS = 1  # SINGLE thread for debugging
BATCH_SIZE = 1   # ONE at a time
RETRY_DELAY = 5

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
    """Robust scraper with FULL logging"""
    print(f"🎯 scrape_sector START: {symbol}")
    
    for attempt in range(retries):
        try:
            print(f"   📡 [{symbol}] Request {attempt+1}/{retries}...")
            session = create_session()
            url = f"https://www.screener.in/company/{symbol.upper()}/"
            
            print(f"   🌐 [{symbol}] GET {url}")
            response = session.get(url, timeout=15)
            print(f"   📊 [{symbol}] HTTP {response.status_code}")
            
            if response.status_code == 429:
                wait_time = (2 ** attempt) * RETRY_DELAY
                print(f"   ⏳ [{symbol}] 429 Rate limit. Wait {wait_time}s...")
                time.sleep(wait_time)
                continue
                
            if response.status_code != 200:
                print(f"   ❌ [{symbol}] HTTP {response.status_code}")
                return [symbol, 'HTTP_Error', 'HTTP_Error', 'HTTP_Error', 'HTTP_Error', 'HTTP_Error', 'HTTP_Error']
            
            print(f"   🔍 [{symbol}] Parsing HTML...")
            soup = BeautifulSoup(response.text, 'html.parser')
            
            result = {'level1': 'N/A', 'level2': 'N/A', 'level3': 'N/A', 'level4': 'N/A', 'sector': 'N/A', 'industry': 'N/A'}
            
            # Company info table
            sector_row = soup.find('td', string='Sector')
            if sector_row:
                result['sector'] = sector_row.find_next_sibling('td').get_text(strip=True)
                print(f"   ✅ [{symbol}] Sector: {result['sector']}")
            
            industry_row = soup.find('td', string='Industry')
            if industry_row:
                result['industry'] = industry_row.find_next_sibling('td').get_text(strip=True)
                print(f"   ✅ [{symbol}] Industry: {result['industry']}")
            
            # Breadcrumb hierarchy
            breadcrumbs_found = False
            for selector in ['ol.breadcrumb', 'div.company-header', 'nav.breadcrumb']:
                breadcrumbs = soup.select_one(selector)
                if breadcrumbs:
                    links = breadcrumbs.find_all('a')
                    path = [link.get_text(strip=True) for link in links if link.get_text(strip=True)]
                    for i, level in enumerate(path[:4]):
                        result[f'level{i+1}'] = level
                    print(f"   ✅ [{symbol}] Breadcrumbs: {path[:4]}")
                    breadcrumbs_found = True
                    break
            
            if not breadcrumbs_found:
                print(f"   ⚠️ [{symbol}] No breadcrumbs found")
            
            # Random delay
            delay = random.uniform(3, 6)
            print(f"   😴 [{symbol}] Delay {delay:.1f}s...")
            time.sleep(delay)
            
            print(f"🎯 scrape_sector SUCCESS: {symbol} -> {list(result.values())[:4]}")
            return [
                symbol, result['level1'], result['level2'], result['level3'], result['level4'],
                result['sector'], result['industry']
            ]
            
        except Exception as e:
            error_msg = str(e)[:50]
            print(f"   ❌ [{symbol}] ERROR: {error_msg}")
            if "timeout" in error_msg.lower() or "connection" in error_msg.lower():
                wait_time = (2 ** attempt) * 2
                print(f"   ⏳ [{symbol}] Connection error. Retry in {wait_time}s...")
                time.sleep(wait_time)
                continue
            break
    
    print(f"🎯 scrape_sector FAILED: {symbol}")
    return [symbol, 'Failed', 'Failed', 'Failed', 'Failed', 'Failed', 'Failed']

# ---------------- GOOGLE SHEETS ---------------- #
print("📊 Connecting to Google Sheets...")
try:
    creds_env = os.getenv("GSPREAD_CREDENTIALS")
    if creds_env:
        gs_client = gspread.service_account_from_dict(json.loads(creds_env))
        print("✅ Using env credentials")
    else:
        gs_client = gspread.service_account(filename="credentials.json")
        print("✅ Using file credentials")
        
    source_sheet = gs_client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet   = gs_client.open_by_url(NEW_MV2_URL).worksheet("Sheet10")
    
    full_data = source_sheet.get_all_values()[1:]
    print(f"✅ Sheets connected! Source rows: {len(full_data)}")
except Exception as e:
    print(f"❌ Google Sheets Error: {e}")
    raise

# Resume logic
last_i = START_INDEX
if os.path.exists(CHECKPOINT_FILE):
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            last_i = int(f.read().strip())
        print(f"✅ Resuming from checkpoint: {last_i}")
    except:
        print("⚠️ Invalid checkpoint, starting from START_INDEX")

print(f"🔧 Config: {START_INDEX}-{END_INDEX} | Resume: {last_i} | Workers: {MAX_WORKERS}")

def process_single_row(args: Tuple[int, List[str]]) -> Tuple[List[str], int]:
    idx, row = args
    symbol = row[0].strip()
    print(f"\n🔍 [{idx+1}/{len(to_process)}] Processing {symbol}")
    
    result = scrape_sector(symbol)
    print(f"✅ [{idx+1}] {symbol} COMPLETE: {result[1]} | {result[2]}")
    return result, idx

# ---------------- MAIN PROCESSING ---------------- #
print(f"\n🚀 Starting SAFE scraping from index {last_i}...")
print("⏳ First request takes 20-30s. Watch progress above...")

to_process = [(i, row) for i, row in enumerate(full_data) if last_i <= i < END_INDEX]
print(f"📋 {len(to_process)} symbols to process")

if not to_process:
    print("✅ Nothing to process (caught up to END_INDEX)")
    exit()

success_count = 0

with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    for batch_start in range(0, len(to_process), BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, len(to_process))
        batch_args = to_process[batch_start:batch_end]
        print(f"\n📦 Processing batch {batch_start//BATCH_SIZE + 1} ({len(batch_args)} symbols)")
        
        futures = [executor.submit(process_single_row, arg) for arg in batch_args]
        batch_results = []
        
        for future in concurrent.futures.as_completed(futures):
            res, idx = future.result()
            batch_results.append(res)
            if res[1] not in ['N/A', 'Error', 'Failed', 'HTTP_Error']:
                success_count += 1
        
        if batch_results:
            try:
                print(f"💾 Writing {len(batch_results)} rows to Sheet10...")
                dest_sheet.append_rows(batch_results)
                current_checkpoint = to_process[batch_end-1][0] + 1
                with open(CHECKPOINT_FILE, "w") as f:
                    f.write(str(current_checkpoint))
                print(f"✅ Batch saved! Checkpoint: {current_checkpoint} | Success: {success_count}")
                print(f"😴 Batch break 10s...")
                time.sleep(10)
            except Exception as e:
                print(f"❌ Write Error: {e}")

print(f"\n🎉 FINISHED! Processed {len(to_process)} | Success: {success_count}")
