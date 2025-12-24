import os
import time
import json
import gspread
import concurrent.futures
import requests
from bs4 import BeautifulSoup
import random
from typing import List, Tuple

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

START_INDEX = int(os.getenv("START_INDEX", "0"))
END_INDEX   = int(os.getenv("END_INDEX", "2500"))
CHECKPOINT_FILE = "checkpoint.txt"
MAX_WORKERS = 2      # Safe for production
BATCH_SIZE = 10      # Safe batch size

def load_cookies():
    """YOUR Screener.in cookies - HARDCODED"""
    cookies = [
        {"name": "sessionid", "value": "2yzlslz6eofrfic3shh9ajm0prxqxbvt"},
        {"name": "csrftoken", "value": "WDXkXHPnc6F6VNM4L7Kym7yC7pmby0HX"}
    ]
    print(f"✅ Loaded {len(cookies)} cookies (sessionid + csrftoken)")
    return {c['name']: c['value'] for c in cookies}

# Setup session WITH YOUR COOKIES
cookies = load_cookies()
session = requests.Session()
session.cookies.update(cookies)
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'https://www.screener.in/',
    'Connection': 'keep-alive',
})

def scrape_sector(symbol: str) -> List[str]:
    """✅ PRODUCTION: Handles 429 + Real sectors"""
    for retry in range(2):  # 2 retries per symbol
        try:
            url = f"https://www.screener.in/company/{symbol.upper()}/"
            response = session.get(url, timeout=12)
            
            # ✅ 429 HANDLING
            if response.status_code == 429:
                wait_time = (retry + 1) * 30  # 30s, 60s
                print(f"⏳ [{symbol}] 429 Rate limited - wait {wait_time}s (retry {retry+1}/2)")
                time.sleep(wait_time)
                continue
            
            if response.status_code != 200:
                print(f"⚠️ [{symbol}] HTTP {response.status_code}")
                return [symbol, f"HTTP_{response.status_code}"] * 7
            
            soup = BeautifulSoup(response.text, 'html.parser')
            result = [symbol, 'N/A', 'N/A', 'N/A', 'N/A', 'N/A', 'N/A']
            
            # ✅ PRIORITY 1: Breadcrumb market links
            breadcrumb = soup.find('nav', class_='u-p-0') or soup.find('ol', class_='breadcrumb')
            if breadcrumb:
                links = breadcrumb.find_all('a', href=lambda x: x and '/market/' in x)
                path = [link.get_text(strip=True) for link in links]
                for i, level in enumerate(path[:4]):
                    result[i+1] = level
            
            # ✅ PRIORITY 2: All market links (backup)
            if result[1] == 'N/A':
                market_links = soup.find_all('a', href=lambda x: x and '/market/' in x)
                path = [link.get_text(strip=True) for link in market_links[:4]]
                for i, level in enumerate(path):
                    result[i+1] = level
            
            # ✅ PRIORITY 3: Company info table
            table = soup.find('table')
            if table:
                for row in table.find_all('tr')[:15]:  # First 15 rows
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        label = cells[0].get_text(strip=True).lower()
                        value = cells[1].get_text(strip=True)
                        if 'sector' in label:
                            result[5] = value
                        if 'industry' in label or 'group' in label:
                            result[6] = value
            
            # ✅ Success counter
            success_levels = sum(1 for x in result[1:5] if x != 'N/A')
            if success_levels > 0:
                print(f"✅ [{symbol}] {success_levels}/4 levels: {result[1:5]}")
            else:
                print(f"⚠️ [{symbol}] No sectors found")
            
            time.sleep(random.uniform(2.5, 4.0))  # 2.5-4s delay (SAFE)
            return result
            
        except Exception as e:
            print(f"❌ [{symbol}] Error: {str(e)[:30]}")
            time.sleep(10)
            continue
    
    print(f"⏳ [{symbol}] Rate Limited - SKIPPED")
    return [symbol, 'Rate_Limited'] * 7

# ---------------- GOOGLE SHEETS ---------------- #
try:
    creds_env = os.getenv("GSPREAD_CREDENTIALS")
    if creds_env:
        gs_client = gspread.service_account_from_dict(json.loads(creds_env))
        print("✅ Google Sheets: Env credentials")
    else:
        gs_client = gspread.service_account(filename="credentials.json")
        print("✅ Google Sheets: File credentials")
        
    source_sheet = gs_client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet = gs_client.open_by_url(NEW_MV2_URL).worksheet("Sheet13")
    full_data = source_sheet.get_all_values()[1:]
    print(f"✅ Sheets ready | Source rows: {len(full_data)} | Target: Sheet13")
except Exception as e:
    print(f"❌ Sheets ERROR: {e}")
    raise

# ---------------- RESUME LOGIC ---------------- #
last_i = START_INDEX
if os.path.exists(CHECKPOINT_FILE):
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            last_i = int(f.read().strip())
        print(f"✅ RESUME from checkpoint: {last_i}")
    except:
        print("⚠️ Checkpoint invalid, using START_INDEX")

print(f"🔧 Range: {START_INDEX}-{END_INDEX} | Resume: {last_i} | Workers: {MAX_WORKERS}")

def process_row(args: Tuple[int, List[str]]) -> List[str]:
    idx, row = args
    symbol = row[0].strip()
    result = scrape_sector(symbol)
    print(f"[{idx+1:3d}] {symbol:10s}: {result[1]:20s} > {result[2]}")
    return result

# ---------------- MAIN PROCESSING ---------------- #
to_process = [(i, row) for i, row in enumerate(full_data) if last_i <= i < END_INDEX]
total_symbols = len(to_process)
print(f"\n🚀 STARTING {total_symbols} symbols → **Sheet13**")
print(f"⏱️  ETA: ~{total_symbols*3.5/60:.1f} hours (safe speed)")

success_count = 0
batch_num = 0

with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    for batch_start in range(0, len(to_process), BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, len(to_process))
        batch_args = to_process[batch_start:batch_end]
        batch_num += 1
        
        print(f"\n📦 BATCH {batch_num} ({len(batch_args)} symbols)")
        futures = [executor.submit(process_row, arg) for arg in batch_args]
        batch_results = [future.result() for future in concurrent.futures.as_completed(futures)]
        
        # Count successes
        batch_success = sum(1 for r in batch_results if r[1] not in ['N/A', 'Rate_Limited', 'HTTP_429', 'Error'])
        success_count += batch_success
        
        if batch_results:
            try:
                dest_sheet.append_rows(batch_results)
                current_checkpoint = to_process[batch_end-1][0] + 1
                with open(CHECKPOINT_FILE, "w") as f:
                    f.write(str(current_checkpoint))
                
                progress = (batch_end / total_symbols) * 100
                print(f"💾 **Sheet13**: {len(batch_results)} rows | "
                      f"Progress: {progress:.1f}% | Success: {success_count}/{batch_end}")
                print(f"📍 Next checkpoint: {current_checkpoint}")
                
                # Batch break
                print("😴 15s batch break...")
                time.sleep(15)
                
            except Exception as e:
                print(f"❌ Sheet13 write ERROR: {e}")

print(f"\n🎉 **Sheet13** COMPLETE!")
print(f"📊 Total: {total_symbols} | Success: {success_count} | Rate: {success_count/total_symbols*100:.1f}%")
print(f"📍 Final checkpoint: {last_i + total_symbols}")
