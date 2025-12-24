import os
import time
import json
import gspread
import concurrent.futures
import requests
from bs4 import BeautifulSoup
from typing import List, Tuple

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

START_INDEX = int(os.getenv("START_INDEX", "0"))
END_INDEX   = int(os.getenv("END_INDEX", "2500"))
CHECKPOINT_FILE = "checkpoint.txt"
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5"))  # Reduced for scraping
BATCH_SIZE = 20  # Smaller batches for scraping

# Session for connection reuse
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
})

def scrape_sector(symbol: str) -> List[str]:
    """Get full sector hierarchy from Screener.in"""
    try:
        url = f"https://www.screener.in/company/{symbol.upper()}/"
        response = session.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        result = {'level1': 'N/A', 'level2': 'N/A', 'level3': 'N/A', 'level4': 'N/A'}
        
        # Company info table
        sector_row = soup.find('td', string='Sector')
        if sector_row:
            result['sector'] = sector_row.find_next_sibling('td').get_text(strip=True)
        
        industry_row = soup.find('td', string='Industry')
        if industry_row:
            result['industry'] = industry_row.find_next_sibling('td').get_text(strip=True)
        
        # Breadcrumb hierarchy (Commodities > Metals & Mining > etc.)
        breadcrumbs = soup.find('ol', class_='breadcrumb') or soup.find('div', class_='company-header')
        if breadcrumbs:
            links = breadcrumbs.find_all('a')
            path = [link.get_text(strip=True) for link in links if link.get_text(strip=True)]
            for i, level in enumerate(path[:4]):
                result[f'level{i+1}'] = level
        
        # Return as list: [symbol, level1, level2, level3, level4, sector, industry]
        return [
            symbol,
            result['level1'],
            result['level2'], 
            result['level3'],
            result['level4'],
            result.get('sector', 'N/A'),
            result.get('industry', 'N/A')
        ]
        
    except Exception as e:
        print(f"   ⚠️ Scrape Error ({symbol}): {str(e)[:50]}")
        return [symbol, 'Error', 'Error', 'Error', 'Error', 'Error', 'Error']

# ---------------- GOOGLE SHEETS ---------------- #
try:
    creds_env = os.getenv("GSPREAD_CREDENTIALS")
    if creds_env:
        gs_client = gspread.service_account_from_dict(json.loads(creds_env))
    else:
        gs_client = gspread.service_account(filename="credentials.json")
        
    source_sheet = gs_client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet   = gs_client.open_by_url(NEW_MV2_URL).worksheet("Sheet10")
    
    full_data = source_sheet.get_all_values()[1:]
    print(f"✅ Connected. Total rows in source: {len(full_data)}")
except Exception as e:
    print(f"❌ Google Sheets Error: {e}")
    raise

# Resume logic
last_i = START_INDEX
if os.path.exists(CHECKPOINT_FILE):
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            last_i = int(f.read().strip())
    except:
        pass

print(f"🔧 Range: {START_INDEX}-{END_INDEX} | Resume: {last_i} | Workers: {MAX_WORKERS}")

# ---------------- PROCESSING ---------------- #
def process_single_row(args: Tuple[int, List[str]]) -> Tuple[List[str], int]:
    idx, row = args
    symbol = row[0].strip()
    result = scrape_sector(symbol)
    print(f"[{idx+1}] {symbol} -> {result[1]}")
    time.sleep(0.5)  # Rate limit per request
    return result, idx

print(f"\n🚀 Starting Screener.in Scraping from index {last_i}...")

to_process = [(i, row) for i, row in enumerate(full_data) if last_i <= i < END_INDEX]
success_count = 0

with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    for batch_start in range(0, len(to_process), BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, len(to_process))
        batch_args = to_process[batch_start:batch_end]
        
        futures = [executor.submit(process_single_row, arg) for arg in batch_args]
        batch_results = []
        
        for future in concurrent.futures.as_completed(futures):
            res, _ = future.result()
            batch_results.append(res)
            if res[1] not in ['N/A', 'Error']:
                success_count += 1
        
        if batch_results:
            try:
                dest_sheet.append_rows(batch_results)
                current_checkpoint = to_process[batch_end-1][0] + 1
                with open(CHECKPOINT_FILE, "w") as f:
                    f.write(str(current_checkpoint))
                print(f"💾 Saved batch ({len(batch_results)} rows). Next: {current_checkpoint}")
            except Exception as e:
                print(f"❌ Write Error: {e}")

print(f"\n🎉 DONE! Processed {len(to_process)} symbols. Success: {success_count}")
session.close()
