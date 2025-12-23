import os
import time
import json
import gspread
import concurrent.futures
from datetime import date
from openai import OpenAI

# ---------------- CONFIG ---------------- #
STOCK_LIST_URL = "https://docs.google.com/spreadsheets/d/1V8DsH-R3vdUbXqDKZYWHk_8T0VRjqTEVyj7PhlIDtG4/edit?gid=0#gid=0"
NEW_MV2_URL    = "https://docs.google.com/spreadsheets/d/1GKlzomaK4l_Yh8pzVtzucCogWW5d-ikVeqCxC6gvBuc/edit?gid=0#gid=0"

START_INDEX = int(os.getenv("START_INDEX", "0"))
END_INDEX   = int(os.getenv("END_INDEX", "2500"))
CHECKPOINT_FILE = "checkpoint.txt"
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))
BATCH_SIZE = 50

# Resume logic
last_i = START_INDEX
if os.path.exists(CHECKPOINT_FILE):
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            last_i = int(f.read().strip())
    except:
        pass

print(f"🔧 Range: {START_INDEX}-{END_INDEX} | Resume: {last_i} | Workers: {MAX_WORKERS}")

# ---------------- GOOGLE SHEETS ---------------- #
try:
    creds_json = os.getenv("GSPREAD_CREDENTIALS")
    if creds_json:
        # Renamed to gs_client to avoid conflict with OpenAI client
        gs_client = gspread.service_account_from_dict(json.loads(creds_json))
    else:
        gs_client = gspread.service_account(filename="credentials.json")
        
    source_sheet = gs_client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet   = gs_client.open_by_url(NEW_MV2_URL).worksheet("Sheet10")
    data_rows = source_sheet.get_all_values()[1:]
    print(f"✅ Connected. Total rows found: {len(data_rows)}")
except Exception as e:
    print(f"❌ Connection Error: {e}")
    raise

# ---------------- AI CLIENT ---------------- #
# Initialize OpenAI client clearly
ai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def ask_ai_sector(symbol):
    try:
        response = ai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a stock market expert. Respond ONLY in format 'Sector: [name] | Industry: [name]'. If unknown, use 'N/A | N/A'."},
                {"role": "user", "content": f"Symbol: {symbol}"}
            ],
            max_tokens=50,
            temperature=0.1
        )
        
        text = response.choices[0].message.content.strip()
        if "Sector:" in text and "|" in text:
            parts = text.split("|")
            sector = parts[0].replace("Sector:", "").strip()
            industry = parts[1].replace("Industry:", "").strip()
            return [sector, industry]
        return ["N/A", "N/A"]
            
    except Exception as e:
        print(f"   ❌ AI Error for {symbol}: {e}")
        return ["N/A", "N/A"]

# ---------------- PROCESSING ---------------- #
def process_single_row(args):
    i, row = args
    symbol = row[0].strip()
    sector, industry = ask_ai_sector(symbol)
    return [symbol, sector, industry], i

print(f"\n🚀 AI MODE: Starting processing...")

to_process = []
for i, row in enumerate(data_rows):
    if last_i <= i < END_INDEX:
        to_process.append((i, row))

print(f"📋 {len(to_process)} symbols remaining")

success_count = 0
start_time = time.time()

with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    for batch_start in range(0, len(to_process), BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, len(to_process))
        batch_args = to_process[batch_start:batch_end]
        
        futures = [executor.submit(process_single_row, args) for args in batch_args]
        
        batch_results = []
        for future in concurrent.futures.as_completed(futures):
            try:
                row_data, orig_i = future.result(timeout=20)
                batch_results.append(row_data)
                if row_data[1] != "N/A":
                    success_count += 1
            except Exception as e:
                print(f"⚠️ Worker Error: {e}")
        
        if batch_results:
            try:
                # Basic write logic: this appends or updates based on your needs
                # For simplicity, we write the batch we just finished
                dest_sheet.append_rows(batch_results)
                print(f"💾 Saved batch up to index {batch_start + len(batch_results)}")
            except Exception as e:
                print(f"❌ Batch write error: {e}")
        
        # Save checkpoint
        with open(CHECKPOINT_FILE, "w") as f:
            f.write(str(last_i + batch_end))

elapsed = time.time() - start_time
print(f"\n🎉 COMPLETE! Processed: {len(to_process)} | Success: {success_count}")
