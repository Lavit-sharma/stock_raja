import os
import time
import json
import gspread
import concurrent.futures
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
    creds_env = os.getenv("GSPREAD_CREDENTIALS")
    if creds_env:
        gs_client = gspread.service_account_from_dict(json.loads(creds_env))
    else:
        gs_client = gspread.service_account(filename="credentials.json")
        
    source_sheet = gs_client.open_by_url(STOCK_LIST_URL).worksheet("Sheet1")
    dest_sheet   = gs_client.open_by_url(NEW_MV2_URL).worksheet("Sheet10")
    
    # Get data and slice it based on range
    full_data = source_sheet.get_all_values()[1:]
    data_rows = full_data  # We will filter during the loop
    print(f"✅ Connected. Total rows in source: {len(full_data)}")
except Exception as e:
    print(f"❌ Google Sheets Connection Error: {e}")
    raise

# ---------------- AI CLIENT ---------------- #
# Initializing with explicit environment variable check
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise ValueError("❌ OPENAI_API_KEY is missing from secrets!")

ai_client = OpenAI(api_key=api_key)

def ask_ai_sector(symbol):
    try:
        response = ai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Respond ONLY as 'Sector: [Name] | Industry: [Name]'. Use 'N/A' if unknown."},
                {"role": "user", "content": f"What is the sector/industry for {symbol}?"}
            ],
            max_tokens=40,
            temperature=0
        )
        text = response.choices[0].message.content.strip()
        if "|" in text:
            parts = [p.split(":")[-1].strip() for p in text.split("|")]
            return parts if len(parts) == 2 else ["N/A", "N/A"]
        return ["N/A", "N/A"]
    except Exception as e:
        print(f"   ⚠️ AI Error ({symbol}): {e}")
        return ["Error", "Error"]

# ---------------- PROCESSING ---------------- #
def process_single_row(args):
    idx, row = args
    symbol = row[0].strip()
    sector, industry = ask_ai_sector(symbol)
    print(f"[{idx+1}] {symbol} -> {sector}")
    return [symbol, sector, industry], idx

print(f"\n🚀 Starting AI Processing from index {last_i}...")

to_process = [(i, row) for i, row in enumerate(data_rows) if last_i <= i < END_INDEX]
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
            if res[1] not in ["N/A", "Error"]:
                success_count += 1
        
        if batch_results:
            try:
                dest_sheet.append_rows(batch_results)
                # Update checkpoint
                current_checkpoint = to_process[batch_end-1][0] + 1
                with open(CHECKPOINT_FILE, "w") as f:
                    f.write(str(current_checkpoint))
                print(f"💾 Saved batch. Next start index: {current_checkpoint}")
            except Exception as e:
                print(f"❌ Write Error: {e}")

print(f"\n🎉 DONE! Processed {len(to_process)} symbols. Success: {success_count}")
