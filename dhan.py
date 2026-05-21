import sys
import os
import time
from datetime import datetime, timedelta
import pandas as pd
from kiteconnect import KiteConnect
import gspread

# ---------------- LOG ---------------- #
def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

# ---------------- CONFIG & CREDENTIALS ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_SIZE = int(os.getenv("SHARD_SIZE", "500"))
START_ROW = SHARD_INDEX * SHARD_SIZE
END_ROW = START_ROW + SHARD_SIZE

# Updated variable configurations
API_KEY = os.getenv("API_KEY", "your_api_key_here")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN", "your_daily_access_token_here")

# ---------------- GOOGLE SHEETS ---------------- #
def connect_sheets():
    gc = gspread.service_account("credentials.json")
    
    # Open the single spreadsheet workbook
    spreadsheet = gc.open("Tradingview Data Reel Experimental May")
    
    sh_source = spreadsheet.worksheet("Sheet5")
    sh_target = spreadsheet.worksheet("Sheet18")
    
    return sh_source, sh_target

# ---------------- MAIN ---------------- #
try:
    sheet_source, sheet_target = connect_sheets()
    
    # Read headers to locate dynamic column mappings
    headers = [h.strip().lower() for h in sheet_source.row_values(1)]
    
    # Match the specified column: 'symbol'
    symbol_col_idx = headers.index("symbol") + 1 if "symbol" in headers else 1
    
    # Extract column values
    symbol_list = sheet_source.col_values(symbol_col_idx)

    log(f"✅ Sheets mapped successfully. Columns found -> Symbol: Col {symbol_col_idx}")

except Exception as e:
    log(f"❌ Initialization/Sheet Reading error: {e}")
    sys.exit(1)

# Initialize Zerodha KiteConnect Client
try:
    kite = KiteConnect(api_key=API_KEY)
    kite.set_access_token(ACCESS_TOKEN)
    log("✅ Zerodha KiteConnect Session initialized.")
    
    # Fetch Instrument Master once to dynamically map symbols to Instrument Tokens
    log("🔄 Downloading Zerodha Instrument Master...")
    instruments = kite.instruments("NSE")
    
    # Robust map layout handling spaces/stripping cleanly
    token_map = {inst['tradingsymbol'].strip().upper(): inst['instrument_token'] for inst in instruments}
    log("✅ Instrument Master processed successfully.")
except Exception as kite_err:
    log(f"❌ Zerodha Client Configuration Error: {kite_err}")
    sys.exit(1)

# Prepare target headers on Sheet18 if it's brand new/empty
try:
    if not sheet_target.row_values(1):
        sheet_target.append_row(["Symbol", "Target Date Label", "Datetime", "Open", "High", "Low", "Close", "Adj Close", "Volume"])
except Exception as e:
    log(f"⚠️ Header setup warning: {e}")

all_rows_payload = []

# Process rows within shard boundaries (skipping header row 0 in index calculation)
total_rows = len(symbol_list)
start_idx = max(1, START_ROW)
end_idx = min(END_ROW, total_rows)

# Pre-calculate the historical window (Kite allows up to 60 days of 1-minute data)
to_date = datetime.now()
from_date = to_date - timedelta(days=30)

for i in range(start_idx, end_idx):
    if i >= len(symbol_list):
        break
        
    raw_symbol = symbol_list[i].strip()
    if not raw_symbol or raw_symbol.lower() == "symbol":
        continue

    # Clean symbol variations reliably (.NS, .BSE, whitespaces)
    trading_symbol = raw_symbol.split('.')[0].strip().upper()
    
    # Lookup the symbol's numeric token inside our compiled map dictionary
    instrument_token = token_map.get(trading_symbol)
    
    if not instrument_token:
        log(f"⚠️ Symbol '{trading_symbol}' not found in NSE instrument master list. Skipping.")
        continue
    
    log(f"🔄 Processing row [{i+1}/{total_rows}] — Symbol: {trading_symbol} (Token: {instrument_token})")
    
    try:
        # Fetching historical candles from Zerodha API
        records = kite.historical_data(
            instrument_token=instrument_token,
            from_date=from_date,
            to_date=to_date,
            interval="minute"
        )
        
        if not records:
            log(f"   ❌ No 1m records retrieved for {trading_symbol} in the last 30 days.")
            continue
            
        df = pd.DataFrame(records)
        
        # Format and convert dataframes into simple row arrays
        for _, row_data in df.iterrows():
            # Stripping timezone details off datetime so Google Sheets formats it nicely as a native timestamp
            dt_naive = row_data['date'].replace(tzinfo=None) if hasattr(row_data['date'], 'tzinfo') else pd.to_datetime(row_data['date']).replace(tzinfo=None)
            ts = dt_naive.strftime('%Y-%m-%d %H:%M:%S')
            
            payload_row = [
                f"{trading_symbol}.NS", 
                "Last 30 Days", 
                ts,
                float(row_data['open']),
                float(row_data['high']),
                float(row_data['low']),
                float(row_data['close']),
                float(row_data['close']), 
                int(row_data['volume'])
            ]
            all_rows_payload.append(payload_row)
            
    except Exception as data_err:
        log(f"   ❌ Error executing Zerodha data fetch for {trading_symbol}: {data_err}")
        
    # Rate limit buffer: Safe pacing window for Kite API
    time.sleep(0.35)

# Write output records using safe, chunked upload loops to protect Gspread constraints
if all_rows_payload:
    total_payload_size = len(all_rows_payload)
    log(f"🚀 Pushing {total_payload_size} technical rows into Sheet18...")
    
    # Split upload into chunks of 30,000 rows to prevent Google API timeouts/payload rejections
    chunk_size = 30000
    for chunk_start in range(0, total_payload_size, chunk_size):
        chunk_end = min(chunk_start + chunk_size, total_payload_size)
        sub_payload = all_rows_payload[chunk_start:chunk_end]
        
        try:
            sheet_target.append_rows(sub_payload, value_input_option="USER_ENTERED")
            log(f"   📊 Chunk [{chunk_start}/{total_payload_size}] written successfully.")
            time.sleep(1) # Small breather between major sheet writes
        except Exception as upload_err:
            log(f"   ❌ Error uploading chunk range [{chunk_start}:{chunk_end}]: {upload_err}")
            
    log("✅ Upload operation finished.")
else:
    log("⚠️ No new datasets fetched inside execution window metrics framework.")

log("🏁 DONE")
