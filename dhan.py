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

# Zerodha API credentials - Read from GitHub secrets / environment variables
API_KEY = os.getenv("ZERODHA_API_KEY", "your_api_key_here")
ACCESS_TOKEN = os.getenv("ZERODHA_ACCESS_TOKEN", "your_daily_access_token_here")

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
    
    # Read headers to locate dynamic column mappings dynamically
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
    # Map layout: {'20MICRONS': 356865, 'INFY': 408065, ...}
    token_map = {inst['tradingsymbol']: inst['instrument_token'] for inst in instruments}
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

# Pre-calculate the historical window (Kite allows up to 60 continuous days of 1-minute data in a single call)
to_date = datetime.now()
from_date = to_date - timedelta(days=30)

for i in range(start_idx, end_idx):
    if i >= len(symbol_list):
        break
        
    raw_symbol = symbol_list[i].strip()
    if not raw_symbol:
        continue

    # Clean the symbol name (Kite requires pure tradingsymbol without exchange extensions like '.NS')
    trading_symbol = raw_symbol.replace(".NS", "").replace(".BSE", "").strip()
    
    # Lookup the symbol's numeric token inside our compiled map dictionary
    instrument_token = token_map.get(trading_symbol)
    
    if not instrument_token:
        log(f"⚠️ Symbol '{trading_symbol}' not found in NSE instrument master list. Skipping.")
        continue
    
    log(f"🔄 Processing row [{i+1}/{total_rows}] — Symbol: {trading_symbol} (Token: {instrument_token})")
    log(f"   Downloading last 30 days of 1m candle data from Zerodha...")
    
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
        
        # Format and convert dataframes into simple row arrays for Google Sheets upload
        for _, row_data in df.iterrows():
            # Convert timestamp object to clean readable string format
            ts = str(row_data['date'])
            
            payload_row = [
                f"{trading_symbol}.NS", # Keeps symbol layout fully identical for your target worksheet 
                "Last 30 Days", 
                ts,
                float(row_data['open']),
                float(row_data['high']),
                float(row_data['low']),
                float(row_data['close']),
                float(row_data['close']), # Zerodha does not compute standalone 'Adj Close' - map close here
                int(row_data['volume'])
            ]
            all_rows_payload.append(payload_row)
            
    except Exception as data_err:
        log(f"   ❌ Error executing Zerodha data fetch for {trading_symbol}: {data_err}")
        
    # Rate limit buffer: Restricts pacing to safe limits within Kite API ceilings
    time.sleep(0.5)

# Write output records iteratively using efficient multi-row append actions
if all_rows_payload:
    log(f"🚀 Pushing {len(all_rows_payload)} technical rows into Sheet18...")
    try:
        sheet_target.append_rows(all_rows_payload, value_input_option="RAW")
        log("✅ Upload successfully complete.")
    except Exception as upload_err:
        log(f"❌ Error uploading metrics array to target worksheet: {upload_err}")
else:
    log("⚠️ No new datasets fetched inside execution window metrics framework.")

log("🏁 DONE")
