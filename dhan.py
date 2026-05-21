import sys
import os
import time
from datetime import datetime, timedelta
import pandas as pd
from kiteconnect import KiteConnect
from kiteconnect.exceptions import TokenException, NetworkException, RateLimitException
import gspread

# ---------------- LOGGING UTILITY ---------------- #
def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

# ---------------- CONFIGURATION & ENVIRONMENT ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_SIZE = int(os.getenv("SHARD_SIZE", "500"))
START_ROW = SHARD_INDEX * SHARD_SIZE
END_ROW = START_ROW + SHARD_SIZE

# Zerodha API Credentials - Safely read from environment setups
API_KEY = os.getenv("ZERODHA_API_KEY", "1n0kjsoryxh6wed1")
ACCESS_TOKEN = os.getenv("ZERODHA_ACCESS_TOKEN", "6R11kEjLTeo9su1E1iwwv4IU5FjvTgPv")

# ---------------- GOOGLE SHEETS CONNECTOR ---------------- #
def connect_sheets():
    try:
        gc = gspread.service_account("credentials.json")
        spreadsheet = gc.open("Tradingview Data Reel Experimental May")
        sh_source = spreadsheet.worksheet("Sheet5")
        sh_target = spreadsheet.worksheet("Sheet18")
        return sh_source, sh_target
    except Exception as e:
        log(f"❌ Critical Google Sheets Connection Error: {e}")
        sys.exit(1)

# ---------------- MAIN EXECUTION ENGINE ---------------- #
# 1. Initialize Google Sheets
sheet_source, sheet_target = connect_sheets()

try:
    headers = [h.strip().lower() for h in sheet_source.row_values(1)]
    symbol_col_idx = headers.index("symbol") + 1 if "symbol" in headers else 1
    symbol_list = sheet_source.col_values(symbol_col_idx)
    log(f"✅ Sheets mapped successfully. Symbol target detected in Column: {symbol_col_idx}")
except Exception as sheet_err:
    log(f"❌ Failed to parse source sheets data: {sheet_err}")
    sys.exit(1)

# 2. Initialize Kite Connect Client
try:
    # Enabled debug output to verify low-level payload responses
    kite = KiteConnect(api_key=API_KEY, debug=True)
    kite.set_access_token(ACCESS_TOKEN)
    log("✅ Zerodha KiteConnect Session Instance loaded.")
    
    log("🔄 Pulling primary Instrument Master maps from Exchange...")
    instruments = kite.instruments("NSE")
    token_map = {inst['tradingsymbol']: inst['instrument_token'] for inst in instruments}
    log("✅ Instrument Master parsed successfully.")
except TokenException as auth_err:
    log(f"🛑 Terminating: Your current API token setup is missing core scope permissions or expired. Error: {auth_err}")
    sys.exit(1)
except Exception as init_err:
    log(f"❌ Unexpected Broker Configuration error: {init_err}")
    sys.exit(1)

# 3. Setup Target Header Schema
try:
    if not sheet_target.row_values(1):
        sheet_target.append_row(["Symbol", "Target Date Label", "Datetime", "Open", "High", "Low", "Close", "Adj Close", "Volume"])
except Exception as e:
    log(f"⚠️ Target sheet header initialization bypass: {e}")

all_rows_payload = []
total_rows = len(symbol_list)
start_idx = max(1, START_ROW)
end_idx = min(END_ROW, total_rows)

# Precompute target range (Max 30 days window)
to_date = datetime.now()
from_date = to_date - timedelta(days=30)

# 4. Engine Scraper Pipeline Loop
for i in range(start_idx, end_idx):
    if i >= len(symbol_list):
        break
        
    raw_symbol = symbol_list[i].strip()
    if not raw_symbol:
        continue

    # Clean ticker configurations
    trading_symbol = raw_symbol.replace(".NS", "").replace(".BSE", "").strip()
    instrument_token = token_map.get(trading_symbol)
    
    if not instrument_token:
        log(f"⚠️ Ticker '{trading_symbol}' was not located in active NSE masters. Skipping.")
        continue
        
    log(f"🔄 [{i+1}/{total_rows}] Scrape Request -> Ticker: {trading_symbol} | Token Identifier: {instrument_token}")
    
    retries = 3
    while retries > 0:
        try:
            records = kite.historical_data(
                instrument_token=instrument_token,
                from_date=from_date,
                to_date=to_date,
                interval="minute"
            )
            
            if not records:
                log(f"   ⚠️ Request structural warning: Empty bars returned for {trading_symbol}.")
                break
                
            df = pd.DataFrame(records)
            for _, row_data in df.iterrows():
                all_rows_payload.append([
                    f"{trading_symbol}.NS",
                    "Last 30 Days",
                    str(row_data['date']),
                    float(row_data['open']),
                    float(row_data['high']),
                    float(row_data['low']),
                    float(row_data['close']),
                    float(row_data['close']), # Fallback mapping configuration
                    int(row_data['volume'])
                ])
            break  # Break retry loop on successful execution
            
        except TokenException as token_fail:
            log(f"🛑 Critical Authentication Exception: Historical endpoints rejected API Token scopes. Execution Terminated -> {token_fail}")
            sys.exit(1)
        except RateLimitException:
            log("   ⚠️ Throttled: Zerodha Rate-limit threshold hit. Cooling down engine execution pipeline...")
            time.sleep(5)
            retries -= 1
        except NetworkException:
            log("   ⚠️ Network dropout detected. Re-attempting execution bridge connection...")
            time.sleep(2)
            retries -= 1
        except Exception as query_err:
            log(f"   ❌ Execution failure on fetching symbol {trading_symbol}: {query_err}")
            break
            
    # Regular spacing buffer to avoid exceeding 3 calls/sec constraints
    time.sleep(0.4)

# 5. Flush Payload Cache to Target Sheet 
if all_rows_payload:
    log(f"🚀 Pushing payload block ({len(all_rows_payload)} records) down to Sheet18...")
    try:
        sheet_target.append_rows(all_rows_payload, value_input_option="RAW")
        log("✅ Scrape run transaction completed successfully.")
    except Exception as commit_err:
        log(f"❌ Sheet push operations tracking failed: {commit_err}")
else:
    log("⚠️ Operational sequence notice: No fresh candle records saved during this batch execution loop cycle.")
