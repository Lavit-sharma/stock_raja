import sys
import os
import time
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf
import gspread

# ---------------- LOG ---------------- #
def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

# ---------------- CONFIG ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_SIZE = int(os.getenv("SHARD_SIZE", "500"))
START_ROW = SHARD_INDEX * SHARD_SIZE
END_ROW = START_ROW + SHARD_SIZE

# ---------------- GOOGLE SHEETS ---------------- #
def connect_sheets():
    gc = gspread.service_account("credentials.json")
    spreadsheet = gc.open("Tradingview Data Reel Experimental May")
    sh_source = spreadsheet.worksheet("Sheet5")
    sh_target = spreadsheet.worksheet("Sheet18")
    return sh_source, sh_target

def parse_date(date_str):
    if not date_str:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None

def get_next_day_string(dt_obj):
    next_day = dt_obj + timedelta(days=1)
    return next_day.strftime("%Y-%m-%d")

# ---------------- MAIN ---------------- #
try:
    sheet_source, sheet_target = connect_sheets()
    headers = [h.strip().lower() for h in sheet_source.row_values(1)]
    
    symbol_col_idx = headers.index("symbol") + 1 if "symbol" in headers else 1
    date1_col_idx = headers.index("date1") + 1 if "date1" in headers else 10
    date2_col_idx = headers.index("date2") + 1 if "date2" in headers else 11
    
    symbol_list = sheet_source.col_values(symbol_col_idx)
    date1_list = sheet_source.col_values(date1_col_idx)
    date2_list = sheet_source.col_values(date2_col_idx)

    log(f"✅ Sheets mapped successfully. Columns found -> Symbol: Col {symbol_col_idx}, Date1: Col {date1_col_idx}, Date2: Col {date2_col_idx}")

except Exception as e:
    log(f"❌ Initialization/Sheet Reading error: {e}")
    sys.exit(1)

# Ensure Sheet18 Headers exist
try:
    if not sheet_target.row_values(1):
        sheet_target.append_row(["Symbol", "Target Date Label", "Interval Used", "Datetime/Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"])
except Exception as e:
    log(f"⚠️ Header setup warning: {e}")

all_rows_payload = []
total_rows = len(symbol_list)
start_idx = max(1, START_ROW)
end_idx = min(END_ROW, total_rows)

for i in range(start_idx, end_idx):
    if i >= len(symbol_list):
        break
        
    raw_symbol = symbol_list[i].strip()
    if not raw_symbol:
        continue

    stock_symbol = raw_symbol if raw_symbol.endswith(".NS") else f"{raw_symbol}.NS"
    raw_d1 = date1_list[i] if i < len(date1_list) else ""
    raw_d2 = date2_list[i] if i < len(date2_list) else ""
    
    target_dates = [("Date1", parse_date(raw_d1)), ("Date2", parse_date(raw_d2))]
    
    log(f"🔄 Processing row [{i+1}/{total_rows}] — Symbol: {stock_symbol}")
    
    for label, dt_obj in target_dates:
        if dt_obj is None:
            log(f"   ⚠️ Missing or invalid format for {label}, skipping.")
            continue
            
        start_str = dt_obj.strftime("%Y-%m-%d")
        end_str = get_next_day_string(dt_obj)
        
        # Calculate row age in days dynamically to avoid triggering hard server exceptions
        days_ago = (datetime.now() - dt_obj).days
        
        df = pd.DataFrame()
        interval_used = "1d"

        # --- STEP 1: Attempt 1-Minute Granularity (Strictly within last 7 Days) ---
        if days_ago <= 7:
            log(f"   Attempting 1m resolution download for {start_str}...")
            try:
                df = yf.download(tickers=stock_symbol, start=start_str, end=end_str, interval="1m", progress=False)
                if not df.empty:
                    interval_used = "1m"
            except Exception:
                df = pd.DataFrame()

        # --- STEP 2: Fallback to 5-Minute Granularity (Strictly within last 60 Days) ---
        if df.empty and days_ago <= 60:
            log(f"   ⚠️ 1m unavailable/restricted. Trying 5m interval extraction...")
            try:
                df = yf.download(tickers=stock_symbol, start=start_str, end=end_str, interval="5m", progress=False)
                if not df.empty:
                    interval_used = "5m"
            except Exception:
                df = pd.DataFrame()

        # --- STEP 3: Historical Fallback to Daily Bar (No Time Restraints) ---
        if df.empty:
            log(f"   ⚠️ Intraday tracking limits exceeded or weekend. Extracting 1d daily bar fallback...")
            interval_used = "1d"
            try:
                # If weekend or holiday, expand the search window by 3 days forward to capture next real candle
                alt_end_str = (dt_obj + timedelta(days=4)).strftime("%Y-%m-%d")
                df = yf.download(tickers=stock_symbol, start=start_str, end=alt_end_str, interval="1d", progress=False)
                if not df.empty:
                    # Keep only the single earliest available date row
                    df = df.iloc[[0]]
            except Exception as daily_err:
                log(f"   ❌ Master API download failed for {stock_symbol}: {daily_err}")
                continue

        if df.empty:
            log(f"   ❌ Zero records returned for {stock_symbol} on {start_str} (Asset delisted or halted)")
            continue
            
        # Clean up Pandas structure if multi-indexed columns return
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
            
        df.reset_index(inplace=True)
        
        # Isolate index column name string dynamically
        time_col = 'Datetime' if 'Datetime' in df.columns else ('Date' if 'Date' in df.columns else df.columns[0])
        
        for _, row_data in df.iterrows():
            ts = str(row_data[time_col])
            
            payload_row = [
                stock_symbol,
                label,
                interval_used,
                ts,
                float(row_data['Open']),
                float(row_data['High']),
                float(row_data['Low']),
                float(row_data['Close']),
                float(row_data['Adj Close']) if 'Adj Close' in row_data else float(row_data['Close']),
                int(row_data['Volume'])
            ]
            all_rows_payload.append(payload_row)
            
    # Minor cooldown pause to mitigate scraping security filters
    time.sleep(0.6)

# Batch execute array upload straight into Sheet18
if all_rows_payload:
    log(f"🚀 Pushing {len(all_rows_payload)} rows of historical metrics to Sheet18...")
    try:
        sheet_target.append_rows(all_rows_payload, value_input_option="RAW")
        log("✅ Upload successfully complete.")
    except Exception as upload_err:
        log(f"❌ Error uploading matrix payload to target layout: {upload_err}")
else:
    log("⚠️ No new datasets fetched inside execution window metrics framework.")

log("🏁 DONE")
