import sys
import os
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf
import gspread

def fetch_30d_1min_data(symbol: str) -> pd.DataFrame:
    """
    Downloads 30 days of 1-minute data by chunking requests into 5-day windows
    to bypass yfinance's strict 7-day restriction on 1m intraday data.
    """
    # Append the .NS suffix for National Stock Exchange if not present
    ticker = f"{symbol.upper()}.NS" if not symbol.endswith(".NS") else symbol.upper()
    
    combined_df = []
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    current_start = start_date
    print(f"[INFO] Initializing 30-day 1-minute download sequence for {ticker}...")

    # Loop through the 30 days in 5-day intervals
    while current_start < end_date:
        current_end = min(current_start + timedelta(days=5), end_date)
        
        start_str = current_start.strftime('%Y-%m-%d')
        end_str = current_end.strftime('%Y-%m-%d')
        
        print(f"[PROCESS] Downloading chunk: {start_str} to {end_str}")
        try:
            df_chunk = yf.download(
                tickers=ticker,
                start=start_str,
                end=end_str,
                interval="1m",
                progress=False
            )
            
            if not df_chunk.empty:
                combined_df.append(df_chunk)
        except Exception as e:
            print(f"[WARNING] Failed downloading chunk {start_str} to {end_str}: {e}")
            
        # Move forward to the next chunk interval
        current_start = current_end

    if not combined_df:
        print(f"[ERROR] No intraday rows returned across the entire 30-day window.")
        return pd.DataFrame()

    # Merge all separate data blocks together
    final_df = pd.concat(combined_df)
    
    # Clean multi-level column indexes if injected by yfinance
    if isinstance(final_df.columns, pd.MultiIndex):
        final_df.columns = final_df.columns.get_level_values(0)
        
    # Bring the Datetime index out as a usable data column
    final_df = final_df.reset_index()
    
    # Standardize column naming conventions
    final_df.rename(columns={
        'Datetime': 'DATE',
        'Open': 'OPEN',
        'High': 'HIGH',
        'Low': 'LOW',
        'Close': 'CLOSE',
        'Volume': 'VOLUME'
    }, inplace=True)
    
    # Remove duplicate rows overlapping between date boundaries and sort chronologically
    final_df.drop_duplicates(subset=['DATE'], inplace=True)
    final_df = final_df.sort_values('DATE').reset_index(drop=True)
    
    # Drop columns that are not part of the standard tracking matrix (e.g., Adj Close)
    target_cols = ['DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']
    final_df = final_df[[col for col in target_cols if col in final_df.columns]]
    
    print(f"[SUCCESS] Compilation complete. Total records gathered: {len(final_df)} lines.")
    return final_df

def upload_to_sheets(df: pd.DataFrame, spreadsheet_name: str, worksheet_name: str):
    """
    Connects to Google Sheets and uploads data matrices safely by stripping out 
    un-serializable NaN values.
    """
    if df.empty:
        print("[INFO] Synchronization bypassed due to empty dataset.")
        return

    try:
        if not os.path.exists('credentials.json'):
            raise FileNotFoundError("System key resource 'credentials.json' is missing.")
            
        print(f"[INFO] Connecting to Google Sheets: '{spreadsheet_name}' -> '{worksheet_name}'")
        gc = gspread.service_account(filename='credentials.json')
        sh = gc.open(spreadsheet_name)
        
        try:
            worksheet = sh.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            # Giving it ample room for 30 days of 1-minute data (approx 15,000+ rows)
            worksheet = sh.add_worksheet(title=worksheet_name, rows="15000", cols="10")

        # Create a deep copy to manipulate field formats safely
        df_copy = df.copy()
        
        # ✅ CRITICAL FIX: Convert all NaNs/Null values into clean strings 
        # This prevents the raw math json payload encoder crash.
        df_copy = df_copy.fillna('')
        
        # Clean up timestamps to a clean string format (strips timezone info out)
        if 'DATE' in df_copy.columns:
            df_copy['DATE'] = pd.to_datetime(df_copy['DATE']).dt.strftime('%Y-%m-%d %H:%M:%S')
            
        # Convert all underlying NumPy types (float64/int64) to native Python primitives
        headers = df_copy.columns.tolist()
        rows = df_copy.to_dict(orient='records')
        data_matrix = [[row[col] for col in headers] for row in rows]
        
        payload = [headers] + data_matrix
        
        # Wipe the sheet and push the complete dataset
        print("[PROCESS] Clearing older worksheet elements...")
        worksheet.clear()
        
        print(f"[PROCESS] Uploading payload matrix block ({len(payload)} rows total)...")
        worksheet.update_values('A1', payload)
        print("[SUCCESS] Operational metrics safely synchronized onto Google Sheets.")

    except Exception as e:
        print(f"[ERROR] Sheets synchronization layer failure: {e}")

def main():
    args = sys.argv[1:]
    print(f"[START] Runner active with arguments: {args}")
    
    target_ticker = "RELIANCE" 
    target_spreadsheet = "NSE_Automation_Dashboard"
    target_worksheet = "Raw_Historical"

    if len(args) >= 3 and args[0] == "update" and args[1] == "int" and args[2] == "his":
        # Extract the deep 30-day 1-minute matrix
        thirty_day_intraday = fetch_30d_1min_data(symbol=target_ticker)
        
        if not thirty_day_intraday.empty:
            print("\n--- Data Target Extraction Overview (Tail View) ---")
            print(thirty_day_intraday.tail(5))
            
            # Send data over to Google Sheets
            upload_to_sheets(thirty_day_intraday, target_spreadsheet, target_worksheet)
    else:
        print("[ERROR] Argument structural routing invalid. Expected: 'update int his'")
        sys.exit(1)

if __name__ == "__main__":
    main()
```</Response>
