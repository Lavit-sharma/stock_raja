import sys
import os
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf
import gspread

def fetch_nse_intraday_1min(symbol: str) -> pd.DataFrame:
    """
    Downloads raw 1-minute intraday data for the last active trading session.
    """
    # Format symbol tracking for Yahoo Finance (.NS extension)
    ticker = f"{symbol.upper()}.NS" if not symbol.endswith(".NS") else symbol.upper()
    
    try:
        print(f"[INFO] Initializing yfinance download for {ticker} (Interval: 1m)...")
        
        # Pull last 1 day of intraday data
        df = yf.download(
            tickers=ticker,
            period="1d",
            interval="1m"
        )
        
        if df.empty:
            print(f"[WARNING] No intraday rows returned for target symbol: {ticker}")
            return pd.DataFrame()
            
        # Clear multi-level row headers injected by newer yfinance versions
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
            
        # Pull Datetime index out into an explicit processing column
        df = df.reset_index()
        
        # Standardize structural key capitalization mapping
        df.rename(columns={
            'Datetime': 'DATE',
            'Open': 'OPEN',
            'High': 'HIGH',
            'Low': 'LOW',
            'Close': 'CLOSE',
            'Volume': 'VOLUME'
        }, inplace=True)
        
        # Filter down strictly to standard core metric properties
        target_cols = ['DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']
        df = df[[col for col in target_cols if col in df.columns]]
        
        print(f"[SUCCESS] Extracted {len(df)} lines of 1-minute timeline data.")
        return df

    except Exception as e:
        print(f"[ERROR] Engine encountered failure during data fetch: {e}")
        return pd.DataFrame()

def upload_to_sheets(df: pd.DataFrame, spreadsheet_name: str, worksheet_name: str):
    """
    Connects to Google Drive Sheets API and pushes array blocks via update_values.
    """
    if df.empty:
        print("[INFO] Synchronization sequence bypassed due to empty dataset.")
        return

    try:
        if not os.path.exists('credentials.json'):
            raise FileNotFoundError("System key resource 'credentials.json' is missing.")
            
        print(f"[INFO] Establishing connection: '{spreadsheet_name}' -> '{worksheet_name}'")
        gc = gspread.service_account(filename='credentials.json')
        sh = gc.open(spreadsheet_name)
        
        try:
            worksheet = sh.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sh.add_worksheet(title=worksheet_name, rows="500", cols="10")

        # Create a deep copy to manipulate field formats safely
        df_copy = df.copy()
        
        # Stringify timestamp values so JSON dump structures accept them
        if 'DATE' in df_copy.columns:
            df_copy['DATE'] = df_copy['DATE'].astype(str)
            
        # Parse payload lists [Headers, Rows...]
        payload = [df_copy.columns.values.tolist()] + df_copy.values.tolist()
        
        # Clear out old metrics completely to prevent tail overlapping
        worksheet.clear()
        
        # Atomically push matrix block onto grid coordinates
        worksheet.update_values('A1', payload)
        print("[SUCCESS] Operational metrics safely synchronized onto Google Sheets.")

    except Exception as e:
        print(f"[ERROR] Sheets synchronization layer failure: {e}")

def main():
    # Process argument flags from execution string
    args = sys.argv[1:]
    print(f"[START] Initializing runner interface with processing flags: {args}")
    
    # Extraction target constants
    target_ticker = "RELIANCE" 
    target_spreadsheet = "NSE_Automation_Dashboard"
    target_worksheet = "Raw_Historical"

    # Evaluate execution string matching 'update int his'
    if len(args) >= 3 and args[0] == "update" and args[1] == "int" and args[2] == "his":
        print("[PROCESS] Match verified: Executing 1-minute core matrix loop...")
        
        # Fetch high-frequency dataset
        intraday_data = fetch_nse_intraday_1min(symbol=target_ticker)
        
        if not intraday_data.empty:
            print("\n--- Data Target Extraction Overview (1-Minute Intervals) ---")
            print(intraday_data.tail(5))
            
            # Flush dataset into sheet grid cells
            upload_to_sheets(intraday_data, target_spreadsheet, target_worksheet)
    else:
        print("[ERROR] Arguments missing or misaligned. Expected pattern: 'update int his'")
        sys.exit(1)

if __name__ == "__main__":
    main()
