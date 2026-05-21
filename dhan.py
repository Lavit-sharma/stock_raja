import sys
import os
from datetime import date, timedelta
import pandas as pd
import gspread
from jugaad_data.nse import stock_df

def fetch_nse_historical(symbol: str, days_back: int = 30) -> pd.DataFrame:
    """
    Fetches historical equity data using jugaad-data and cleans headers.
    """
    end_date = date.today()
    start_date = end_date - timedelta(days=days_back)
    
    try:
        print(f"[INFO] Extracting {symbol} from {start_date} to {end_date} via jugaad-data...")
        # Fetching standard equity segment series 'EQ'
        df = stock_df(symbol=symbol, from_date=start_date, to_date=end_date, series="EQ")
        
        if df.empty:
            print(f"[WARNING] No data returned for symbol: {symbol}")
            return pd.DataFrame()
            
        # Standardize date format & sort chronologically
        df['DATE'] = pd.to_datetime(df['DATE'])
        df = df.sort_values('DATE').reset_index(drop=True)
        
        # Ensure column labels match standard uppercase tracking expectations
        df.columns = [col.upper() for col in df.columns]
        return df

    except Exception as e:
        print(f"[ERROR] Failed fetching data for {symbol}: {e}")
        return pd.DataFrame()

def upload_to_sheets(df: pd.DataFrame, spreadsheet_name: str, worksheet_name: str):
    """
    Authenticates and updates target worksheet with dataframe metrics.
    """
    if df.empty:
        print("[INFO] Empty dataframe skipped from sheets synchronization.")
        return

    try:
        if not os.path.exists('credentials.json'):
            raise FileNotFoundError("Authentication 'credentials.json' file is missing.")
            
        print(f"[INFO] Connecting to Google Sheets: '{spreadsheet_name}' -> '{worksheet_name}'")
        gc = gspread.service_account(filename='credentials.json')
        sh = gc.open(spreadsheet_name)
        
        try:
            worksheet = sh.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sh.add_worksheet(title=worksheet_name, rows="100", cols="20")

        # Prep payload data (convert timestamp columns safely to string format)
        df_copy = df.copy()
        if 'DATE' in df_copy.columns:
            df_copy['DATE'] = df_copy['DATE'].dt.strftime('%Y-%m-%d')
            
        # Format payload structure [headers, values...]
        payload = [df_copy.columns.values.tolist()] + df_copy.values.tolist()
        
        # Atomically clear old calculations and dump fresh target vectors
        worksheet.clear()
        worksheet.update('A1', payload)
        print("[SUCCESS] Operational metrics safely synchronized onto Google Sheets.")

    except Exception as e:
        print(f"[ERROR] Sheets synchronization layer failure: {e}")

def main():
    # Capture positional flags passing down from GitHub Runner execution string: `update int his`
    args = sys.argv[1:]
    print(f"[START] Running script execution routine with arguments: {args}")
    
    # Target evaluation setup
    target_ticker = "RELIANCE" 
    target_spreadsheet = "NSE_Automation_Dashboard"
    target_worksheet = "Raw_Historical"

    # Match execution criteria flags matching your exact positional runtime call
    if len(args) >= 3 and args[0] == "update" and args[1] == "int" and args[2] == "his":
        print("[PROCESS] Trigger matching execution routine: 'update int his'")
        
        # Pull transactional window historical markers (e.g. 30 days)
        historical_metrics = fetch_nse_historical(symbol=target_ticker, days_back=30)
        
        if not historical_metrics.empty:
            print("\n--- Data Target Extraction Overview ---")
            print(historical_metrics[['DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME']].tail(5))
            
            # Send cleaned datasets downstream to target Google Sheet grids
            upload_to_sheets(historical_metrics, target_spreadsheet, target_worksheet)
    else:
        print("[ERROR] Argument structural routing invalid or unhandled. Expected: 'update int his'")
        sys.exit(1)

if __name__ == "__main__":
    main()
