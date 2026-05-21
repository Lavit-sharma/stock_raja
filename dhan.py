import pandas as pd
from datetime import date, timedelta
from jugaad_data.nse import stock_df, NSELive

def get_historical_data(symbol: str, days_back: int = 30, series: str = "EQ") -> pd.DataFrame:
    """
    Fetches historical stock data from NSE and returns a clean Pandas DataFrame.
    """
    end_date = date.today()
    start_date = end_date - timedelta(days=days_back)
    
    try:
        print(f"Fetching historical data for {symbol} ({start_date} to {end_date})...")
        # Pulls data directly into a dataframe
        df = stock_df(
            symbol=symbol, 
            from_date=start_date, 
            to_date=end_date, 
            series=series
        )
        
        if df.empty:
            print(f"No data found for {symbol} in the given range.")
            return pd.DataFrame()
            
        # Clean up columns and sort by date chronologically
        df['DATE'] = pd.to_datetime(df['DATE'])
        df = df.sort_values('DATE').reset_index(drop=True)
        return df
        
    except Exception as e:
        print(f"Error fetching historical data: {e}")
        return pd.DataFrame()

def get_live_quote(symbol: str):
    """
    Fetches live market snapshot data for a specific symbol.
    """
    live = NSELive()
    try:
        print(f"\nFetching live quote for {symbol}...")
        quote = live.stock_quote(symbol)
        
        # Extract metadata and price info
        meta = quote.get('metadata', {})
        price_info = quote.get('priceInfo', {})
        
        print(f"Company: {meta.get('companyName', symbol)}")
        print(f"Last Price: ₹{price_info.get('lastPrice')}")
        print(f"Change: {price_info.get('change')} ({price_info.get('pChange')}%)")
        print(f"Open: ₹{price_info.get('open')} | High: ₹{price_info.get('intraDayHighLow', {}).get('max')}")
        
        return quote
    except Exception as e:
        print(f"Error fetching live quote: {e}")
        return None

if __name__ == "__main__":
    # Target Symbol
    ticker = "RELIANCE"
    
    # 1. Get Historical Data (Last 15 days)
    historical_df = get_historical_data(symbol=ticker, days_back=15)
    
    if not historical_df.empty:
        print("\n--- Historical Data Preview ---")
        # Displaying key columns typically used for technical filtering
        preview_cols = ['DATE', 'OPEN', 'HIGH', 'LOW', 'CLOSE', 'VOLUME', 'TOTALTRADES']
        # Filter columns to only those that exist in the dataframe to prevent KeyErrors
        available_cols = [col for col in preview_cols if col in historical_df.columns]
        print(historical_df[available_cols].tail(5))
    
    # 2. Get Live Quote Data
    live_data = get_live_quote(symbol=ticker)
