import yfinance as yf
import pandas as pd
import os
from datetime import datetime

# =========================
# STOCK LIST
# =========================
stocks = [
    "RELIANCE.NS",
    "TCS.NS",
    "INFY.NS",
    "HDFCBANK.NS",
    "SBIN.NS"
]

# =========================
# CREATE DATA FOLDER
# =========================
os.makedirs("data", exist_ok=True)

# =========================
# DOWNLOAD DATA
# =========================
today = datetime.now().strftime("%Y-%m-%d")

for stock in stocks:

    print(f"Downloading {stock}...")

    try:
        df = yf.download(
            tickers=stock,
            interval="1m",
            period="1d",
            progress=False
        )

        if df.empty:
            print(f"No data for {stock}")
            continue

        # Reset index
        df.reset_index(inplace=True)

        # Clean filename
        clean_name = stock.replace(".NS", "")

        filename = f"{clean_name}_{today}.csv"

        filepath = os.path.join("data", filename)

        # Save CSV
        df.to_csv(filepath, index=False)

        print(f"Saved: {filepath}")

    except Exception as e:
        print(f"Error downloading {stock}: {e}")

print("DONE")
