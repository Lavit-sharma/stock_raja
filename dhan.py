import yfinance as yf
import pandas as pd
import os

# =========================
# STOCK LIST
# =========================
stocks = [
    "RELIANCE.NS",
    "TCS.NS",
    "INFY.NS"
]

# =========================
# CREATE DATA FOLDER
# =========================
os.makedirs("data", exist_ok=True)

# =========================
# DOWNLOAD 1 MINUTE DATA
# =========================
for stock in stocks:

    print(f"Downloading {stock}...")

    df = yf.download(
        tickers=stock,
        interval="1m",
        period="1d",
        progress=False
    )

    # Skip if no data
    if df.empty:
        print(f"No data for {stock}")
        continue

    # Reset index
    df.reset_index(inplace=True)

    # Clean filename
    filename = stock.replace(".NS", "") + "_1min.csv"

    # Save CSV in repo folder
    filepath = os.path.join("data", filename)

    df.to_csv(filepath, index=False)

    print(f"Saved: {filepath}")

print("DONE")
