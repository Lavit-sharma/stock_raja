import os
import time
from datetime import datetime, timedelta

import yfinance as yf
import pandas as pd
import gspread

# =========================================
# GOOGLE SHEETS
# =========================================

gc = gspread.service_account("credentials.json")

sheet = gc.open("STOCKLIST 2").worksheet("Sheet1")

# =========================================
# READ ALL DATA
# =========================================

rows = sheet.get_all_records()

print("Sheet Connected")

# =========================================
# START COLUMN
# =========================================

# K = 11
START_COL = 11

# =========================================
# CLEAR OLD DATA
# =========================================

sheet.batch_clear(["K:ZZ"])

# =========================================
# PROCESS EACH ROW
# =========================================

for idx, row in enumerate(rows):

    try:

        # =========================================
        # READ SYMBOL
        # =========================================

        symbol = row["symbol"]

        # =========================================
        # READ DATES
        # =========================================

        date1 = str(row["date1"]).strip()
        date2 = str(row["date2"]).strip()

        print(f"\nProcessing {symbol}")

        # =========================================
        # ROW POSITION
        # =========================================

        base_row = (idx * 500) + 1

        # =========================================
        # FUNCTION
        # =========================================

        def download_and_store(target_date, start_row, title):

            if not target_date:
                return

            print(f"Downloading {title} : {target_date}")

            start_date = datetime.strptime(
                target_date,
                "%Y-%m-%d"
            )

            end_date = start_date + timedelta(days=1)

            # =========================================
            # DOWNLOAD
            # =========================================

            df = yf.download(
                tickers=symbol,
                interval="1m",
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d"),
                progress=False
            )

            if df.empty:
                print(f"No data for {symbol}")
                return

            # =========================================
            # RESET INDEX
            # =========================================

            df.reset_index(inplace=True)

            # =========================================
            # KEEP ONLY REQUIRED
            # =========================================

            df = df[[
                "Datetime",
                "Close",
                "Volume"
            ]]

            # =========================================
            # HEADER
            # =========================================

            values = []

            values.append(
                [f"{symbol} - {title} - {target_date}"]
            )

            values.append([
                "Datetime",
                "Close",
                "Volume"
            ])

            # =========================================
            # DATA ROWS
            # =========================================

            for _, r in df.iterrows():

                values.append([
                    str(r["Datetime"]),
                    float(r["Close"]),
                    int(r["Volume"])
                ])

            # =========================================
            # STORE IN SHEET
            # =========================================

            sheet.update(
                f"K{start_row}",
                values
            )

            print(f"Stored {title}")

        # =========================================
        # STORE DATE1
        # =========================================

        download_and_store(
            date1,
            base_row,
            "DATE1"
        )

        # =========================================
        # STORE DATE2
        # =========================================

        download_and_store(
            date2,
            base_row + 220,
            "DATE2"
        )

        time.sleep(2)

    except Exception as e:

        print(f"Error: {e}")

print("\nDONE")
