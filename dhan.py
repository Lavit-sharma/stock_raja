
import os
import time
from datetime import datetime, timedelta

import yfinance as yf
import pandas as pd
import gspread

# =========================================
# DEBUG
# =========================================

print("FILE EXISTS:", os.path.exists("credentials.json"))

with open("credentials.json", "r") as f:
    print(f.read()[:300])

# =========================================
# GOOGLE SHEETS
# =========================================

gc = gspread.service_account(
    filename="credentials.json"
)

sheet = gc.open("STOCKLIST 2").worksheet("Sheet1")

print("✅ Sheet Connected")

# =========================================
# READ ALL DATA
# =========================================

rows = sheet.get_all_records()

# =========================================
# START COLUMN
# =========================================

START_COL = 11  # K

# =========================================
# CLEAR OLD OUTPUT
# =========================================

sheet.batch_clear(["K:ZZ"])

# =========================================
# PROCESS EACH SYMBOL
# =========================================

for idx, row in enumerate(rows):

    try:

        # =========================================
        # SYMBOL
        # =========================================

        symbol = str(row["symbol"]).strip()

        if not symbol:
            continue

        # =========================================
        # DATES
        # =========================================

        date1 = str(row["date1"]).strip()
        date2 = str(row["date2"]).strip()

        print(f"\n========================")
        print(f"Processing: {symbol}")
        print(f"Date1: {date1}")
        print(f"Date2: {date2}")

        # =========================================
        # BASE ROW
        # =========================================

        base_row = (idx * 500) + 1

        # =========================================
        # DOWNLOAD FUNCTION
        # =========================================

        def process_date(target_date, start_row, title):

            if not target_date:
                return

            print(f"\nDownloading {title}")

            # =========================================
            # DATE CONVERT
            # =========================================

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

            # =========================================
            # EMPTY CHECK
            # =========================================

            if df.empty:
                print(f"No data found for {symbol}")
                return

            # =========================================
            # RESET INDEX
            # =========================================

            df.reset_index(inplace=True)

            # =========================================
            # REQUIRED COLUMNS ONLY
            # =========================================

            df = df[[
                "Datetime",
                "Close",
                "Volume"
            ]]

            # =========================================
            # PREPARE VALUES
            # =========================================

            values = []

            values.append([
                f"{symbol} - {title} - {target_date}"
            ])

            values.append([
                "Datetime",
                "Close",
                "Volume"
            ])

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
        # DATE1
        # =========================================

        process_date(
            date1,
            base_row,
            "DATE1"
        )

        # =========================================
        # DATE2
        # =========================================

        process_date(
            date2,
            base_row + 220,
            "DATE2"
        )

        time.sleep(2)

    except Exception as e:

        print(f"\n❌ ERROR: {e}")

print("\n✅ DONE")
