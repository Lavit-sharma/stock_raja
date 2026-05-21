```python
import sys
import os
import time
from datetime import datetime, timedelta

import pandas as pd
import gspread

from kiteconnect import KiteConnect
from kiteconnect.exceptions import (
    TokenException,
    NetworkException,
    InputException,
    DataException
)

# =========================================================
# LOGGING
# =========================================================
def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# =========================================================
# SHARD CONFIGURATION
# =========================================================
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_SIZE = int(os.getenv("SHARD_SIZE", "500"))

START_ROW = SHARD_INDEX * SHARD_SIZE
END_ROW = START_ROW + SHARD_SIZE


# =========================================================
# ZERODHA CREDENTIALS (FROM GITHUB SECRETS)
# =========================================================
API_KEY = os.getenv("ZERODHA_API_KEY")
ACCESS_TOKEN = os.getenv("ZERODHA_ACCESS_TOKEN")

if not API_KEY:
    raise Exception("❌ Missing GitHub Secret: ZERODHA_API_KEY")

if not ACCESS_TOKEN:
    raise Exception("❌ Missing GitHub Secret: ZERODHA_ACCESS_TOKEN")


# =========================================================
# GOOGLE SHEETS CONNECTION
# =========================================================
def connect_sheets():
    try:
        gc = gspread.service_account("credentials.json")

        spreadsheet = gc.open(
            "Tradingview Data Reel Experimental May"
        )

        source_sheet = spreadsheet.worksheet("Sheet5")
        target_sheet = spreadsheet.worksheet("Sheet18")

        return source_sheet, target_sheet

    except Exception as e:
        log(f"❌ Google Sheets Connection Failed: {e}")
        sys.exit(1)


# =========================================================
# CONNECT SHEETS
# =========================================================
sheet_source, sheet_target = connect_sheets()


# =========================================================
# READ SYMBOLS
# =========================================================
try:
    headers = [
        h.strip().lower()
        for h in sheet_source.row_values(1)
    ]

    symbol_col_idx = (
        headers.index("symbol") + 1
        if "symbol" in headers
        else 1
    )

    symbol_list = sheet_source.col_values(symbol_col_idx)

    log(
        f"✅ Sheets mapped successfully. "
        f"Symbol column found -> Col {symbol_col_idx}"
    )

except Exception as e:
    log(f"❌ Failed parsing source sheet: {e}")
    sys.exit(1)


# =========================================================
# INITIALIZE ZERODHA
# =========================================================
try:
    kite = KiteConnect(api_key=API_KEY)

    kite.set_access_token(ACCESS_TOKEN)

    log("✅ Zerodha KiteConnect Session initialized.")

    log("🔄 Downloading Zerodha Instrument Master...")

    instruments = kite.instruments("NSE")

    token_map = {
        inst["tradingsymbol"]: inst["instrument_token"]
        for inst in instruments
    }

    log("✅ Instrument Master processed successfully.")

except TokenException as e:
    log(f"❌ Invalid or expired access token: {e}")
    sys.exit(1)

except Exception as e:
    log(f"❌ Zerodha initialization failed: {e}")
    sys.exit(1)


# =========================================================
# CREATE TARGET HEADER IF EMPTY
# =========================================================
try:
    if not sheet_target.row_values(1):

        sheet_target.append_row([
            "Symbol",
            "Target Date Label",
            "Datetime",
            "Open",
            "High",
            "Low",
            "Close",
            "Adj Close",
            "Volume"
        ])

except Exception as e:
    log(f"⚠️ Header initialization skipped: {e}")


# =========================================================
# DATE RANGE
# =========================================================
to_date = datetime.now()
from_date = to_date - timedelta(days=30)


# =========================================================
# SCRAPER LOOP
# =========================================================
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

    trading_symbol = (
        raw_symbol
        .replace(".NS", "")
        .replace(".BSE", "")
        .strip()
    )

    instrument_token = token_map.get(trading_symbol)

    if not instrument_token:
        log(
            f"⚠️ Symbol not found in NSE instruments: "
            f"{trading_symbol}"
        )
        continue

    log(
        f"🔄 Processing row [{i+1}/{total_rows}] "
        f"— Symbol: {trading_symbol} "
        f"(Token: {instrument_token})"
    )

    retries = 3

    while retries > 0:

        try:
            log(
                "   Downloading last 30 days "
                "of 1m candle data from Zerodha..."
            )

            records = kite.historical_data(
                instrument_token=instrument_token,
                from_date=from_date,
                to_date=to_date,
                interval="minute"
            )

            if not records:
                log(
                    f"   ⚠️ No candle data returned "
                    f"for {trading_symbol}"
                )
                break

            df = pd.DataFrame(records)

            for _, row in df.iterrows():

                all_rows_payload.append([
                    f"{trading_symbol}.NS",
                    "Last 30 Days",
                    str(row["date"]),
                    float(row["open"]),
                    float(row["high"]),
                    float(row["low"]),
                    float(row["close"]),
                    float(row["close"]),
                    int(row["volume"])
                ])

            log(
                f"   ✅ Successfully fetched "
                f"{len(df)} candles"
            )

            break

        except TokenException as e:
            log(
                f"   ❌ Invalid API Key or Access Token: {e}"
            )
            sys.exit(1)

        except (InputException, DataException) as e:

            err = str(e).lower()

            if "rate" in err or "limit" in err:

                log(
                    "   ⚠️ Rate limit hit. "
                    "Sleeping for 5 seconds..."
                )

                time.sleep(5)

                retries -= 1

            else:
                log(
                    f"   ❌ Zerodha query failed "
                    f"for {trading_symbol}: {e}"
                )
                break

        except NetworkException:

            log(
                "   ⚠️ Network issue detected. "
                "Retrying..."
            )

            time.sleep(2)

            retries -= 1

        except Exception as e:

            log(
                f"   ❌ Error executing Zerodha "
                f"data fetch for {trading_symbol}: {e}"
            )

            break

    # Zerodha allows max ~3 req/sec
    time.sleep(0.4)


# =========================================================
# PUSH TO GOOGLE SHEETS
# =========================================================
if all_rows_payload:

    log(
        f"🚀 Uploading {len(all_rows_payload)} "
        f"records to Sheet18..."
    )

    try:
        sheet_target.append_rows(
            all_rows_payload,
            value_input_option="RAW"
        )

        log("✅ Data upload completed successfully.")

    except Exception as e:

        log(f"❌ Google Sheets upload failed: {e}")

else:
    log("⚠️ No candle data collected.")
```
