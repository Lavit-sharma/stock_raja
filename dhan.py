
import sys
import os
import time
from datetime import datetime

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
# ZERODHA CREDENTIALS
# =========================================================
API_KEY = os.getenv("ZERODHA_API_KEY")
ACCESS_TOKEN = os.getenv("ZERODHA_ACCESS_TOKEN")

if not API_KEY:
    raise Exception(
        "❌ Missing GitHub Secret: ZERODHA_API_KEY"
    )

if not ACCESS_TOKEN:
    raise Exception(
        "❌ Missing GitHub Secret: ZERODHA_ACCESS_TOKEN"
    )


# =========================================================
# GOOGLE SHEETS CONNECTION
# =========================================================
def connect_sheets():

    try:

        gc = gspread.service_account(
            "credentials.json"
        )

        spreadsheet = gc.open(
            "Tradingview Data Reel Experimental May"
        )

        source_sheet = spreadsheet.worksheet(
            "Sheet20"
        )

        target_sheet = spreadsheet.worksheet(
            "Sheet18"
        )

        return source_sheet, target_sheet

    except Exception as e:

        log(
            f"❌ Google Sheets Connection Failed: {e}"
        )

        sys.exit(1)


# =========================================================
# CONNECT SHEETS
# =========================================================
sheet_source, sheet_target = connect_sheets()


# =========================================================
# LOAD SOURCE DATA
# =========================================================
try:

    all_data = sheet_source.get_all_records()

    log(
        "✅ Source sheet loaded successfully."
    )

except Exception as e:

    log(
        f"❌ Failed loading source sheet: {e}"
    )

    sys.exit(1)


# =========================================================
# INITIALIZE ZERODHA
# =========================================================
try:

    kite = KiteConnect(api_key=API_KEY)

    kite.set_access_token(ACCESS_TOKEN)

    log(
        "✅ Zerodha KiteConnect Session initialized."
    )

    log(
        "🔄 Downloading Zerodha Instrument Master..."
    )

    instruments = kite.instruments("NSE")

    token_map = {

        inst["tradingsymbol"]:
        inst["instrument_token"]

        for inst in instruments
    }

    log(
        "✅ Instrument Master processed successfully."
    )

except TokenException as e:

    log(
        f"❌ Invalid or expired access token: {e}"
    )

    sys.exit(1)

except Exception as e:

    log(
        f"❌ Zerodha initialization failed: {e}"
    )

    sys.exit(1)


# =========================================================
# CREATE TARGET HEADER
# =========================================================
try:

    if not sheet_target.row_values(1):

        sheet_target.append_row([

            "Symbol",
            "Source Date Label",
            "Requested Date",
            "Datetime",
            "Close",
            "Volume"

        ])

except Exception as e:

    log(
        f"⚠️ Header initialization skipped: {e}"
    )


# =========================================================
# MAIN SCRAPER LOOP
# =========================================================
total_rows = len(all_data)

start_idx = max(0, START_ROW)

end_idx = min(
    END_ROW,
    total_rows
)


for i in range(start_idx, end_idx):

    row = all_data[i]

    raw_symbol = str(
        row.get("symbol", "")
    ).strip()

    if not raw_symbol:
        continue


    trading_symbol = (
        raw_symbol
        .replace(".NS", "")
        .replace(".BSE", "")
        .strip()
    )


    instrument_token = token_map.get(
        trading_symbol
    )

    if not instrument_token:

        log(
            f"⚠️ Symbol not found in NSE instruments: "
            f"{trading_symbol}"
        )

        continue


    # =====================================================
    # FETCH DATE1 + DATE2
    # =====================================================
    dates_to_fetch = [

        (
            "date1",
            row.get("date1")
        ),

        (
            "date2",
            row.get("date2")
        )

    ]


    for date_label, raw_date in dates_to_fetch:

        if not raw_date:
            continue

        try:

            target_date = datetime.strptime(
                str(raw_date),
                "%Y-%m-%d"
            )

        except Exception:

            log(
                f"⚠️ Invalid date format for "
                f"{trading_symbol}: {raw_date}"
            )

            continue


        from_date = target_date.replace(
            hour=0,
            minute=0,
            second=0
        )

        to_date = target_date.replace(
            hour=23,
            minute=59,
            second=59
        )


        log(
            f"🔄 Processing row "
            f"[{i+1}/{total_rows}] "
            f"— Symbol: {trading_symbol} "
            f"| {date_label}: {raw_date}"
        )


        retries = 3

        while retries > 0:

            try:

                log(
                    f"   Downloading 1m candles "
                    f"for {raw_date}..."
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

                symbol_rows = []


                for _, candle in df.iterrows():

                    symbol_rows.append([

                        f"{trading_symbol}.NS",

                        date_label,

                        raw_date,

                        str(candle["date"]),

                        float(candle["close"]),

                        int(candle["volume"])

                    ])


                # =========================================
                # IMMEDIATE GOOGLE SHEETS UPLOAD
                # =========================================
                sheet_target.append_rows(

                    symbol_rows,

                    value_input_option="RAW"

                )


                log(
                    f"   ✅ Uploaded "
                    f"{len(symbol_rows)} rows "
                    f"to Sheet18"
                )

                break


            except TokenException as e:

                log(
                    f"❌ Invalid API Key "
                    f"or Access Token: {e}"
                )

                sys.exit(1)


            except (
                InputException,
                DataException
            ) as e:

                err = str(e).lower()

                if (
                    "rate" in err
                    or
                    "limit" in err
                ):

                    log(
                        "⚠️ Rate limit hit. "
                        "Sleeping for 5 seconds..."
                    )

                    time.sleep(5)

                    retries -= 1

                else:

                    log(
                        f"❌ Zerodha query failed "
                        f"for {trading_symbol}: {e}"
                    )

                    break


            except NetworkException:

                log(
                    "⚠️ Network issue detected. "
                    "Retrying..."
                )

                time.sleep(2)

                retries -= 1


            except Exception as e:

                log(
                    f"❌ Error fetching "
                    f"{trading_symbol}: {e}"
                )

                break


        # Zerodha safe throttle
        time.sleep(0.4)


log("✅ Script execution completed.")
