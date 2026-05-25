
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
            "ML DATA "
        )

        target_sheet = spreadsheet.worksheet(
            "Sheet1"
        )

        return target_sheet

    except Exception as e:

        log(
            f"❌ Google Sheets Connection Failed: {e}"
        )

        sys.exit(1)


# =========================================================
# CONNECT SHEETS
# =========================================================
sheet_target = connect_sheets()


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
# TARGET SYMBOL
# =========================================================
trading_symbol = "TATASTEEL"


instrument_token = token_map.get(
    trading_symbol
)

if not instrument_token:

    log(
        f"❌ Symbol not found: "
        f"{trading_symbol}"
    )

    sys.exit(1)


# =========================================================
# CLEAR OLD SHEET DATA
# =========================================================
try:

    sheet_target.clear()

    sheet_target.append_row([

        "Symbol",
        "Datetime",
        "Close",
        "Volume"

    ])

    log(
        "✅ Sheet18 cleared successfully."
    )

except Exception as e:

    log(
        f"❌ Failed clearing sheet: {e}"
    )

    sys.exit(1)


# =========================================================
# FETCH LAST 100 DAYS
# =========================================================
today = datetime.now()

all_rows = []


for day in range(100):

    target_date = today - timedelta(days=day)

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
        f"🔄 Downloading "
        f"{trading_symbol} "
        f"for "
        f"{target_date.strftime('%Y-%m-%d')}"
    )


    retries = 3

    while retries > 0:

        try:

            records = kite.historical_data(

                instrument_token=instrument_token,

                from_date=from_date,

                to_date=to_date,

                interval="minute"

            )

            if not records:

                log(
                    f"⚠️ No candle data returned."
                )

                break


            df = pd.DataFrame(records)


            for _, candle in df.iterrows():

                all_rows.append([

                    f"{trading_symbol}.NS",

                    str(candle["date"]),

                    float(candle["close"]),

                    int(candle["volume"])

                ])


            log(
                f"✅ Downloaded "
                f"{len(df)} candles"
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
                    f"❌ Zerodha query failed: {e}"
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
                f"❌ Error fetching data: {e}"
            )

            break


    # =====================================================
    # SAFE THROTTLE
    # =====================================================
    time.sleep(0.5)


# =========================================================
# UPLOAD TO GOOGLE SHEETS
# =========================================================
try:

    if all_rows:

        log(
            f"🔄 Uploading "
            f"{len(all_rows)} rows..."
        )

        sheet_target.append_rows(

            all_rows,

            value_input_option="RAW"

        )

        log(
            f"✅ Successfully uploaded "
            f"{len(all_rows)} rows "
            f"to Sheet18"
        )

    else:

        log(
            "⚠️ No data available to upload."
        )


except Exception as e:

    log(
        f"❌ Upload failed: {e}"
    )


# =========================================================
# DONE
# =========================================================
log(
    "✅ Script execution completed."
)

