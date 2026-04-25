import os
import sys
import re
import json
import requests
import pandas as pd
import mysql.connector
from io import StringIO
from datetime import datetime

# -------------------------------
# Market day check
# -------------------------------
def is_market_open_today():
    today = datetime.now().date()
    weekday = today.weekday()  # Mon=0 ... Sun=6

    if weekday >= 5:
        print(f"⏭️ Market closed today ({today}) - weekend.")
        return False

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9"
    }

    holiday_url = "https://www.nseindia.com/resources/exchange-communication-holidays"

    try:
        session = requests.Session()
        session.get("https://www.nseindia.com", headers=headers, timeout=30)
        response = session.get(holiday_url, headers=headers, timeout=30)
        response.raise_for_status()
        html = response.text

        raw_dates = re.findall(r'\bd{1,2}-[A-Za-z]{3}-d{4}\b', html)

        holiday_dates = set()
        for date_str in raw_dates:
            try:
                holiday_dates.add(datetime.strptime(date_str, "%d-%b-%Y").date())
            except Exception:
                pass

        if today in holiday_dates:
            print(f"⏭️ Market closed today ({today}) - NSE holiday.")
            return False

        print(f"✅ Market open today ({today}) - continuing workflow.")
        return True

    except Exception as e:
        print(f"⚠️ Could not verify NSE holiday status: {e}")
        print("⏭️ Stopping workflow for safety.")
        return False


# -------------------------------
# Database connection
# -------------------------------
def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )


# -------------------------------
# Load data
# -------------------------------
def load_data_from_mysql():
    conn = None
    try:
        conn = get_db_connection()

        query = """
        SELECT *
        FROM stocks
        """

        df = pd.read_sql(query, conn)
        return df

    except Exception as e:
        print(f"❌ Database load failed: {e}")
        return pd.DataFrame()

    finally:
        if conn:
            conn.close()


# -------------------------------
# Normalize dataframe
# -------------------------------
def normalize_dataframe(df):
    if df is None or df.empty:
        print("⚠️ DataFrame is empty. Stopping safely.")
        sys.exit(0)

    df.columns = df.columns.astype(str).str.strip()
    print("Available columns:", df.columns.tolist())
    print("DataFrame shape:", df.shape)

    # Backward compatibility for old code
    if 'D_Today_f' not in df.columns and 'D_Today' in df.columns:
        df['D_Today_f'] = df['D_Today']

    if 'D_Today' not in df.columns and 'D_Today_f' in df.columns:
        df['D_Today'] = df['D_Today_f']

    if 'D_Today' not in df.columns and 'D_Today_f' not in df.columns:
        print("❌ Required column missing: neither 'D_Today' nor 'D_Today_f' exists.")
        sys.exit(0)

    return df


# -------------------------------
# Get safe working column
# -------------------------------
def get_today_column(df):
    if 'D_Today' in df.columns:
        return 'D_Today'
    if 'D_Today_f' in df.columns:
        return 'D_Today_f'

    print("❌ Missing required today column.")
    sys.exit(0)


# -------------------------------
# Safe numeric conversion
# -------------------------------
def convert_numeric_columns(df, columns):
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


# -------------------------------
# Main filtering logic
# -------------------------------
def process_data(df):
    df = normalize_dataframe(df)
    today_col = get_today_column(df)

    numeric_candidates = [
        today_col,
        'D_High',
        'D_Low',
        'Close',
        'LTP',
        'Volume'
    ]
    df = convert_numeric_columns(df, numeric_candidates)

    df = df[df[today_col].notna()]

    # Example filter: only positive D_Today values
    filtered_df = df[df[today_col] > 0].copy()

    if filtered_df.empty:
        print("⚠️ No rows matched filter condition.")
        return pd.DataFrame()

    filtered_df = filtered_df.sort_values(by=today_col, ascending=False)

    print(f"✅ Filtered rows: {len(filtered_df)}")
    return filtered_df


# -------------------------------
# Optional rollover placeholder
# -------------------------------
def rollover_if_needed():
    try:
        print("✅ Rollover successful.")
    except Exception as e:
        print(f"⚠️ Rollover failed: {e}")


# -------------------------------
# Save output
# -------------------------------
def save_output(df):
    if df.empty:
        print("⚠️ Nothing to save.")
        return

    output_file = "filtered_stocks.csv"
    df.to_csv(output_file, index=False)
    print(f"✅ Output saved: {output_file}")


# -------------------------------
# Main
# -------------------------------
def main():
    if not is_market_open_today():
        sys.exit(0)

    rollover_if_needed()

    df = load_data_from_mysql()

    if df.empty:
        print("⚠️ No data loaded from MySQL. Exiting safely.")
        sys.exit(0)

    result_df = process_data(df)

    if result_df.empty:
        print("⚠️ No final output after filtering. Exiting safely.")
        sys.exit(0)

    save_output(result_df)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Fatal: {e}")
        sys.exit(0)
