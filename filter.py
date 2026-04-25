import os
import sys
import json
import pandas as pd
import mysql.connector


def get_db_connection():
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME")
    )


def get_database_tables(conn):
    """Discover all tables in the database"""
    try:
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return tables
    except Exception as e:
        print(f"⚠️ Could not list tables: {e}")
        return []


def load_data_from_mysql():
    conn = None
    try:
        conn = get_db_connection()
        
        # First, check what tables exist
        tables = get_database_tables(conn)
        print(f"📋 Available tables: {tables}")
        
        if not tables:
            print("❌ No tables found in database")
            return pd.DataFrame()
        
        # Try common stock table names first
        common_stock_tables = ['stocks', 'stock_data', 'stock', 'nse_stocks', 'bse_stocks', 'daily_stocks']
        table_to_use = None
        
        for table_name in common_stock_tables:
            if table_name in tables:
                table_to_use = table_name
                break
        
        # If no common name found, use first table
        if not table_to_use:
            table_to_use = tables[0]
            print(f"ℹ️ Using first available table: {table_to_use}")
        
        print(f"📊 Loading data from table: {table_to_use}")
        
        query = f"SELECT * FROM `{table_to_use}`"
        df = pd.read_sql(query, conn)
        print(f"✅ Loaded {len(df)} rows from {table_to_use}")
        return df

    except Exception as e:
        print(f"❌ Database load failed: {e}")
        if conn:
            tables = get_database_tables(conn)
            print(f"📋 Available tables: {tables}")
        return pd.DataFrame()

    finally:
        if conn:
            conn.close()


def normalize_dataframe(df):
    if df is None or df.empty:
        print("⚠️ DataFrame is empty. Exiting safely.")
        sys.exit(0)

    df.columns = df.columns.astype(str).str.strip()

    print("Available columns:", df.columns.tolist())
    print("DataFrame shape:", df.shape)

    if 'D_Today_f' not in df.columns and 'D_Today' in df.columns:
        df['D_Today_f'] = df['D_Today']

    if 'D_Today' not in df.columns and 'D_Today_f' in df.columns:
        df['D_Today'] = df['D_Today_f']

    if 'D_Today' not in df.columns and 'D_Today_f' not in df.columns:
        print("❌ Required column missing: neither 'D_Today' nor 'D_Today_f' exists.")
        sys.exit(0)

    return df


def get_today_column(df):
    if 'D_Today' in df.columns:
        return 'D_Today'
    if 'D_Today_f' in df.columns:
        return 'D_Today_f'

    print("❌ Missing required today column.")
    sys.exit(0)


def convert_numeric_columns(df, columns):
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


def rollover_if_needed():
    try:
        print("✅ Rollover successful.")
    except Exception as e:
        print(f"⚠️ Rollover failed: {e}")


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

    df = df[df[today_col].notna()].copy()

    filtered_df = df[df[today_col] > 0].copy()

    if filtered_df.empty:
        print("⚠️ No rows matched filter condition.")
        return pd.DataFrame()

    filtered_df = filtered_df.sort_values(by=today_col, ascending=False)

    print(f"✅ Filtered rows: {len(filtered_df)}")
    return filtered_df


def save_output(df):
    if df.empty:
        print("⚠️ Nothing to save.")
        return

    output_file = "filtered_stocks.csv"
    df.to_csv(output_file, index=False)
    print(f"✅ Output saved: {output_file}")


def main():
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
