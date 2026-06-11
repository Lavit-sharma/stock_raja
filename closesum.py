import os
import mysql.connector
from datetime import datetime

# --- CONFIGURATION ---
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "connect_timeout": 15
}

def calculate_and_save_daily_sum():
    """
    Fetches CURR_DQ and D_CLOSE from wp_mv2, multiplies them row-by-row,
    and upserts the accumulated matrix sum into the closesum table tagged by current date.
    """
    print("\n" + "="*60)
    print("🧮  STARTING DAY-WISE VALUE SUMMATION CALCULATION ROUTINE")
    print("="*60)
    
    today_date = datetime.now().strftime('%Y-%m-%d')
    print(f"📆  Targeting Execution Date: {today_date}")
    
    conn = None
    try:
        print("🔌  [DB MATH] Connecting to database...")
        conn = mysql.connector.connect(**DB_CONFIG)
        print("✅  [DB MATH] Database connected successfully.")
        cursor = conn.cursor(dictionary=True)

        # 1. Fetch the target data columns from wp_mv2
        print("📥  [DB MATH] Fetching all rows (Symbol, CURR_DQ, D_CLOSE) from 'wp_mv2' table...")
        cursor.execute("SELECT Symbol, CURR_DQ, D_CLOSE FROM wp_mv2")
        rows = cursor.fetchall()
        print(f"📋  [DB MATH] Successfully fetched {len(rows)} records from 'wp_mv2'.")

        grand_total = 0.0
        processed_count = 0

        # 2. Row-by-Row multiplication and compounding
        print("⚙️  [DB MATH] Beginning row-by-row multiplication (CURR_DQ * D_CLOSE)...")
        for idx, row in enumerate(rows, start=1):
            symbol = row.get("Symbol", "UNKNOWN")
            curr_dq_str = row.get("CURR_DQ")
            d_close_str = row.get("D_CLOSE")

            if curr_dq_str is not None and d_close_str is not None:
                try:
                    # Clean strings (remove commas, spaces, currency symbols) and convert to float
                    curr_dq = float(str(curr_dq_str).replace(",", "").strip())
                    d_close = float(str(d_close_str).replace(",", "").strip())
                    
                    # Row multiplication
                    row_product = curr_dq * d_close
                    grand_total += row_product
                    processed_count += 1
                    
                    # Periodic summary logging every 100 rows
                    if idx % 100 == 0 or idx == len(rows):
                        print(f"    ↳ Processing row {idx}/{len(rows)} | Current Accumulated Sum: {grand_total:,.2f}")
                        
                except ValueError:
                    # Skip problematic text inputs gracefully
                    continue
            else:
                if idx % 500 == 0:
                    print(f"    ↳ Line item status check: Row {idx}/{len(rows)} processed.")

        print("-"*60)
        print(f"📊  [DB MATH SUMMARY] Calculated {processed_count} valid rows successfully.")
        print(f"💎  [DB MATH SUMMARY] Final Generated Sum Product: {grand_total:,.2f}")
        print("-"*60)

        # 3. Save the final calculated value to closesum table matching today's date
        print(f"📤  [DB MATH] Upserting daily total value into 'closesum' for date: {today_date}...")
        save_query = """
            INSERT INTO closesum (calculation_date, total_dq_value) 
            VALUES (%s, %s) 
            ON DUPLICATE KEY UPDATE 
                total_dq_value = VALUES(total_dq_value)
        """
        cursor.execute(save_query, (today_date, str(grand_total)))
        conn.commit()
        print(f"🚀  [DB MATH] Successfully processed and recorded metrics for context date {today_date}!")

        cursor.close()
    except Exception as e:
        print(f"❌  [DB MATH GLOBAL ERROR] Critical failure during execution context: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()
            print("🔌  [DB MATH] Closed database connection pipeline safely.")
    print("="*60 + "\n")

if __name__ == "__main__":
    calculate_and_save_daily_sum()
