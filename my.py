import sys
import time
import os
import requests
import pymysql
from datetime import datetime
from contextlib import closing

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- CONFIG FROM SECRETS ---------------- #
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'charset': 'utf8mb4'
}

# Local folder to catch the auto-downloaded file
DOWNLOAD_DIR = os.path.join(os.getcwd(), "downloads")
if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def extract_video_id(url):
    if "v=" in url:
        return url.split("v=")[1].split("&")[0]
    return None

# ---------------- DRIVER ---------------- #
def create_driver():
    log("🌐 Starting browser (Headless Download Mode)...")
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    
    # Configure Chrome to allow downloads in headless mode
    prefs = {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

    # Required for Headless Chrome to actually save files
    driver.execute_cdp_cmd("Page.setDownloadBehavior", {
        "behavior": "allow",
        "downloadPath": DOWNLOAD_DIR
    })

    return driver

# ---------------- GET TRANSCRIPT ---------------- #
def get_transcript(youtube_url):
    driver = create_driver()
    try:
        downsub_url = f"https://downsub.com/?url={youtube_url}"
        log(f"🌐 Opening: {downsub_url}")
        driver.get(downsub_url)

        wait = WebDriverWait(driver, 45)
        
        # Wait for the TXT button
        smart_xpath = "//div[@id='app']//button[contains(., 'TXT')]"
        txt_button = wait.until(EC.element_to_be_clickable((By.XPATH, smart_xpath)))

        log("✅ Button found. Triggering download...")
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", txt_button)
        time.sleep(2)
        driver.execute_script("arguments[0].click();", txt_button)

        log("⏳ Monitoring downloads folder...")
        
        timeout = 60
        start_time = time.time()
        downloaded_file = None

        while time.time() - start_time < timeout:
            files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith('.txt')]
            if files:
                downloaded_file = os.path.join(DOWNLOAD_DIR, files[0])
                break
            time.sleep(2)

        if not downloaded_file:
            raise Exception("File did not download. Check logs/screenshots.")

        log(f"⬇️ Successfully downloaded: {os.path.basename(downloaded_file)}")
        
        with open(downloaded_file, "r", encoding="utf-8") as f:
            return f.read()

    except Exception as e:
        log(f"❌ Error: {e}")
        return None
    finally:
        driver.quit()
        log("🛑 Browser closed")

# ---------------- MAIN ---------------- #
def fetch_and_store(youtube_url):
    video_id = extract_video_id(youtube_url)
    transcript_text = get_transcript(youtube_url)

    if not transcript_text:
        return

    # Save locally as artifact
    with open("transcript.txt", "w", encoding="utf-8") as f:
        f.write(transcript_text)
    log("📄 File saved to transcript.txt")

    # Save to Remote Database using Secrets
    try:
        if not DB_CONFIG['host']:
            log("⚠️ DB_HOST is empty. Check your GitHub Secrets.")
            return

        with closing(pymysql.connect(**DB_CONFIG)) as conn:
            with conn.cursor() as cursor:
                sql = """
                    INSERT INTO wp_transcript (video_id, video_url, content)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE content = VALUES(content)
                """
                cursor.execute(sql, (video_id, youtube_url, transcript_text))
            conn.commit()
        log("✅ Database updated successfully")
    except Exception as e:
        log(f"❌ DB storage error: {e}")

if __name__ == "__main__":
    url = sys.argv[1] if len(sys.argv) > 1 else "https://www.youtube.com/watch?v=huW5sxhm3ow"
    fetch_and_store(url)
