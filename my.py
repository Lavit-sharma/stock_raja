import sys
import time
import os
import requests
import pymysql
import re
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

# Local folder to catch downloads
DOWNLOAD_DIR = os.path.join(os.getcwd(), "downloads")
if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def extract_video_id(url):
    if "v=" in url:
        return url.split("v=")[1].split("&")[0]
    return None

# ---------------- NEW: GET LATEST VIDEOS ---------------- #
def get_latest_videos(channel_url, max_results=3):
    log("📡 Fetching latest videos...")

    headers = {"User-Agent": "Mozilla/5.0"}
    res = requests.get(channel_url, headers=headers)

    video_ids = re.findall(r'"videoId":"(.*?)"', res.text)

    seen = set()
    unique = []

    for vid in video_ids:
        if vid not in seen:
            seen.add(vid)
            unique.append(vid)

    videos = [f"https://www.youtube.com/watch?v={vid}" for vid in unique[:max_results]]

    log(f"✅ Found {len(videos)} videos")
    return videos

# ---------------- DRIVER ---------------- #
def create_driver():
    log("🌐 Starting browser...")
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")

    prefs = {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)
    options.add_argument("--disable-blink-features=AutomationControlled")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

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

        txt_button = wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//div[@id='app']//button[contains(., 'TXT')]")
            )
        )

        log("✅ Clicking TXT...")
        driver.execute_script("arguments[0].click();", txt_button)

        log("⏳ Waiting for download...")

        start = time.time()
        downloaded_file = None

        while time.time() - start < 60:
            files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith(".txt")]
            if files:
                downloaded_file = os.path.join(DOWNLOAD_DIR, files[0])
                break
            time.sleep(2)

        if not downloaded_file:
            raise Exception("Download failed")

        log(f"⬇️ Downloaded: {downloaded_file}")

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

    with open("transcript.txt", "w", encoding="utf-8") as f:
        f.write(transcript_text)

    log("📄 Saved transcript")

    try:
        if not DB_CONFIG['host']:
            log("⚠️ DB not configured")
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

        log("✅ DB updated")

    except Exception as e:
        log(f"❌ DB error: {e}")

# ---------------- ENTRY ---------------- #
if __name__ == "__main__":

    channel_url = "https://www.youtube.com/@stockmarketcommando/videos"

    videos = get_latest_videos(channel_url)

    for url in videos:
        log(f"🚀 Processing: {url}")
        fetch_and_store(url)
