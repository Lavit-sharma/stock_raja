import sys
import time
import os
import requests
import pymysql
import xml.etree.ElementTree as ET
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

DOWNLOAD_DIR = os.path.join(os.getcwd(), "downloads")
if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def extract_video_id(url):
    if "v=" in url:
        return url.split("v=")[1].split("&")[0]
    return None

# ---------------- GET LATEST VIDEOS ---------------- #
def get_latest_videos(channel_id, max_results=3):
    feed_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
    log("📡 Fetching latest videos...")

    response = requests.get(feed_url)
    if response.status_code != 200:
        log("❌ Failed to fetch channel feed")
        return []

    root = ET.fromstring(response.content)

    videos = []
    for entry in root.findall("{http://www.w3.org/2005/Atom}entry")[:max_results]:
        video_id = entry.find("{http://www.youtube.com/xml/schemas/2015}videoId").text
        video_url = f"https://www.youtube.com/watch?v={video_id}"
        videos.append(video_url)

    log(f"✅ Found {len(videos)} videos")
    return videos

# ---------------- CHECK DUPLICATE ---------------- #
def is_video_processed(video_id):
    try:
        with closing(pymysql.connect(**DB_CONFIG)) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT 1 FROM wp_transcript WHERE video_id=%s LIMIT 1",
                    (video_id,)
                )
                return cursor.fetchone() is not None
    except:
        return False

# ---------------- DRIVER ---------------- #
def create_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    prefs = {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
    }
    options.add_experimental_option("prefs", prefs)

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
        driver.get(f"https://downsub.com/?url={youtube_url}")

        wait = WebDriverWait(driver, 45)
        btn = wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//button[contains(., 'TXT')]")
        ))

        driver.execute_script("arguments[0].click();", btn)

        timeout = 60
        start = time.time()

        while time.time() - start < timeout:
            files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith(".txt")]
            if files:
                path = os.path.join(DOWNLOAD_DIR, files[0])
                with open(path, "r", encoding="utf-8") as f:
                    return f.read()
            time.sleep(2)

        return None

    finally:
        driver.quit()

# ---------------- MAIN PROCESS ---------------- #
def fetch_and_store(youtube_url):
    video_id = extract_video_id(youtube_url)

    if is_video_processed(video_id):
        log(f"⏭️ Skipping already processed: {video_id}")
        return

    log(f"🚀 Processing: {youtube_url}")

    transcript = get_transcript(youtube_url)
    if not transcript:
        log("❌ Transcript failed")
        return

    with open("transcript.txt", "w", encoding="utf-8") as f:
        f.write(transcript)

    try:
        with closing(pymysql.connect(**DB_CONFIG)) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO wp_transcript (video_id, video_url, content)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE content = VALUES(content)
                """, (video_id, youtube_url, transcript))
            conn.commit()

        log("✅ Saved to DB")

    except Exception as e:
        log(f"❌ DB error: {e}")

# ---------------- ENTRY ---------------- #
if __name__ == "__main__":
    CHANNEL_ID = "UChneGqGy_lmvfcR1v_avL6g"   # 🔴 PUT SKMC CHANNEL ID

    videos = get_latest_videos(CHANNEL_ID, 3)

    for url in videos:
        fetch_and_store(url)
