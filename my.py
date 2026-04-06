import os
import time
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

# ---------------- CONFIG ---------------- #
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'charset': 'utf8mb4'
}

CHANNEL_ID = os.getenv("YOUTUBE_CHANNEL_ID")

DOWNLOAD_DIR = os.path.join(os.getcwd(), "downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def extract_video_id(url):
    return url.split("v=")[1].split("&")[0] if "v=" in url else None

# ---------------- FETCH LATEST VIDEOS ---------------- #
def get_latest_videos(max_results=3):
    feed_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={CHANNEL_ID}"
    log("📡 Fetching latest videos...")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }

    try:
        res = requests.get(feed_url, headers=headers, timeout=10)

        if res.status_code != 200:
            log(f"❌ Failed to fetch feed | Status: {res.status_code}")
            return []

        root = ET.fromstring(res.content)

        videos = []
        for entry in root.findall("{http://www.w3.org/2005/Atom}entry")[:max_results]:
            vid = entry.find("{http://www.youtube.com/xml/schemas/2015}videoId").text
            videos.append(f"https://www.youtube.com/watch?v={vid}")

        log(f"✅ Found {len(videos)} videos")
        return videos

    except Exception as e:
        log(f"❌ Feed error: {e}")
        return []
# ---------------- CHECK DUPLICATE ---------------- #
def is_video_processed(video_id):
    try:
        with closing(pymysql.connect(**DB_CONFIG)) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM wp_transcript WHERE video_id=%s LIMIT 1", (video_id,))
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
        "download.prompt_for_download": False
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
def get_transcript(url):
    driver = create_driver()
    try:
        driver.get(f"https://downsub.com/?url={url}")

        wait = WebDriverWait(driver, 45)
        btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'TXT')]")))

        driver.execute_script("arguments[0].click();", btn)

        start = time.time()
        while time.time() - start < 60:
            files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith(".txt")]
            if files:
                path = os.path.join(DOWNLOAD_DIR, files[0])
                with open(path, "r", encoding="utf-8") as f:
                    return f.read()
            time.sleep(2)

        return None

    finally:
        driver.quit()

# ---------------- PROCESS ---------------- #
def process_video(url):
    video_id = extract_video_id(url)

    if is_video_processed(video_id):
        log(f"⏭️ Skipping: {video_id}")
        return

    log(f"🚀 Processing: {url}")

    transcript = get_transcript(url)
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
                """, (video_id, url, transcript))
            conn.commit()

        log("✅ Saved to DB")

    except Exception as e:
        log(f"❌ DB Error: {e}")

# ---------------- MAIN ---------------- #
if __name__ == "__main__":
    if not CHANNEL_ID:
        log("❌ Missing YOUTUBE_CHANNEL_ID")
        exit(1)

    videos = get_latest_videos(3)

    for v in videos:
        process_video(v)
