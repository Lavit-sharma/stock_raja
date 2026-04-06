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

API_KEY = os.getenv("YOUTUBE_API_KEY")
CHANNEL_ID = os.getenv("YOUTUBE_CHANNEL_ID")

DOWNLOAD_DIR = os.path.join(os.getcwd(), "downloads")
if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def extract_video_id(url):
    if "v=" in url:
        return url.split("v=")[1].split("&")[0]
    return None


# ---------------- YOUTUBE API ---------------- #
def get_latest_videos(channel_id, max_results=3):
    log("📡 Fetching latest videos...")

    url = "https://www.googleapis.com/youtube/v3/search"

    params = {
        "key": API_KEY,
        "channelId": channel_id,
        "part": "snippet",
        "order": "date",
        "maxResults": max_results,
        "type": "video"
    }

    res = requests.get(url, params=params)
    data = res.json()

    videos = []

    for item in data.get("items", []):
        video_id = item["id"]["videoId"]
        title = item["snippet"]["title"]
        video_url = f"https://www.youtube.com/watch?v={video_id}"

        log(f"🎬 Found: {title}")
        videos.append(video_url)

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


# ---------------- MAIN ---------------- #
def fetch_and_store(youtube_url):
    video_id = extract_video_id(youtube_url)

    transcript = get_transcript(youtube_url)
    if not transcript:
        return

    with open("transcript.txt", "w", encoding="utf-8") as f:
        f.write(transcript)

    log("📄 Saved transcript")

    try:
        with closing(pymysql.connect(**DB_CONFIG)) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO wp_transcript (video_id, video_url, content)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE content = VALUES(content)
                """, (video_id, youtube_url, transcript))
            conn.commit()

        log("✅ DB updated")

    except Exception as e:
        log(f"❌ DB error: {e}")


# ---------------- ENTRY ---------------- #
if __name__ == "__main__":

    if not API_KEY or not CHANNEL_ID:
        log("❌ Missing API key or Channel ID")
        sys.exit(1)

    videos = get_latest_videos(CHANNEL_ID)

    for url in videos:
        log(f"🚀 Processing: {url}")
        fetch_and_store(url)
