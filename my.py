import requests
from bs4 import BeautifulSoup
import urllib.parse
import re

# ---------------- GET LATEST VIDEOS ---------------- #
def get_latest_videos(channel_url, max_results=3):
    print("📡 Fetching latest videos...")

    headers = {"User-Agent": "Mozilla/5.0"}
    res = requests.get(channel_url, headers=headers)

    video_ids = re.findall(r'"videoId":"(.*?)"', res.text)

    unique = []
    seen = set()

    for vid in video_ids:
        if vid not in seen:
            seen.add(vid)
            unique.append(vid)

    videos = [f"https://www.youtube.com/watch?v={vid}" for vid in unique[:max_results]]

    print(f"✅ Found {len(videos)} videos")
    return videos


# ---------------- IMPORT YOUR WORKING LOGIC ---------------- #
# (FROM YOUR FILE — NO CHANGE)

import sys
import time
import os
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


DOWNLOAD_DIR = os.path.join(os.getcwd(), "downloads")
if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def extract_video_id(url):
    if "v=" in url:
        return url.split("v=")[1].split("&")[0]
    return None


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

        with open(downloaded_file, "r", encoding="utf-8") as f:
            return f.read()

    except Exception as e:
        log(f"❌ Error: {e}")
        return None

    finally:
        driver.quit()


# ---------------- MAIN ---------------- #
if __name__ == "__main__":

    channel_url = "https://www.youtube.com/@stockmarketcommando/videos"

    videos = get_latest_videos(channel_url)

    for url in videos:
        print(f"\n🚀 Processing: {url}")

        transcript = get_transcript(url)

        if transcript:
            print("✅ Transcript fetched")

            filename = f"transcript_{url.split('v=')[1]}.txt"

            with open(filename, "w", encoding="utf-8") as f:
                f.write(transcript)

            print(f"📄 Saved: {filename}")
