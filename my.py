import sys
import time
import os
import requests
import pymysql
import urllib.parse
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

DOWNLOAD_DIR = os.path.join(os.getcwd(), "downloads")
if not os.path.exists(DOWNLOAD_DIR):
    os.makedirs(DOWNLOAD_DIR)

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def extract_video_id(url):
    """Robust Video ID Extraction"""
    parsed = urllib.parse.urlparse(url)
    if parsed.hostname == "youtu.be":
        return parsed.path[1:]
    if "youtube.com" in parsed.hostname:
        query = urllib.parse.parse_qs(parsed.query)
        return query.get("v", [None])[0]
    return None

def create_driver():
    log("🌐 Starting Headless Chrome...")
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
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

    driver.execute_cdp_cmd("Page.setDownloadBehavior", {
        "behavior": "allow",
        "downloadPath": DOWNLOAD_DIR
    })
    return driver

def get_latest_video_from_channel(channel_url):
    """Uses Selenium to find the most recent video link if a channel URL is provided"""
    driver = create_driver()
    try:
        log(f"📺 Finding latest video from: {channel_url}")
        driver.get(f"{channel_url.rstrip('/')}/videos")
        wait = WebDriverWait(driver, 20)
        # Find the first video thumbnail link
        video_element = wait.until(EC.presence_of_element_located((By.XPATH, '//*[@id="video-title-link"]')))
        video_url = video_element.get_attribute("href")
        log(f"✅ Found latest video: {video_url}")
        return video_url
    except Exception as e:
        log(f"❌ Could not find latest video: {e}")
        return channel_url # Fallback to input
    finally:
        driver.quit()

def download_transcript(youtube_url):
    """Uses DownSub to download the TXT transcript"""
    driver = create_driver()
    try:
        downsub_url = f"https://downsub.com/?url={urllib.parse.quote(youtube_url)}"
        log(f"🌐 Opening DownSub: {downsub_url}")
        driver.get(downsub_url)

        wait = WebDriverWait(driver, 45)
        # Locate the TXT download button
        txt_button_xpath = "//button[contains(., 'TXT') or contains(., '[TXT]')]"
        txt_button = wait.until(EC.element_to_be_clickable((By.XPATH, txt_button_xpath)))

        log("🚀 Triggering download...")
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", txt_button)
        time.sleep(2)
        driver.execute_script("arguments[0].click();", txt_button)

        # Wait for file to appear in folder
        timeout = 30
        start_time = time.time()
        while time.time() - start_time < timeout:
            files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith('.txt')]
            if files:
                file_path = os.path.join(DOWNLOAD_DIR, files[0])
                log(f"⬇️ Downloaded: {files[0]}")
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()
                os.remove(file_path) # Clean up
                return content
            time.sleep(2)
        
        return None
    except Exception as e:
        log(f"❌ Transcript Download Error: {e}")
        return None
    finally:
        driver.quit()

def save_to_db(video_id, url, content):
    if not DB_CONFIG['host']:
        log("⚠️ No DB Config found. Skipping database save.")
        return
    try:
        with closing(pymysql.connect(**DB_CONFIG)) as conn:
            with conn.cursor() as cursor:
                sql = """
                    INSERT INTO wp_transcript (video_id, video_url, content)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE content = VALUES(content)
                """
                cursor.execute(sql, (video_id, url, content))
            conn.commit()
        log("✅ Database updated successfully")
    except Exception as e:
        log(f"❌ DB Error: {e}")

if __name__ == "__main__":
    input_url = sys.argv[1] if len(sys.argv) > 1 else "https://www.youtube.com/watch?v=huW5sxhm3ow"
    
    # 1. Logic to handle if input is a channel or a direct video
    final_video_url = input_url
    if "channel" in input_url or "/@" in input_url:
        final_video_url = get_latest_video_from_channel(input_url)
    
    video_id = extract_video_id(final_video_url)
    
    # 2. Get Transcript
    transcript_text = download_transcript(final_video_url)

    if transcript_text:
        # 3. Save locally
        with open("transcript.txt", "w", encoding="utf-8") as f:
            f.write(transcript_text)
        
        # 4. Save to DB
        save_to_db(video_id, final_video_url, transcript_text)
    else:
        log("❌ Operation failed: No transcript retrieved.")
