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
    parsed = urllib.parse.urlparse(url)
    if parsed.hostname == "youtu.be":
        return parsed.path[1:]
    if "youtube.com" in parsed.hostname:
        query = urllib.parse.parse_qs(parsed.query)
        return query.get("v", [None])[0]
    return None

def create_driver():
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

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.execute_cdp_cmd("Page.setDownloadBehavior", {"behavior": "allow", "downloadPath": DOWNLOAD_DIR})
    return driver

def get_latest_videos_from_channel(channel_url, count=3):
    """Finds the top N latest video URLs from a channel"""
    driver = create_driver()
    video_links = []
    try:
        log(f"📺 Accessing channel: {channel_url}")
        # Navigate to the /videos tab specifically
        target_url = f"{channel_url.rstrip('/')}/videos"
        driver.get(target_url)
        
        wait = WebDriverWait(driver, 20)
        # Wait for video titles to load
        wait.until(EC.presence_of_element_located((By.ID, "video-title-link")))
        
        # Get all video elements
        elements = driver.find_elements(By.ID, "video-title-link")
        
        for i in range(min(count, len(elements))):
            url = elements[i].get_attribute("href")
            if url:
                video_links.append(url)
        
        log(f"✅ Found {len(video_links)} recent videos.")
    except Exception as e:
        log(f"❌ Error finding videos: {e}")
    finally:
        driver.quit()
    return video_links

def download_transcript(youtube_url):
    """Downloads transcript using DownSub"""
    driver = create_driver()
    try:
        downsub_url = f"https://downsub.com/?url={urllib.parse.quote(youtube_url)}"
        driver.get(downsub_url)
        wait = WebDriverWait(driver, 30)
        
        # Clear downloads folder before starting
        for f in os.listdir(DOWNLOAD_DIR): os.remove(os.path.join(DOWNLOAD_DIR, f))

        txt_button_xpath = "//button[contains(., 'TXT')]"
        txt_button = wait.until(EC.element_to_be_clickable((By.XPATH, txt_button_xpath)))
        
        driver.execute_script("arguments[0].click();", txt_button)
        
        # Wait for file
        timeout = 20
        start_time = time.time()
        while time.time() - start_time < timeout:
            files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith('.txt')]
            if files:
                file_path = os.path.join(DOWNLOAD_DIR, files[0])
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()
                return content
            time.sleep(1)
        return None
    except Exception as e:
        log(f"⚠️ Could not get transcript for {youtube_url}")
        return None
    finally:
        driver.quit()

def save_to_db(video_id, url, content):
    if not DB_CONFIG['host'] or not content: return
    try:
        with closing(pymysql.connect(**DB_CONFIG)) as conn:
            with conn.cursor() as cursor:
                sql = "INSERT INTO wp_transcript (video_id, video_url, content) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE content = VALUES(content)"
                cursor.execute(sql, (video_id, url, content))
            conn.commit()
        log(f"✅ DB Updated for {video_id}")
    except Exception as e:
        log(f"❌ DB Error: {e}")

if __name__ == "__main__":
    # If no URL is passed, we default to the SMKC channel (your stock channel example)
    target = sys.argv[1] if len(sys.argv) > 1 else "https://www.youtube.com/@SMKC"
    
    # 1. Identify if it's a channel or a single video
    if "channel" in target or "/@" in target:
        urls_to_process = get_latest_videos_from_channel(target, count=3)
    else:
        urls_to_process = [target]

    # 2. Loop through all found videos
    for video_url in urls_to_process:
        log(f"🎬 Processing: {video_url}")
        vid_id = extract_video_id(video_url)
        text = download_transcript(video_url)
        if text:
            save_to_db(vid_id, video_url, text)
        time.sleep(2) # Small delay between videos
