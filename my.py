import sys
import time
import os
import requests
import pymysql
import urllib.parse
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

def get_latest_videos(channel_url, count=3):
    video_links = []
    try:
        clean_url = channel_url.split('?')[0].rstrip('/')
        if not clean_url.endswith('/videos'):
            clean_url += '/videos'
            
        log(f"📺 Fetching videos from: {clean_url}")
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        response = requests.get(clean_url, headers=headers)
        video_ids = re.findall(r'"videoId":"([^"]+)"', response.text)
        
        if video_ids:
            unique_ids = []
            for vid in video_ids:
                if vid not in unique_ids:
                    unique_ids.append(vid)
            
            for vid in unique_ids[:count]:
                video_links.append(f"https://www.youtube.com/watch?v={vid}")
            log(f"✅ Found {len(video_links)} videos.")
    except Exception as e:
        log(f"❌ Error fetching videos: {e}")
    return video_links

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
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.execute_cdp_cmd("Page.setDownloadBehavior", {"behavior": "allow", "downloadPath": DOWNLOAD_DIR})
    return driver

def get_video_data(youtube_url):
    driver = create_driver()
    video_title = "Unknown Title"
    transcript_text = None
    
    try:
        downsub_url = f"https://downsub.com/?url={urllib.parse.quote(youtube_url)}"
        driver.get(downsub_url)
        wait = WebDriverWait(driver, 30)
        
        # 1. Wait for and Extract Title
        # DownSub puts the title inside a card-header b tag
        try:
            title_el = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, ".card-header b")))
            video_title = title_el.text.replace("Download Subtitles", "").strip()
            # If title is empty or just generic text, try the fallback
            if not video_title or len(video_title) < 3:
                video_title = driver.title.split('-')[0].strip()
        except Exception:
            log("⚠️ Title element not found, attempting fallback...")
            video_title = driver.title

        # 2. Handle Download
        for f in os.listdir(DOWNLOAD_DIR):
            try: os.remove(os.path.join(DOWNLOAD_DIR, f))
            except: pass

        txt_xpath = "//button[contains(., 'TXT') or contains(., '[TXT]')]"
        txt_button = wait.until(EC.element_to_be_clickable((By.XPATH, txt_xpath)))
        driver.execute_script("arguments[0].click();", txt_button)
        
        timeout = 25
        start_time = time.time()
        while time.time() - start_time < timeout:
            files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith('.txt')]
            if files:
                file_path = os.path.join(DOWNLOAD_DIR, files[0])
                time.sleep(1)
                with open(file_path, "r", encoding="utf-8") as f:
                    transcript_text = f.read()
                break
            time.sleep(2)
            
        return video_title, transcript_text
    except Exception as e:
        log(f"❌ Error: {e}")
        return video_title, None
    finally:
        driver.quit()

def save_to_db(video_id, url, title, content):
    if not DB_CONFIG['host'] or not content: return
    try:
        with closing(pymysql.connect(**DB_CONFIG)) as conn:
            with conn.cursor() as cursor:
                sql = """
                INSERT INTO wp_transcript (video_id, video_url, title, content) 
                VALUES (%s, %s, %s, %s) 
                ON DUPLICATE KEY UPDATE 
                    title = VALUES(title),
                    content = VALUES(content)
                """
                cursor.execute(sql, (video_id, url, title, content))
            conn.commit()
        log(f"✅ Saved: {title[:50]}...")
    except Exception as e:
        log(f"❌ DB Error: {e}")

if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "https://www.youtube.com/@stockmarketcommando"
    
    urls_to_process = get_latest_videos(target, count=3) if ("channel" in target or "/@" in target) else [target]

    if not urls_to_process:
        log("❌ No videos found.")
        sys.exit(1)

    for video_url in urls_to_process:
        log(f"🎬 Processing: {video_url}")
        vid_id = extract_video_id(video_url)
        title, text = get_video_data(video_url)
        
        if text:
            save_to_db(vid_id, video_url, title, text)
        
        time.sleep(2)
