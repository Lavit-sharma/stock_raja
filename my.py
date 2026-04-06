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
    options.add_argument("--lang=en-US") # Force English to handle buttons easily
    
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
    driver = create_driver()
    video_links = []
    try:
        log(f"📺 Accessing channel: {channel_url}")
        target_url = f"{channel_url.rstrip('/')}/videos"
        driver.get(target_url)
        
        wait = WebDriverWait(driver, 15)

        # 1. Handle potential Cookie Consent Popups
        try:
            consent_button = driver.find_elements(By.XPATH, "//button[contains(@aria-label, 'Accept') or contains(@aria-label, 'Agree')]")
            if consent_button:
                consent_button[0].click()
                time.sleep(2)
        except:
            pass

        # 2. Scroll a bit to trigger lazy loading
        driver.execute_script("window.scrollBy(0, 500);")
        time.sleep(3)

        # 3. Find video links using a more generic CSS Selector
        # This targets standard YouTube thumbnail links
        elements = wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a#video-title-link, a[href*='/watch?v=']")))
        
        seen_urls = set()
        for el in elements:
            url = el.get_attribute("href")
            if url and "/watch?v=" in url and url not in seen_urls:
                video_links.append(url)
                seen_urls.add(url)
                if len(video_links) >= count:
                    break
        
        log(f"✅ Found {len(video_links)} recent videos.")
    except Exception as e:
        log(f"❌ Error finding videos: {str(e)[:100]}") # Print first 100 chars of error
    finally:
        driver.quit()
    return video_links

def download_transcript(youtube_url):
    driver = create_driver()
    try:
        downsub_url = f"https://downsub.com/?url={urllib.parse.quote(youtube_url)}"
        driver.get(downsub_url)
        wait = WebDriverWait(driver, 45)
        
        for f in os.listdir(DOWNLOAD_DIR): os.remove(os.path.join(DOWNLOAD_DIR, f))

        # Wait for any "TXT" button to appear
        txt_xpath = "//button[contains(., 'TXT')]"
        txt_button = wait.until(EC.element_to_be_clickable((By.XPATH, txt_xpath)))
        
        log(f"🚀 Found TXT button for {youtube_url[:40]}...")
        driver.execute_script("arguments[0].click();", txt_button)
        
        timeout = 30
        start_time = time.time()
        while time.time() - start_time < timeout:
            files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith('.txt')]
            if files:
                file_path = os.path.join(DOWNLOAD_DIR, files[0])
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()
                return content
            time.sleep(2)
        return None
    except Exception as e:
        log(f"⚠️ Transcript skip: {youtube_url[:40]}...")
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
        log(f"✅ DB Updated: {video_id}")
    except Exception as e:
        log(f"❌ DB Error: {e}")

if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "https://www.youtube.com/@SMKC"
    
    if "channel" in target or "/@" in target:
        urls_to_process = get_latest_videos_from_channel(target, count=3)
    else:
        urls_to_process = [target]

    if not urls_to_process:
        log("❌ No videos found to process.")
        sys.exit(1)

    for video_url in urls_to_process:
        log(f"🎬 Processing: {video_url}")
        vid_id = extract_video_id(video_url)
        text = download_transcript(video_url)
        if text:
            save_to_db(vid_id, video_url, text)
        else:
            log(f"⏭️ Skipping {vid_id} (No transcript found)")
        time.sleep(3)
        
