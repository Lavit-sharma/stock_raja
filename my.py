import sys
import time
import os
import requests
import pymysql
import urllib.parse
from bs4 import BeautifulSoup
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

def get_latest_videos_via_rss(channel_url, count=3):
    """
    Scrapes the Channel's RSS feed or Main Page using BeautifulSoup.
    This is much more reliable than Selenium for finding URLs.
    """
    video_links = []
    try:
        log(f"📺 Fetching videos from: {channel_url}")
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(channel_url, headers=headers)
        
        # If it's a standard handle URL, we try to find the Channel ID for the RSS feed
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Look for the canonical link which contains the channel ID
        canonical = soup.find("link", rel="canonical")
        if canonical:
            channel_id = canonical['href'].split('/')[-1]
            rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
            rss_resp = requests.get(rss_url)
            rss_soup = BeautifulSoup(rss_resp.content, "xml")
            
            entries = rss_soup.find_all("entry")
            for entry in entries[:count]:
                video_links.append(entry.link['href'])
                
        if not video_links:
            # Fallback: Scrape links directly from page if RSS fails
            links = soup.find_all("a", href=True)
            for link in links:
                href = link['href']
                if "/watch?v=" in href:
                    full_url = f"https://www.youtube.com{href}" if href.startswith("/") else href
                    if full_url not in video_links:
                        video_links.append(full_url)
                if len(video_links) >= count: break

    except Exception as e:
        log(f"❌ Error fetching video list: {e}")
    
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
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.execute_cdp_cmd("Page.setDownloadBehavior", {"behavior": "allow", "downloadPath": DOWNLOAD_DIR})
    return driver

def download_transcript(youtube_url):
    """Uses Selenium to get transcript from DownSub"""
    driver = create_driver()
    try:
        downsub_url = f"https://downsub.com/?url={urllib.parse.quote(youtube_url)}"
        driver.get(downsub_url)
        wait = WebDriverWait(driver, 30)
        
        # Clean folder
        for f in os.listdir(DOWNLOAD_DIR): os.remove(os.path.join(DOWNLOAD_DIR, f))

        txt_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'TXT')]")))
        driver.execute_script("arguments[0].click();", txt_button)
        
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
    except Exception as e:
        log(f"⚠️ Transcript skip for {youtube_url[:30]}")
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
    target = sys.argv[1] if len(sys.argv) > 1 else "https://www.youtube.com/@SMKC"
    
    # 1. Fetch latest 3 videos using BeautifulSoup (Reliable)
    if "channel" in target or "/@" in target:
        urls_to_process = get_latest_videos_via_rss(target, count=3)
    else:
        urls_to_process = [target]

    if not urls_to_process:
        log("❌ Could not find any videos.")
        sys.exit(1)

    # 2. Process each video using Selenium (For DownSub)
    for video_url in urls_to_process:
        log(f"🎬 Processing: {video_url}")
        vid_id = extract_video_id(video_url)
        text = download_transcript(video_url)
        if text:
            save_to_db(vid_id, video_url, text)
        time.sleep(2)
