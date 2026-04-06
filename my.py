import os
import requests
import pymysql
import xml.etree.ElementTree as ET
from datetime import datetime
from contextlib import closing
from bs4 import BeautifulSoup
import urllib.parse

# ---------------- CONFIG ---------------- #
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'charset': 'utf8mb4'
}

CHANNEL_ID = os.getenv("YOUTUBE_CHANNEL_ID")

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

# ---------------- GET LATEST VIDEOS ---------------- #
def get_latest_videos(max_results=3):
    if not CHANNEL_ID:
        log("❌ Missing CHANNEL_ID")
        return []

    log(f"📡 Using CHANNEL_ID: {CHANNEL_ID}")

    url = f"https://www.youtube.com/feeds/videos.xml?channel_id={CHANNEL_ID}"

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    res = requests.get(url, headers=headers)

    if res.status_code != 200:
        log(f"❌ Feed failed: {res.status_code}")
        return []

    root = ET.fromstring(res.content)

    videos = []
    for entry in root.findall("{http://www.w3.org/2005/Atom}entry")[:max_results]:
        vid = entry.find("{http://www.youtube.com/xml/schemas/2015}videoId").text
        videos.append(f"https://www.youtube.com/watch?v={vid}")

    return videos

# ---------------- TRANSCRIPT (TACTIQ) ---------------- #
def fetch_transcript(url):
    tactiq_url = f"https://tactiq.io/tools/run/youtube_transcript?yt={urllib.parse.quote(url)}"

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    res = requests.get(tactiq_url, headers=headers)

    if res.status_code != 200:
        return None

    soup = BeautifulSoup(res.text, "html.parser")
    blocks = soup.find_all("p")

    return "\n".join([b.get_text(strip=True) for b in blocks])

# ---------------- DB CHECK ---------------- #
def is_processed(video_id):
    try:
        with closing(pymysql.connect(**DB_CONFIG)) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 FROM wp_transcript WHERE video_id=%s LIMIT 1", (video_id,))
                return cursor.fetchone() is not None
    except:
        return False

def extract_video_id(url):
    return url.split("v=")[1].split("&")[0]

# ---------------- PROCESS ---------------- #
def process(url):
    vid = extract_video_id(url)

    if is_processed(vid):
        log(f"⏭️ Skipping {vid}")
        return

    log(f"🚀 Processing {url}")

    transcript = fetch_transcript(url)

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
                """, (vid, url, transcript))
            conn.commit()

        log("✅ Saved")

    except Exception as e:
        log(f"❌ DB Error: {e}")

# ---------------- MAIN ---------------- #
if __name__ == "__main__":
    videos = get_latest_videos(3)

    for v in videos:
        process(v)
