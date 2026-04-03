import os
import requests
import mysql.connector
from bs4 import BeautifulSoup

# ---------------- CONFIG ---------------- #
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}

TABLE_PREFIX = os.getenv("WP_TABLE_PREFIX", "wp_")
TABLE_NAME = f"{TABLE_PREFIX}transcript"

# 🔥 CHANNEL HANDLE
CHANNEL_HANDLE = "stockmarketcommando"


# ---------------- GET CHANNEL ID ---------------- #
def get_channel_id_from_handle(handle):
    url = f"https://www.googleapis.com/youtube/v3/search?part=snippet&q={handle}&type=channel&key={YOUTUBE_API_KEY}"
    res = requests.get(url).json()

    if "items" not in res or not res["items"]:
        return None

    return res["items"][0]["snippet"]["channelId"]


# ---------------- GET LATEST VIDEOS ---------------- #
def get_latest_videos(channel_id, max_results=5):
    url = f"https://www.googleapis.com/youtube/v3/search?key={YOUTUBE_API_KEY}&channelId={channel_id}&part=snippet,id&order=date&maxResults={max_results}"

    res = requests.get(url).json()

    videos = []
    for item in res.get("items", []):
        if item["id"]["kind"] == "youtube#video":
            vid = item["id"]["videoId"]
            videos.append(f"https://www.youtube.com/watch?v={vid}")

    return videos


# ---------------- FETCH TRANSCRIPT ---------------- #
def fetch_transcript(url):
    tactiq_url = f"https://tactiq.io/tools/run/youtube_transcript?yt={url}"

    headers = {"User-Agent": "Mozilla/5.0"}
    res = requests.get(tactiq_url, headers=headers)

    soup = BeautifulSoup(res.text, "html.parser")
    blocks = soup.find_all("p")

    return "\n".join([b.get_text(strip=True) for b in blocks])


# ---------------- SAVE TO DB ---------------- #
def save_to_db(video_id, video_url, transcript):
    conn = None
    cursor = None

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        query = f"""
        INSERT INTO {TABLE_NAME} (video_id, video_url, content)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE content = VALUES(content)
        """

        cursor.execute(query, (video_id, video_url, transcript))
        conn.commit()

        print(f"✅ Saved: {video_id}")

    except Exception as e:
        print("❌ DB Error:", e)

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# ---------------- MAIN ---------------- #
if __name__ == "__main__":
    print("🔍 Finding channel ID...")

    channel_id = get_channel_id_from_handle(CHANNEL_HANDLE)

    if not channel_id:
        print("❌ Channel not found")
        exit()

    print("📺 Fetching latest videos...")
    videos = get_latest_videos(channel_id, max_results=5)

    for url in videos:
        video_id = url.split("v=")[-1]

        print(f"\n🚀 Processing: {url}")

        transcript = fetch_transcript(url)

        if transcript.strip():
            save_to_db(video_id, url, transcript)
        else:
            print("❌ No transcript found")
