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

CHANNEL_HANDLE = "stockmarketcommando"


# ---------------- GET CHANNEL ID ---------------- #
def get_channel_id(handle):
    url = f"https://www.googleapis.com/youtube/v3/search?part=snippet&q={handle}&type=channel&key={YOUTUBE_API_KEY}"
    res = requests.get(url).json()

    if not res.get("items"):
        return None

    return res["items"][0]["snippet"]["channelId"]


# ---------------- GET LATEST VIDEOS ---------------- #
def get_latest_videos(channel_id, max_results=3):
    url = f"https://www.googleapis.com/youtube/v3/search?key={YOUTUBE_API_KEY}&channelId={channel_id}&part=snippet,id&order=date&maxResults={max_results}"

    res = requests.get(url).json()

    videos = []
    for item in res.get("items", []):
        if item["id"]["kind"] == "youtube#video":
            vid = item["id"]["videoId"]
            title = item["snippet"]["title"]

            videos.append({
                "video_id": vid,
                "url": f"https://www.youtube.com/watch?v={vid}",
                "title": title
            })

    return videos


# ---------------- FETCH TRANSCRIPT ---------------- #
def fetch_transcript(video_url):
    tactiq_url = f"https://tactiq.io/tools/run/youtube_transcript?yt={video_url}"

    headers = {"User-Agent": "Mozilla/5.0"}
    res = requests.get(tactiq_url, headers=headers)

    soup = BeautifulSoup(res.text, "html.parser")
    blocks = soup.find_all("p")

    transcript = "\n".join([b.get_text(strip=True) for b in blocks])
    return transcript


# ---------------- CHECK IF EXISTS ---------------- #
def already_exists(video_id):
    conn = None
    cursor = None

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        query = f"SELECT id FROM {TABLE_NAME} WHERE video_id = %s LIMIT 1"
        cursor.execute(query, (video_id,))

        return cursor.fetchone() is not None

    except:
        return False

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# ---------------- SAVE TO DB ---------------- #
def save_to_db(video_id, video_url, title, transcript):
    conn = None
    cursor = None

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        query = f"""
        INSERT INTO {TABLE_NAME} (video_id, video_url, title, content)
        VALUES (%s, %s, %s, %s)
        """

        cursor.execute(query, (video_id, video_url, title, transcript))
        conn.commit()

        print(f"✅ Saved: {title}")

    except Exception as e:
        print("❌ DB Error:", e)

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# ---------------- MAIN ---------------- #
if __name__ == "__main__":
    print("🔍 Getting channel...")

    channel_id = get_channel_id(CHANNEL_HANDLE)

    if not channel_id:
        print("❌ Channel not found")
        exit()

    print("📺 Fetching latest videos...")
    videos = get_latest_videos(channel_id, max_results=3)  # 🔥 only latest 3

    for video in videos:
        video_id = video["video_id"]
        url = video["url"]
        title = video["title"]

        print(f"\n🚀 Processing: {title}")

        # ✅ Skip if already stored
        if already_exists(video_id):
            print("⏭ Already exists, skipping...")
            continue

        transcript = fetch_transcript(url)

        if transcript.strip():
            save_to_db(video_id, url, title, transcript)
        else:
            print("❌ No transcript found")
