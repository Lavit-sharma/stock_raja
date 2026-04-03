import os
import requests
import mysql.connector
from bs4 import BeautifulSoup
from datetime import datetime, timezone

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
def get_latest_videos(channel_id, max_results=5):
    url = f"https://www.googleapis.com/youtube/v3/search?key={YOUTUBE_API_KEY}&channelId={channel_id}&part=snippet,id&order=date&maxResults={max_results}"

    res = requests.get(url).json()

    videos = []
    for item in res.get("items", []):
        if item["id"]["kind"] == "youtube#video":
            vid = item["id"]["videoId"]
            title = item["snippet"]["title"]
            published_at = item["snippet"]["publishedAt"]

            videos.append({
                "video_id": vid,
                "url": f"https://www.youtube.com/watch?v={vid}",
                "title": title,
                "published_at": published_at
            })

    return videos


# ---------------- CHECK IF TODAY ---------------- #
def is_today(published_at):
    video_date = datetime.fromisoformat(published_at.replace("Z", "+00:00")).date()
    today = datetime.now(timezone.utc).date()
    return video_date == today


# ---------------- FETCH TRANSCRIPT ---------------- #
def fetch_transcript(video_url):
    tactiq_url = f"https://tactiq.io/tools/run/youtube_transcript?yt={video_url}"

    headers = {"User-Agent": "Mozilla/5.0"}
    res = requests.get(tactiq_url, headers=headers)

    soup = BeautifulSoup(res.text, "html.parser")
    blocks = soup.find_all("p")

    return "\n".join([b.get_text(strip=True) for b in blocks])


# ---------------- DELETE OLD DATA ---------------- #
def delete_old_records():
    conn = None
    cursor = None

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        query = f"""
        DELETE FROM {TABLE_NAME}
        WHERE DATE(created_at) < CURDATE()
        """

        cursor.execute(query)
        conn.commit()

        print("🧹 Old records deleted")

    except Exception as e:
        print("❌ Delete Error:", e)

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
        ON DUPLICATE KEY UPDATE 
            title = VALUES(title),
            content = VALUES(content)
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
    print("🧹 Cleaning old data...")
    delete_old_records()

    print("🔍 Getting channel...")
    channel_id = get_channel_id(CHANNEL_HANDLE)

    if not channel_id:
        print("❌ Channel not found")
        exit()

    print("📺 Fetching latest videos...")
    videos = get_latest_videos(channel_id)

    for video in videos:
        if not is_today(video["published_at"]):
            continue  # ❌ skip old videos

        video_id = video["video_id"]
        url = video["url"]
        title = video["title"]

        print(f"\n🚀 Processing today's video: {title}")

        transcript = fetch_transcript(url)

        if transcript.strip():
            save_to_db(video_id, url, title, transcript)
        else:
            print("❌ No transcript found")
