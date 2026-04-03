import os
import requests
from bs4 import BeautifulSoup
import urllib.parse
import sys
import mysql.connector


# ---------------- DB CONFIG ---------------- #
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),       # MUST NOT be localhost in GitHub
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "connect_timeout": 20
}

# WordPress table prefix (default wp_)
TABLE_PREFIX = os.getenv("WP_TABLE_PREFIX", "wp_")
TABLE_NAME = f"{TABLE_PREFIX}transcript"


# ---------------- GET VIDEO ID ---------------- #
def get_video_id(youtube_url):
    parsed = urllib.parse.urlparse(youtube_url)

    if parsed.hostname == "youtu.be":
        return parsed.path[1:]

    if parsed.hostname and "youtube.com" in parsed.hostname:
        query = urllib.parse.parse_qs(parsed.query)
        return query.get("v", [None])[0]

    return None


# ---------------- FETCH TRANSCRIPT ---------------- #
def fetch_transcript(youtube_url):
    video_id = get_video_id(youtube_url)

    if not video_id:
        print("❌ Invalid YouTube URL")
        return None, None

    tactiq_url = f"https://tactiq.io/tools/run/youtube_transcript?yt={urllib.parse.quote(youtube_url)}"

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    response = requests.get(tactiq_url, headers=headers)

    if response.status_code != 200:
        print("❌ Failed to fetch page")
        return None, None

    soup = BeautifulSoup(response.text, "html.parser")

    transcript_blocks = soup.find_all("p")
    transcript = "\n".join([p.get_text(strip=True) for p in transcript_blocks])

    if not transcript.strip():
        print("❌ Empty transcript")
        return None, None

    return video_id, transcript


# ---------------- SAVE TO WORDPRESS DB ---------------- #
def save_to_db(video_id, video_url, transcript):
    conn = None
    cursor = None

    try:
        print(f"🔌 Connecting to DB: {DB_CONFIG['host']}")

        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Optional: prevent duplicate video_id
        query = f"""
        INSERT INTO {TABLE_NAME} (video_id, video_url, content)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            content = VALUES(content),
            video_url = VALUES(video_url)
        """

        cursor.execute(query, (video_id, video_url, transcript))
        conn.commit()

        print(f"✅ Saved to WordPress table: {TABLE_NAME}")

    except Exception as e:
        print(f"❌ DB Error: {e}")
        sys.exit(1)

    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


# ---------------- MAIN ---------------- #
if __name__ == "__main__":
    if len(sys.argv) > 1:
        youtube_url = sys.argv[1]
    else:
        youtube_url = "https://www.youtube.com/watch?v=huW5sxhm3ow"

    print("🚀 Starting transcript fetch...")

    video_id, transcript = fetch_transcript(youtube_url)

    if transcript:
        print("\n===== TRANSCRIPT FETCHED =====\n")
        save_to_db(video_id, youtube_url, transcript)
    else:
        print("❌ No transcript found")
        sys.exit(1)
