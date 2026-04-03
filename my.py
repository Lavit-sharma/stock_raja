import os
import requests
from bs4 import BeautifulSoup
import urllib.parse
import sys
import mysql.connector


# ---------------- DB CONFIG (same as your existing code) ---------------- #
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "connect_timeout": 20
}


# ---------------- YOUTUBE VIDEO ID ---------------- #
def get_video_id(youtube_url):
    parsed = urllib.parse.urlparse(youtube_url)

    if parsed.hostname == "youtu.be":
        return parsed.path[1:]

    if "youtube.com" in parsed.hostname:
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

    return video_id, transcript


# ---------------- SAVE TO DATABASE ---------------- #
def save_to_db(video_id, video_url, transcript):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        query = """
        INSERT INTO transcript (video_id, video_url, content)
        VALUES (%s, %s, %s)
        """

        cursor.execute(query, (video_id, video_url, transcript))
        conn.commit()

        print("✅ Transcript saved to database")

    except Exception as e:
        print(f"❌ DB Error: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# ---------------- MAIN ---------------- #
if __name__ == "__main__":
    if len(sys.argv) > 1:
        youtube_url = sys.argv[1]
    else:
        youtube_url = "https://www.youtube.com/watch?v=huW5sxhm3ow"

    video_id, transcript = fetch_transcript(youtube_url)

    if transcript:
        print("\n===== TRANSCRIPT FETCHED =====\n")

        save_to_db(video_id, youtube_url, transcript)

    else:
        print("❌ No transcript found")
