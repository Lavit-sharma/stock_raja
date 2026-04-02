import os
import re
import sys
import time
import mysql.connector
from youtube_transcript_api import YouTubeTranscriptApi

# ---------------- CONFIG ---------------- #
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "connect_timeout": 20,
    "autocommit": True
}

COOKIES_FILE = "cookies.txt"

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

class DBManager:
    def __init__(self, config):
        self.config = config
        self.conn = None

    def connect(self):
        try:
            self.conn = mysql.connector.connect(**self.config)
            log("✅ Connected to Database.")
        except mysql.connector.Error as err:
            log(f"❌ Connection Failed: {err}")
            raise

    def get_conn(self):
        if not self.conn or not self.conn.is_connected():
            self.connect()
        return self.conn

def extract_video_id(url):
    pattern = r'(?:v=|\/)([0-9A-Za-z_-]{11}).*'
    match = re.search(pattern, url)
    return match.group(1) if match else None

def run_transcript_job(video_url):
    db = DBManager(DB_CONFIG)
    video_id = extract_video_id(video_url)

    if not video_id:
        log(f"❌ Invalid URL: {video_url}")
        return

    try:
        log(f"🔍 Fetching transcript for ID: {video_id}")

        # Check if cookies file exists and has content
        proxy_cookies = None
        if os.path.exists(COOKIES_FILE) and os.path.getsize(COOKIES_FILE) > 0:
            log(f"🍪 Loading {COOKIES_FILE}...")
            proxy_cookies = COOKIES_FILE
        else:
            log("⚠️ No valid cookies.txt found. Attempting without cookies...")

        # FIXED API CALL: In 1.2.4, use the class method directly
        transcript_list = YouTubeTranscriptApi.list_transcripts(video_id, cookies=proxy_cookies)
        
        try:
            # Prefer Hindi, fallback to English
            transcript = transcript_list.find_transcript(['hi', 'en'])
        except:
            # Fallback to any available (including auto-generated)
            transcript = transcript_list.find_generated_transcript(['hi', 'en'])

        data = transcript.fetch()
        full_text = " ".join([entry['text'] for entry in data])

        conn = db.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transcript (
                id INT AUTO_INCREMENT PRIMARY KEY,
                video_id VARCHAR(50) UNIQUE,
                video_url VARCHAR(255),
                content LONGTEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        sql = """
            INSERT INTO transcript (video_id, video_url, content)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE content = VALUES(content)
        """
        cursor.execute(sql, (video_id, video_url, full_text))

        log("🚀 Transcript saved successfully.")
        cursor.close()

    except Exception as e:
        log(f"⚠️ Error Detail: {str(e)}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_transcript_job(sys.argv[1])
    else:
        log("❌ No URL provided.")
