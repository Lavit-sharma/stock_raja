import os
import re
import sys
import time
import mysql.connector
import youtube_transcript_api

# ---------------- CONFIG ---------------- #
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "connect_timeout": 20,
    "autocommit": True
}

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

class DBManager:
    def __init__(self, config):
        self.config = config
        self.conn = None

    def connect(self):
        try:
            self.conn = mysql.connector.connect(**self.config)
            log("✅ Connected to WordPress Database.")
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
        log(f"❌ Invalid YouTube URL: {video_url}")
        return

    try:
        log(f"🔍 Searching transcript for: {video_id}")
        
        # CHANGED: Explicit call using the module's instance
        loader = youtube_transcript_api.YouTubeTranscriptApi
        transcript_list = loader.get_transcript(video_id, languages=['hi', 'en'])
        
        full_text = " ".join([entry['text'] for entry in transcript_list])

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

        sql = "INSERT IGNORE INTO transcript (video_id, video_url, content) VALUES (%s, %s, %s)"
        cursor.execute(sql, (video_id, video_url, full_text))
        
        if cursor.rowcount > 0:
            log(f"🚀 Success! Transcript saved for {video_id}")
        else:
            log(f"ℹ️ Video {video_id} already exists. Skipping.")

        cursor.close()

    except Exception as e:
        log(f"⚠️ Error: {str(e)}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_transcript_job(sys.argv[1])
    else:
        log("❌ No URL provided.")
