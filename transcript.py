import os
import re
import time
import mysql.connector
from youtube_transcript_api import YouTubeTranscriptApi

# ---------------- CONFIG ---------------- #
# This pulls from your existing environment variables
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
    """Independent DB Manager using your existing connection logic"""
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
    """Extracts the 11-char ID from a YouTube URL"""
    pattern = r'(?:v=|\/)([0-9A-Za-z_-]{11}).*'
    match = re.search(pattern, url)
    return match.group(1) if match else None

def run_transcript_job(video_url):
    db = DBManager(DB_CONFIG)
    video_id = extract_video_id(video_url)

    if not video_id:
        log("❌ Invalid YouTube URL provided.")
        return

    try:
        # 1. Fetch Transcript (English & Hindi support)
        log(f"Searching transcript for: {video_id}")
        transcript_list = YouTubeTranscriptApi.get_transcript(video_id, languages=['hi', 'en'])
        full_text = " ".join([entry['text'] for entry in transcript_list])

        # 2. Connect and Save
        conn = db.get_conn()
        cursor = conn.cursor()

        # Create the table if it doesn't exist in your WP database
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transcript (
                id INT AUTO_INCREMENT PRIMARY KEY,
                video_id VARCHAR(50) UNIQUE,
                video_url VARCHAR(255),
                content LONGTEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 3. Insert data (using IGNORE to prevent duplicates of the same video)
        sql = "INSERT IGNORE INTO transcript (video_id, video_url, content) VALUES (%s, %s, %s)"
        cursor.execute(sql, (video_id, video_url, full_text))
        
        if cursor.rowcount > 0:
            log(f"🚀 Success! Transcript for {video_id} saved to 'transcript' table.")
        else:
            log(f"ℹ️ Video {video_id} already exists in the database. Skipping.")

        cursor.close()

    except Exception as e:
        log(f"⚠️ Error occurred: {e}")

if __name__ == "__main__":
    # Input your URL here
    url_to_process = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    
    run_transcript_job(url_to_process)
