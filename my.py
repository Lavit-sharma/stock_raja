import os
import sys
import subprocess
import pymysql
import re
from datetime import datetime
from contextlib import closing

# ---------------- CONFIG FROM SECRETS ---------------- #
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME'),
    'charset': 'utf8mb4'
}

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def clean_srt(srt_text):
    """Removes timestamps, formatting, and line numbers from SRT files"""
    # Remove timestamps
    text = re.sub(r'\d{2}:\d{2}:\d{2},\d{3} --> \d{2}:\d{2}:\d{2},\d{3}', '', srt_text)
    # Remove SRT line numbers
    text = re.sub(r'^\d+$', '', text, flags=re.MULTILINE)
    # Remove HTML-style tags (like <font>)
    text = re.sub(r'<[^>]*>', '', text)
    # Join lines and strip extra whitespace
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return "\n".join(lines)

def process_channel(channel_url):
    log(f"📺 Accessing channel: {channel_url}")
    
    # 1. Get the latest 3 Video IDs using yt-dlp
    cmd_ids = f'yt-dlp --get-id --playlist-end 3 "{channel_url}"'
    result = subprocess.run(cmd_ids, capture_output=True, text=True, shell=True)
    video_ids = [v.strip() for v in result.stdout.split('\n') if v.strip()]
    
    if not video_ids:
        log("❌ No videos found. Check the channel URL.")
        return

    log(f"✅ Found {len(video_ids)} videos. Starting transcript sync...")

    for v_id in video_ids:
        video_url = f"https://www.youtube.com/watch?v={v_id}"
        log(f"🎬 Processing: {v_id}")

        # 2. Download subtitles as SRT
        # We prefer Hindi but allow English fallback
        output_template = f"trans_{v_id}"
        cmd_dl = (
            f'yt-dlp --skip-download --write-auto-subs --sub-lang "hi.*,en.*" '
            f'--convert-subs srt -o "{output_template}" "{video_url}"'
        )
        subprocess.run(cmd_dl, shell=True)

        # 3. Find the downloaded file
        srt_file = None
        for f in os.listdir('.'):
            if f.startswith(output_template) and f.endswith(".srt"):
                srt_file = f
                break

        if srt_file:
            with open(srt_file, 'r', encoding='utf-8') as f:
                raw_data = f.read()
            
            clean_text = clean_srt(raw_data)
            
            # 4. Store in Database
            try:
                with closing(pymysql.connect(**DB_CONFIG)) as conn:
                    with conn.cursor() as cursor:
                        sql = """
                            INSERT INTO wp_transcript (video_id, video_url, content)
                            VALUES (%s, %s, %s)
                            ON DUPLICATE KEY UPDATE content = VALUES(content)
                        """
                        cursor.execute(sql, (v_id, video_url, clean_text))
                    conn.commit()
                log(f"💾 Saved to DB: {v_id}")
            except Exception as e:
                log(f"❌ DB Error: {e}")
            
            os.remove(srt_file) # Delete the temp file
        else:
            log(f"⚠️ Transcript not found for {v_id}")

if __name__ == "__main__":
    # Your target channel
    channel = "https://www.youtube.com/@stockmarketcommando/videos"
    process_channel(channel)
