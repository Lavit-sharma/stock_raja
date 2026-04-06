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
    text = re.sub(r'\d{2}:\d{2}:\d{2},\d{3} --> \d{2}:\d{2}:\d{2},\d{3}', '', srt_text)
    text = re.sub(r'^\d+$', '', text, flags=re.MULTILINE)
    text = re.sub(r'<[^>]*>', '', text)
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return "\n".join(lines)

def process_channel(channel_url):
    # Ensure URL ends with /videos to find the latest uploads
    base_url = channel_url.split('?')[0].rstrip('/')
    if not base_url.endswith("/videos"):
        videos_url = f"{base_url}/videos"
    else:
        videos_url = base_url
        
    log(f"📺 Target URL: {videos_url}")
    
    # 1. Get the latest 3 Video IDs
    # Using --extract-flat and --playlist-end 3 for speed and accuracy
    cmd_ids = f'yt-dlp --get-id --playlist-end 3 --extract-flat "{videos_url}"'
    
    try:
        result = subprocess.run(cmd_ids, capture_output=True, text=True, shell=True)
        # If the output is empty, try the main channel URL as a fallback
        if not result.stdout.strip():
            log("⚠️ Videos tab empty, trying channel root...")
            cmd_ids = f'yt-dlp --get-id --playlist-end 3 --extract-flat "{base_url}"'
            result = subprocess.run(cmd_ids, capture_output=True, text=True, shell=True)
            
        video_ids = [v.strip() for v in result.stdout.split('\n') if v.strip()]
    except Exception as e:
        log(f"❌ Error running yt-dlp: {e}")
        return

    if not video_ids:
        log("❌ No videos found. Check if the channel is public.")
        return

    log(f"✅ Found {len(video_ids)} videos. Starting transcript sync...")

    for v_id in video_ids:
        video_url = f"https://www.youtube.com/watch?v={v_id}"
        log(f"🎬 Processing: {v_id}")

        output_template = f"trans_{v_id}"
        # Download auto-generated subs (Hindi first, then English)
        cmd_dl = (
            f'yt-dlp --skip-download --write-auto-subs --sub-lang "hi.*,en.*" '
            f'--convert-subs srt -o "{output_template}" "{video_url}"'
        )
        subprocess.run(cmd_dl, shell=True)

        # Find the generated srt file
        srt_file = None
        for f in os.listdir('.'):
            if f.startswith(output_template) and f.endswith(".srt"):
                srt_file = f
                break

        if srt_file:
            with open(srt_file, 'r', encoding='utf-8') as f:
                content = clean_srt(f.read())
            
            try:
                with closing(pymysql.connect(**DB_CONFIG)) as conn:
                    with conn.cursor() as cursor:
                        sql = """
                            INSERT INTO wp_transcript (video_id, video_url, content)
                            VALUES (%s, %s, %s)
                            ON DUPLICATE KEY UPDATE content = VALUES(content)
                        """
                        cursor.execute(sql, (v_id, video_url, content))
                    conn.commit()
                log(f"💾 Saved to DB: {v_id}")
            except Exception as e:
                log(f"❌ DB Error: {e}")
            
            os.remove(srt_file) # Cleanup
        else:
            log(f"⚠️ Transcript unavailable for {v_id}")

if __name__ == "__main__":
    # Use the official handle for Stock Market Ka Commando
    channel = "https://www.youtube.com/@stockmarketcommando"
    process_channel(channel)
