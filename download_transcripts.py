import os
import re
import json
import logging
import requests
from datetime import datetime
from youtube_transcript_api import YouTubeTranscriptApi

# ---------------- CONFIG ---------------- #
OUTPUT_FOLDER = "transcripts"
LOG_FILE = "transcript.log"

VIDEO_IDS = [
    "Ks-_Mh1QhMc",
    "3JZ_D3ELwOQ"
]

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# ---------------- API METHOD ---------------- #
def fetch_transcript_api(video_id):
    try:
        transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)

        for t in transcript_list:
            return t.fetch()

    except Exception:
        return None


# ---------------- SCRAPING METHOD ---------------- #
def fetch_transcript_scrape(video_id):
    try:
        url = f"https://www.youtube.com/watch?v={video_id}"
        response = requests.get(url)

        if "captions" not in response.text:
            return None

        match = re.search(r'"captionTracks":(\[.*?\])', response.text)

        if not match:
            return None

        caption_tracks = json.loads(match.group(1))
        caption_url = caption_tracks[0]['baseUrl']

        xml = requests.get(caption_url).text

        texts = re.findall(r'<text start="(.*?)".*?>(.*?)</text>', xml)

        transcript = []
        for start, text in texts:
            clean_text = re.sub(r'<.*?>', '', text)
            transcript.append({
                "start": float(start),
                "text": clean_text
            })

        return transcript

    except Exception as e:
        logging.error(f"Scrape failed {video_id}: {str(e)}")
        return None


# ---------------- SAVE ---------------- #
def save_transcript(video_id, transcript):
    file_path = os.path.join(OUTPUT_FOLDER, f"{video_id}.txt")

    lines = []
    for entry in transcript:
        start = round(entry.get("start", 0), 2)
        text = entry.get("text", "").strip()
        lines.append(f"[{start}s] {text}")

    with open(file_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"✅ Saved: {file_path}")


# ---------------- MAIN ---------------- #
def process(video_id):
    print(f"\n🔍 Processing: {video_id}")

    file_path = os.path.join(OUTPUT_FOLDER, f"{video_id}.txt")

    if os.path.exists(file_path):
        print("⏩ Already exists")
        return

    # 1. Try API
    transcript = fetch_transcript_api(video_id)

    # 2. Fallback to scraping
    if not transcript:
        print("⚠️ API failed, trying scraping...")
        transcript = fetch_transcript_scrape(video_id)

    if transcript:
        save_transcript(video_id, transcript)
    else:
        print("❌ FINAL FAIL: No transcript found")


if __name__ == "__main__":
    start = datetime.now()

    print("🚀 Starting transcript downloader...\n")

    for vid in VIDEO_IDS:
        process(vid)

    print("\n🎉 Done!")
    print(f"⏱ Completed in: {datetime.now() - start}")
