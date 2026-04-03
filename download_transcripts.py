import os
import re
import logging
from datetime import datetime
from youtube_transcript_api import (
    YouTubeTranscriptApi,
    TranscriptsDisabled,
    NoTranscriptFound
)

# ---------------- CONFIG ---------------- #
OUTPUT_FOLDER = "transcripts"
URL_FILE = "video_urls.txt"
LOG_FILE = "transcript.log"

LANGUAGES = ["en", "hi"]  # preferred languages

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# ---------------- LOGGING ---------------- #
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# ---------------- FUNCTIONS ---------------- #
def extract_video_id(url: str):
    """
    Extract YouTube video ID from all URL formats
    """
    patterns = [
        r"(?:v=|\/)([0-9A-Za-z_-]{11})",
        r"youtu\.be\/([0-9A-Za-z_-]{11})",
        r"youtube\.com\/shorts\/([0-9A-Za-z_-]{11})"
    ]

    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)

    return None


def fetch_transcript(video_id: str):
    """
    Fetch transcript using multiple fallback strategies
    """
    try:
        # Primary method
        return YouTubeTranscriptApi.get_transcript(video_id, languages=LANGUAGES)

    except (TranscriptsDisabled, NoTranscriptFound):
        logging.warning(f"Primary transcript not available: {video_id}")

    except Exception as e:
        logging.error(f"Primary fetch error {video_id}: {str(e)}")

    # 🔁 Fallback method
    try:
        transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)

        # 1. Try manually created transcripts
        for transcript in transcript_list:
            if not transcript.is_generated:
                logging.info(f"Using manual transcript: {video_id}")
                return transcript.fetch()

        # 2. Try auto-generated transcripts
        for transcript in transcript_list:
            if transcript.is_generated:
                logging.info(f"Using auto transcript: {video_id}")
                return transcript.fetch()

    except Exception as e:
        logging.error(f"Fallback failed {video_id}: {str(e)}")

    return None


def save_transcript(video_id: str, transcript: list):
    """
    Save transcript with timestamps
    """
    try:
        file_path = os.path.join(OUTPUT_FOLDER, f"{video_id}.txt")

        lines = []
        for entry in transcript:
            start = round(entry.get("start", 0), 2)
            text = entry.get("text", "").strip()
            lines.append(f"[{start}s] {text}")

        with open(file_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))

        logging.info(f"Saved: {file_path}")
        print(f"✅ Saved: {file_path}")

    except Exception as e:
        logging.error(f"Save failed {video_id}: {str(e)}")
        print(f"❌ Save failed: {video_id}")


def process_video(url: str):
    """
    Process single video
    """
    video_id = extract_video_id(url)

    if not video_id:
        logging.warning(f"Invalid URL: {url}")
        print(f"❌ Invalid URL: {url}")
        return

    file_path = os.path.join(OUTPUT_FOLDER, f"{video_id}.txt")

    if os.path.exists(file_path):
        logging.info(f"Skipped: {video_id}")
        print(f"⏩ Skipped: {video_id}")
        return

    transcript = fetch_transcript(video_id)

    if transcript:
        save_transcript(video_id, transcript)
    else:
        logging.warning(f"No transcript: {video_id}")
        print(f"⚠️ No transcript: {video_id}")


def process_all_videos():
    """
    Main execution
    """
    if not os.path.exists(URL_FILE):
        logging.error("video_urls.txt not found")
        print("❌ video_urls.txt not found")
        return

    with open(URL_FILE, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]

    logging.info(f"Processing {len(urls)} videos")

    for url in urls:
        process_video(url)

    logging.info("Completed all videos")


# ---------------- ENTRY ---------------- #
if __name__ == "__main__":
    start = datetime.now()

    print("🚀 Starting transcript downloader...\n")

    process_all_videos()

    duration = datetime.now() - start
    print(f"\n🎉 Done!")
    print(f"⏱ Completed in: {duration}")
