import os
import re
import logging
from datetime import datetime
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound

# ---------------- CONFIG ---------------- #
OUTPUT_FOLDER = "transcripts"
URL_FILE = "video_urls.txt"
LOG_FILE = "transcript.log"

# Preferred languages (fallback order)
LANGUAGES = ["en", "hi"]

# Create folders
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# ---------------- LOGGING SETUP ---------------- #
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# ---------------- UTIL FUNCTIONS ---------------- #
def extract_video_id(url: str) -> str | None:
    """
    Extract YouTube video ID from URL
    """
    pattern = r"(?:v=|\/)([0-9A-Za-z_-]{11})"
    match = re.search(pattern, url)
    return match.group(1) if match else None


def fetch_transcript(video_id: str):
    """
    Try fetching transcript in preferred languages
    """
    try:
        transcript = YouTubeTranscriptApi.get_transcript(video_id, languages=LANGUAGES)
        return transcript

    except TranscriptsDisabled:
        logging.warning(f"Transcripts disabled for video: {video_id}")
    except NoTranscriptFound:
        logging.warning(f"No transcript found for video: {video_id}")
    except Exception as e:
        logging.error(f"Unexpected error for {video_id}: {str(e)}")

    return None


def save_transcript(video_id: str, transcript: list):
    """
    Save transcript as TXT file
    """
    try:
        file_path = os.path.join(OUTPUT_FOLDER, f"{video_id}.txt")

        # Format with timestamps
        formatted_text = []
        for entry in transcript:
            start = round(entry.get("start", 0), 2)
            text = entry.get("text", "").strip()
            formatted_text.append(f"[{start}s] {text}")

        with open(file_path, "w", encoding="utf-8") as f:
            f.write("\n".join(formatted_text))

        logging.info(f"Saved transcript: {file_path}")
        print(f"✅ Saved: {file_path}")

    except Exception as e:
        logging.error(f"Failed to save transcript for {video_id}: {str(e)}")
        print(f"❌ Save failed: {video_id}")


def process_video(url: str):
    """
    Process a single video URL
    """
    video_id = extract_video_id(url)

    if not video_id:
        logging.warning(f"Invalid URL: {url}")
        print(f"❌ Invalid URL: {url}")
        return

    file_path = os.path.join(OUTPUT_FOLDER, f"{video_id}.txt")

    # Skip if already exists
    if os.path.exists(file_path):
        logging.info(f"Skipped (already exists): {video_id}")
        print(f"⏩ Skipped: {video_id}")
        return

    transcript = fetch_transcript(video_id)

    if transcript:
        save_transcript(video_id, transcript)
    else:
        print(f"⚠️ No transcript: {video_id}")


def process_all_videos():
    """
    Main execution function
    """
    if not os.path.exists(URL_FILE):
        logging.error("video_urls.txt not found")
        print("❌ video_urls.txt not found")
        return

    with open(URL_FILE, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]

    logging.info(f"Starting transcript download for {len(urls)} videos")

    for url in urls:
        process_video(url)

    logging.info("Processing completed")
    print("🎉 Done!")


# ---------------- ENTRY POINT ---------------- #
if __name__ == "__main__":
    start_time = datetime.now()
    print("🚀 Starting transcript downloader...\n")

    process_all_videos()

    duration = datetime.now() - start_time
    print(f"\n⏱ Completed in: {duration}")
