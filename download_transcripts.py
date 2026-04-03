import os
import logging
from datetime import datetime
from youtube_transcript_api import YouTubeTranscriptApi

# ---------------- CONFIG ---------------- #
OUTPUT_FOLDER = "transcripts"
LOG_FILE = "transcript.log"

# ✅ HARDCODED WORKING VIDEO IDS (educational videos with captions)
VIDEO_IDS = [
    "Ks-_Mh1QhMc",   # Example: educational (usually works)
    "3JZ_D3ELwOQ"    # Example: popular video with captions
]

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# ---------------- LOGGING ---------------- #
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# ---------------- FUNCTIONS ---------------- #
def fetch_transcript(video_id):
    """
    Fetch transcript using all possible fallbacks
    """
    try:
        # Try direct method
        return YouTubeTranscriptApi.get_transcript(video_id)

    except Exception:
        pass

    try:
        # Fallback: list transcripts
        transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)

        # Try manual transcripts
        for t in transcript_list:
            if not t.is_generated:
                return t.fetch()

        # Try auto-generated
        for t in transcript_list:
            if t.is_generated:
                return t.fetch()

    except Exception as e:
        logging.warning(f"Failed for {video_id}: {str(e)}")

    return None


def save_transcript(video_id, transcript):
    """
    Save transcript to file
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

        print(f"✅ Saved: {file_path}")
        logging.info(f"Saved {video_id}")

    except Exception as e:
        print(f"❌ Save failed: {video_id}")
        logging.error(str(e))


def process_videos():
    """
    Process all hardcoded videos
    """
    for video_id in VIDEO_IDS:
        file_path = os.path.join(OUTPUT_FOLDER, f"{video_id}.txt")

        # Skip existing
        if os.path.exists(file_path):
            print(f"⏩ Skipped: {video_id}")
            continue

        transcript = fetch_transcript(video_id)

        if transcript:
            save_transcript(video_id, transcript)
        else:
            print(f"⚠️ Skipped (no transcript available): {video_id}")
            logging.warning(f"No transcript: {video_id}")


# ---------------- MAIN ---------------- #
if __name__ == "__main__":
    start = datetime.now()

    print("🚀 Starting transcript downloader...\n")

    process_videos()

    duration = datetime.now() - start

    print("\n🎉 Done!")
    print(f"⏱ Completed in: {duration}")
