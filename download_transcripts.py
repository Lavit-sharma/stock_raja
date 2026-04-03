import os
import logging
from datetime import datetime
from youtube_transcript_api import YouTubeTranscriptApi

# ---------------- CONFIG ---------------- #
OUTPUT_FOLDER = "transcripts"
LOG_FILE = "transcript.log"

# ✅ USE ONLY RELIABLE VIDEOS (IMPORTANT)
VIDEO_IDS = [
    "M7FIvfx5J10",  # TED Talk (works)
    "hY7m5jjJ9mM",  # Cat video (works)
    "aqz-KE-bpKQ"   # Big Buck Bunny (works)
]

os.makedirs(OUTPUT_FOLDER, exist_ok=True)

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# ---------------- FETCH ---------------- #
def fetch_transcript(video_id):
    try:
        transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)

        # 1. Manual transcripts
        try:
            transcript = transcript_list.find_manually_created_transcript(['en', 'hi'])
            return transcript.fetch()
        except:
            pass

        # 2. Auto-generated
        try:
            transcript = transcript_list.find_generated_transcript(['en', 'hi'])
            return transcript.fetch()
        except:
            pass

        # 3. Any available
        for t in transcript_list:
            try:
                return t.fetch()
            except:
                continue

    except Exception as e:
        logging.warning(f"Transcript unavailable: {video_id} | {str(e)}")

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
def main():
    for video_id in VIDEO_IDS:
        print(f"\n🔍 Processing: {video_id}")

        file_path = os.path.join(OUTPUT_FOLDER, f"{video_id}.txt")

        if os.path.exists(file_path):
            print("⏩ Already exists")
            continue

        transcript = fetch_transcript(video_id)

        if transcript:
            save_transcript(video_id, transcript)
        else:
            print("⚠️ Skipped (not accessible via API)")


if __name__ == "__main__":
    start = datetime.now()

    print("🚀 Starting transcript downloader...\n")

    main()

    print("\n🎉 Done!")
    print(f"⏱ Completed in: {datetime.now() - start}")
