import os
import logging
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


def fetch_transcript(video_id):
    """
    MAXIMUM fallback logic (handles almost all cases)
    """
    try:
        transcript_list = YouTubeTranscriptApi.list_transcripts(video_id)

        # ✅ 1. Try manually created transcripts
        try:
            transcript = transcript_list.find_manually_created_transcript(['en', 'hi'])
            return transcript.fetch()
        except:
            pass

        # ✅ 2. Try generated transcripts
        try:
            transcript = transcript_list.find_generated_transcript(['en', 'hi'])
            return transcript.fetch()
        except:
            pass

        # ✅ 3. Try ANY transcript (force fetch)
        for t in transcript_list:
            try:
                return t.fetch()
            except:
                continue

        # ✅ 4. Try translation (VERY IMPORTANT)
        for t in transcript_list:
            try:
                translated = t.translate('en')
                return translated.fetch()
            except:
                continue

    except Exception as e:
        logging.error(f"Error fetching {video_id}: {str(e)}")

    return None


def save_transcript(video_id, transcript):
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

    except Exception as e:
        print(f"❌ Save failed: {video_id}")
        logging.error(str(e))


def main():
    for video_id in VIDEO_IDS:
        print(f"\n🔍 Processing: {video_id}")

        file_path = os.path.join(OUTPUT_FOLDER, f"{video_id}.txt")

        if os.path.exists(file_path):
            print("⏩ Already exists, skipping")
            continue

        transcript = fetch_transcript(video_id)

        if transcript:
            save_transcript(video_id, transcript)
        else:
            print("❌ FAILED: No transcript found")


if __name__ == "__main__":
    start = datetime.now()

    print("🚀 Starting transcript downloader...\n")

    main()

    print("\n🎉 Done!")
    print(f"⏱ Completed in: {datetime.now() - start}")
