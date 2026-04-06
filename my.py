import requests
from bs4 import BeautifulSoup
import urllib.parse
import sys

# ---------------- GET VIDEO ID ---------------- #
def get_video_id(youtube_url):
    parsed = urllib.parse.urlparse(youtube_url)

    if parsed.hostname == "youtu.be":
        return parsed.path[1:]

    if "youtube.com" in parsed.hostname:
        query = urllib.parse.parse_qs(parsed.query)
        return query.get("v", [None])[0]

    return None


# ---------------- FETCH TRANSCRIPT ---------------- #
def fetch_transcript(youtube_url):
    video_id = get_video_id(youtube_url)

    if not video_id:
        print("❌ Invalid YouTube URL")
        return None

    tactiq_url = f"https://tactiq.io/tools/run/youtube_transcript?yt={urllib.parse.quote(youtube_url)}"

    headers = {"User-Agent": "Mozilla/5.0"}

    response = requests.get(tactiq_url, headers=headers)

    if response.status_code != 200:
        print("❌ Failed to fetch page")
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    transcript_blocks = soup.find_all("p")
    transcript = "\n".join([p.get_text(strip=True) for p in transcript_blocks])

    return transcript


# ---------------- NEW: GET LATEST VIDEOS ---------------- #
def get_latest_videos(channel_url, max_results=3):
    print("📡 Fetching latest videos from channel...")

    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(channel_url, headers=headers)

    if response.status_code != 200:
        print("❌ Failed to load channel page")
        return []

    html = response.text

    # YouTube embeds video IDs in JSON
    import re
    video_ids = re.findall(r'"videoId":"(.*?)"', html)

    # Remove duplicates
    seen = set()
    unique_ids = []
    for vid in video_ids:
        if vid not in seen:
            seen.add(vid)
            unique_ids.append(vid)

    latest_ids = unique_ids[:max_results]

    videos = [f"https://www.youtube.com/watch?v={vid}" for vid in latest_ids]

    print(f"✅ Found {len(videos)} videos")

    return videos


# ---------------- MAIN ---------------- #
if __name__ == "__main__":

    # 👉 Your channel URL
    channel_url = "https://www.youtube.com/@stockmarketcommando/videos"

    videos = get_latest_videos(channel_url)

    for youtube_url in videos:
        print(f"\n🚀 Processing: {youtube_url}")

        transcript = fetch_transcript(youtube_url)

        if transcript:
            print("\n===== TRANSCRIPT =====\n")
            print(transcript[:500])  # preview

            with open("transcript.txt", "w", encoding="utf-8") as f:
                f.write(transcript)

            print("✅ Saved to transcript.txt")
