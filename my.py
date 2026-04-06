import sys
import time
import pymysql
import urllib.parse
from contextlib import closing
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


# ---------------- CONFIG ---------------- #
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'database': 'your_database',
    'cursorclass': pymysql.cursors.DictCursor
}


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def extract_video_id(url):
    if "v=" in url:
        return url.split("v=")[1].split("&")[0]
    elif "youtu.be" in url:
        return url.split("/")[-1]
    return None


# ---------------- DRIVER (MATCH YOUR WORKING CODE) ---------------- #
def create_driver():
    log("🌐 Initializing browser...")

    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--incognito")

    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36"
    )

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts
    )

    driver.set_page_load_timeout(60)
    return driver


# ---------------- MAIN ---------------- #
def fetch_and_store(youtube_url):

    video_id = extract_video_id(youtube_url)
    if not video_id:
        log("❌ Invalid URL")
        return

    driver = create_driver()

    try:
        target_url = f"https://tactiq.io/tools/run/youtube_transcript?yt={urllib.parse.quote(youtube_url)}"

        log(f"🌐 Opening: {target_url}")
        driver.get(target_url)

        time.sleep(5)

        log(f"📄 Title: {driver.title}")

        transcript_text = ""
        attempt = 0

        while True:
            attempt += 1
            log(f"🔄 Attempt {attempt}")

            transcript_text = driver.execute_script("""
                let btn = document.querySelector('#copy');
                if (!btn) return '';

                let txt = btn.getAttribute('data-clipboard-text');
                if (txt && txt.length > 500) return txt;

                return '';
            """)

            length = len(transcript_text) if transcript_text else 0
            log(f"📊 Length: {length}")

            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            if transcript_text and length > 500:
                log("✅ Transcript captured")
                break

            if attempt % 5 == 0:
                driver.save_screenshot(f"debug_{attempt}.png")

            time.sleep(4)

        # Save file
        with open("transcript.txt", "w", encoding="utf-8") as f:
            f.write(transcript_text)

        log("📄 Transcript saved")

        # Save DB
        log("💾 Saving to DB...")

        with closing(pymysql.connect(**DB_CONFIG)) as conn:
            with conn.cursor() as cursor:

                sql = """
                INSERT INTO wp_transcript (video_id, video_url, title, content, created_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON DUPLICATE KEY UPDATE content = VALUES(content)
                """

                cursor.execute(sql, (
                    video_id,
                    youtube_url,
                    f"YouTube Video {video_id}",
                    transcript_text
                ))

            conn.commit()

        log("✅ DB saved")

    except Exception as e:
        log(f"❌ ERROR: {e}")
        driver.save_screenshot("error.png")

    finally:
        driver.quit()
        log("🛑 Browser closed")


if __name__ == "__main__":
    url_input = sys.argv[1] if len(sys.argv) > 1 else "https://www.youtube.com/watch?v=huW5sxhm3ow"
    fetch_and_store(url_input)
