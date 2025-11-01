from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup
import gspread
from datetime import date
import os
import time


# ---------------- SETUP ---------------- #

# Chrome Options
chrome_options = Options()
chrome_options.add_argument("--headless")  # Important for GitHub Actions
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--remote-debugging-port=9222")
chrome_options.add_argument("--window-size=1920,1080")

# Detect environment (GitHub Actions or local)
CHROMEDRIVER_PATH = "/usr/bin/chromedriver" if os.environ.get("GITHUB_ACTIONS") else r"E:\\chromedriver\\chromedriver.exe"

driver_service = Service(CHROMEDRIVER_PATH)

# Google Sheets Auth
credentials = {
  "type": "service_account",
  "project_id": "newsscraper-367610",
  "private_key_id": "3d5bdcb962863986c978effd26e057145e918832",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC+T26d98/KP5LX\nJGWRy9i6ydDEKB+fh+0PFqVw9H4T36cRdHnmdTjfcYU0ix5pMVKxOSZCRcK8iTbb\nGZPS2pBOxD7nUYnxYXCffYuzjzQPCpm0Kki7IngtJ1wk2D8oT2DfDytrM0BWljpA\nuLPGHgar6KelrABc4U/opCYGOnMP+qu3olZnOKgfG6TeJ9Pj3qzSivyIBwaCC3RW\nWtleUO8sRE3RV8o82hX9DwGMAxXHSFwgV9Ftetin5Xth94PFhoP+UH0zn0ceuRpi\nUc1DCPeL4rjiV2WcxP4J9DsOMRRGmPGlwUk/gZquCXn+SRRcKte2dRoUkf1Q2Xgd\nj+Zh0tgZAgMBAAECggEAG6U90DBGzN5B1kN5Br4qQG4fR9N9TocoGnRDeb5mjzwC\n0MAPEjmOlrTusQSRmR3bYZfH9UIdiRZUs+zKAswRwQN0kNVRONGbEuwhrmab3SCA\n1gF7ecXG1+ZbFhYczJ4FWDnjqIm0K6/ci/jY0qubc/8S1XgAQH3Rhh3MUUTN4LDc\n9RPO7YEc35tT6rPqH5ei2wfbRvb5XULwGj3I9Wd1161PU/h+2BSGXZbcWNX2WNQK\nDcGq2qIUKWEX4KyJsK45BOYbNelLvSe+yWBJJIXd3YZYm/2ENwmL+1goIpkJsUMr\nXZkNcqkwTMKxxuFUr2m1LfHTzQxEhLJhoVcHq4VktQKBgQDkaisXARJZy1Ws0BDJ\n+4acqLEZjhHskfHuvFHSm0frAMvimOCC9I5cigaadWQYLJdGZzU/43G0HRRM41t/\nms1hIia+UVfaywgj/m6IF/WR3NX4agCepFgBckIZ4s6OJKzf5eBuH4q90OiiItgE\nEZh1brwaj8Q7JdOJKOUwb00HjQKBgQDVSzMs4+gV7tKZlS70kfWevJx+vee6Zd63\njUdppRogC1wMuKz7nJzygA/K3YZxHACYXgTU9TpdIHQRYrhSW3a4Fv0IARMcem1C\n+EPfR8D2R/6mr9mgGyrX522FKPXJRo6MjJgw6yYJIgVQQWECLlAxClbWC1HCi6RO\nmQH+3XWZvQKBgQC2dTSMgLem8O00SVRP9FMYSwyFLF6XChInMVlvEclGKPG0xYf6\nM96Qf1U9Bu74/I2umH0J1uaaCOyRasBJU2Ah6kTmnFXAmZScI/8pSXJJnQ1zDUIH\nd6IxLZMt2GZFV3ictaUscpfCfuGFuq2xulh93gH2ecc3tESg+QDzSsVOdQKBgQC/\n182FuM+MuqwNm5MKQyYKylZv4NtGWk4CBj6PFQL0g4MdphVgkjLQIsMRkkJUBf27\nDgWGjbMbBA+he4uR99ZGKdcYle7clHkl3Sse7ujuGz8+KbiyOv1ECFIHjRnVGXMp\nk2NnzHj/iewBjWx1i7ZxBX2pM7EsLHFyiEi1NfeKeQKBgDhtVrzdBdpqMosqbX5F\nA2o1truihRlc5sUbQgX8h+YH2QZ737XnJX8ioNaaws+GKJtBzduIDqo8jVlTd38a\nQNqcMbwAcmXvRd9uRYYWp75rRThfgEBg2oYYV3ClmdeOgh8sfYNosYy178l1T2OJ\nSLhyEuCOC1FPXHgMOrUIiGoM\n-----END PRIVATE KEY-----\n",
  "client_email": "newsscraper-367610@appspot.gserviceaccount.com",
  "client_id": "102898210369510643810",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/newsscraper-367610%40appspot.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}

gc = gspread.service_account_from_dict(credentials)
sheet_main = gc.open('Stock List').worksheet('Sheet1')
sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')

# Load values
company_list = sheet_main.col_values(5)
name_list = sheet_main.col_values(1)
current_date = date.today().strftime("%m/%d/%Y")

# Checkpoint
checkpoint_file = "checkpoint_new_1.txt"
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 1


# ---------------- SCRAPER FUNCTION ---------------- #
def scrape_tradingview(company_url):
    driver = webdriver.Chrome(service=driver_service, options=chrome_options)
    driver.set_window_size(1920, 1080)
    try:
        driver.get(company_url)
        WebDriverWait(driver, 30).until(
            EC.visibility_of_element_located((By.XPATH,
                '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'))
        )
        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [
            el.get_text().replace('−', '-').replace('∅', 'None')
            for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        ]
        return values

    except NoSuchElementException:
        print(f"Data element not found for URL: {company_url}")
        return []

    finally:
        driver.quit()


# ---------------- MAIN LOOP ---------------- #
for i, company_url in enumerate(company_list[last_i:], last_i):
    if i > 1100:
        break

    name = name_list[i]
    print(f"Scraping {i}: {name} | {company_url}")

    values = scrape_tradingview(company_url)
    if values:
        row = [name, current_date] + values
        sheet_data.append_row(row, table_range='A1')

    with open(checkpoint_file, "w") as f:
        f.write(str(i))

    time.sleep(1)
