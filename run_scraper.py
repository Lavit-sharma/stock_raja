#!/usr/bin/env python3
"""
Robust TradingView scraper for common stock fields.

Usage (local):
  export GOOGLE_CREDENTIALS='{"type":...}'   # gspread service account JSON
  export COOKIES_JSON='[{"name":"...","value":"...","domain":".tradingview.com",...}]'
  python run_tradingview_scraper.py --urls "https://in.tradingview.com/chart/.../?symbol=NSE%3A20MICRONS"

Or (file):
  python run_tradingview_scraper.py --file urls.txt

Notes:
 - Requires: selenium, webdriver-manager, beautifulsoup4, gspread, google-api-python-client (if using sheets)
"""
import argparse, json, os, time, random, sys, traceback
from datetime import datetime, date
from pathlib import Path

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Optional Google Sheets
try:
    import gspread
except Exception:
    gspread = None

# --------------- CONFIG ---------------
HEADLESS = True
PAGE_LOAD_TIMEOUT = 30
WAIT_TIMEOUT = 18
MAX_RETRIES_PER_URL = 3
RATE_DELAY = 1.2
SCROLL_PASS = 4
SAVE_FAILED_HTML_DIR = Path("failed_html")
SAVE_FAILED_HTML_DIR.mkdir(exist_ok=True)

# Desired output columns (order)
COLUMNS = [
    "Name", "Date",
    "Last Price", "Prev Close", "Open", "High", "Low",
    "Change Absolute", "Change Percent",
    "Volume", "Market Cap", "P/E Ratio", "Dividend Yield"
]

# Heuristics for label matching (common variants on TradingView)
LABEL_VARIANTS = {
    "Last Price": ["Last", "Last Price", "Price"],
    "Prev Close": ["Prev Close", "Previous Close", "Prev. Close", "Previous close"],
    "Open": ["Open"],
    "High": ["High"],
    "Low": ["Low"],
    "Change Absolute": ["Change", "Chg", "Change Absolute"],
    "Change Percent": ["% Chg", "Change %", "Change percent", "%"],
    "Volume": ["Volume", "Vol."],
    "Market Cap": ["Market Cap", "Market Capitalization", "Market cap"],
    "P/E Ratio": ["P/E Ratio", "PE Ratio", "P/E"],
    "Dividend Yield": ["Dividend Yield", "Dividend %", "Yield"]
}

# --------------- HELPERS ---------------
def build_driver(headless=HEADLESS):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1600,1000")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    # stable UA
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
    return driver

def safe_text(el):
    try:
        return el.get_text(" ", strip=True)
    except:
        try:
            return el.text.strip()
        except:
            return ""

def save_failed_html(symbol_tag, html):
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    fname = SAVE_FAILED_HTML_DIR / f"{symbol_tag}_{ts}.html"
    fname.write_text(html, encoding="utf-8")
    return str(fname)

# map label variants to canonical label
def canonical_label(s):
    s = s.strip().replace("\xa0", " ").replace(":", "").lower()
    for canon, variants in LABEL_VARIANTS.items():
        for v in variants:
            if v.lower() in s:
                return canon
    return None

# --------------- CORE SCRAPE LOGIC ---------------
def extract_fields_from_page(driver, url):
    """
    Returns dict mapping canonical field names to extracted values (strings).
    Uses multiple heuristics:
     - DOM scanning for label/value pairs (data-name, aria-label, adjacent nodes)
     - Search for price quote element for current price
     - Fallback to regex on visible text
    """
    # Wait + scroll + parse
    try:
        driver.get(url)
    except Exception as e:
        # sometimes get() raises if slow - continue
        print("warning: driver.get raised:", e)
    wait = WebDriverWait(driver, WAIT_TIMEOUT)
    # Wait for main price quote element or general body
    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))
    except:
        pass
    # small initial sleep for JS
    time.sleep(1.2 + random.random() * 1.5)

    # Scroll in passes to trigger lazy load
    for _ in range(SCROLL_PASS):
        try:
            driver.execute_script("window.scrollBy(0, document.body.scrollHeight/4);")
        except:
            pass
        time.sleep(0.6)

    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")
    text_blob = soup.get_text(" ", strip=True)

    out = {k: "" for k in COLUMNS}
    out["Date"] = date.today().strftime("%m/%d/%Y")

    # 1) Symbol / Name: try tv-symbol header or title
    try:
        # many TradingView pages show the symbol in meta or header
        el = soup.select_one(".tv-symbol-header__first-line, .tv-symbol-header, .tv-symbol-idea__title")
        if el:
            name = safe_text(el)
        else:
            # fallback to title or url symbol param
            name = soup.title.string.strip() if soup.title else url.split("symbol=")[-1]
        out["Name"] = name
    except:
        out["Name"] = url

    # 2) Last Price: try known selectors
    last_price = ""
    selectors = [
        ".tv-symbol-price-quote__value",                   # common
        ".tv-symbol-price-quote__value.js-symbol-last",
        "div[class*='price']", "div[class*='quote']", 
        "span[data-role='last-price']"
    ]
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            last_price = safe_text(el)
            break
    # fallback: try to parse near "Last" label
    if not last_price:
        import re
        m = re.search(r"Last\s*[:\s]\s*([0-9,.\-+%]+)", text_blob)
        if m: last_price = m.group(1)
    out["Last Price"] = last_price

    # 3) Label-value pair scanning (most robust)
    # Find candidate label nodes: elements that look like labels in overview tables
    candidates = []
    # Attributes that often contain label names
    for el in soup.find_all():
        # skip too long texts
        t = el.get_text(" ", strip=True)
        if not t or len(t) > 60: 
            continue
        # heuristics: if element has small text and a following sibling with value
        next_el = el.find_next_sibling()
        if next_el and len(next_el.get_text(" ", strip=True)) < 120:
            candidates.append((t, safe_text(next_el)))
        # check data-name or aria-label attrs
        for attr in ("data-name", "aria-label", "data-test", "title"):
            if el.has_attr(attr):
                candidates.append((el.get(attr), safe_text(next_el) if next_el else safe_text(el)))
    # Convert candidate labels to canonical and fill mapping
    found = {}
    for lab, val in candidates:
        if not lab: continue
        canon = canonical_label(lab)
        if canon and val:
            # prefer first found
            if not out.get(canon):
                out[canon] = val

    # 4) Additional targeted extraction where label scanning missed
    # Try to find a small overview table blocks (TradingView uses pairs in many structures)
    # We'll search for elements containing common label words and pick their text siblings
    possible_labels = sum(LABEL_VARIANTS.values(), [])
    for v in possible_labels:
        # look for elements whose text exactly equals variant or startswith it
        el = soup.find(lambda tag: tag.name in ["div", "span", "td", "th", "p"] and tag.get_text(" ",strip=True).strip().lower().startswith(v.lower()))
        if el:
            # try sibling or parent sibling
            val = ""
            nxt = el.find_next_sibling()
            if nxt:
                val = safe_text(nxt)
            else:
                # maybe parent contains value later
                parent = el.parent
                if parent:
                    # try to find numeric-ish child
                    candidate = parent.find(lambda t: t.name in ["div", "span", "td"] and len(t.get_text(strip=True))<60 and any(ch.isdigit() for ch in t.get_text()))
                    if candidate:
                        val = safe_text(candidate)
            if val:
                canon = canonical_label(v)
                if canon and not out.get(canon):
                    out[canon] = val

    # 5) Regex fallback for specific fields
    import re
    def regex_find(patterns):
        for p in patterns:
            m = re.search(p, text_blob, flags=re.IGNORECASE)
            if m:
                return m.group(1).strip()
        return ""

    if not out["Prev Close"]:
        out["Prev Close"] = regex_find([r"Prev(?:ious)?(?:\.|\s)*Close[:\s]*([0-9,.\-]+)"])
    if not out["Open"]:
        out["Open"] = regex_find([r"\bOpen[:\s]*([0-9,.\-]+)"])
    if not out["High"]:
        out["High"] = regex_find([r"\bHigh[:\s]*([0-9,.\-]+)"])
    if not out["Low"]:
        out["Low"] = regex_find([r"\bLow[:\s]*([0-9,.\-]+)"])
    if not out["Volume"]:
        out["Volume"] = regex_find([r"Volume[:\s]*([0-9,.,KMkmb]+)"])
    if not out["Market Cap"]:
        out["Market Cap"] = regex_find([r"Market Cap[:\s]*([0-9,.,KMkmb]+)"])
    if not out["P/E Ratio"]:
        out["P/E Ratio"] = regex_find([r"(?:P\/E|P\.E|PE)[:\s]*([0-9.,\-]+)"])
    if not out["Dividend Yield"]:
        out["Dividend Yield"] = regex_find([r"Dividend\s*Yield[:\s]*([0-9.,\-]+%?)"])

    # 6) Change absolute and percent extraction heuristics
    # Sometimes change is shown together like "+1.23 (+0.56%)" near price
    if not out["Change Absolute"] or not out["Change Percent"]:
        # look near last price element in DOM
        el = soup.select_one(".tv-symbol-price-quote__change")
        if el:
            txt = safe_text(el)
            m = re.search(r"([+\-]?[0-9,.\-]+)\s*\(?([+\-]?[0-9.,\-]+%?)\)?", txt)
            if m:
                out["Change Absolute"] = out["Change Absolute"] or m.group(1)
                out["Change Percent"] = out["Change Percent"] or m.group(2)
        else:
            m = re.search(r"([+\-]?[0-9,.]+)\s*\(\s*([+\-]?[0-9.,\-]+%)\s*\)", text_blob)
            if m:
                out["Change Absolute"] = out["Change Absolute"] or m.group(1)
                out["Change Percent"] = out["Change Percent"] or m.group(2)

    # Normalize simple issues
    for k in out:
        if isinstance(out[k], str):
            out[k] = out[k].replace("\n", " ").strip()

    return out, html

# --------------- RUN for multiple urls ---------------
def run(urls, to_sheets=False):
    driver = build_driver()
    results = []
    for url in urls:
        symbol_tag = url.split("symbol=")[-1] if "symbol=" in url else url.split("/")[-2] if '/' in url else url
        print(f"Scraping {symbol_tag} ...", end=" ", flush=True)
        success = False
        for attempt in range(1, MAX_RETRIES_PER_URL + 1):
            try:
                out, html = extract_fields_from_page(driver, url)
                # Check some minimum completeness: last price and at least 4 other fields
                filled = sum(1 for k in COLUMNS if out.get(k))
                if out.get("Last Price") and filled >= 5:
                    results.append(out)
                    print(f"OK ({filled} fields)")
                    success = True
                    break
                else:
                    print(f"partial({filled})", end="")
                    time.sleep(1.5 * attempt)
            except Exception as e:
                print(f"err:{e}", end="")
                time.sleep(1.5 * attempt)
        if not success:
            # save for debugging
            try:
                _, html = extract_fields_from_page(driver, url)
                f = save_failed_html(symbol_tag, html)
                print(f" FAILED -> saved {f}")
            except Exception:
                print(" FAILED (no html)")
            # still append minimal row
            minimal = {k: "" for k in COLUMNS}
            minimal["Name"] = symbol_tag
            minimal["Date"] = date.today().strftime("%m/%d/%Y")
            results.append(minimal)
        time.sleep(RATE_DELAY + random.random()*0.5)
    driver.quit()

    # Optionally append to sheets
    if to_sheets and gspread:
        creds = json.loads(os.getenv("GOOGLE_CREDENTIALS"))
        gc = gspread.service_account_from_dict(creds)
        ss = gc.open("Tradingview Data Reel Experimental May")
        try:
            ws = ss.worksheet("Sheet5")
        except Exception:
            ws = ss.add_worksheet("Sheet5", rows=4000, cols=30)
        rows = []
        for r in results:
            rows.append([r.get(c, "") for c in COLUMNS])
        # append rows in batches
        CHUNK = 100
        for i in range(0, len(rows), CHUNK):
            ws.append_rows(rows[i:i+CHUNK], value_input_option="USER_ENTERED")
    else:
        # Print CSV-like
        print("\n--- Results ---")
        print("\t".join(COLUMNS))
        for r in results:
            print("\t".join(r.get(c, "") for c in COLUMNS))

# --------------- CLI ---------------
if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--urls", help="Comma-separated TradingView URLs")
    p.add_argument("--file", help="File with one URL per line")
    p.add_argument("--to-sheets", action="store_true", help="Append to Google Sheet (requires GOOGLE_CREDENTIALS)")
    args = p.parse_args()
    urls = []
    if args.urls:
        urls = [u.strip() for u in args.urls.split(",") if u.strip()]
    if args.file:
        urls += [line.strip() for line in open(args.file, encoding="utf-8") if line.strip()]
    if not urls:
        print("No URLs provided. Use --urls or --file.")
        sys.exit(1)
    run(urls, to_sheets=args.to_sheets)
