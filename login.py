# save_cookies.py
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import pickle

chrome_options = Options()
# Visible mode
driver = webdriver.Chrome(options=chrome_options)
driver.get("https://www.tradingview.com")

print("Browser opened. Please login to TradingView manually...")
input("After login complete, press Enter here to save cookies...")

cookies = driver.get_cookies()
with open("tradingview_cookies.pkl", "wb") as f:
    pickle.dump(cookies, f)
    
print("✓ Cookies saved successfully to tradingview_cookies.pkl")
driver.quit()
