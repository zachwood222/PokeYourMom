from flask import Flask, render_template, request, jsonify, send_from_directory
from flask_socketio import SocketIO, emit
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import selenium_stealth
import time
import random
import json
import os
import requests
import threading
import atexit
from datetime import datetime
from PIL import Image
import io

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

CONFIG_FILE = "bot_config.json"
LOG_FILE = "target_bot_log.txt"
SCREENSHOTS_DIR = "screenshots"

os.makedirs(SCREENSHOTS_DIR, exist_ok=True)

bot_running = False
driver = None
bot_thread = None
products = []
config = {}

def load_config():
    global products, config
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE) as f:
                data = json.load(f)
                products = data.get("products", [])
                config = data.get("config", {})
        except:
            pass
    
    config.setdefault("AUTO_FULL_CHECKOUT", False)
    config.setdefault("AGGRESSIVE_MODE", False)
    config.setdefault("TRUST_BUILDING", True)
    config.setdefault("USE_PROXY", True)
    config.setdefault("DISCORD_WEBHOOK", os.getenv("DISCORD_WEBHOOK", ""))
    config.setdefault("PROXIES", ["http://user:pass@residential-proxy-ip:port"])
    config.setdefault("QUANTITY", 1)
    config.setdefault("ENABLE_ALERT_LISTENER", True)
    config.setdefault("ALERT_WEBHOOKS", [])

load_config()

# Hot Products - April 2026
PRODUCTS = [
    {"name": "Prismatic Evolutions Elite Trainer Box", "url": "https://www.target.com/p/2024-pok-scarlet-violet-s8-5-elite-trainer-box/-/A-93954435"},
    {"name": "Prismatic Evolutions ETB Alt", "url": "https://www.target.com/p/pokemon-tcg-scarlet-violet-elite-trainer-box-prismatic-evolutions-of-the-pokemon-tcg-1-fully-illustrated-promo-card-9-booster-packs-premium/-/A-1008746912"},
    {"name": "Surging Sparks Elite Trainer Box", "url": "https://www.target.com/p/pokemon-trading-card-game-scarlet-38-violet-surging-sparks-elite-trainer-box/-/A-91619922"},
    {"name": "Surging Sparks Booster Bundle", "url": "https://www.target.com/p/pokemon-scarlet-violet-surging-sparks-booster-trading-cards/-/A-93486336"},
    {"name": "Scarlet & Violet 151 Elite Trainer Box", "url": "https://www.target.com/p/pokemon-trading-card-game-scarlet-38-violet-151-elite-trainer-box/-/A-88897899"},
    {"name": "Mega Evolution Perfect Order ETB", "url": "https://www.target.com/p/pok-233-mon-trading-card-game-mega-evolution-perfect-order-elite-trainer-box/-/A-95230445"},
    {"name": "Mega Evolution Ascended Heroes ETB", "url": "https://www.target.com/p/2025-pok-me-2-5-elite-trainer-box/-/A-95082118"},
]

def log(message):
    timestamp = datetime.now().strftime("%H:%M:%S")
    entry = f"[{timestamp}] {message}"
    print(entry)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(f"{datetime.now()} - {message}\n")
    except:
        pass
    socketio.emit('log', {'message': entry})

def send_alert(product, success=False, screenshot=None):
    title = "🛒 ORDER PLACED!" if success else "🎉 STOCK FOUND!"
    log(f"{title} - {product['name']}")
    if config.get("DISCORD_WEBHOOK"):
        try:
            requests.post(config["DISCORD_WEBHOOK"], json={"content": f"**{title}**\n{product['name']}\n{product['url']}"})
        except:
            pass

# FIXED get_driver for Render
def get_driver():
    try:
        options = uc.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--window-size=1920,1080")
        options.binary_location = "/usr/bin/google-chrome"   # ← This fixes the error

        if config.get("USE_PROXY") and config.get("PROXIES"):
            proxy = random.choice(config["PROXIES"])
            options.add_argument(f'--proxy-server={proxy}')
            log(f"🔌 Using proxy: {proxy}")

        driver = uc.Chrome(options=options, version_main=None, headless=True)
        selenium_stealth.stealth(driver, languages=["en-US", "en"], vendor="Google Inc.", platform="Win32", fix_hairline=True)
        
        log("✅ Driver started successfully")
        return driver
    except Exception as e:
        log(f"Driver failed: {str(e)}")
        raise

# (Rest of the functions remain the same - human_behavior, detect_challenge, etc.)

def human_behavior(driver, intensive=False):
    try:
        for _ in range(4 if intensive else 2):
            driver.execute_script(f"window.scrollBy(0, {random.randint(200, 900)});")
            time.sleep(random.uniform(1.2, 4.5))
    except:
        pass

def detect_challenge(driver):
    text = driver.page_source.lower()
    if any(k in text for k in ["captcha", "perimeterx", "human", "verify you are human", "challenge"]):
        log("⚠️ CHALLENGE DETECTED")
        return True
    return False

def build_trust(driver):
    if config.get("TRUST_BUILDING"):
        log("🌐 Building trust...")
        try:
            driver.get("https://www.target.com")
            time.sleep(random.uniform(7, 14))
            human_behavior(driver)
            driver.get("https://www.target.com/c/pokemon-cards/-/N-5xt1z")
            time.sleep(random.uniform(6, 13))
            human_behavior(driver)
        except:
            pass

def is_in_stock(driver):
    text = driver.page_source.lower()
    if any(w in text for w in ["out of stock", "sold out", "unavailable", "notify me when available", "busy right now"]):
        return False
    try:
        return len(driver.find_elements(By.XPATH, "//button[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'add to cart')]")) > 0
    except:
        return "add to cart" in text

def add_to_cart(driver):
    log("🛒 Attempting Add to Cart...")
    selectors = ["//button[@data-test='add-to-cart']", "//button[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'add to cart')]"]
    for sel in selectors:
        try:
            btn = WebDriverWait(driver, 8).until(EC.element_to_be_clickable((By.XPATH, sel)))
            human_behavior(driver)
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
            btn.click()
            time.sleep(random.uniform(4, 8))
            return True
        except:
            continue
    return False

# ... (keep the rest of your functions: go_to_cart, proceed_to_checkout, review_and_place_order, check_product, alert_listener, bot_loop, etc.)

# I'll keep it short here - use the previous full version and only replace the get_driver() function with the one above.

# ====================== WEB ROUTES ======================
@app.route('/')
def index():
    return render_template('index.html', products=products, config=config, bot_running=bot_running)

@app.route('/api/products', methods=['GET'])
def get_products():
    return jsonify(products)

# (Keep all other routes as before)

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000, debug=False)
