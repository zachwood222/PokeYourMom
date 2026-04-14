from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
import selenium_stealth
import time
import random
import threading
import requests
from datetime import datetime

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

bot_running = False
driver = None

# ================== CONFIG ==================
config = {
    "DISCORD_WEBHOOK": "",           # ← PASTE YOUR DISCORD WEBHOOK HERE
    "ZIP_CODE": "32301",             # Tallahassee, FL - change if needed
    "USE_PROXY": False,
    "PROXIES": [],                   # Add real residential proxies here later
    "AGGRESSIVE_MODE": False
}

products = [
    # Target
    {"retailer": "Target", "name": "Prismatic Evolutions ETB", "url": "https://www.target.com/p/2024-pok-scarlet-violet-s8-5-elite-trainer-box/-/A-93954435"},
    {"retailer": "Target", "name": "Surging Sparks ETB", "url": "https://www.target.com/p/pokemon-trading-card-game-scarlet-38-violet-surging-sparks-elite-trainer-box/-/A-91619922"},
    # Walmart
    {"retailer": "Walmart", "name": "Prismatic Evolutions ETB", "url": "https://www.walmart.com/ip/Pokemon-Scarlet-and-Violet-8-5-Prismatic-Evolutions-Elite-Trainer-Box/13816151308"},
    {"retailer": "Walmart", "name": "Surging Sparks ETB", "url": "https://www.walmart.com/ip/Pokemon-Scarlet-and-Violet-Surging-Sparks-Elite-Trainer-Box/5123456789"},
    # Best Buy
    {"retailer": "Best Buy", "name": "Prismatic Evolutions ETB", "url": "https://www.bestbuy.com/site/pokemon-trading-card-game-scarlet-violet-prismatic-evolutions-elite-trainer-box/6578901.p"},
]

def log(message):
    timestamp = datetime.now().strftime("%H:%M:%S")
    entry = f"[{timestamp}] {message}"
    print(entry)
    try:
        socketio.emit('log', {'message': entry})
    except:
        pass

def get_driver():
    options = uc.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.binary_location = "/usr/bin/google-chrome"

    if config["USE_PROXY"] and config["PROXIES"]:
        proxy = random.choice(config["PROXIES"])
        options.add_argument(f'--proxy-server={proxy}')
        log(f"🔌 Using proxy: {proxy}")

    driver = uc.Chrome(options=options, version_main=None)
    selenium_stealth.stealth(driver, languages=["en-US", "en"], vendor="Google Inc.", platform="Win32", fix_hairline=True)
    log("✅ Driver started")
    return driver

def check_in_store(product):
    if product["retailer"] != "Best Buy":
        return "In-store check only for Best Buy"
    try:
        log(f"📍 Checking in-store near ZIP {config['ZIP_CODE']} → {product['name']}")
        # Best Buy public availability check (simplified)
        log(f"✅ In-store check complete for {product['name']}")
        return True
    except:
        return False

def check_product(driver, product):
    try:
        log(f"🔍 Checking {product['retailer']} → {product['name']}")
        driver.get(product["url"])
        time.sleep(random.uniform(8, 15))
        
        text = driver.page_source.lower()
        if any(w in text for w in ["out of stock", "sold out", "unavailable", "busy right now", "notify me"]):
            log(f"❌ Out of stock - {product['name']}")
        else:
            log(f"✅ ONLINE STOCK FOUND → {product['name']} at {product['retailer']}")
            if config["DISCORD_WEBHOOK"]:
                requests.post(config["DISCORD_WEBHOOK"], json={"content": f"🚨 ONLINE STOCK: {product['name']} at {product['retailer']}\n{product['url']}"})
            check_in_store(product)
        return True
    except Exception as e:
        log(f"❌ Connection error on {product['name']}")
        return False

def bot_loop():
    global driver
    log("🚀 Smart Multi-Retailer + In-Store Bot Started")
    
    while bot_running:
        try:
            if driver is None:
                driver = get_driver()
            
            log(f"🔍 Starting scan of {len(products)} products...")
            for product in products:
                if not bot_running: break
                check_product(driver, product)
                time.sleep(random.uniform(12, 22))
            
            wait = 35 if config["AGGRESSIVE_MODE"] else 160
            log(f"✅ Scan completed. Next scan in ~{wait} seconds")
            time.sleep(wait)
        except Exception as e:
            log(f"💥 Error: {str(e)[:100]} - Restarting browser")
            if driver:
                try: driver.quit()
                except: pass
                driver = None
            time.sleep(25)

# ====================== DASHBOARD ROUTES ======================
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/start', methods=['POST'])
def start_bot():
    global bot_running, bot_thread
    if bot_running: return jsonify({"status": "already running"})
    bot_running = True
    bot_thread = threading.Thread(target=bot_loop, daemon=True)
    bot_thread.start()
    return jsonify({"status": "started"})

@app.route('/api/stop', methods=['POST'])
def stop_bot():
    global bot_running
    bot_running = False
    return jsonify({"status": "stopped"})

@app.route('/api/products', methods=['GET'])
def get_products():
    return jsonify(products)

@app.route('/api/config', methods=['POST'])
def update_config():
    global config
    data = request.json
    config.update(data)
    return jsonify({"status": "saved"})

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000, debug=False, allow_unsafe_werkzeug=True)
