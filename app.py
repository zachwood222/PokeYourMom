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

config = {
    "DISCORD_WEBHOOK": "",   # ← PUT YOUR REAL WEBHOOK HERE
    "ZIP_CODE": "32301",
    "USE_PROXY": True,
    "PROXIES": [
        "http://185.219.159.38:443",
        "http://190.113.112.147:4443",
        "http://185.219.159.26:443",
        "http://206.188.212.16:8443",
        "http://37.59.112.197:443",
        "http://185.219.159.36:443",
        "http://47.243.181.85:41700",
        "http://47.243.181.85:41400",
        "http://47.243.181.85:41716",
        "http://47.243.181.85:8081",
        "http://47.243.181.85:41402",
        "http://47.243.181.85:41396",
        "http://47.243.181.85:42535",
        "http://47.243.181.85:41798",
        "http://47.243.181.85:42536",
        "http://47.243.181.85:41698",
        "http://47.243.181.85:55001",
        "http://89.124.8.39:443",
        "http://185.145.4.165:443",
        "http://142.171.195.26:443",
        "http://81.180.222.73:443",
        "http://66.249.156.130:443",
        "http://46.243.119.92:443",
        "http://141.148.230.225:443",
        "http://199.127.197.211:443",
        "http://208.169.72.58:443",
        "http://66.249.146.210:443",
        "http://170.80.111.178:443",
        "http://37.203.35.8:443",
        "http://37.120.147.146:443",
        "http://51.15.135.81:443",
        "http://103.164.114.91:443",
        "http://51.158.194.107:443",
        "http://192.241.132.92:443",
        "http://51.68.192.76:443",
        "http://186.46.220.117:443",
        "http://51.158.194.16:443",
        "http://211.34.105.110:443",
        "http://37.120.156.34:443",
        "http://37.59.110.73:443",
        "http://89.124.8.78:443",
        "http://47.243.181.85:55017",
        "http://101.255.106.178:443",
        "http://213.163.97.16:443",
        "http://89.124.8.84:443",
        "http://47.243.181.85:41692",
        "http://47.243.181.85:55002",
        "http://47.243.181.85:41419"
    ],
    "AGGRESSIVE_MODE": False
}

products = [
    {"retailer": "Target", "name": "Prismatic Evolutions ETB", "url": "https://www.target.com/p/2024-pok-scarlet-violet-s8-5-elite-trainer-box/-/A-93954435"},
    {"retailer": "Target", "name": "Surging Sparks ETB", "url": "https://www.target.com/p/pokemon-trading-card-game-scarlet-38-violet-surging-sparks-elite-trainer-box/-/A-91619922"},
    {"retailer": "Walmart", "name": "Prismatic Evolutions ETB", "url": "https://www.walmart.com/ip/Pokemon-Scarlet-and-Violet-8-5-Prismatic-Evolutions-Elite-Trainer-Box/13816151308"},
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

def check_product(driver, product):
    try:
        log(f"🔍 Checking {product['retailer']} → {product['name']}")
        driver.get(product["url"])
        time.sleep(random.uniform(8, 14))
        
        page_text = driver.page_source.lower()
        page_source = driver.page_source

        # Strong out-of-stock indicators
        out_of_stock_indicators = [
            "out of stock", "sold out", "unavailable", 
            "notify me when available", "get notified", 
            "currently unavailable", "temporarily out of stock"
        ]
        
        has_add_to_cart = any(btn.is_displayed() for btn in driver.find_elements(By.XPATH, "//button[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'add to cart')]")) or "add to cart" in page_text

        if any(indicator in page_text for indicator in out_of_stock_indicators):
            log(f"❌ Out of stock - {product['name']}")
            return False
        elif has_add_to_cart:
            log(f"✅ REAL STOCK DETECTED → {product['name']}")
            if config["DISCORD_WEBHOOK"]:
                requests.post(config["DISCORD_WEBHOOK"], json={
                    "content": f"🚨 **REAL STOCK ALERT!**\n{product['name']} at {product['retailer']}\n{product['url']}"
                })
            return True
        else:
            log(f"❌ No clear stock signal - {product['name']}")
            return False

    except Exception as e:
        log(f"❌ Connection error on {product['name']}")
        return False

def bot_loop():
    global driver
    log("🚀 Smart Multi-Retailer Bot Running - Improved Stock Detection")
    
    while bot_running:
        try:
            if driver is None:
                driver = get_driver()
            
            log(f"🔍 Starting scan of {len(products)} products...")
            for product in products:
                if not bot_running: break
                check_product(driver, product)
                time.sleep(random.uniform(15, 25))
            
            wait = 35 if config["AGGRESSIVE_MODE"] else 180
            log(f"✅ Scan completed. Next scan in ~{wait} seconds")
            time.sleep(wait)
        except Exception as e:
            log(f"💥 Error: {str(e)[:100]} - Restarting browser")
            if driver:
                try: driver.quit()
                except: pass
                driver = None
            time.sleep(25)

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

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000, debug=False, allow_unsafe_werkzeug=True)
