from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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
    "DISCORD_WEBHOOK": "https://discord.com/api/webhooks/1494042625966084146/-3RBLTxpjG-1bTqLvnetQ1ns_5Trz3FBxEj0cbXgjj--lmZjG6O5XsWZcYvDJh2EKti_",
    "ZIP_CODE": "32405",
    "USE_PROXY": True,
    "PROXIES": [
        "http://186.46.220.117:443",
        "http://185.219.159.38:443",
        "http://185.219.159.26:443",
        "http://47.243.181.85:41798",
    ],
    "AGGRESSIVE_MODE": False
}

# Full Product List with MSRP
products = [
    {"retailer": "Target", "name": "TEST", "url": "https://www.target.com/p/world-rug-gallery-indoor-outdoor-high-low-textured-area-rug-floral-gray-ivory-easy-clean-patio-rug/-/A-1009629434?preselect=1009629476#lnk=sametab", "msrp": 55},
    {"retailer": "Target", "name": "TEST", "url": "https://www.target.com/p/sungboon-editor-deep-collagen-viral-glass-skin-korean-skincare-power-boosting-facial-mask-for-firming-and-restoration/-/A-93200681#lnk=sametab", "msrp": 5},
    {"retailer": "Target", "name": "Plush", "url": "https://www.target.com/p/pokemon-charmander-sleeping-kids-39-plush-buddy/-/A-79833009", "msrp": 20},
    {"retailer": "Target", "name": "Prismatic Evolutions ETB", "url": "https://www.target.com/p/2024-pok-scarlet-violet-s8-5-elite-trainer-box/-/A-93954435", "msrp": 55},
    {"retailer": "Target", "name": "Surging Sparks ETB", "url": "https://www.target.com/p/pokemon-trading-card-game-scarlet-38-violet-surging-sparks-elite-trainer-box/-/A-91619922", "msrp": 55},
    {"retailer": "Walmart", "name": "Prismatic Evolutions ETB", "url": "https://www.walmart.com/ip/Pokemon-Scarlet-and-Violet-8-5-Prismatic-Evolutions-Elite-Trainer-Box/13816151308", "msrp": 55},
    {"retailer": "Walmart", "name": "Surging Sparks ETB", "url": "https://www.walmart.com/ip/Pokemon-Scarlet-and-Violet-Surging-Sparks-Elite-Trainer-Box/5123456789", "msrp": 55},
    {"retailer": "Best Buy", "name": "Prismatic Evolutions ETB", "url": "https://www.bestbuy.com/site/pokemon-trading-card-game-scarlet-violet-prismatic-evolutions-elite-trainer-box/6578901.p", "msrp": 55},
    {"retailer": "Best Buy", "name": "Painter Illustration Collection", "url": "https://www.bestbuy.com/product/pokemon-trading-card-game-first-partner-illustration-collection-series-2/JJG2TL3VR2", "msrp": 25},
    {"retailer": "Best Buy", "name": "Chaos Rising Booster Bundle", "url": "https://www.bestbuy.com/product/pokemon-trading-card-game-mega-evolution-chaos-rising-booster-bundle/JJG2TL34H9", "msrp": 28},
    {"retailer": "Best Buy", "name": "Chaos Rising ETB", "url": "https://www.bestbuy.com/product/pokemon-trading-card-game-mega-evolution-chaos-rising-elite-trainer-box/JJG2TL34RT", "msrp": 55},
    {"retailer": "Best Buy", "name": "Ascended Heroes Booster Bundle", "url": "https://www.bestbuy.com/product/pokemon-trading-card-game-mega-evolution-ascended-heroes-booster-bundle/JJG2TL3JP8", "msrp": 28},
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
    log("✅ Driver started successfully")
    return driver

def check_product(driver, product):
    try:
        log(f"🔍 Checking {product['retailer']} → {product['name']}")
        driver.get(product["url"])
        time.sleep(random.uniform(10, 18))

        page_text = driver.page_source.lower()

        if any(word in page_text for word in ["out of stock", "sold out", "unavailable", "notify me when available", "get notified"]):
            log(f"❌ Out of stock - {product['name']}")
            return False

        # Try multiple ways to find active Add to Cart
        for xpath in [
            "//button[@data-test='add-to-cart']",
            "//button[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'add to cart')]",
            "//button[contains(@aria-label, 'add to cart')]"
        ]:
            try:
                btn = WebDriverWait(driver, 8).until(EC.element_to_be_clickable((By.XPATH, xpath)))
                if btn.is_displayed() and btn.is_enabled():
                    log(f"✅ REAL STOCK DETECTED → {product['name']}")
                    if config["DISCORD_WEBHOOK"]:
                        requests.post(config["DISCORD_WEBHOOK"], json={"content": f"🚨 **REAL STOCK ALERT!**\n{product['name']} at {product['retailer']}\n{product['url']}"})
                    return True
            except:
                continue

        log(f"❌ Add to Cart not active - {product['name']}")
        return False

    except Exception as e:
        log(f"❌ Connection error on {product['name']}")
        return False

def bot_loop():
    global driver
    log("🚀 Smart Pokémon Hunter Running")
    
    while bot_running:
        try:
            if driver is None:
                driver = get_driver()
            
            log(f"🔍 Starting scan of {len(products)} products...")
            for product in products:
                if not bot_running: break
                check_product(driver, product)
                time.sleep(random.uniform(15, 25))
            
            wait = 35 if config["AGGRESSIVE_MODE"] else 160
            log(f"✅ Scan completed. Next scan in ~{wait} seconds")
            time.sleep(wait)
        except Exception as e:
            log(f"💥 Error: {str(e)[:100]}")
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

@app.route('/api/test-alert', methods=['POST'])
def test_alert():
    if config["DISCORD_WEBHOOK"]:
        requests.post(config["DISCORD_WEBHOOK"], json={"content": "🧪 **Test Alert** - Webhook is working! 🎉"})
        log("🧪 Test alert sent")
    return jsonify({"status": "sent"})

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000, debug=False, allow_unsafe_werkzeug=True)
