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
    "ZIP_CODE": "32301",
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
    {"retailer": "Target", "name": "TEST", "url": "https://www.target.com/p/sungboon-editor-deep-collagen-viral-glass-skin-korean-skincare-power-boosting-facial-mask-for-firming-and-restoration/-/A-93200681#lnk=sametab", "msrp": 55},
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
        time.sleep(random.uniform(10, 16))   # Give page time to fully load

        # Scroll down to force button to load
        driver.execute_script("window.scrollBy(0, 1000);")
        time.sleep(3)

        page_text = driver.page_source.lower()

        # Out of stock check
        if any(word in page_text for word in ["out of stock", "sold out", "unavailable", "notify me when available", "get notified", "temporarily out of stock"]):
            log(f"❌ Out of stock - {product['name']}")
            return False

        # === ROBUST ADD TO CART DETECTION ===
        selectors = [
            "//button[@data-test='add-to-cart']",
            "//button[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'add to cart')]",
            "//button[contains(@aria-label, 'add to cart')]",
            "//button[contains(@class, 'add-to-cart')]",
            "//button[contains(text(), 'Add') and contains(text(), 'Cart')]"
        ]

        for sel in selectors:
            try:
                btn = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, sel))
                )
                if btn.is_displayed():
                    # Click it once to see if it works (safe because we don't actually add)
                    driver.execute_script("arguments[0].click();", btn)
                    time.sleep(2)
                    
                    # If it didn't redirect to error or "sold out", it's real stock
                    if "add to cart" in driver.page_source.lower() or "cart" in driver.current_url.lower():
                        log(f"✅ REAL STOCK DETECTED → {product['name']}")
                        if config["DISCORD_WEBHOOK"]:
                            requests.post(config["DISCORD_WEBHOOK"], json={
                                "content": f"🚨 **REAL STOCK ALERT!**\n{product['name']} at {product['retailer']}\n{product['url']}"
                            })
                        return True
            except:
                continue

        log(f"❌ No active Add to Cart button found (likely grayed out or phantom)")
        return False

    except Exception as e:
        log(f"❌ Connection error on {product['name']}")
        return False

def bot_loop():
    global driver
    log("🚀 Smart Multi-Retailer Bot Running with MSRP + Sold-By Filter")
    
    while bot_running:
        try:
            if driver is None:
                driver = get_driver()
            
            log(f"🔍 Starting scan of {len(products)} products...")
            for product in products:
                if not bot_running: break
                check_product(driver, product)
                time.sleep(random.uniform(14, 24))
            
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

# ====================== ROUTES ======================
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
    if config.get("DISCORD_WEBHOOK"):
        try:
            r = requests.post(config["DISCORD_WEBHOOK"], json={
                "content": "🧪 **TEST ALERT** - The Pokémon Sniper webhook is working correctly! 🎉\nIf you see this, alerts are working."
            })
            if r.status_code == 204:
                log("🧪 Test alert sent successfully to Discord")
            else:
                log(f"❌ Test alert failed (status {r.status_code})")
        except Exception as e:
            log(f"❌ Test alert error: {str(e)}")
    else:
        log("⚠️ No Discord webhook configured")
    return jsonify({"status": "sent"})

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000, debug=False, allow_unsafe_werkzeug=True)
