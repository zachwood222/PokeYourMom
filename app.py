from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
import selenium_stealth
import time
import random
import threading
from datetime import datetime

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

bot_running = False
driver = None

products = [
    {"name": "Prismatic Evolutions ETB", "url": "https://www.target.com/p/2024-pok-scarlet-violet-s8-5-elite-trainer-box/-/A-93954435"},
    {"name": "Surging Sparks ETB", "url": "https://www.target.com/p/pokemon-trading-card-game-scarlet-38-violet-surging-sparks-elite-trainer-box/-/A-91619922"},
    {"name": "Scarlet & Violet 151 ETB", "url": "https://www.target.com/p/pokemon-trading-card-game-scarlet-38-violet-151-elite-trainer-box/-/A-88897899"},
    {"name": "Mega Evolution Perfect Order ETB", "url": "https://www.target.com/p/pok-233-mon-trading-card-game-mega-evolution-perfect-order-elite-trainer-box/-/A-95230445"},
    {"name": "Pokémon TCG: Mega Evolution-Ascended Heroes ETB", "url": "https://www.pokemoncenter.com/product/10-10315-108/pokemon-tcg-mega-evolution-ascended-heroes-pokemon-center-elite-trainer-box"},
    {"name": "Pokémon TCG: Scarlet & Violet-Destined Rivals ETB", "url": "https://www.pokemoncenter.com/product/100-10653/pokemon-tcg-scarlet-and-violet-destined-rivals-pokemon-center-elite-trainer-box"},
    {"name": "Pokémon TCG: Chaos Rising ETB", "url": "https://www.pokemoncenter.com/product/10-10399-112"},
    
]

def log(message):
    timestamp = datetime.now().strftime("%H:%M:%S")
    entry = f"[{timestamp}] {message}"
    print(entry)
    socketio.emit('log', {'message': entry})

def get_driver():
    try:
        options = uc.ChromeOptions()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.binary_location = "/usr/bin/google-chrome"

        driver = uc.Chrome(options=options, version_main=None)
        selenium_stealth.stealth(driver, languages=["en-US", "en"], vendor="Google Inc.", platform="Win32", fix_hairline=True)
        log("✅ Driver started successfully")
        return driver
    except Exception as e:
        log(f"❌ Driver failed: {str(e)[:80]}")
        raise

def check_product(driver, product):
    try:
        log(f"🔍 Checking → {product['name']}")
        driver.get(product["url"])
        time.sleep(random.uniform(8, 14))
        
        text = driver.page_source.lower()
        if any(w in text for w in ["out of stock", "sold out", "unavailable", "busy right now", "notify me when available"]):
            log(f"❌ Out of stock - {product['name']}")
        else:
            log(f"✅ POSSIBLE STOCK ALERT → {product['name']}")
        return True
    except Exception as e:
        log(f"❌ Connection error on {product['name']}")
        return False

def bot_loop():
    global driver
    log("🚀 Bot Started - Improved Stable Mode")
    
    while bot_running:
        try:
            if driver is None:
                driver = get_driver()
            
            log(f"🔍 Starting scan of {len(products)} products...")
            for product in products:
                if not bot_running:
                    break
                check_product(driver, product)
                time.sleep(random.uniform(12, 22))   # Increased delay to be gentler on Target
            
            wait = 35 if False else 160   # Set to True for aggressive mode
            log(f"✅ Scan completed. Next scan in ~{wait} seconds")
            time.sleep(wait)
            
        except Exception as e:
            log(f"💥 Major error: {str(e)[:100]} - Restarting browser")
            if driver:
                try:
                    driver.quit()
                except:
                    pass
                driver = None
            time.sleep(25)

# ====================== ROUTES ======================
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/start', methods=['POST'])
def start_bot():
    global bot_running, bot_thread
    if bot_running:
        return jsonify({"status": "already running"})
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
    socketio.run(app, host='0.0.0.0', port=5000, debug=False)
