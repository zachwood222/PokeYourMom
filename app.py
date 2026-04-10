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
import base64
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
products = PRODUCTS = [
    {
        "name": "Prismatic Evolutions Elite Trainer Box",
        "url": "https://www.target.com/p/2024-pok-scarlet-violet-s8-5-elite-trainer-box/-/A-93954435"
    },
    {
        "name": "Prismatic Evolutions Elite Trainer Box (Alt Listing)",
        "url": "https://www.target.com/p/pokemon-tcg-scarlet-violet-elite-trainer-box-prismatic-evolutions-of-the-pokemon-tcg-1-fully-illustrated-promo-card-9-booster-packs-premium/-/A-1008746912"
    },
    {
        "name": "Surging Sparks Elite Trainer Box",
        "url": "https://www.target.com/p/pokemon-trading-card-game-scarlet-38-violet-surging-sparks-elite-trainer-box/-/A-91619922"
    },
    {
        "name": "Surging Sparks Booster Bundle",
        "url": "https://www.target.com/p/pokemon-scarlet-violet-surging-sparks-booster-trading-cards/-/A-93486336"
    },
    {
        "name": "Scarlet & Violet 151 Elite Trainer Box",
        "url": "https://www.target.com/p/pokemon-trading-card-game-scarlet-38-violet-151-elite-trainer-box/-/A-88897899"
    },
    {
        "name": "Mega Evolution - Perfect Order Elite Trainer Box",
        "url": "https://www.target.com/p/pok-233-mon-trading-card-game-mega-evolution-perfect-order-elite-trainer-box/-/A-95230445"
    },
    {
        "name": "Mega Evolution - Ascended Heroes Elite Trainer Box",
        "url": "https://www.target.com/p/2025-pok-me-2-5-elite-trainer-box/-/A-95082118"
    },
    {
        "name": "Pokémon Day 2026 Collection",
        "url": "https://www.target.com/p/2025-pok-pokemon-day/-/A-95082138"
    },
    {
        "name": "Destined Rivals Elite Trainer Box",
        "url": "https://www.target.com/p/pok-233-mon-trading-card-game-scarlet-38-violet-8212-destined-rivals-elite-trainer-box/-/A-94300069"
    },
    {
        "name": "Twilight Masquerade Elite Trainer Box",
        "url": "https://www.target.com/p/pok-233-mon-trading-card-game-scarlet-38-violet-8212-twilight-masquerade-elite-trainer-box/-/A-91619960"
    }
]
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

def save_config():
    data = {"products": products, "config": config}
    with open(CONFIG_FILE, "w") as f:
        json.dump(data, f, indent=2)

load_config()

def log(message):
    timestamp = datetime.now().strftime("%H:%M:%S")
    entry = f"[{timestamp}] {message}"
    print(entry)
    with open(LOG_FILE, "a") as f:
        f.write(f"{datetime.now()} - {message}\n")
    socketio.emit('log', {'message': entry})

def send_alert(product, success=False, screenshot=None):
    title = "🛒 ORDER PLACED!" if success else "🎉 STOCK FOUND!"
    msg = f"{product['name']}\n{'Order submitted!' if success else 'Adding to cart...'}"
    log(f"{title} - {product['name']}")
    
    if config["DISCORD_WEBHOOK"]:
        try:
            requests.post(config["DISCORD_WEBHOOK"], json={"content": f"**{title}**\n{msg}\nURL: {product['url']}"})
            if screenshot:
                requests.post(config["DISCORD_WEBHOOK"], files={"file": ("success.png", screenshot, "image/png")})
        except:
            pass

# ==================== SELENIUM CORE ====================
def get_driver():
    options = uc.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")

    if config.get("USE_PROXY") and config.get("PROXIES"):
        proxy = random.choice(config["PROXIES"])
        options.add_argument(f'--proxy-server={proxy}')
        log(f"🔌 Using proxy: {proxy}")

    driver = uc.Chrome(options=options, version_main=None)
    selenium_stealth.stealth(driver, languages=["en-US", "en"], vendor="Google Inc.", platform="Win32", fix_hairline=True)
    
    driver.execute_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
        Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 12});
    """)
    return driver

def human_behavior(driver, intensive=False):
    try:
        for _ in range(4 if intensive else 2):
            driver.execute_script(f"window.scrollBy(0, {random.randint(200, 900)});")
            time.sleep(random.uniform(1.2, 4.5))
        driver.execute_script("document.dispatchEvent(new MouseEvent('mousemove', {clientX: Math.random()*window.innerWidth, clientY: Math.random()*window.innerHeight}));")
    except:
        pass

def detect_challenge(driver):
    text = driver.page_source.lower()
    if any(k in text for k in ["captcha", "perimeterx", "human", "verify you are human", "challenge", "cloudflare"]):
        log("⚠️ CHALLENGE/CAPTCHA DETECTED - Solve manually if possible")
        return True
    return False

def build_trust(driver):
    if config.get("TRUST_BUILDING"):
        log("🌐 Building trust (homepage + Pokémon category)")
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
    selectors = [
        "//button[@data-test='add-to-cart']",
        "//button[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'add to cart')]",
        "//button[contains(@aria-label, 'add to cart')]"
    ]
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

def go_to_cart(driver):
    try:
        driver.find_element(By.XPATH, "//a[contains(@href, '/cart')]").click()
        time.sleep(5)
        return True
    except:
        return False

def proceed_to_checkout(driver):
    try:
        btn = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'Checkout')]")))
        btn.click()
        time.sleep(6)
        return True
    except:
        return False

def review_and_place_order(driver):
    log("📦 Placing order (spam retries)...")
    for attempt in range(1, 6):
        try:
            btn = WebDriverWait(driver, 8).until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'Place order') or contains(text(),'Pay now')]")))
            human_behavior(driver, intensive=True)
            btn.click()
            time.sleep(7)
            if "thank you" in driver.page_source.lower() or "order confirmation" in driver.page_source.lower():
                return True
        except:
            time.sleep(3)
    return False

def check_product(driver, product):
    try:
        driver.get(product["url"])
        time.sleep(random.uniform(6, 12))
        human_behavior(driver)

        if detect_challenge(driver):
            return "CHALLENGE"

        if is_in_stock(driver):
            log(f"✅ STOCK DETECTED: {product['name']}")
            send_alert(product)
            
            if config.get("AUTO_FULL_CHECKOUT"):
                if add_to_cart(driver) and go_to_cart(driver) and proceed_to_checkout(driver):
                    if review_and_place_order(driver):
                        screenshot = driver.get_screenshot_as_png()
                        send_alert(product, success=True, screenshot=screenshot)
                        # Save screenshot
                        img = Image.open(io.BytesIO(screenshot))
                        img.save(f"{SCREENSHOTS_DIR}/success_{int(time.time())}.png")
                        return "ORDER_PLACED"
            else:
                add_to_cart(driver)
            return "IN_STOCK"
        return "OOS"
    except Exception as e:
        log(f"Error checking {product['name']}: {str(e)}")
        return "ERROR"

def bot_loop():
    global driver
    log("🚀 Pokémon Target Bot STARTED - Web Dashboard Mode")
    while bot_running:
        try:
            if driver is None:
                driver = get_driver()
                build_trust(driver)
            
            for product in products[:]:
                if not bot_running: break
                result = check_product(driver, product)
                if result == "ORDER_PLACED":
                    log("🎉 SUCCESS! Long cooldown activated.")
                    time.sleep(random.randint(35, 65) * 60)
                    break
                time.sleep(random.uniform(25, 50))
            
            wait = random.randint(15, 45) if config.get("AGGRESSIVE_MODE") else random.randint(180, 420)
            log(f"✅ Full scan done. Next scan in ~{wait} seconds")
            time.sleep(wait)
        except Exception as e:
            log(f"Major loop error: {str(e)}")
            time.sleep(30)

# ==================== WEB ROUTES ====================
@app.route('/')
def index():
    return render_template('index.html', products=products, config=config, bot_running=bot_running)

@app.route('/api/start', methods=['POST'])
def start_bot():
    global bot_running, bot_thread
    if bot_running:
        return jsonify({"status": "already running"})
    bot_running = True
    bot_thread = threading.Thread(target=bot_loop, daemon=True)
    bot_thread.start()
    log("Bot started from dashboard")
    return jsonify({"status": "started"})

@app.route('/api/stop', methods=['POST'])
def stop_bot():
    global bot_running, driver
    bot_running = False
    if driver:
        try: driver.quit()
        except: pass
        driver = None
    log("Bot stopped from dashboard")
    return jsonify({"status": "stopped"})

@app.route('/api/config', methods=['POST'])
def update_config():
    global config
    data = request.json
    config.update(data)
    save_config()
    return jsonify({"status": "saved"})

@app.route('/api/products', methods=['POST'])
def add_product():
    data = request.json
    products.append({"name": data["name"], "url": data["url"]})
    save_config()
    return jsonify({"status": "added"})

@app.route('/api/products/<int:index>', methods=['DELETE'])
def remove_product(index):
    if 0 <= index < len(products):
        del products[index]
        save_config()
        return jsonify({"status": "removed"})
    return jsonify({"status": "error"}), 400

@app.route('/api/proxies', methods=['POST'])
def update_proxies():
    config["PROXIES"] = request.json.get("proxies", [])
    save_config()
    return jsonify({"status": "proxies updated"})

@app.route('/screenshots')
def list_screenshots():
    files = [f for f in os.listdir(SCREENSHOTS_DIR) if f.endswith('.png')]
    return jsonify(files)

@app.route('/screenshots/<filename>')
def get_screenshot(filename):
    return send_from_directory(SCREENSHOTS_DIR, filename)

@socketio.on('connect')
def handle_connect():
    emit('log', {'message': '✅ Connected to live logs'})

def shutdown_handler():
    global bot_running
    bot_running = False
    if driver:
        try: driver.quit()
        except: pass

atexit.register(shutdown_handler)

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000, debug=False)
