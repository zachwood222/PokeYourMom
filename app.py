from flask import Flask, render_template, request, jsonify
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
]

config = {"AUTO_FULL_CHECKOUT": False, "AGGRESSIVE_MODE": False, "TRUST_BUILDING": False}

def log(message):
    timestamp = datetime.now().strftime("%H:%M:%S")
    entry = f"[{timestamp}] {message}"
    print(entry)
    socketio.emit('log', {'message': entry})

def get_driver():
    options = uc.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.binary_location = "/usr/bin/google-chrome"
    
    driver = uc.Chrome(options=options, version_main=None)
    selenium_stealth.stealth(driver, languages=["en-US", "en"], vendor="Google Inc.", platform="Win32", fix_hairline=True)
    log("✅ Driver started")
    return driver

def is_in_stock(driver):
    text = driver.page_source.lower()
    if any(word in text for word in ["out of stock", "sold out", "unavailable", "busy right now"]):
        return False
    try:
        return len(driver.find_elements(By.XPATH, "//button[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'add to cart')]")) > 0
    except:
        return "add to cart" in text

def check_product(driver, product):
    try:
        log(f"🔍 Checking → {product['name']}")
        driver.get(product["url"])
        time.sleep(random.uniform(5, 9))
        
        if is_in_stock(driver):
            log(f"✅ STOCK FOUND! → {product['name']}")
            return "IN_STOCK"
        else:
            log(f"❌ Out of stock - {product['name']}")
            return "OOS"
    except Exception as e:
        log(f"Error checking {product['name']}: {str(e)[:80]}")
        return "ERROR"

def bot_loop():
    global driver
    log("🚀 Bot Started - Simplified Mode")
    
    while bot_running:
        try:
            if driver is None:
                driver = get_driver()
            
            log(f"🔍 Scanning {len(products)} products...")
            for p in products:
                if not bot_running: break
                check_product(driver, p)
                time.sleep(random.uniform(8, 15))
            
            wait = 30 if config["AGGRESSIVE_MODE"] else 180
            log(f"✅ Scan done. Waiting {wait} seconds...")
            time.sleep(wait)
        except Exception as e:
            log(f"Loop error: {str(e)[:100]}")
            time.sleep(30)

# ====================== ROUTES ======================
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/start', methods=['POST'])
def start():
    global bot_running, bot_thread
    if bot_running: return jsonify({"status": "running"})
    bot_running = True
    bot_thread = threading.Thread(target=bot_loop, daemon=True)
    bot_thread.start()
    return jsonify({"status": "started"})

@app.route('/api/stop', methods=['POST'])
def stop():
    global bot_running
    bot_running = False
    return jsonify({"status": "stopped"})

@app.route('/api/products', methods=['GET'])
def get_products():
    return jsonify(products)

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000, debug=False)
