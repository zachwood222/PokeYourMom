from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import selenium_stealth
import time
import random
import threading
import requests
from datetime import datetime
import json
import os
import re

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

CONFIG_PATH = "config.json"

DEFAULT_CONFIG = {
    "DISCORD_WEBHOOK": "",
    "ZIP_CODE": "32405",
    "USE_PROXY": False,
    "PROXIES": [],
    "AGGRESSIVE_MODE": False,
    "SCAN_DELAY_SECONDS": 120,
    "PAGE_LOAD_TIMEOUT": 30,
    "MAX_RETRIES_PER_PRODUCT": 2,
}

# Product list with MSRP in USD.
products = [
    {
        "retailer": "Target",
        "name": "Prismatic Evolutions ETB",
        "url": "https://www.target.com/p/2024-pok-scarlet-violet-s8-5-elite-trainer-box/-/A-93954435",
        "msrp": 54.99,
    },
    {
        "retailer": "Target",
        "name": "Surging Sparks ETB",
        "url": "https://www.target.com/p/pokemon-trading-card-game-scarlet-38-violet-surging-sparks-elite-trainer-box/-/A-91619922",
        "msrp": 54.99,
    },
    {
        "retailer": "Walmart",
        "name": "Prismatic Evolutions ETB",
        "url": "https://www.walmart.com/ip/Pokemon-Scarlet-and-Violet-8-5-Prismatic-Evolutions-Elite-Trainer-Box/13816151308",
        "msrp": 54.99,
    },
]

config = DEFAULT_CONFIG.copy()
bot_running = False
bot_thread = None
driver = None
lock = threading.Lock()

status = {
    "last_scan_started": None,
    "last_scan_finished": None,
    "last_successful_check": None,
    "products_checked": 0,
    "stock_hits": 0,
    "last_error": None,
}


def log(message):
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    entry = f"[{timestamp}] {message}"
    print(entry)
    socketio.emit("log", {"message": entry})


def load_config():
    global config
    loaded = {}

    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                loaded = json.load(f)
        except Exception as e:
            log(f"⚠️ Could not parse config.json, using defaults ({e})")

    config = DEFAULT_CONFIG.copy()
    config.update({k: v for k, v in loaded.items() if k in DEFAULT_CONFIG})

    env_webhook = os.getenv("DISCORD_WEBHOOK")
    if env_webhook:
        config["DISCORD_WEBHOOK"] = env_webhook


def save_config():
    safe_config = {k: v for k, v in config.items() if k in DEFAULT_CONFIG}
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(safe_config, f, indent=2)


def send_discord_alert(content):
    webhook = config.get("DISCORD_WEBHOOK", "").strip()
    if not webhook:
        return
    try:
        requests.post(webhook, json={"content": content}, timeout=8)
    except Exception as e:
        log(f"⚠️ Discord alert failed: {e}")


def get_driver():
    options = uc.ChromeOptions()
    options.page_load_strategy = "eager"
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-background-networking")
    options.add_argument("--disable-renderer-backgrounding")
    options.add_argument("--disable-background-timer-throttling")
    options.add_argument("--disable-ipc-flooding-protection")
    options.add_argument("--window-size=1920,1080")
    options.add_experimental_option(
        "prefs",
        {
            "profile.managed_default_content_settings.images": 2,
        },
    )

    if config["USE_PROXY"] and config["PROXIES"]:
        proxy = random.choice(config["PROXIES"])
        options.add_argument(f"--proxy-server={proxy}")
        log(f"🔌 Using proxy: {proxy}")

    driver_local = uc.Chrome(options=options, version_main=None)
    driver_local.set_page_load_timeout(config["PAGE_LOAD_TIMEOUT"])

    selenium_stealth.stealth(
        driver_local,
        languages=["en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        fix_hairline=True,
    )
    log("✅ Browser driver ready")
    return driver_local


def parse_price(page_text):
    # Common price formats: $54.99, "54.99", etc.
    matches = re.findall(r"\$\s?(\d{1,4}(?:\.\d{2})?)", page_text)
    numeric = []
    for m in matches:
        try:
            value = float(m)
            if 5 <= value <= 300:
                numeric.append(value)
        except ValueError:
            continue

    if not numeric:
        return None

    return min(numeric)


def detect_stock(page_text, retailer):
    lowered = page_text.lower()

    out_phrases = {
        "sold out",
        "out of stock",
        "unavailable",
        "not available",
        "notify me",
        "coming soon",
        "check stores",
    }

    for phrase in out_phrases:
        if phrase in lowered:
            return False

    add_to_cart_signals = [
        "add to cart",
        "buy now",
        "pickup",
    ]

    if retailer.lower() == "walmart":
        add_to_cart_signals.append("add to cart button")

    return any(signal in lowered for signal in add_to_cart_signals)


def add_to_cart_button_clickable(driver_obj):
    button_xpaths = [
        "//button[@data-test='add-to-cart']",
        "//button[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'add to cart')]",
        "//button[contains(translate(@aria-label,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'add to cart')]",
        "//button[contains(translate(text(),'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'), 'buy now')]",
    ]

    for xpath in button_xpaths:
        try:
            btn = WebDriverWait(driver_obj, 5).until(EC.element_to_be_clickable((By.XPATH, xpath)))
            if btn.is_displayed() and btn.is_enabled():
                return True
        except Exception:
            continue
    return False


def is_msrp_or_below(found_price, msrp):
    if found_price is None:
        return True
    return found_price <= (msrp + 1.00)


def should_reset_driver(exc):
    if isinstance(exc, WebDriverException):
        return True

    msg = str(exc).lower()
    reset_signals = [
        "timed out receiving message from renderer",
        "session deleted because of page crash",
        "invalid session id",
        "disconnected",
    ]
    return any(signal in msg for signal in reset_signals)


def check_product(driver_obj, product):
    global driver
    retries = config["MAX_RETRIES_PER_PRODUCT"]

    for attempt in range(1, retries + 2):
        try:
            log(
                f"🔎 [{product['retailer']}] Checking {product['name']} "
                f"(attempt {attempt}/{retries + 1})"
            )
            try:
                driver_obj.get(product["url"])
            except TimeoutException:
                # Continue with partially loaded DOM instead of hard-failing every timeout.
                log("⏳ Page load timeout reached, proceeding with current DOM snapshot")
                driver_obj.execute_script("window.stop();")
            time.sleep(random.uniform(4, 8))

            page_text = driver_obj.page_source
            in_stock_signal = detect_stock(page_text, product["retailer"])
            clickable = add_to_cart_button_clickable(driver_obj)
            found_price = parse_price(page_text)
            msrp_ok = is_msrp_or_below(found_price, float(product["msrp"]))

            price_fragment = "unknown"
            if found_price is not None:
                price_fragment = f"${found_price:.2f}"

            if in_stock_signal and clickable and msrp_ok:
                status["stock_hits"] += 1
                alert = (
                    f"🚨 STOCK + MSRP ALERT\n"
                    f"{product['name']} at {product['retailer']}\n"
                    f"Price: {price_fragment} (MSRP ${float(product['msrp']):.2f})\n"
                    f"{product['url']}"
                )
                send_discord_alert(alert)
                log(f"✅ HIT: {product['name']} in stock near MSRP ({price_fragment})")
                return True

            reason = "not in stock"
            if in_stock_signal and clickable and not msrp_ok:
                reason = f"price too high ({price_fragment} > MSRP ${float(product['msrp']):.2f})"

            log(f"❌ Miss: {product['name']} ({reason})")
            return False
        except Exception as e:
            status["last_error"] = str(e)
            log(f"⚠️ Error checking {product['name']}: {e}")

            if should_reset_driver(e):
                log("♻️ Resetting browser driver after Selenium/browser error")
                dispose_driver()
                ensure_driver()
                driver_obj = driver

            if attempt <= retries:
                log(f"🔁 Retrying {product['name']} ({attempt}/{retries + 1} attempts used)")
            time.sleep(random.uniform(1.5, 3.5))

    return False


def ensure_driver():
    global driver
    if driver is None:
        driver = get_driver()


def dispose_driver():
    global driver
    if driver:
        try:
            driver.quit()
        except Exception:
            pass
    driver = None


def bot_loop():
    global bot_running

    log("🚀 Pokémon MSRP sniper started")

    while bot_running:
        try:
            status["last_scan_started"] = datetime.utcnow().isoformat()
            ensure_driver()

            product_count = len(products)
            log(f"🔁 Starting scan for {product_count} products")

            for idx, product in enumerate(products, start=1):
                if not bot_running:
                    break

                hit = check_product(driver, product)
                status["products_checked"] += 1
                status["last_successful_check"] = datetime.utcnow().isoformat()

                if hit:
                    # Keep scanning but pause very briefly to reduce immediate repeats.
                    time.sleep(random.uniform(2, 4))
                else:
                    # Spread requests to avoid tripping anti-bot systems.
                    base = 6 if config["AGGRESSIVE_MODE"] else 14
                    jitter = random.uniform(1, 6)
                    log(f"⏱️ Cooldown after item {idx}/{product_count}: {base + jitter:.1f}s")
                    time.sleep(base + jitter)

            status["last_scan_finished"] = datetime.utcnow().isoformat()
            pause = 40 if config["AGGRESSIVE_MODE"] else max(60, int(config["SCAN_DELAY_SECONDS"]))
            log(f"✅ Scan complete. Waiting {pause}s until next cycle")
            time.sleep(pause)
        except Exception as e:
            status["last_error"] = str(e)
            log(f"💥 Loop error: {e}")
            dispose_driver()
            time.sleep(15)

    dispose_driver()
    log("🛑 Bot stopped")


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/start", methods=["POST"])
def start_bot():
    global bot_running, bot_thread

    with lock:
        if bot_running:
            return jsonify({"status": "already running"})

        bot_running = True
        bot_thread = threading.Thread(target=bot_loop, daemon=True)
        bot_thread.start()

    return jsonify({"status": "started"})


@app.route("/api/stop", methods=["POST"])
def stop_bot():
    global bot_running

    with lock:
        bot_running = False

    return jsonify({"status": "stopped"})


@app.route("/api/status", methods=["GET"])
def get_status():
    return jsonify(
        {
            "running": bot_running,
            "status": status,
            "products": len(products),
        }
    )


@app.route("/api/products", methods=["GET"])
def get_products():
    return jsonify(products)


@app.route("/api/products", methods=["POST"])
def add_product():
    payload = request.get_json(force=True)
    required = ["retailer", "name", "url", "msrp"]

    for key in required:
        if key not in payload:
            return jsonify({"error": f"Missing field: {key}"}), 400

    try:
        payload["msrp"] = float(payload["msrp"])
    except Exception:
        return jsonify({"error": "msrp must be a number"}), 400

    products.append(payload)
    return jsonify({"status": "added", "count": len(products)})


@app.route("/api/config", methods=["GET"])
def get_config():
    redacted = config.copy()
    if redacted.get("DISCORD_WEBHOOK"):
        redacted["DISCORD_WEBHOOK"] = redacted["DISCORD_WEBHOOK"][:30] + "..."
    return jsonify(redacted)


@app.route("/api/config", methods=["POST"])
def set_config():
    payload = request.get_json(force=True)

    allowed = set(DEFAULT_CONFIG.keys())
    for key, value in payload.items():
        if key not in allowed:
            continue
        config[key] = value

    # Normalize numeric fields.
    config["SCAN_DELAY_SECONDS"] = int(config.get("SCAN_DELAY_SECONDS", 120))
    config["PAGE_LOAD_TIMEOUT"] = int(config.get("PAGE_LOAD_TIMEOUT", 30))
    config["MAX_RETRIES_PER_PRODUCT"] = int(config.get("MAX_RETRIES_PER_PRODUCT", 2))

    save_config()
    return jsonify({"status": "saved"})


@app.route("/api/test-alert", methods=["POST"])
def test_alert():
    send_discord_alert("🧪 Test alert from Pokémon MSRP Sniper")
    log("🧪 Test alert sent")
    return jsonify({"status": "sent"})


load_config()

if __name__ == "__main__":
    socketio.run(app, host="0.0.0.0", port=5000, debug=False, allow_unsafe_werkzeug=True)
