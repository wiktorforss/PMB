"""
PMB — Polymarket Brain
Railway Edition: Flask API + background polling + Telegram bot controls.

Telegram commands:
  /status               — current settings + today's spend + scan info
  /autobuy on|off       — enable/disable auto-buying
  /setsize <amount>     — set trade size per signal (e.g. /setsize 25)
  /setdailylimit <amt>  — set max daily spend (e.g. /setdailylimit 100)
  /setmaxprice <price>  — max entry price in cents (e.g. /setmaxprice 70)
  /setminoverlap <n>    — min traders overlapping to trigger (e.g. /setminoverlap 3)
  /setcategory <cat>    — leaderboard category (OVERALL, CRYPTO, POLITICS, SPORTS...)
  /setperiod <period>   — leaderboard time period (DAY, WEEK, MONTH, ALL)
  /pause                — pause all scanning temporarily
  /resume               — resume scanning
  /help                 — show all commands
"""

import os
import time
import json
import logging
import threading
import requests
from datetime import datetime, timezone
from collections import defaultdict
from pathlib import Path
from flask import Flask, jsonify, request
from flask_cors import CORS

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ─── Paths ────────────────────────────────────────────────────────────────────
TRIGGERED_FILE = Path("triggered.json")
BOUGHT_FILE    = Path("bought.json")
SETTINGS_FILE  = Path("settings.json")

DATA_API = "https://data-api.polymarket.com"

# ─── Default config (from env vars) ──────────────────────────────────────────
DEFAULT_SETTINGS = {
    # Leaderboard
    "lb_time_period":    os.getenv("LB_TIME_PERIOD", "WEEK"),
    "lb_order_by":       os.getenv("LB_ORDER_BY", "PNL"),
    "lb_limit":      int(os.getenv("LB_LIMIT", "50")),
    "lb_category":       os.getenv("LB_CATEGORY", "OVERALL"),
    "lb_refresh_s":  int(os.getenv("LB_REFRESH_S", "900")),

    # Polling
    "poll_interval_s":   int(os.getenv("POLL_INTERVAL_S", "300")),
    "request_delay_s": float(os.getenv("REQUEST_DELAY_S", "0.5")),

    # Signal detection
    "min_overlap":       int(os.getenv("MIN_OVERLAP", "2")),
    "min_position_usd": float(os.getenv("MIN_POSITION_USD", "100")),
    "max_entry_price":  float(os.getenv("MAX_ENTRY_PRICE", "0.85")),

    # Notifications
    "notify_threshold":  int(os.getenv("NOTIFY_THRESHOLD", "2")),

    # Auto-buy
    "autobuy_enabled":      os.getenv("AUTOBUY_ENABLED", "false").lower() == "true",
    "autobuy_min_overlap":  int(os.getenv("AUTOBUY_MIN_OVERLAP", "3")),
    "autobuy_max_price":  float(os.getenv("AUTOBUY_MAX_PRICE", "0.70")),
    "autobuy_size_usd":   float(os.getenv("AUTOBUY_SIZE_USD", "10")),
    "autobuy_daily_limit":float(os.getenv("AUTOBUY_DAILY_LIMIT", "50")),

    # Misc
    "paused": False,
}

# Fixed env vars (not changeable at runtime)
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
PRIVATE_KEY      = os.getenv("POLYMARKET_PRIVATE_KEY", "")
PORT             = int(os.getenv("PORT", "8080"))

VALID_CATEGORIES = {"OVERALL","POLITICS","SPORTS","CRYPTO","CULTURE",
                    "MENTIONS","WEATHER","ECONOMICS","TECH","FINANCE"}
VALID_PERIODS    = {"DAY","WEEK","MONTH","ALL"}


# ─── Settings persistence ─────────────────────────────────────────────────────

def load_settings() -> dict:
    if SETTINGS_FILE.exists():
        try:
            saved = json.loads(SETTINGS_FILE.read_text())
            merged = {**DEFAULT_SETTINGS, **saved}
            log.info("Settings loaded from disk")
            return merged
        except Exception as e:
            log.warning(f"Could not load settings.json: {e}")
    return dict(DEFAULT_SETTINGS)


def save_settings(s: dict):
    try:
        SETTINGS_FILE.write_text(json.dumps(s, indent=2))
    except Exception as e:
        log.warning(f"Could not save settings.json: {e}")


# ─── Triggered (notified) store ───────────────────────────────────────────────

def load_triggered() -> dict:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if TRIGGERED_FILE.exists():
        try:
            data = json.loads(TRIGGERED_FILE.read_text())
            if data.get("date") == today:
                log.info(f"Loaded {len(data.get('keys',[]))} triggered signals")
                return data
        except Exception as e:
            log.warning(f"Could not read triggered.json: {e}")
    return {"date": today, "keys": []}


def save_triggered(store: dict):
    try:
        TRIGGERED_FILE.write_text(json.dumps(store))
    except Exception as e:
        log.warning(f"Could not save triggered.json: {e}")


def maybe_reset_triggered(store: dict) -> dict:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if store.get("date") != today:
        log.info("New day — resetting triggered signals")
        store = {"date": today, "keys": []}
        save_triggered(store)
    return store


# ─── Bought (per-market auto-buy dedup) store ─────────────────────────────────

def load_bought() -> dict:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if BOUGHT_FILE.exists():
        try:
            data = json.loads(BOUGHT_FILE.read_text())
            if data.get("date") == today:
                log.info(f"Loaded {len(data.get('keys',[]))} bought markets")
                return data
        except Exception as e:
            log.warning(f"Could not read bought.json: {e}")
    return {"date": today, "keys": [], "total_spent": 0.0}


def save_bought(store: dict):
    try:
        BOUGHT_FILE.write_text(json.dumps(store))
    except Exception as e:
        log.warning(f"Could not save bought.json: {e}")


def maybe_reset_bought(store: dict) -> dict:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if store.get("date") != today:
        log.info("New day — resetting bought markets")
        store = {"date": today, "keys": [], "total_spent": 0.0}
        save_bought(store)
    return store


# ─── Shared state ─────────────────────────────────────────────────────────────
_lock            = threading.Lock()
_settings        = load_settings()
_triggered_store = load_triggered()
_bought_store    = load_bought()

_state = {
    "results":          None,
    "leaderboard":      [],
    "last_lb_fetch":    0,
    "scan_count":       0,
    "started_at":       datetime.now(timezone.utc).isoformat(),
}

# ─── Flask app ────────────────────────────────────────────────────────────────
app = Flask(__name__)
CORS(app, origins="*", methods=["GET", "POST"], allow_headers=["Content-Type"])


@app.route("/")
def index():
    return jsonify({"status": "ok", "service": "PMB — Polymarket Brain"})


@app.route("/results")
def results():
    with _lock:
        if _state["results"] is None:
            return jsonify({"status": "loading", "message": "First scan in progress..."}), 202
        return jsonify(_state["results"])


@app.route("/health")
def health():
    with _lock:
        return jsonify({
            "status":           "ok",
            "scan_count":       _state["scan_count"],
            "wallets_tracked":  len(_state["leaderboard"]),
            "started_at":       _state["started_at"],
            "triggered_today":  len(_triggered_store.get("keys", [])),
            "bought_today":     len(_bought_store.get("keys", [])),
            "spent_today":      _bought_store.get("total_spent", 0.0),
            "paused":           _settings.get("paused", False),
        })


# ─── Telegram webhook ─────────────────────────────────────────────────────────
@app.route("/telegram", methods=["POST"])
def telegram_webhook():
    """Receives Telegram updates and processes bot commands."""
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"ok": True})

    msg = (data.get("message") or data.get("edited_message") or {})
    text = msg.get("text", "").strip()
    chat_id = str(msg.get("chat", {}).get("id", ""))

    # Only respond to the configured chat
    if chat_id != TELEGRAM_CHAT_ID:
        log.warning(f"Ignoring message from unknown chat: {chat_id}")
        return jsonify({"ok": True})

    if not text.startswith("/"):
        return jsonify({"ok": True})

    parts   = text.split()
    command = parts[0].lower().split("@")[0]  # strip @botname if present
    args    = parts[1:]

    reply = handle_command(command, args)
    if reply:
        send_message(reply)

    return jsonify({"ok": True})


def handle_command(command: str, args: list) -> str:
    global _settings, _triggered_store, _bought_store

    with _lock:
        s = _settings

    if command == "/help":
        return (
            "🤖 *PMB Bot Commands*\n\n"
            "`/status` — current settings & stats\n"
            "`/autobuy on|off` — toggle auto-buying\n"
            "`/setsize <$>` — trade size per signal\n"
            "`/setdailylimit <$>` — max daily spend\n"
            "`/setmaxprice <¢>` — max entry price in cents\n"
            "`/setminoverlap <n>` — min traders to trigger buy\n"
            "`/setcategory <cat>` — leaderboard category\n"
            "  Options: OVERALL CRYPTO POLITICS SPORTS\n"
            "  ECONOMICS TECH FINANCE CULTURE\n"
            "`/setperiod <p>` — leaderboard period\n"
            "  Options: DAY WEEK MONTH ALL\n"
            "`/pause` — pause scanning\n"
            "`/resume` — resume scanning\n"
            "`/help` — show this message"
        )

    elif command == "/status":
        with _lock:
            scans   = _state["scan_count"]
            wallets = len(_state["leaderboard"])
            spent   = _bought_store.get("total_spent", 0.0)
            bought  = len(_bought_store.get("keys", []))
            alerted = len(_triggered_store.get("keys", []))

        ab_status = "✅ ON" if s["autobuy_enabled"] else "❌ OFF"
        paused    = "⏸ PAUSED" if s.get("paused") else "▶️ RUNNING"

        return (
            f"📊 *PMB Status*\n\n"
            f"*Scanner:* {paused}\n"
            f"*Scans run:* {scans}\n"
            f"*Wallets tracked:* {wallets}\n"
            f"*Signals today:* {alerted}\n\n"
            f"*Auto-buy:* {ab_status}\n"
            f"*Trade size:* `${s['autobuy_size_usd']:.0f}`\n"
            f"*Daily limit:* `${s['autobuy_daily_limit']:.0f}`\n"
            f"*Spent today:* `${spent:.2f}` ({bought} trades)\n"
            f"*Max price:* `{s['autobuy_max_price']*100:.0f}¢`\n"
            f"*Min overlap:* `{s['autobuy_min_overlap']} traders`\n\n"
            f"*Category:* `{s['lb_category']}`\n"
            f"*Period:* `{s['lb_time_period']}`\n"
            f"*Poll interval:* `{s['poll_interval_s']}s`"
        )

    elif command == "/autobuy":
        if not args:
            state = "ON" if s["autobuy_enabled"] else "OFF"
            return f"Auto-buy is currently *{state}*. Use `/autobuy on` or `/autobuy off`."
        val = args[0].lower()
        if val == "on":
            with _lock:
                _settings["autobuy_enabled"] = True
                save_settings(_settings)
            return "✅ Auto-buy *enabled*. Will buy when signal conditions are met."
        elif val == "off":
            with _lock:
                _settings["autobuy_enabled"] = False
                save_settings(_settings)
            return "❌ Auto-buy *disabled*. You'll still get alerts."
        else:
            return "Usage: `/autobuy on` or `/autobuy off`"

    elif command == "/setsize":
        if not args:
            return f"Current trade size: `${s['autobuy_size_usd']:.0f}`. Usage: `/setsize 25`"
        try:
            val = float(args[0].replace("$", ""))
            if val <= 0:
                return "❌ Trade size must be greater than $0."
            if val > 1000:
                return "❌ Trade size capped at $1000 for safety."
            with _lock:
                _settings["autobuy_size_usd"] = val
                save_settings(_settings)
            return f"✅ Trade size set to `${val:.0f}` per signal."
        except ValueError:
            return "❌ Invalid amount. Usage: `/setsize 25`"

    elif command == "/setdailylimit":
        if not args:
            return f"Current daily limit: `${s['autobuy_daily_limit']:.0f}`. Usage: `/setdailylimit 100`"
        try:
            val = float(args[0].replace("$", ""))
            if val <= 0:
                return "❌ Daily limit must be greater than $0."
            with _lock:
                _settings["autobuy_daily_limit"] = val
                save_settings(_settings)
            return f"✅ Daily spend limit set to `${val:.0f}`."
        except ValueError:
            return "❌ Invalid amount. Usage: `/setdailylimit 100`"

    elif command == "/setmaxprice":
        if not args:
            cur = s["autobuy_max_price"] * 100
            return f"Current max price: `{cur:.0f}¢`. Usage: `/setmaxprice 70`"
        try:
            val = float(args[0].replace("¢", "").replace("%", ""))
            if not 1 <= val <= 99:
                return "❌ Price must be between 1 and 99 cents."
            with _lock:
                _settings["autobuy_max_price"] = round(val / 100, 2)
                save_settings(_settings)
            return f"✅ Max entry price set to `{val:.0f}¢`."
        except ValueError:
            return "❌ Invalid price. Usage: `/setmaxprice 70`"

    elif command == "/setminoverlap":
        if not args:
            return f"Current min overlap: `{s['autobuy_min_overlap']} traders`. Usage: `/setminoverlap 3`"
        try:
            val = int(args[0])
            if val < 2:
                return "❌ Min overlap must be at least 2."
            with _lock:
                _settings["autobuy_min_overlap"] = val
                save_settings(_settings)
            return f"✅ Min overlap set to `{val} traders` before auto-buying."
        except ValueError:
            return "❌ Invalid number. Usage: `/setminoverlap 3`"

    elif command == "/setcategory":
        if not args:
            opts = ", ".join(sorted(VALID_CATEGORIES))
            return f"Current category: `{s['lb_category']}`\nOptions: {opts}"
        val = args[0].upper()
        if val not in VALID_CATEGORIES:
            opts = ", ".join(sorted(VALID_CATEGORIES))
            return f"❌ Invalid category. Options: {opts}"
        with _lock:
            _settings["lb_category"] = val
            _state["last_lb_fetch"]  = 0  # force leaderboard refresh
            save_settings(_settings)
        return f"✅ Leaderboard category set to `{val}`. Refreshing wallet list..."

    elif command == "/setperiod":
        if not args:
            opts = ", ".join(sorted(VALID_PERIODS))
            return f"Current period: `{s['lb_time_period']}`\nOptions: {opts}"
        val = args[0].upper()
        if val not in VALID_PERIODS:
            opts = ", ".join(sorted(VALID_PERIODS))
            return f"❌ Invalid period. Options: {opts}"
        with _lock:
            _settings["lb_time_period"] = val
            _state["last_lb_fetch"]     = 0  # force leaderboard refresh
            save_settings(_settings)
        return f"✅ Leaderboard period set to `{val}`. Refreshing wallet list..."

    elif command == "/pause":
        with _lock:
            _settings["paused"] = True
            save_settings(_settings)
        return "⏸ Scanner *paused*. Use `/resume` to restart."

    elif command == "/resume":
        with _lock:
            _settings["paused"] = False
            save_settings(_settings)
        return "▶️ Scanner *resumed*."

    else:
        return f"Unknown command: `{command}`. Try `/help`."


# ─── Telegram helpers ─────────────────────────────────────────────────────────
def send_message(text: str, parse_mode: str = "Markdown"):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={
                "chat_id":    TELEGRAM_CHAT_ID,
                "text":       text,
                "parse_mode": parse_mode,
            },
            timeout=10,
        )
        if not resp.ok:
            log.error(f"Telegram send error {resp.status_code}: {resp.text}")
    except Exception as e:
        log.error(f"Telegram send exception: {e}")


def setup_telegram_webhook(railway_url: str):
    """Register Railway URL as Telegram webhook."""
    if not TELEGRAM_TOKEN or not railway_url:
        return
    webhook_url = f"{railway_url.rstrip('/')}/telegram"
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setWebhook",
            json={"url": webhook_url},
            timeout=10,
        )
        if resp.ok:
            log.info(f"Telegram webhook set: {webhook_url}")
        else:
            log.error(f"Webhook setup failed: {resp.text}")
    except Exception as e:
        log.error(f"Webhook setup exception: {e}")


def notify_signal(signal: dict):
    emoji     = "🟢" if signal["outcome"].lower() == "yes" else "🔴"
    pnl_emoji = "📈" if signal["avgPnl"] > 0 else "📉"
    url       = f"https://polymarket.com/event/{signal.get('slug', '')}"

    with _lock:
        ab_enabled = _settings["autobuy_enabled"]
        size       = _settings["autobuy_size_usd"]

    buy_note = f"\n💸 Auto-buying `${size:.0f}`..." if ab_enabled else "\n🔕 Auto-buy is OFF"

    send_message(
        f"🎯 *PMB Signal*\n\n"
        f"{emoji} *{signal['title']}*\n"
        f"Outcome: *{signal['outcome']}* @ `{signal['curPrice']:.3f}`\n\n"
        f"👥 *{signal['holderCount']} top traders in this position*\n"
        f"💰 Combined: `${signal['totalValue']:,.0f}`\n"
        f"{pnl_emoji} Avg PnL: `{signal['avgPnl']:.1f}%`"
        f"{buy_note}\n\n"
        f"🔗 [View on Polymarket]({url})"
    )


# ─── API helpers ──────────────────────────────────────────────────────────────
def get_api(url, params=None, retries=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=15)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.warning(f"GET {url} failed (attempt {attempt+1}): {e}")
            time.sleep(2 ** attempt)
    return None


# ─── Leaderboard ──────────────────────────────────────────────────────────────
def fetch_leaderboard() -> list:
    with _lock:
        s = dict(_settings)

    data = get_api(f"{DATA_API}/v1/leaderboard", {
        "timePeriod": s["lb_time_period"],
        "orderBy":    s["lb_order_by"],
        "limit":      s["lb_limit"],
        "category":   s["lb_category"],
    })

    if not isinstance(data, list) or len(data) == 0:
        log.warning("Leaderboard returned no data")
        return []

    wallets = [
        e.get("proxyWallet", "")
        for e in data
        if e.get("proxyWallet", "").startswith("0x")
    ]

    log.info(f"Leaderboard: {len(wallets)} wallets "
             f"({s['lb_category']} / {s['lb_time_period']} / {s['lb_order_by']})")

    for e in data[:5]:
        log.info(f"  #{e.get('rank')} {e.get('userName','anon')} "
                 f"PnL=${e.get('pnl',0):,.0f} Vol=${e.get('vol',0):,.0f}")

    return wallets


# ─── Positions ────────────────────────────────────────────────────────────────
def fetch_positions(wallet: str) -> list:
    with _lock:
        threshold = _settings["min_position_usd"]
    data = get_api(f"{DATA_API}/positions", {
        "user":          wallet,
        "sizeThreshold": max(threshold / 10, 10),
    })
    return data if isinstance(data, list) else []


# ─── Overlap detection ────────────────────────────────────────────────────────
def detect_overlaps(wallets: list) -> list:
    with _lock:
        s = dict(_settings)

    market_holders = defaultdict(list)

    for i, wallet in enumerate(wallets):
        positions = fetch_positions(wallet)
        for pos in positions:
            cid           = pos.get("conditionId", "")
            outcome       = pos.get("outcome", "")
            cur_price     = pos.get("curPrice", 1.0)
            current_value = pos.get("currentValue", 0)

            if current_value < s["min_position_usd"]:
                continue
            if cur_price > s["max_entry_price"]:
                continue

            key = f"{cid}|{outcome}"
            market_holders[key].append({
                "wallet":       wallet,
                "conditionId":  cid,
                "outcome":      outcome,
                "curPrice":     cur_price,
                "currentValue": current_value,
                "title":        pos.get("title", "Unknown"),
                "slug":         pos.get("slug", ""),
                "size":         pos.get("size", 0),
                "avgPrice":     pos.get("avgPrice", 0),
                "percentPnl":   pos.get("percentPnl", 0),
            })

        if i < len(wallets) - 1:
            time.sleep(s["request_delay_s"])

    overlaps = []
    for key, holders in market_holders.items():
        if len(holders) >= s["min_overlap"]:
            h = holders[0]
            overlaps.append({
                "conditionId": h["conditionId"],
                "outcome":     h["outcome"],
                "title":       h["title"],
                "slug":        h["slug"],
                "curPrice":    h["curPrice"],
                "holderCount": len(holders),
                "holders":     holders,
                "totalValue":  sum(x["currentValue"] for x in holders),
                "avgPnl":      sum(x["percentPnl"] for x in holders) / len(holders),
            })

    overlaps.sort(key=lambda x: x["holderCount"], reverse=True)
    return overlaps


# ─── Auto-buy ─────────────────────────────────────────────────────────────────
def attempt_autobuy(signal: dict) -> bool:
    with _lock:
        s     = dict(_settings)
        spent = _bought_store.get("total_spent", 0.0)

    if not s["autobuy_enabled"]:
        return False

    remaining = s["autobuy_daily_limit"] - spent
    if remaining <= 0:
        log.warning("Daily auto-buy limit reached")
        send_message("⚠️ Daily auto-buy limit reached. No more buys today.")
        return False

    if not PRIVATE_KEY:
        log.error("No POLYMARKET_PRIVATE_KEY set")
        return False

    size_usd = min(s["autobuy_size_usd"], remaining)

    try:
        from py_clob_client_v2 import ClobClient
        from py_clob_client_v2.clob_types import OrderArgs, OrderType
        from py_clob_client_v2.constants import POLYGON

        temp   = ClobClient("https://clob.polymarket.com", key=PRIVATE_KEY, chain_id=POLYGON)
        creds  = temp.create_or_derive_api_creds()
        client = ClobClient("https://clob.polymarket.com", key=PRIVATE_KEY, chain_id=POLYGON, creds=creds)

        market_info   = client.get_market(condition_id=signal["conditionId"])
        tokens        = market_info.get("tokens", [])
        outcome_index = 0 if signal["outcome"].lower() == "yes" else 1
        token_id      = tokens[outcome_index]["token_id"]

        order = client.create_order(OrderArgs(
            token_id=token_id,
            price=round(signal["curPrice"] + 0.01, 3),
            size=round(size_usd / signal["curPrice"], 2),
            side="BUY",
        ))
        resp = client.post_order(order, OrderType.GTC)

        if resp and resp.get("success"):
            order_id = resp.get("orderID", "unknown")
            with _lock:
                _bought_store["total_spent"] += size_usd
                save_bought(_bought_store)
            log.info(f"Auto-buy success: {order_id} ${size_usd:.0f}")
            send_message(
                f"✅ *Auto-buy executed*\n"
                f"{signal['title']}\n"
                f"{signal['outcome']} @ `{signal['curPrice']:.3f}`\n"
                f"Size: `${size_usd:.0f}` | Order: `{order_id}`"
            )
            return True
        else:
            log.error(f"Auto-buy failed: {resp}")
            send_message(f"❌ Auto-buy failed for {signal['title']}")
            return False
    except Exception as e:
        log.error(f"Auto-buy error: {e}")
        send_message(f"❌ Auto-buy error: {e}")
        return False


# ─── Polling loop ─────────────────────────────────────────────────────────────
def polling_loop():
    global _triggered_store, _bought_store
    last_positions_poll = 0

    while True:
        now = time.time()

        with _lock:
            paused       = _settings.get("paused", False)
            lb_refresh_s = _settings["lb_refresh_s"]
            poll_s       = _settings["poll_interval_s"]
            last_lb      = _state["last_lb_fetch"]
            wallets      = _state["leaderboard"]

        # Daily resets
        with _lock:
            _triggered_store = maybe_reset_triggered(_triggered_store)
            _bought_store    = maybe_reset_bought(_bought_store)

        if paused:
            time.sleep(10)
            continue

        # Refresh leaderboard
        if now - last_lb > lb_refresh_s:
            new_wallets = fetch_leaderboard()
            if new_wallets:
                with _lock:
                    _state["leaderboard"]   = new_wallets
                    _state["last_lb_fetch"] = now
                wallets = new_wallets

        if not wallets:
            log.info("Waiting for leaderboard wallets...")
            time.sleep(30)
            continue

        # Scan positions
        if now - last_positions_poll >= poll_s:
            log.info(f"Scanning {len(wallets)} wallets...")
            overlaps = detect_overlaps(wallets)

            with _lock:
                _state["scan_count"] += 1
                _state["results"] = {
                    "timestamp":   datetime.now(timezone.utc).isoformat(),
                    "walletCount": len(wallets),
                    "scanCount":   _state["scan_count"],
                    "overlaps":    overlaps,
                    "config": {
                        "minOverlap":        _settings["min_overlap"],
                        "pollIntervalS":     _settings["poll_interval_s"],
                        "leaderboardWindow": _settings["lb_time_period"],
                        "category":          _settings["lb_category"],
                        "autobuyEnabled":    _settings["autobuy_enabled"],
                    },
                }

            log.info(f"Scan #{_state['scan_count']}: {len(overlaps)} signals")

            for signal in overlaps:
                key = f"{signal['conditionId']}|{signal['outcome']}"

                with _lock:
                    already_notified = key in _triggered_store["keys"]
                    already_bought   = key in _bought_store["keys"]
                    ab_enabled       = _settings["autobuy_enabled"]
                    ab_min_overlap   = _settings["autobuy_min_overlap"]
                    ab_max_price     = _settings["autobuy_max_price"]

                # Notify once per signal per day
                if not already_notified:
                    notify_signal(signal)
                    with _lock:
                        _triggered_store["keys"].append(key)
                        save_triggered(_triggered_store)
                    log.info(f"Notified: {signal['title']} ({signal['outcome']})")

                # Buy once per market per day
                if (ab_enabled
                        and not already_bought
                        and signal["holderCount"] >= ab_min_overlap
                        and signal["curPrice"] <= ab_max_price):
                    success = attempt_autobuy(signal)
                    if success:
                        with _lock:
                            _bought_store["keys"].append(key)
                            save_bought(_bought_store)

            last_positions_poll = now

        time.sleep(10)


# ─── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("Starting PMB — Polymarket Brain...")

    # Register Telegram webhook if RAILWAY_PUBLIC_DOMAIN is set
    railway_url = os.getenv("RAILWAY_PUBLIC_DOMAIN", "")
    if railway_url:
        if not railway_url.startswith("http"):
            railway_url = f"https://{railway_url}"
        setup_telegram_webhook(railway_url)
    else:
        log.warning("RAILWAY_PUBLIC_DOMAIN not set — Telegram webhook not registered. "
                    "Set this in Railway variables to enable bot commands.")

    try:
        t = threading.Thread(target=polling_loop, daemon=True)
        t.start()
        log.info("Polling thread started OK")
    except Exception as e:
        log.error(f"Thread failed to start: {e}")

    app.run(host="0.0.0.0", port=PORT)
