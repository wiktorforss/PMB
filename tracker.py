"""
PMB — Polymarket Brain
Railway Edition: Flask API + background polling + Telegram bot controls.

Fixes in this version:
  - Telegram uses HTML parse mode — no more markdown entity errors
  - Auto-buy uses correct py-clob-client API (no create_or_derive_api_creds)
  - Only buys NEW signals — positions that existed before PMB started are skipped
  - Separate notification vs buy dedup — alerts and buys tracked independently
  - New signals store tracks first-seen timestamp per market

Telegram commands:
  /status               — current settings + today's spend + scan info
  /autobuy on|off       — enable/disable auto-buying
  /setsize <amount>     — trade size per signal (e.g. /setsize 25)
  /setdailylimit <amt>  — max daily spend (e.g. /setdailylimit 100)
  /setmaxprice <cents>  — max entry price in cents (e.g. /setmaxprice 70)
  /setminoverlap <n>    — min traders overlapping to trigger (e.g. /setminoverlap 3)
  /setcategory <cat>    — leaderboard category (OVERALL, CRYPTO, POLITICS...)
  /setperiod <period>   — leaderboard time period (DAY, WEEK, MONTH, ALL)
  /pause                — pause all scanning
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
TRIGGERED_FILE   = Path("triggered.json")    # notified signals (once per day)
BOUGHT_FILE      = Path("bought.json")       # bought markets (once per day)
SETTINGS_FILE    = Path("settings.json")     # runtime settings
FIRST_SEEN_FILE  = Path("first_seen.json")   # when each signal was first detected

DATA_API = "https://data-api.polymarket.com"

# ─── Fixed env vars ───────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
PRIVATE_KEY      = os.getenv("POLYMARKET_PRIVATE_KEY", "")
PORT             = int(os.getenv("PORT", "8080"))

VALID_CATEGORIES = {
    "OVERALL", "POLITICS", "SPORTS", "CRYPTO",
    "CULTURE", "ECONOMICS", "TECH", "FINANCE", "WEATHER",
}
VALID_PERIODS = {"DAY", "WEEK", "MONTH", "ALL"}

# ─── Default settings ─────────────────────────────────────────────────────────
DEFAULT_SETTINGS = {
    "lb_time_period":      os.getenv("LB_TIME_PERIOD", "WEEK"),
    "lb_order_by":         os.getenv("LB_ORDER_BY", "PNL"),
    "lb_limit":        int(os.getenv("LB_LIMIT", "50")),
    "lb_category":         os.getenv("LB_CATEGORY", "OVERALL"),
    "lb_refresh_s":    int(os.getenv("LB_REFRESH_S", "900")),
    "poll_interval_s": int(os.getenv("POLL_INTERVAL_S", "60")),
    "request_delay_s":float(os.getenv("REQUEST_DELAY_S", "0.2")),
    "min_overlap":     int(os.getenv("MIN_OVERLAP", "2")),
    "min_position_usd":float(os.getenv("MIN_POSITION_USD", "100")),
    "max_entry_price": float(os.getenv("MAX_ENTRY_PRICE", "0.85")),
    "notify_threshold":int(os.getenv("NOTIFY_THRESHOLD", "2")),
    "autobuy_enabled":     os.getenv("AUTOBUY_ENABLED", "false").lower() == "true",
    "autobuy_min_overlap":int(os.getenv("AUTOBUY_MIN_OVERLAP", "2")),
    "autobuy_max_price":  float(os.getenv("AUTOBUY_MAX_PRICE", "0.70")),
    "autobuy_size_usd":   float(os.getenv("AUTOBUY_SIZE_USD", "10")),
    "autobuy_daily_limit":float(os.getenv("AUTOBUY_DAILY_LIMIT", "100")),
    "paused": False,
}


# ─── Persistence helpers ──────────────────────────────────────────────────────

def load_settings() -> dict:
    if SETTINGS_FILE.exists():
        try:
            saved  = json.loads(SETTINGS_FILE.read_text())
            merged = {**DEFAULT_SETTINGS, **saved}
            log.info("Settings loaded from disk")
            return merged
        except Exception as e:
            log.warning(f"Could not load settings: {e}")
    return dict(DEFAULT_SETTINGS)


def save_settings(s: dict):
    try:
        SETTINGS_FILE.write_text(json.dumps(s, indent=2))
    except Exception as e:
        log.warning(f"Could not save settings: {e}")


def _load_daily(path: Path, extra: dict = None) -> dict:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if path.exists():
        try:
            data = json.loads(path.read_text())
            if data.get("date") == today:
                return data
        except Exception:
            pass
    base = {"date": today, "keys": []}
    if extra:
        base.update(extra)
    return base


def _save(path: Path, data: dict):
    try:
        path.write_text(json.dumps(data))
    except Exception as e:
        log.warning(f"Could not save {path}: {e}")


def _maybe_reset(store: dict, path: Path, extra: dict = None) -> dict:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if store.get("date") != today:
        log.info(f"New day — resetting {path.name}")
        store = {"date": today, "keys": []}
        if extra:
            store.update(extra)
        _save(path, store)
    return store


def load_first_seen() -> dict:
    """Load first-seen timestamps — NOT reset daily, persists indefinitely."""
    if FIRST_SEEN_FILE.exists():
        try:
            return json.loads(FIRST_SEEN_FILE.read_text())
        except Exception:
            pass
    return {}


def save_first_seen(data: dict):
    try:
        FIRST_SEEN_FILE.write_text(json.dumps(data))
    except Exception as e:
        log.warning(f"Could not save first_seen: {e}")


# ─── Shared state ─────────────────────────────────────────────────────────────
_lock            = threading.Lock()
_settings        = load_settings()
_triggered_store = _load_daily(TRIGGERED_FILE)
_bought_store    = _load_daily(BOUGHT_FILE, {"total_spent": 0.0})
_first_seen      = load_first_seen()   # { "conditionId|outcome": ISO timestamp }

_state = {
    "results":       None,
    "leaderboard":   [],
    "last_lb_fetch": 0,
    "scan_count":    0,
    "started_at":    datetime.now(timezone.utc).isoformat(),
}

# Track which keys existed on the very first scan so we never auto-buy them
_baseline_keys: set = set()
_baseline_set = False   # flipped to True after first scan completes


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
            "status":          "ok",
            "scan_count":      _state["scan_count"],
            "wallets_tracked": len(_state["leaderboard"]),
            "started_at":      _state["started_at"],
            "triggered_today": len(_triggered_store.get("keys", [])),
            "bought_today":    len(_bought_store.get("keys", [])),
            "spent_today":     _bought_store.get("total_spent", 0.0),
            "paused":          _settings.get("paused", False),
            "baseline_locked": _baseline_set,
        })


# ─── Telegram webhook ─────────────────────────────────────────────────────────
@app.route("/telegram", methods=["POST"])
def telegram_webhook():
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"ok": True})

    msg     = data.get("message") or data.get("edited_message") or {}
    text    = msg.get("text", "").strip()
    chat_id = str(msg.get("chat", {}).get("id", ""))

    if chat_id != TELEGRAM_CHAT_ID:
        log.warning(f"Ignoring message from unknown chat: {chat_id}")
        return jsonify({"ok": True})

    if not text.startswith("/"):
        return jsonify({"ok": True})

    parts   = text.split()
    command = parts[0].lower().split("@")[0]
    args    = parts[1:]

    reply = handle_command(command, args)
    if reply:
        send_html(reply)

    return jsonify({"ok": True})


def handle_command(command: str, args: list) -> str:
    global _settings

    with _lock:
        s = dict(_settings)

    if command == "/help":
        return (
            "🤖 <b>PMB Bot Commands</b>\n\n"
            "<code>/status</code> — settings and stats\n"
            "<code>/autobuy on|off</code> — toggle auto-buying\n"
            "<code>/setsize 25</code> — trade size per signal\n"
            "<code>/setdailylimit 100</code> — max daily spend\n"
            "<code>/setmaxprice 70</code> — max entry price in cents\n"
            "<code>/setminoverlap 3</code> — min traders to trigger buy\n"
            "<code>/setcategory CRYPTO</code> — leaderboard category\n"
            "  Options: OVERALL CRYPTO POLITICS SPORTS\n"
            "  ECONOMICS TECH FINANCE CULTURE\n"
            "<code>/setperiod WEEK</code> — leaderboard period\n"
            "  Options: DAY WEEK MONTH ALL\n"
            "<code>/pause</code> — pause scanning\n"
            "<code>/resume</code> — resume scanning\n"
            "<code>/help</code> — show this message"
        )

    elif command == "/status":
        with _lock:
            scans   = _state["scan_count"]
            wallets = len(_state["leaderboard"])
            spent   = _bought_store.get("total_spent", 0.0)
            bought  = len(_bought_store.get("keys", []))
            alerted = len(_triggered_store.get("keys", []))

        ab  = "✅ ON" if s["autobuy_enabled"] else "❌ OFF"
        psd = "⏸ PAUSED" if s.get("paused") else "▶️ RUNNING"

        return (
            f"📊 <b>PMB Status</b>\n\n"
            f"<b>Scanner:</b> {psd}\n"
            f"<b>Scans run:</b> {scans}\n"
            f"<b>Wallets tracked:</b> {wallets}\n"
            f"<b>Signals today:</b> {alerted}\n\n"
            f"<b>Auto-buy:</b> {ab}\n"
            f"<b>Trade size:</b> <code>${s['autobuy_size_usd']:.0f}</code>\n"
            f"<b>Daily limit:</b> <code>${s['autobuy_daily_limit']:.0f}</code>\n"
            f"<b>Spent today:</b> <code>${spent:.2f}</code> ({bought} trades)\n"
            f"<b>Max price:</b> <code>{s['autobuy_max_price']*100:.0f}c</code>\n"
            f"<b>Min overlap:</b> <code>{s['autobuy_min_overlap']} traders</code>\n\n"
            f"<b>Category:</b> <code>{s['lb_category']}</code>\n"
            f"<b>Period:</b> <code>{s['lb_time_period']}</code>\n"
            f"<b>Poll interval:</b> <code>{s['poll_interval_s']}s</code>"
        )

    elif command == "/autobuy":
        if not args:
            state = "ON" if s["autobuy_enabled"] else "OFF"
            return f"Auto-buy is currently <b>{state}</b>. Use /autobuy on or /autobuy off."
        val = args[0].lower()
        if val == "on":
            with _lock:
                _settings["autobuy_enabled"] = True
                save_settings(_settings)
            return "✅ Auto-buy <b>enabled</b>."
        elif val == "off":
            with _lock:
                _settings["autobuy_enabled"] = False
                save_settings(_settings)
            return "❌ Auto-buy <b>disabled</b>. You will still get alerts."
        return "Usage: /autobuy on or /autobuy off"

    elif command == "/setsize":
        if not args:
            return f"Current trade size: <code>${s['autobuy_size_usd']:.0f}</code>. Usage: /setsize 25"
        try:
            val = float(args[0].replace("$", ""))
            if val <= 0:
                return "❌ Trade size must be greater than $0."
            if val > 1000:
                return "❌ Trade size capped at $1000 for safety."
            with _lock:
                _settings["autobuy_size_usd"] = val
                save_settings(_settings)
            return f"✅ Trade size set to <code>${val:.0f}</code> per signal."
        except ValueError:
            return "❌ Invalid amount. Usage: /setsize 25"

    elif command == "/setdailylimit":
        if not args:
            return f"Current daily limit: <code>${s['autobuy_daily_limit']:.0f}</code>. Usage: /setdailylimit 100"
        try:
            val = float(args[0].replace("$", ""))
            if val <= 0:
                return "❌ Daily limit must be greater than $0."
            with _lock:
                _settings["autobuy_daily_limit"] = val
                save_settings(_settings)
            return f"✅ Daily limit set to <code>${val:.0f}</code>."
        except ValueError:
            return "❌ Invalid amount. Usage: /setdailylimit 100"

    elif command == "/setmaxprice":
        if not args:
            cur = s["autobuy_max_price"] * 100
            return f"Current max price: <code>{cur:.0f}c</code>. Usage: /setmaxprice 70"
        try:
            val = float(args[0].replace("c", "").replace("%", ""))
            if not 1 <= val <= 99:
                return "❌ Price must be between 1 and 99."
            with _lock:
                _settings["autobuy_max_price"] = round(val / 100, 2)
                save_settings(_settings)
            return f"✅ Max entry price set to <code>{val:.0f}c</code>."
        except ValueError:
            return "❌ Invalid price. Usage: /setmaxprice 70"

    elif command == "/setminoverlap":
        if not args:
            return f"Current min overlap: <code>{s['autobuy_min_overlap']}</code>. Usage: /setminoverlap 3"
        try:
            val = int(args[0])
            if val < 2:
                return "❌ Min overlap must be at least 2."
            with _lock:
                _settings["autobuy_min_overlap"] = val
                save_settings(_settings)
            return f"✅ Min overlap set to <code>{val} traders</code>."
        except ValueError:
            return "❌ Invalid number. Usage: /setminoverlap 3"

    elif command == "/setcategory":
        if not args:
            opts = ", ".join(sorted(VALID_CATEGORIES))
            return f"Current: <code>{s['lb_category']}</code>\nOptions: {opts}"
        val = args[0].upper()
        if val not in VALID_CATEGORIES:
            return f"❌ Invalid. Options: {', '.join(sorted(VALID_CATEGORIES))}"
        with _lock:
            _settings["lb_category"]    = val
            _state["last_lb_fetch"]     = 0
            save_settings(_settings)
        return f"✅ Category set to <code>{val}</code>. Refreshing wallets..."

    elif command == "/setperiod":
        if not args:
            opts = ", ".join(sorted(VALID_PERIODS))
            return f"Current: <code>{s['lb_time_period']}</code>\nOptions: {opts}"
        val = args[0].upper()
        if val not in VALID_PERIODS:
            return f"❌ Invalid. Options: {', '.join(sorted(VALID_PERIODS))}"
        with _lock:
            _settings["lb_time_period"] = val
            _state["last_lb_fetch"]     = 0
            save_settings(_settings)
        return f"✅ Period set to <code>{val}</code>. Refreshing wallets..."

    elif command == "/pause":
        with _lock:
            _settings["paused"] = True
            save_settings(_settings)
        return "⏸ Scanner <b>paused</b>. Use /resume to restart."

    elif command == "/resume":
        with _lock:
            _settings["paused"] = False
            save_settings(_settings)
        return "▶️ Scanner <b>resumed</b>."

    else:
        return f"Unknown command: <code>{command}</code>. Try /help."


# ─── Telegram send helpers ────────────────────────────────────────────────────

def _escape_html(text: str) -> str:
    return (text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;"))


def send_html(text: str):
    """Send a Telegram message using HTML parse mode."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={
                "chat_id":    TELEGRAM_CHAT_ID,
                "text":       text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        if not resp.ok:
            log.error(f"Telegram error {resp.status_code}: {resp.text}")
    except Exception as e:
        log.error(f"Telegram exception: {e}")


def setup_telegram_webhook(railway_url: str):
    if not TELEGRAM_TOKEN or not railway_url:
        return
    webhook = f"{railway_url.rstrip('/')}/telegram"
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setWebhook",
            json={"url": webhook},
            timeout=10,
        )
        if resp.ok:
            log.info(f"Telegram webhook registered: {webhook}")
        else:
            log.error(f"Webhook failed: {resp.text}")
    except Exception as e:
        log.error(f"Webhook exception: {e}")


def notify_signal(signal: dict, is_new: bool):
    emoji     = "🟢" if signal["outcome"].lower() == "yes" else "🔴"
    pnl_emoji = "📈" if signal["avgPnl"] > 0 else "📉"
    url       = f"https://polymarket.com/event/{signal.get('slug', '')}"
    title     = _escape_html(signal["title"])

    with _lock:
        ab_enabled = _settings["autobuy_enabled"]
        size       = _settings["autobuy_size_usd"]

    if is_new and ab_enabled:
        buy_note = f"\n💸 Auto-buying <code>${size:.0f}</code>..."
    elif not is_new:
        buy_note = "\n⏩ Existing position — skipping auto-buy"
    else:
        buy_note = "\n🔕 Auto-buy is OFF"

    send_html(
        f"🎯 <b>PMB Signal</b>\n\n"
        f"{emoji} <b>{title}</b>\n"
        f"Outcome: <b>{signal['outcome']}</b> @ <code>{signal['curPrice']:.3f}</code>\n\n"
        f"👥 <b>{signal['holderCount']} top traders in this position</b>\n"
        f"💰 Combined: <code>${signal['totalValue']:,.0f}</code>\n"
        f"{pnl_emoji} Avg PnL: <code>{signal['avgPnl']:.1f}%</code>"
        f"{buy_note}\n\n"
        f"<a href='{url}'>View on Polymarket</a>"
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

    if not isinstance(data, list) or not data:
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
        send_html("⚠️ Daily auto-buy limit reached. No more buys today.")
        return False

    if not PRIVATE_KEY:
        log.error("No POLYMARKET_PRIVATE_KEY set")
        return False

    size_usd = min(s["autobuy_size_usd"], remaining)
    title    = _escape_html(signal["title"])

    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds, OrderArgs, OrderType
        from py_clob_client.constants import POLYGON

        # Correct auth flow for py-clob-client (no v2)
        client = ClobClient(
            host     = "https://clob.polymarket.com",
            key      = PRIVATE_KEY,
            chain_id = POLYGON,
        )

        # Derive L2 API credentials from L1 wallet key
        api_creds = client.derive_api_key()
        client.set_api_creds(ApiCreds(
            api_key        = api_creds["apiKey"],
            api_secret     = api_creds["secret"],
            api_passphrase = api_creds["passphrase"],
        ))

        # Resolve token ID for the outcome
        market_info   = client.get_market(condition_id=signal["conditionId"])
        tokens        = market_info.get("tokens", [])
        outcome_index = 0 if signal["outcome"].lower() == "yes" else 1

        if outcome_index >= len(tokens):
            log.error(f"Could not resolve token for outcome: {signal['outcome']}")
            return False

        token_id = tokens[outcome_index]["token_id"]

        # Place order
        order = client.create_order(OrderArgs(
            token_id = token_id,
            price    = round(signal["curPrice"] + 0.01, 3),
            size     = round(size_usd / signal["curPrice"], 2),
            side     = "BUY",
        ))
        resp = client.post_order(order, OrderType.GTC)

        if resp and resp.get("success"):
            order_id = resp.get("orderID", "unknown")
            with _lock:
                _bought_store["total_spent"] += size_usd
                save_bought(_bought_store)
            log.info(f"Auto-buy success: {order_id} ${size_usd:.0f}")
            send_html(
                f"✅ <b>Auto-buy executed</b>\n"
                f"{title}\n"
                f"{signal['outcome']} @ <code>{signal['curPrice']:.3f}</code>\n"
                f"Size: <code>${size_usd:.0f}</code> | Order: <code>{order_id}</code>"
            )
            return True
        else:
            log.error(f"Auto-buy failed: {resp}")
            send_html(f"❌ Auto-buy failed for {title}")
            return False

    except ImportError:
        log.error("py-clob-client not installed. Add to requirements.txt")
        return False
    except Exception as e:
        log.error(f"Auto-buy error: {e}")
        send_html(f"❌ Auto-buy error: <code>{_escape_html(str(e))}</code>")
        return False


def _save_bought_store():
    _save(BOUGHT_FILE, _bought_store)


# ─── Polling loop ─────────────────────────────────────────────────────────────
def polling_loop():
    global _triggered_store, _bought_store, _first_seen, _baseline_keys, _baseline_set
    last_positions_poll = 0

    while True:
        now = time.time()

        # Daily resets
        with _lock:
            _triggered_store = _maybe_reset(_triggered_store, TRIGGERED_FILE)
            _bought_store    = _maybe_reset(_bought_store, BOUGHT_FILE, {"total_spent": 0.0})

        with _lock:
            paused       = _settings.get("paused", False)
            lb_refresh_s = _settings["lb_refresh_s"]
            poll_s       = _settings["poll_interval_s"]
            last_lb      = _state["last_lb_fetch"]
            wallets      = _state["leaderboard"]

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

            # On first scan, record all current signals as baseline
            # These are pre-existing positions — we notify but don't auto-buy them
            with _lock:
                if not _baseline_set:
                    _baseline_keys = {
                        f"{s['conditionId']}|{s['outcome']}"
                        for s in overlaps
                    }
                    _baseline_set = True
                    log.info(f"Baseline set: {len(_baseline_keys)} pre-existing signals "
                             f"(will not be auto-bought)")

            # Record first-seen timestamps for new signals
            now_iso = datetime.now(timezone.utc).isoformat()
            with _lock:
                for signal in overlaps:
                    key = f"{signal['conditionId']}|{signal['outcome']}"
                    if key not in _first_seen:
                        _first_seen[key] = now_iso
                        save_first_seen(_first_seen)

            with _lock:
                _state["scan_count"] += 1
                _state["results"] = {
                    "timestamp":   now_iso,
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

            log.info(f"Scan #{_state['scan_count']}: {len(overlaps)} signals found")

            for signal in overlaps:
                key = f"{signal['conditionId']}|{signal['outcome']}"

                with _lock:
                    already_notified = key in _triggered_store["keys"]
                    already_bought   = key in _bought_store["keys"]
                    is_baseline      = key in _baseline_keys
                    ab_enabled       = _settings["autobuy_enabled"]
                    ab_min_overlap   = _settings["autobuy_min_overlap"]
                    ab_max_price     = _settings["autobuy_max_price"]

                # New signal — notify once
                if not already_notified:
                    is_new = not is_baseline
                    notify_signal(signal, is_new=is_new)
                    with _lock:
                        _triggered_store["keys"].append(key)
                        _save(TRIGGERED_FILE, _triggered_store)
                    log.info(f"Notified: {signal['title']} | "
                             f"new={is_new} baseline={is_baseline}")

                # Auto-buy only if:
                # - Not a baseline (pre-existing) position
                # - Not already bought today
                # - Meets overlap + price thresholds
                if (ab_enabled
                        and not is_baseline
                        and not already_bought
                        and signal["holderCount"] >= ab_min_overlap
                        and signal["curPrice"] <= ab_max_price):
                    success = attempt_autobuy(signal)
                    if success:
                        with _lock:
                            _bought_store["keys"].append(key)
                            _save(BOUGHT_FILE, _bought_store)

            last_positions_poll = now

        time.sleep(10)


# ─── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("Starting PMB — Polymarket Brain...")

    railway_url = os.getenv("RAILWAY_PUBLIC_DOMAIN", "")
    if railway_url:
        if not railway_url.startswith("http"):
            railway_url = f"https://{railway_url}"
        setup_telegram_webhook(railway_url)
    else:
        log.warning("RAILWAY_PUBLIC_DOMAIN not set — Telegram commands disabled")

    try:
        t = threading.Thread(target=polling_loop, daemon=True)
        t.start()
        log.info("Polling thread started OK")
    except Exception as e:
        log.error(f"Thread failed to start: {e}")

    app.run(host="0.0.0.0", port=PORT)
