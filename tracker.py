"""
PMB — Polymarket Brain
Railway Edition: Flask API + background polling loop.
- Fetches wallets from the REAL Polymarket leaderboard (/v1/leaderboard)
- Detects overlapping positions across top traders
- Sends Telegram alerts ONCE per signal per day
- Persists triggered signals to disk — restarts won't cause repeats
- Resets triggered list daily so new signals re-alert
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
from flask import Flask, jsonify
from flask_cors import CORS

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────
CFG = {
    # Leaderboard
    "lb_time_period":        os.getenv("LB_TIME_PERIOD", "WEEK"),   # DAY, WEEK, MONTH, ALL
    "lb_order_by":           os.getenv("LB_ORDER_BY", "PNL"),       # PNL, VOL
    "lb_limit":          int(os.getenv("LB_LIMIT", "50")),          # max 50
    "lb_category":           os.getenv("LB_CATEGORY", "OVERALL"),   # OVERALL, CRYPTO, POLITICS etc
    "lb_refresh_s":      int(os.getenv("LB_REFRESH_S", "900")),     # refresh every 15 min

    # Polling
    "poll_interval_s":   int(os.getenv("POLL_INTERVAL_S", "300")),  # scan every 5 min
    "request_delay_s": float(os.getenv("REQUEST_DELAY_S", "0.5")), # delay between wallet calls

    # Signal detection
    "min_overlap":       int(os.getenv("MIN_OVERLAP", "2")),
    "min_position_usd": float(os.getenv("MIN_POSITION_USD", "100")),
    "max_entry_price":  float(os.getenv("MAX_ENTRY_PRICE", "0.85")),

    # Notifications
    "notify_threshold":  int(os.getenv("NOTIFY_THRESHOLD", "2")),
    "telegram_token":        os.getenv("TELEGRAM_TOKEN", ""),
    "telegram_chat_id":      os.getenv("TELEGRAM_CHAT_ID", ""),

    # Auto-buy (disabled by default)
    "autobuy_enabled":       os.getenv("AUTOBUY_ENABLED", "false").lower() == "true",
    "autobuy_min_overlap":   int(os.getenv("AUTOBUY_MIN_OVERLAP", "5")),
    "autobuy_max_price":   float(os.getenv("AUTOBUY_MAX_PRICE", "0.70")),
    "autobuy_size_usd":    float(os.getenv("AUTOBUY_SIZE_USD", "10")),
    "autobuy_daily_limit": float(os.getenv("AUTOBUY_DAILY_LIMIT", "50")),
    "private_key":             os.getenv("POLYMARKET_PRIVATE_KEY", ""),

    "port":              int(os.getenv("PORT", "8080")),
}

DATA_API = "https://data-api.polymarket.com"

TRIGGERED_FILE = Path("triggered.json")


# ─── Persistent triggered store ───────────────────────────────────────────────

def load_triggered() -> dict:
    """
    Load triggered signals from disk.
    Format: { "date": "YYYY-MM-DD", "keys": ["conditionId|outcome", ...] }
    If saved date is not today, returns a fresh empty store.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if TRIGGERED_FILE.exists():
        try:
            data = json.loads(TRIGGERED_FILE.read_text())
            if data.get("date") == today:
                log.info(f"Loaded {len(data.get('keys', []))} triggered signals from disk")
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
    """Reset the triggered store if the day has rolled over."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if store.get("date") != today:
        log.info("New day — resetting triggered signals list")
        store = {"date": today, "keys": []}
        save_triggered(store)
    return store


# ─── Shared state ─────────────────────────────────────────────────────────────
_lock = threading.Lock()
_triggered_store = load_triggered()

_state = {
    "results":          None,
    "leaderboard":      [],
    "daily_spend":      0.0,
    "daily_spend_date": "",
    "last_lb_fetch":    0,
    "scan_count":       0,
    "started_at":       datetime.now(timezone.utc).isoformat(),
}

# ─── Flask app ────────────────────────────────────────────────────────────────
app = Flask(__name__)
CORS(app, origins="*", methods=["GET"], allow_headers=["Content-Type"])


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
        })


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
def fetch_leaderboard() -> list[str]:
    """
    Fetch top traders from the official Polymarket leaderboard endpoint.
    Returns a list of proxyWallet addresses.
    """
    data = get_api(f"{DATA_API}/v1/leaderboard", {
        "timePeriod": CFG["lb_time_period"],
        "orderBy":    CFG["lb_order_by"],
        "limit":      CFG["lb_limit"],
        "category":   CFG["lb_category"],
    })

    if not isinstance(data, list) or len(data) == 0:
        log.warning("Leaderboard returned no data")
        return []

    wallets = [
        entry.get("proxyWallet", "")
        for entry in data
        if entry.get("proxyWallet", "").startswith("0x")
    ]

    log.info(f"Leaderboard fetched: {len(wallets)} wallets "
             f"(period={CFG['lb_time_period']}, orderBy={CFG['lb_order_by']})")

    # Log top 5 for visibility
    for entry in data[:5]:
        log.info(f"  #{entry.get('rank')} {entry.get('userName', 'anon')} "
                 f"PnL=${entry.get('pnl', 0):,.0f} Vol=${entry.get('vol', 0):,.0f}")

    return wallets


# ─── Positions ────────────────────────────────────────────────────────────────
def fetch_positions(wallet: str) -> list:
    data = get_api(f"{DATA_API}/positions", {
        "user":          wallet,
        "sizeThreshold": 15,
    })
    return data if isinstance(data, list) else []


# ─── Overlap detection ────────────────────────────────────────────────────────
def detect_overlaps(wallets: list[str]) -> list[dict]:
    market_holders = defaultdict(list)

    for i, wallet in enumerate(wallets):
        positions = fetch_positions(wallet)
        for pos in positions:
            cid           = pos.get("conditionId", "")
            outcome       = pos.get("outcome", "")
            cur_price     = pos.get("curPrice", 1.0)
            current_value = pos.get("currentValue", 0)

            if current_value < CFG["min_position_usd"]:
                continue
            if cur_price > CFG["max_entry_price"]:
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
            time.sleep(CFG["request_delay_s"])

    overlaps = []
    for key, holders in market_holders.items():
        if len(holders) >= CFG["min_overlap"]:
            s = holders[0]
            overlaps.append({
                "conditionId": s["conditionId"],
                "outcome":     s["outcome"],
                "title":       s["title"],
                "slug":        s["slug"],
                "curPrice":    s["curPrice"],
                "holderCount": len(holders),
                "holders":     holders,
                "totalValue":  sum(h["currentValue"] for h in holders),
                "avgPnl":      sum(h["percentPnl"] for h in holders) / len(holders),
            })

    overlaps.sort(key=lambda x: x["holderCount"], reverse=True)
    return overlaps


# ─── Telegram ─────────────────────────────────────────────────────────────────
def notify(signal: dict):
    token   = CFG["telegram_token"]
    chat_id = CFG["telegram_chat_id"]
    if not token or not chat_id:
        return

    emoji     = "🟢" if signal["outcome"].lower() == "yes" else "🔴"
    pnl_emoji = "📈" if signal["avgPnl"] > 0 else "📉"
    url       = f"https://polymarket.com/event/{signal.get('slug', '')}"

    msg = (
        f"🎯 *PMB Signal*\n\n"
        f"{emoji} *{signal['title']}*\n"
        f"Outcome: *{signal['outcome']}* @ `{signal['curPrice']:.3f}`\n\n"
        f"👥 *{signal['holderCount']} top traders in this position*\n"
        f"💰 Combined: `${signal['totalValue']:,.0f}`\n"
        f"{pnl_emoji} Avg PnL: `{signal['avgPnl']:.1f}%`\n\n"
        f"🔗 [View on Polymarket]({url})"
    )

    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"},
            timeout=10,
        )
        if resp.ok:
            log.info(f"Telegram sent: {signal['title']}")
        else:
            log.error(f"Telegram error {resp.status_code}: {resp.text}")
    except Exception as e:
        log.error(f"Telegram exception: {e}")


# ─── Auto-buy ─────────────────────────────────────────────────────────────────
def attempt_autobuy(signal: dict) -> bool:
    if not CFG["autobuy_enabled"]:
        return False

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with _lock:
        if _state["daily_spend_date"] != today:
            _state["daily_spend"]      = 0.0
            _state["daily_spend_date"] = today
        remaining = CFG["autobuy_daily_limit"] - _state["daily_spend"]

    if remaining <= 0:
        log.warning("Daily auto-buy limit reached.")
        return False

    private_key = CFG["private_key"]
    if not private_key:
        log.error("No POLYMARKET_PRIVATE_KEY set.")
        return False

    size_usd = min(CFG["autobuy_size_usd"], remaining)

    try:
        from py_clob_client_v2 import ClobClient
        from py_clob_client_v2.clob_types import OrderArgs, OrderType
        from py_clob_client_v2.constants import POLYGON

        temp   = ClobClient("https://clob.polymarket.com", key=private_key, chain_id=POLYGON)
        creds  = temp.create_or_derive_api_creds()
        client = ClobClient("https://clob.polymarket.com", key=private_key, chain_id=POLYGON, creds=creds)

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
            with _lock:
                _state["daily_spend"] += size_usd
            log.info(f"Auto-buy success: {resp.get('orderID')}")
            return True
        return False
    except Exception as e:
        log.error(f"Auto-buy error: {e}")
        return False


# ─── Polling loop ─────────────────────────────────────────────────────────────
def polling_loop():
    global _triggered_store
    last_positions_poll = 0

    while True:
        now = time.time()

        # Daily reset check
        with _lock:
            _triggered_store = maybe_reset_triggered(_triggered_store)

        # Refresh leaderboard
        with _lock:
            last_lb = _state["last_lb_fetch"]
            wallets = _state["leaderboard"]

        if now - last_lb > CFG["lb_refresh_s"]:
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
        if now - last_positions_poll >= CFG["poll_interval_s"]:
            log.info(f"Scanning {len(wallets)} leaderboard wallets...")
            overlaps = detect_overlaps(wallets)

            with _lock:
                _state["scan_count"] += 1
                _state["results"] = {
                    "timestamp":   datetime.now(timezone.utc).isoformat(),
                    "walletCount": len(wallets),
                    "scanCount":   _state["scan_count"],
                    "overlaps":    overlaps,
                    "config": {
                        "minOverlap":        CFG["min_overlap"],
                        "pollIntervalS":     CFG["poll_interval_s"],
                        "leaderboardWindow": CFG["lb_time_period"],
                    },
                }

            log.info(f"Scan #{_state['scan_count']}: {len(overlaps)} signals found")

            # Notify + auto-buy
            for signal in overlaps:
                key = f"{signal['conditionId']}|{signal['outcome']}"

                with _lock:
                    already = key in _triggered_store["keys"]

                if already:
                    continue

                # New signal — notify
                if signal["holderCount"] >= CFG["notify_threshold"]:
                    notify(signal)
                    with _lock:
                        _triggered_store["keys"].append(key)
                        save_triggered(_triggered_store)
                    log.info(f"Triggered: {signal['title']} ({signal['outcome']})")

                # Auto-buy if enabled
                if (CFG["autobuy_enabled"]
                        and signal["holderCount"] >= CFG["autobuy_min_overlap"]
                        and signal["curPrice"] <= CFG["autobuy_max_price"]):
                    attempt_autobuy(signal)

            last_positions_poll = now

        time.sleep(10)


# ─── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("Starting PMB — Polymarket Brain...")
    try:
        t = threading.Thread(target=polling_loop, daemon=True)
        t.start()
        log.info("Polling thread started OK")
    except Exception as e:
        log.error(f"Thread failed to start: {e}")
    app.run(host="0.0.0.0", port=CFG["port"])
