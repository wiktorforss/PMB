"""
Polymarket Smart Money Tracker — Railway Edition
Runs a polling loop in a background thread + serves results via Flask API.
Railway keeps this process alive 24/7.
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

# ─── Config (from env vars — set these in Railway dashboard) ──────────────────
CFG = {
    "leaderboard_window":          os.getenv("LEADERBOARD_WINDOW", "7d"),
    "leaderboard_limit":       int(os.getenv("LEADERBOARD_LIMIT", "50")),
    "leaderboard_refresh_s":   int(os.getenv("LEADERBOARD_REFRESH_S", "900")),
    "poll_interval_s":         int(os.getenv("POLL_INTERVAL_S", "300")),
    "request_delay_s":       float(os.getenv("REQUEST_DELAY_S", "0.5")),
    "min_overlap":             int(os.getenv("MIN_OVERLAP", "3")),
    "min_position_usd":      float(os.getenv("MIN_POSITION_USD", "100")),
    "max_entry_price":       float(os.getenv("MAX_ENTRY_PRICE", "0.85")),
    "notify_threshold":        int(os.getenv("NOTIFY_THRESHOLD", "3")),
    "telegram_token":              os.getenv("TELEGRAM_TOKEN", ""),
    "telegram_chat_id":            os.getenv("TELEGRAM_CHAT_ID", ""),
    "autobuy_enabled":             os.getenv("AUTOBUY_ENABLED", "false").lower() == "true",
    "autobuy_min_overlap":     int(os.getenv("AUTOBUY_MIN_OVERLAP", "5")),
    "autobuy_max_price":     float(os.getenv("AUTOBUY_MAX_PRICE", "0.70")),
    "autobuy_size_usd":      float(os.getenv("AUTOBUY_SIZE_USD", "10")),
    "autobuy_daily_limit":   float(os.getenv("AUTOBUY_DAILY_LIMIT", "50")),
    "private_key":                 os.getenv("POLYMARKET_PRIVATE_KEY", ""),
    "port":                    int(os.getenv("PORT", "8080")),
}

DATA_API = "https://data-api.polymarket.com"

# ─── Shared state (thread-safe via lock) ─────────────────────────────────────
_lock = threading.Lock()
_state = {
    "results": None,          # Latest scan results
    "leaderboard": [],        # Wallet addresses
    "triggered": set(),       # Already auto-bought market keys
    "daily_spend": 0.0,
    "daily_spend_date": "",
    "last_lb_fetch": 0,
    "scan_count": 0,
    "started_at": datetime.now(timezone.utc).isoformat(),
}

# ─── Flask app ────────────────────────────────────────────────────────────────
app = Flask(__name__)
CORS(app)  # Allow GitHub Pages to fetch from Railway


@app.route("/")
def index():
    return jsonify({"status": "ok", "service": "Polymarket Smart Money Tracker"})


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
            "status": "ok",
            "scan_count": _state["scan_count"],
            "wallets_tracked": len(_state["leaderboard"]),
            "started_at": _state["started_at"],
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
def fetch_leaderboard():
    # Try primary endpoint
    data = get_api(f"{DATA_API}/leaderboard", {
        "window": CFG["leaderboard_window"],
        "limit": CFG["leaderboard_limit"],
    })

    # Fallback — some versions use /rankings
    if not data:
        data = get_api(f"{DATA_API}/rankings", {
            "window": CFG["leaderboard_window"],
            "limit": CFG["leaderboard_limit"],
        })

    if not data:
        return []

    wallets = [
        u.get("proxyWallet") or u.get("address") or u.get("wallet", "")
        for u in data
        if u.get("proxyWallet") or u.get("address") or u.get("wallet")
    ]
    log.info(f"Leaderboard fetched: {len(wallets)} wallets")
    return wallets


# ─── Positions ────────────────────────────────────────────────────────────────
def fetch_positions(wallet):
    data = get_api(f"{DATA_API}/positions", {"user": wallet, "sizeThreshold": 10})
    return data if isinstance(data, list) else []


# ─── Overlap detection ────────────────────────────────────────────────────────
def detect_overlaps(wallets):
    market_holders = defaultdict(list)

    for i, wallet in enumerate(wallets):
        positions = fetch_positions(wallet)
        for pos in positions:
            cid = pos.get("conditionId", "")
            outcome = pos.get("outcome", "")
            cur_price = pos.get("curPrice", 1.0)
            current_value = pos.get("currentValue", 0)

            if current_value < CFG["min_position_usd"]:
                continue
            if cur_price > CFG["max_entry_price"]:
                continue

            key = f"{cid}|{outcome}"
            market_holders[key].append({
                "wallet": wallet,
                "conditionId": cid,
                "outcome": outcome,
                "curPrice": cur_price,
                "currentValue": current_value,
                "title": pos.get("title", "Unknown"),
                "slug": pos.get("slug", ""),
                "size": pos.get("size", 0),
                "avgPrice": pos.get("avgPrice", 0),
                "percentPnl": pos.get("percentPnl", 0),
            })

        if i < len(wallets) - 1:
            time.sleep(CFG["request_delay_s"])

    overlaps = []
    for key, holders in market_holders.items():
        if len(holders) >= CFG["min_overlap"]:
            s = holders[0]
            overlaps.append({
                "conditionId": s["conditionId"],
                "outcome": s["outcome"],
                "title": s["title"],
                "slug": s["slug"],
                "curPrice": s["curPrice"],
                "holderCount": len(holders),
                "holders": holders,
                "totalValue": sum(h["currentValue"] for h in holders),
                "avgPnl": sum(h["percentPnl"] for h in holders) / len(holders),
            })

    overlaps.sort(key=lambda x: x["holderCount"], reverse=True)
    return overlaps


# ─── Telegram ─────────────────────────────────────────────────────────────────
def notify(signal):
    token = CFG["telegram_token"]
    chat_id = CFG["telegram_chat_id"]
    if not token or not chat_id:
        return

    emoji = "🟢" if signal["outcome"].lower() == "yes" else "🔴"
    pnl_emoji = "📈" if signal["avgPnl"] > 0 else "📉"
    url = f"https://polymarket.com/event/{signal.get('slug', '')}"

    msg = (
        f"🎯 *Smart Money Signal*\n\n"
        f"{emoji} *{signal['title']}*\n"
        f"Outcome: *{signal['outcome']}* @ `{signal['curPrice']:.3f}`\n\n"
        f"👥 *{signal['holderCount']} leaderboard traders*\n"
        f"💰 Combined: `${signal['totalValue']:,.0f}`\n"
        f"{pnl_emoji} Avg PnL: `{signal['avgPnl']:.1f}%`\n\n"
        f"🔗 [View on Polymarket]({url})"
    )

    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg, "parse_mode": "Markdown"},
            timeout=10,
        )
        log.info(f"Telegram sent for: {signal['title']}")
    except Exception as e:
        log.error(f"Telegram error: {e}")


# ─── Auto-buy ─────────────────────────────────────────────────────────────────
def attempt_autobuy(signal):
    if not CFG["autobuy_enabled"]:
        return False

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with _lock:
        if _state["daily_spend_date"] != today:
            _state["daily_spend"] = 0.0
            _state["daily_spend_date"] = today
        remaining = CFG["autobuy_daily_limit"] - _state["daily_spend"]

    if remaining <= 0:
        log.warning("Daily auto-buy limit reached.")
        return False

    size_usd = min(CFG["autobuy_size_usd"], remaining)
    private_key = CFG["private_key"]
    if not private_key:
        log.error("No POLYMARKET_PRIVATE_KEY set.")
        return False

    try:
        from py_clob_client_v2 import ClobClient
        from py_clob_client_v2.clob_types import OrderArgs, OrderType
        from py_clob_client_v2.constants import POLYGON

        temp = ClobClient("https://clob.polymarket.com", key=private_key, chain_id=POLYGON)
        creds = temp.create_or_derive_api_creds()
        client = ClobClient("https://clob.polymarket.com", key=private_key, chain_id=POLYGON, creds=creds)

        market_info = client.get_market(condition_id=signal["conditionId"])
        tokens = market_info.get("tokens", [])
        outcome_index = 0 if signal["outcome"].lower() == "yes" else 1
        token_id = tokens[outcome_index]["token_id"]

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


# ─── Polling loop (runs in background thread) ─────────────────────────────────
def polling_loop():
    last_positions_poll = 0

    while True:
        now = time.time()

        # Refresh leaderboard
        with _lock:
            last_lb = _state["last_lb_fetch"]
            wallets = _state["leaderboard"]

        if now - last_lb > CFG["leaderboard_refresh_s"]:
            new_wallets = fetch_leaderboard()
            if new_wallets:
                with _lock:
                    _state["leaderboard"] = new_wallets
                    _state["last_lb_fetch"] = now
                wallets = new_wallets

        if not wallets:
            log.info("Waiting for leaderboard...")
            time.sleep(30)
            continue

        # Poll positions
        if now - last_positions_poll >= CFG["poll_interval_s"]:
            log.info(f"Scanning {len(wallets)} wallets...")
            overlaps = detect_overlaps(wallets)

            result = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "walletCount": len(wallets),
                "scanCount": _state["scan_count"] + 1,
                "overlaps": overlaps,
                "config": {
                    "minOverlap": CFG["min_overlap"],
                    "pollIntervalS": CFG["poll_interval_s"],
                    "leaderboardWindow": CFG["leaderboard_window"],
                },
            }

            with _lock:
                _state["results"] = result
                _state["scan_count"] += 1

            log.info(f"Scan #{_state['scan_count']}: {len(overlaps)} signals")

            # Notifications + auto-buy
            for signal in overlaps:
                key = f"{signal['conditionId']}|{signal['outcome']}"
                with _lock:
                    already = key in _state["triggered"]

                if not already and signal["holderCount"] >= CFG["notify_threshold"]:
                    notify(signal)

                if (not already
                        and CFG["autobuy_enabled"]
                        and signal["holderCount"] >= CFG["autobuy_min_overlap"]
                        and signal["curPrice"] <= CFG["autobuy_max_price"]):
                    success = attempt_autobuy(signal)
                    if success:
                        with _lock:
                            _state["triggered"].add(key)

            last_positions_poll = now

        time.sleep(10)


# ─── Entry point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("Starting Polymarket Smart Money Tracker on Railway...")
    try:
        t = threading.Thread(target=polling_loop, daemon=True)
        t.start()
        log.info("Polling thread started OK")
    except Exception as e:
        log.error(f"Thread failed to start: {e}")
    app.run(host="0.0.0.0", port=CFG["port"])
