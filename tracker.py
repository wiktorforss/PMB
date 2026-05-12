"""
PMB — Polymarket Brain | tracker.py
Market-First Candle Copy Trader

Strategy:
  1. Discover open BTC/ETH 5min & 15min candle markets
  2. Pull recent trades per market to find active candle traders
  3. Score each wallet on candle-market profitability only
  4. Copy positions from top-performing candle traders in real time
"""

import os, json, time, logging, threading, re
from datetime import datetime, timezone
from collections import defaultdict
from flask import Flask, jsonify
import requests

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("PMB")

# ─── Env / Config ───────────────────────────────────────────────────────────
GAMMA_API     = "https://gamma-api.polymarket.com"   # market discovery, events, tags
DATA_API      = "https://data-api.polymarket.com"    # trades, positions, leaderboard
CLOB_HOST     = "https://clob.polymarket.com"        # order placement

POLL_INTERVAL_S      = int(os.getenv("POLL_INTERVAL_S",      "60"))    # main loop cadence
MARKET_REFRESH_S     = int(os.getenv("MARKET_REFRESH_S",     "120"))   # re-discover markets
TRADER_REFRESH_S     = int(os.getenv("TRADER_REFRESH_S",     "300"))   # re-score traders
REQUEST_DELAY_S      = float(os.getenv("REQUEST_DELAY_S",    "0.5"))

MIN_CANDLE_TRADES    = int(os.getenv("MIN_CANDLE_TRADES",    "10"))    # min history to trust
MIN_WIN_RATE         = float(os.getenv("MIN_WIN_RATE",       "0.60"))  # 60 %
MIN_POSITION_USD     = float(os.getenv("MIN_POSITION_USD",   "200"))
MAX_ENTRY_PRICE      = float(os.getenv("MAX_ENTRY_PRICE",    "0.85"))
TRADES_PER_MARKET    = int(os.getenv("TRADES_PER_MARKET",    "100"))
TOP_TRADERS_WATCH    = int(os.getenv("TOP_TRADERS_WATCH",    "20"))    # wallets to follow

NOTIFY_THRESHOLD     = int(os.getenv("NOTIFY_THRESHOLD",     "1"))     # min traders in same position
TELEGRAM_TOKEN       = os.getenv("TELEGRAM_TOKEN",  "")
TELEGRAM_CHAT_ID     = os.getenv("TELEGRAM_CHAT_ID", "")

AUTOBUY_ENABLED      = os.getenv("AUTOBUY_ENABLED",  "false").lower() == "true"
AUTOBUY_MIN_TRADERS  = int(os.getenv("AUTOBUY_MIN_TRADERS",  "2"))
AUTOBUY_MAX_PRICE    = float(os.getenv("AUTOBUY_MAX_PRICE",  "0.70"))
AUTOBUY_SIZE_USD     = float(os.getenv("AUTOBUY_SIZE_USD",   "10"))
AUTOBUY_DAILY_LIMIT  = float(os.getenv("AUTOBUY_DAILY_LIMIT","50"))
POLYMARKET_PRIVATE_KEY = os.getenv("POLYMARKET_PRIVATE_KEY", "")
POLYMARKET_FUNDER    = os.getenv("POLYMARKET_FUNDER",  "")
POLYMARKET_SIG_TYPE  = int(os.getenv("POLYMARKET_SIG_TYPE",  "0"))

PORT = int(os.getenv("PORT", "8080"))

# ─── Persistence helpers ─────────────────────────────────────────────────────
TRIGGERED_FILE   = "triggered.json"
BOUGHT_FILE      = "bought.json"
SETTINGS_FILE    = "settings.json"
LEDGER_FILE      = "trader_ledger.json"   # candle win/loss history per wallet

def _load(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return default

def _save(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

# ─── Shared State ────────────────────────────────────────────────────────────
state = {
    "active_markets":  {},   # conditionId → market_info
    "trader_ledger":   {},   # wallet → {wins, losses, volume_usd, last_seen}
    "top_traders":     [],   # ranked list of wallets
    "live_positions":  {},   # wallet → {conditionId → {outcome, size, price}}
    "signals":         {},   # conditionId → {YES/NO: [wallet, ...]}
    "triggered":       _load(TRIGGERED_FILE, {}),
    "bought":          _load(BOUGHT_FILE, {"markets": {}, "spent_today": 0.0}),
    "settings":        _load(SETTINGS_FILE, {}),
    "stats": {
        "markets_tracked": 0,
        "traders_scored":  0,
        "signals_today":   0,
        "last_scan":       None,
    },
}
state_lock = threading.Lock()

# ─── Candle Market Detection ──────────────────────────────────────────────────
_CANDLE_KEYWORDS = re.compile(
    r"(5.?min|15.?min|5-minute|15-minute)", re.IGNORECASE
)
_TIME_RANGE = re.compile(
    r"\d{1,2}:\d{2}\s*(AM|PM)\s*[-–]\s*\d{1,2}:\d{2}\s*(AM|PM)", re.IGNORECASE
)
_HOUR_SLOT  = re.compile(
    r"\d{1,2}\s*(AM|PM)\s*(ET|EST|EDT)", re.IGNORECASE
)
_UPDOWN     = re.compile(r"up or down", re.IGNORECASE)
_BTC        = re.compile(r"bitcoin|btc", re.IGNORECASE)
_ETH        = re.compile(r"ethereum|eth\b", re.IGNORECASE)

def _is_candle_market(title: str) -> bool:
    """Return True for BTC/ETH 5min or 15min candle markets."""
    if not (_BTC.search(title) or _ETH.search(title)):
        return False
    if _CANDLE_KEYWORDS.search(title):
        return True
    if _UPDOWN.search(title):
        # "Bitcoin Up or Down - May 8, 1:30PM-1:35PM ET"  → 5-min window
        if _TIME_RANGE.search(title):
            return True
        # "Bitcoin Up or Down - May 8, 1PM ET" → hourly (skip)
        if _HOUR_SLOT.search(title) and not _TIME_RANGE.search(title):
            return False
    return False

def _candle_window(title: str) -> int:
    """Return 5 or 15 based on title; default 5."""
    if re.search(r"15.?min|15-minute", title, re.IGNORECASE):
        return 15
    return 5

# ─── API helpers ──────────────────────────────────────────────────────────────
_session = requests.Session()
_session.headers.update({"User-Agent": "PMB/2.0"})

def _get(url, params=None, retries=3):
    for attempt in range(retries):
        try:
            r = _session.get(url, params=params, timeout=10)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == retries - 1:
                log.warning(f"GET failed {url}: {e}")
                return None
            time.sleep(1)

def _telegram(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        _session.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception as e:
        log.warning(f"Telegram error: {e}")

# ─── Step 1: Discover Active Candle Markets ───────────────────────────────────
def discover_candle_markets():
    """
    Fetch open markets from Polymarket and filter to BTC/ETH 5/15min candles.
    Uses /markets endpoint with tag-based filtering where possible.
    """
    found = {}
    for tag in ["bitcoin", "ethereum"]:
        data = _get(f"{DATA_API}/markets", params={
            "tag": tag, "closed": "false", "limit": 200
        })
        if not data:
            continue
        markets = data if isinstance(data, list) else data.get("data", [])
        for m in markets:
            title = m.get("question") or m.get("title") or ""
            cid   = m.get("conditionId") or m.get("condition_id") or m.get("id")
            if not cid:
                continue
            if _is_candle_market(title):
                found[cid] = {
                    "conditionId": cid,
                    "title":       title,
                    "window_min":  _candle_window(title),
                    "asset":       "BTC" if _BTC.search(title) else "ETH",
                    "outcomes":    m.get("outcomes", ["YES", "NO"]),
                    "token_ids":   m.get("clobTokenIds") or m.get("token_ids") or [],
                    "end_date":    m.get("endDate") or m.get("end_date"),
                }
        time.sleep(REQUEST_DELAY_S)

    log.info(f"Discovered {len(found)} active candle markets")
    return found

# ─── Step 2: Pull Recent Trades Per Market ────────────────────────────────────
def fetch_market_trades(condition_id: str) -> list:
    data = _get(f"{DATA_API}/trades", params={
        "market": condition_id,
        "limit":  TRADES_PER_MARKET,
    })
    if not data:
        return []
    return data if isinstance(data, list) else data.get("data", [])

# ─── Step 3: Score Traders on Candle Markets ──────────────────────────────────
def update_trader_ledger(markets: dict):
    """
    For every known candle market with a resolved outcome,
    mark each trader's trade as a win or loss and track volume.
    """
    ledger = state["trader_ledger"]

    for cid, mkt in markets.items():
        trades = fetch_market_trades(cid)
        time.sleep(REQUEST_DELAY_S)

        # Determine resolved outcome if available
        resolved = mkt.get("resolved_outcome")  # set by market refresh

        for t in trades:
            wallet  = t.get("maker") or t.get("user") or t.get("trader")
            outcome = t.get("outcome") or t.get("side")    # "YES"/"NO"
            size    = float(t.get("size") or t.get("amount") or 0)
            price   = float(t.get("price") or 0)
            usd_val = size * price

            if not wallet or usd_val < 1:
                continue

            if wallet not in ledger:
                ledger[wallet] = {
                    "wins": 0, "losses": 0,
                    "volume_usd": 0.0,
                    "markets_traded": set(),
                    "last_seen": None,
                }

            entry = ledger[wallet]
            entry["volume_usd"] += usd_val
            entry["markets_traded"].add(cid)
            entry["last_seen"] = datetime.now(timezone.utc).isoformat()

            # Score win/loss only for resolved markets
            if resolved and outcome:
                if outcome.upper() == resolved.upper():
                    entry["wins"] += 1
                else:
                    entry["losses"] += 1

    # Serialise sets for JSON
    for w, e in ledger.items():
        if isinstance(e.get("markets_traded"), set):
            e["markets_traded"] = list(e["markets_traded"])

    _save(LEDGER_FILE, ledger)
    log.info(f"Ledger updated: {len(ledger)} traders tracked")

def rank_traders() -> list:
    """
    Return wallets sorted by win-rate, filtered by minimum trade count.
    Wallets with no resolved trades yet are ranked by volume (potential).
    """
    ledger = state["trader_ledger"]
    scored = []
    for wallet, e in ledger.items():
        total = e["wins"] + e["losses"]
        win_rate = (e["wins"] / total) if total > 0 else None
        if total > 0 and total < MIN_CANDLE_TRADES:
            continue   # not enough history
        scored.append({
            "wallet":    wallet,
            "win_rate":  win_rate,
            "wins":      e["wins"],
            "losses":    e["losses"],
            "total":     total,
            "volume_usd": e["volume_usd"],
            "last_seen": e.get("last_seen"),
        })

    # Sort: wallets with resolved history first (by win_rate), then by volume
    def _sort_key(x):
        if x["win_rate"] is not None:
            return (1, x["win_rate"], x["volume_usd"])
        return (0, 0, x["volume_usd"])

    ranked = sorted(scored, key=_sort_key, reverse=True)
    qualified = [r for r in ranked if r["win_rate"] is None or r["win_rate"] >= MIN_WIN_RATE]
    log.info(f"Ranked {len(ranked)} traders; {len(qualified)} qualified (≥{MIN_WIN_RATE:.0%} win rate)")
    return qualified[:TOP_TRADERS_WATCH]

# ─── Step 4: Watch Live Positions of Top Traders ─────────────────────────────
def fetch_wallet_positions(wallet: str) -> dict:
    data = _get(f"{DATA_API}/positions", params={"user": wallet})
    if not data:
        return {}
    positions = data if isinstance(data, list) else data.get("data", [])
    result = {}
    for p in positions:
        cid     = p.get("conditionId") or p.get("condition_id") or p.get("market")
        outcome = p.get("outcome") or p.get("side")
        size    = float(p.get("size") or p.get("amount") or 0)
        price   = float(p.get("avgPrice") or p.get("price") or 0)
        usd     = size * price
        if cid and usd >= MIN_POSITION_USD:
            result[cid] = {"outcome": outcome, "size_usd": usd, "price": price}
    return result

def scan_top_trader_positions():
    """
    For each top trader, fetch their open positions.
    Only flag positions that are in our tracked candle markets.
    """
    top_traders  = state["top_traders"]
    active_mkts  = state["active_markets"]
    signals      = defaultdict(lambda: defaultdict(list))   # cid → outcome → [wallets]

    for trader in top_traders:
        wallet = trader["wallet"]
        positions = fetch_wallet_positions(wallet)
        time.sleep(REQUEST_DELAY_S)

        for cid, pos in positions.items():
            if cid not in active_mkts:
                continue   # not a candle market we track
            outcome = (pos.get("outcome") or "").upper()
            price   = pos.get("price", 0)
            if outcome and price <= MAX_ENTRY_PRICE:
                signals[cid][outcome].append({
                    "wallet":    wallet,
                    "win_rate":  trader.get("win_rate"),
                    "size_usd":  pos["size_usd"],
                    "price":     price,
                })

    return signals

# ─── Alerts & Auto-buy ────────────────────────────────────────────────────────
def today_key():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def _already_triggered(cid, outcome):
    key = f"{cid}:{outcome}:{today_key()}"
    return key in state["triggered"]

def _mark_triggered(cid, outcome):
    key = f"{cid}:{outcome}:{today_key()}"
    state["triggered"][key] = datetime.now(timezone.utc).isoformat()
    _save(TRIGGERED_FILE, state["triggered"])

def send_signal_alert(cid, outcome, holders, mkt_info):
    title   = mkt_info.get("title", cid)
    asset   = mkt_info.get("asset", "")
    window  = mkt_info.get("window_min", 5)
    prices  = [h["price"] for h in holders]
    avg_px  = sum(prices) / len(prices) if prices else 0
    wr_list = [f"{h['win_rate']:.0%}" if h["win_rate"] else "?" for h in holders]

    msg = (
        f"🕯️ <b>PMB Candle Signal</b>\n"
        f"<b>{asset} {window}min</b> → <b>{outcome}</b>\n"
        f"📋 {title}\n"
        f"👥 {len(holders)} top trader(s) holding {outcome}\n"
        f"💰 Avg price: {avg_px:.2f}\n"
        f"📊 Win rates: {', '.join(wr_list)}\n"
        f"🔗 https://polymarket.com/event/{cid}"
    )
    _telegram(msg)
    log.info(f"SIGNAL: {asset} {window}min {outcome} @ {avg_px:.2f} ({len(holders)} traders)")
    state["stats"]["signals_today"] += 1

def attempt_autobuy(cid, outcome, holders, mkt_info):
    if not AUTOBUY_ENABLED:
        return
    settings = state["settings"]
    min_t    = settings.get("autobuy_min_traders", AUTOBUY_MIN_TRADERS)
    max_px   = settings.get("autobuy_max_price",   AUTOBUY_MAX_PRICE)
    size     = settings.get("autobuy_size_usd",    AUTOBUY_SIZE_USD)
    daily    = settings.get("autobuy_daily_limit", AUTOBUY_DAILY_LIMIT)

    if len(holders) < min_t:
        return
    avg_px = sum(h["price"] for h in holders) / len(holders)
    if avg_px > max_px:
        return
    if state["bought"]["spent_today"] + size > daily:
        log.info("Daily auto-buy limit reached")
        return
    if cid in state["bought"]["markets"]:
        return   # already bought this market today

    _execute_buy(cid, outcome, mkt_info, size, avg_px)

def _execute_buy(cid, outcome, mkt_info, size_usd, price):
    """Place a buy order via py_clob_client_v2."""
    if not POLYMARKET_PRIVATE_KEY:
        log.warning("No private key — skipping auto-buy")
        return
    try:
        from py_clob_client_v2 import ClobClient, ApiCreds, OrderArgs, OrderType, Side
        l1  = ClobClient(host=CLOB_HOST, chain_id=137, key=POLYMARKET_PRIVATE_KEY)
        if POLYMARKET_SIG_TYPE == 1 and POLYMARKET_FUNDER:
            creds = l1.create_or_derive_api_key(nonce=0)
            client = ClobClient(
                host=CLOB_HOST, chain_id=137, key=POLYMARKET_PRIVATE_KEY,
                creds=creds, funder=POLYMARKET_FUNDER, signature_type=1,
            )
        else:
            creds  = l1.create_or_derive_api_key()
            client = ClobClient(host=CLOB_HOST, chain_id=137, key=POLYMARKET_PRIVATE_KEY, creds=creds)

        token_ids = mkt_info.get("token_ids", [])
        outcomes  = mkt_info.get("outcomes", ["YES", "NO"])
        try:
            token_idx = outcomes.index(outcome)
            token_id  = token_ids[token_idx]
        except (ValueError, IndexError):
            log.warning(f"Cannot resolve token_id for {outcome} in {cid}")
            return

        size_shares = round(size_usd / price, 4)
        order = client.create_order(OrderArgs(
            token_id   = token_id,
            price      = price,
            size       = size_shares,
            side       = Side.BUY,
            order_type = OrderType.GTC,
        ))
        resp = client.post_order(order)
        log.info(f"Auto-buy placed: {outcome} @ {price} size={size_shares:.4f} → {resp}")

        state["bought"]["markets"][cid] = {
            "outcome": outcome, "price": price,
            "size_usd": size_usd, "ts": datetime.now(timezone.utc).isoformat(),
        }
        state["bought"]["spent_today"] = round(state["bought"]["spent_today"] + size_usd, 2)
        _save(BOUGHT_FILE, state["bought"])

        _telegram(
            f"✅ <b>Auto-buy executed</b>\n"
            f"{mkt_info.get('asset','')} {mkt_info.get('window_min',5)}min → {outcome}\n"
            f"Price: {price:.2f} | Size: ${size_usd:.2f}"
        )
    except Exception as e:
        log.error(f"Auto-buy error: {e}")
        _telegram(f"⚠️ Auto-buy error: {e}")

# ─── Main Polling Loop ────────────────────────────────────────────────────────
def main_loop():
    last_market_refresh = 0
    last_trader_refresh = 0

    while True:
        now = time.time()

        # Refresh candle market list
        if now - last_market_refresh > MARKET_REFRESH_S:
            with state_lock:
                state["active_markets"] = discover_candle_markets()
                state["stats"]["markets_tracked"] = len(state["active_markets"])
            last_market_refresh = now

        # Update trader ledger & re-rank
        if now - last_trader_refresh > TRADER_REFRESH_S:
            with state_lock:
                update_trader_ledger(state["active_markets"])
                state["top_traders"] = rank_traders()
                state["stats"]["traders_scored"] = len(state["top_traders"])
            last_trader_refresh = now

        # Scan positions of top traders
        signals = scan_top_trader_positions()

        with state_lock:
            state["signals"] = {k: dict(v) for k, v in signals.items()}

        # Fire alerts
        for cid, outcomes in signals.items():
            mkt_info = state["active_markets"].get(cid, {})
            for outcome, holders in outcomes.items():
                if len(holders) < NOTIFY_THRESHOLD:
                    continue
                if _already_triggered(cid, outcome):
                    continue
                send_signal_alert(cid, outcome, holders, mkt_info)
                _mark_triggered(cid, outcome)
                attempt_autobuy(cid, outcome, holders, mkt_info)

        state["stats"]["last_scan"] = datetime.now(timezone.utc).isoformat()
        log.info(
            f"Scan done | markets={state['stats']['markets_tracked']} "
            f"traders={state['stats']['traders_scored']} "
            f"signals={len(signals)}"
        )
        time.sleep(POLL_INTERVAL_S)

# ─── Telegram Bot Commands ────────────────────────────────────────────────────
def telegram_bot_loop():
    offset = None
    while True:
        try:
            params = {"timeout": 30}
            if offset:
                params["offset"] = offset
            r = _session.get(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                params=params, timeout=35,
            )
            updates = r.json().get("result", [])
            for u in updates:
                offset = u["update_id"] + 1
                msg    = u.get("message", {})
                text   = msg.get("text", "").strip()
                chat   = str(msg.get("chat", {}).get("id", ""))
                if chat != TELEGRAM_CHAT_ID:
                    continue
                _handle_command(text)
        except Exception as e:
            log.warning(f"Telegram poll error: {e}")
        time.sleep(1)

def _handle_command(text: str):
    s = state["settings"]
    parts = text.split()
    cmd   = parts[0].lower() if parts else ""

    def reply(msg):
        _telegram(msg)

    if cmd == "/status":
        st = state["stats"]
        reply(
            f"📊 <b>PMB Status</b>\n"
            f"Markets tracked: {st['markets_tracked']}\n"
            f"Traders scored:  {st['traders_scored']}\n"
            f"Signals today:   {st['signals_today']}\n"
            f"Spent today:     ${state['bought']['spent_today']:.2f}\n"
            f"Autobuy:         {'ON' if s.get('autobuy_enabled', AUTOBUY_ENABLED) else 'OFF'}\n"
            f"Last scan:       {st['last_scan']}"
        )
    elif cmd == "/autobuy":
        val = parts[1].lower() if len(parts) > 1 else ""
        s["autobuy_enabled"] = (val == "on")
        _save(SETTINGS_FILE, s); reply(f"Autobuy {'ON' if s['autobuy_enabled'] else 'OFF'}")

    elif cmd == "/setsize" and len(parts) > 1:
        s["autobuy_size_usd"] = float(parts[1])
        _save(SETTINGS_FILE, s); reply(f"Trade size set to ${s['autobuy_size_usd']}")

    elif cmd == "/setdailylimit" and len(parts) > 1:
        s["autobuy_daily_limit"] = float(parts[1])
        _save(SETTINGS_FILE, s); reply(f"Daily limit set to ${s['autobuy_daily_limit']}")

    elif cmd == "/setmaxprice" and len(parts) > 1:
        s["autobuy_max_price"] = float(parts[1]) / 100 if float(parts[1]) > 1 else float(parts[1])
        _save(SETTINGS_FILE, s); reply(f"Max price set to {s['autobuy_max_price']:.2f}")

    elif cmd == "/setmintraders" and len(parts) > 1:
        s["autobuy_min_traders"] = int(parts[1])
        _save(SETTINGS_FILE, s); reply(f"Min traders to copy set to {s['autobuy_min_traders']}")

    elif cmd == "/setwinrate" and len(parts) > 1:
        s["min_win_rate"] = float(parts[1]) / 100 if float(parts[1]) > 1 else float(parts[1])
        _save(SETTINGS_FILE, s); reply(f"Min win rate set to {s['min_win_rate']:.0%}")

    elif cmd == "/toptraders":
        top = state["top_traders"][:10]
        if not top:
            reply("No traders ranked yet."); return
        lines = ["👑 <b>Top Candle Traders</b>"]
        for i, t in enumerate(top, 1):
            wr = f"{t['win_rate']:.0%}" if t["win_rate"] else "?"
            lines.append(f"{i}. {t['wallet'][:8]}… WR={wr} ({t['wins']}W/{t['losses']}L) ${t['volume_usd']:.0f}")
        reply("\n".join(lines))

    elif cmd == "/markets":
        mkts = list(state["active_markets"].values())[:10]
        if not mkts:
            reply("No candle markets found yet."); return
        lines = ["🕯️ <b>Active Candle Markets</b>"]
        for m in mkts:
            lines.append(f"• {m['asset']} {m['window_min']}min — {m['title'][:60]}")
        reply("\n".join(lines))

    elif cmd == "/signals":
        sigs = state["signals"]
        if not sigs:
            reply("No active signals."); return
        lines = ["🚨 <b>Current Signals</b>"]
        for cid, outcomes in list(sigs.items())[:5]:
            mkt = state["active_markets"].get(cid, {})
            for outcome, holders in outcomes.items():
                lines.append(f"• {mkt.get('asset','')} {mkt.get('window_min','')}min → {outcome} ({len(holders)} traders)")
        reply("\n".join(lines))

    elif cmd == "/pause":
        s["paused"] = True;  _save(SETTINGS_FILE, s); reply("⏸ Scanning paused")
    elif cmd == "/resume":
        s["paused"] = False; _save(SETTINGS_FILE, s); reply("▶️ Scanning resumed")

    elif cmd == "/help":
        reply(
            "📖 <b>PMB Commands</b>\n"
            "/status — stats overview\n"
            "/toptraders — ranked candle traders\n"
            "/markets — active BTC/ETH candle markets\n"
            "/signals — current live signals\n"
            "/autobuy on|off — toggle auto-buy\n"
            "/setsize 10 — $ per trade\n"
            "/setdailylimit 50 — max daily spend\n"
            "/setmaxprice 70 — max entry (cents)\n"
            "/setmintraders 2 — min traders to trigger buy\n"
            "/setwinrate 60 — min win rate % to follow trader\n"
            "/pause | /resume"
        )

# ─── Flask API ────────────────────────────────────────────────────────────────
app = Flask(__name__)

@app.route("/")
def health():
    return jsonify({"status": "ok", "stats": state["stats"]})

@app.route("/signals")
def api_signals():
    out = []
    for cid, outcomes in state["signals"].items():
        mkt = state["active_markets"].get(cid, {})
        for outcome, holders in outcomes.items():
            out.append({
                "conditionId": cid,
                "title":       mkt.get("title"),
                "asset":       mkt.get("asset"),
                "window_min":  mkt.get("window_min"),
                "outcome":     outcome,
                "holder_count": len(holders),
                "holders":     holders,
                "avg_price":   round(sum(h["price"] for h in holders) / len(holders), 3) if holders else 0,
            })
    out.sort(key=lambda x: x["holder_count"], reverse=True)
    return jsonify(out)

@app.route("/markets")
def api_markets():
    return jsonify(list(state["active_markets"].values()))

@app.route("/traders")
def api_traders():
    return jsonify(state["top_traders"])

@app.route("/stats")
def api_stats():
    return jsonify({
        **state["stats"],
        "spent_today": state["bought"]["spent_today"],
        "top_trader_count": len(state["top_traders"]),
    })

# ─── Entry point ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("PMB Candle Copy Trader starting…")

    # Load persisted ledger
    state["trader_ledger"] = _load(LEDGER_FILE, {})

    # Start background threads
    threading.Thread(target=main_loop,         daemon=True).start()
    if TELEGRAM_TOKEN:
        threading.Thread(target=telegram_bot_loop, daemon=True).start()

    app.run(host="0.0.0.0", port=PORT)
