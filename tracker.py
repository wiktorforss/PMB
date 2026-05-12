"""
PMB — Polymarket Brain | tracker.py
BTC/ETH Candle Copy Trader — v3 ground-up rewrite

CONFIRMED API FACTS (verified against live responses):
  - Market discovery : gamma-api.polymarket.com/events?seriesSlug=btc-up-or-down-5m&closed=false
  - Trades per market: data-api.polymarket.com/trades?conditionId=0x...&limit=100
  - User positions   : data-api.polymarket.com/positions?user=0x...
  - Closed markets   : gamma-api.polymarket.com/events?seriesSlug=...&closed=true
  - Trade fields     : proxyWallet, side (BUY/SELL), outcome ("Up"/"Down"), size, price
  - Position fields  : proxyWallet, conditionId, outcome, size, avgPrice, currentValue
  - Resolution       : outcomePrices ["1","0"] or ["0","1"] when settled (>=0.99)

STRATEGY:
  1. Every 5 min: fetch last N closed BTC+ETH candle markets, score each
     trade as win/loss, build a per-wallet ledger.
  2. Rank wallets by win rate (min MIN_SCORED_TRADES trades, min MIN_WIN_RATE).
  3. Every 60s: fetch recent trades on open candle markets.
     If a top-ranked wallet traded BUY within MAX_SIGNAL_AGE_S seconds, signal.
  4. Alert via Telegram. Auto-buy if enabled.

WHY TRADES NOT POSITIONS:
  5min candles expire before a positions poll cycle can catch them.
  Watching trades on open markets is the only reliable real-time approach.
"""

import os, json, time, logging, threading
from datetime import datetime, timezone
from flask import Flask, jsonify, request
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("PMB")

# ── Config ────────────────────────────────────────────────────────────────────
GAMMA_API  = "https://gamma-api.polymarket.com"
DATA_API   = "https://data-api.polymarket.com"
CLOB_HOST  = "https://clob.polymarket.com"

POLL_INTERVAL_S   = int(os.getenv("POLL_INTERVAL_S",   "60"))
LEDGER_REFRESH_S  = int(os.getenv("LEDGER_REFRESH_S",  "300"))
MARKET_REFRESH_S  = int(os.getenv("MARKET_REFRESH_S",  "120"))
REQUEST_DELAY_S   = float(os.getenv("REQUEST_DELAY_S", "0.4"))

MIN_WIN_RATE      = float(os.getenv("MIN_WIN_RATE",     "0.60"))
MIN_SCORED_TRADES = int(os.getenv("MIN_SCORED_TRADES",  "3"))
MIN_TRADE_USD     = float(os.getenv("MIN_TRADE_USD",    "20"))
TOP_TRADERS_N     = int(os.getenv("TOP_TRADERS_N",      "30"))
TRADES_PER_MKT    = int(os.getenv("TRADES_PER_MKT",    "100"))
CLOSED_MKTS_N     = int(os.getenv("CLOSED_MKTS_N",     "50"))
NOTIFY_THRESHOLD  = int(os.getenv("NOTIFY_THRESHOLD",  "1"))
MAX_SIGNAL_AGE_S  = int(os.getenv("MAX_SIGNAL_AGE_S",  "120"))

TELEGRAM_TOKEN    = os.getenv("TELEGRAM_TOKEN",  "")
TELEGRAM_CHAT_ID  = os.getenv("TELEGRAM_CHAT_ID", "")

AUTOBUY_ENABLED   = os.getenv("AUTOBUY_ENABLED",  "false").lower() == "true"
AUTOBUY_MIN_TOP   = int(os.getenv("AUTOBUY_MIN_TOP",   "2"))
AUTOBUY_MAX_PRICE = float(os.getenv("AUTOBUY_MAX_PRICE","0.70"))
AUTOBUY_SIZE_USD  = float(os.getenv("AUTOBUY_SIZE_USD", "10"))
AUTOBUY_DAILY_MAX = float(os.getenv("AUTOBUY_DAILY_MAX","50"))
PRIVATE_KEY       = os.getenv("POLYMARKET_PRIVATE_KEY", "")
FUNDER            = os.getenv("POLYMARKET_FUNDER", "")
SIG_TYPE          = int(os.getenv("POLYMARKET_SIG_TYPE", "0"))
PORT              = int(os.getenv("PORT", "8080"))

# ── Confirmed series slugs (verified live) ────────────────────────────────────
CANDLE_SERIES = [
    {"slug": "btc-up-or-down-5m",  "asset": "BTC", "window": 5},
    {"slug": "btc-up-or-down-15m", "asset": "BTC", "window": 15},
    {"slug": "eth-up-or-down-5m",  "asset": "ETH", "window": 5},
    {"slug": "eth-up-or-down-15m", "asset": "ETH", "window": 15},
]

# ── Persistence ───────────────────────────────────────────────────────────────
F_LEDGER    = "ledger.json"
F_TRIGGERED = "triggered.json"
F_BOUGHT    = "bought.json"
F_SETTINGS  = "settings.json"

def _load(path, default):
    try:
        with open(path) as f: return json.load(f)
    except Exception: return default

def _save(path, data):
    try:
        with open(path, "w") as f: json.dump(data, f, indent=2)
    except Exception as e: log.warning(f"Save failed {path}: {e}")

# ── Shared state ──────────────────────────────────────────────────────────────
_lock = threading.Lock()
state = {
    "open_markets": {},    # conditionId -> market info dict
    "ledger":       {},    # proxyWallet -> {wins, losses, vol_usd}
    "top_traders":  set(), # qualifying proxyWallet addresses
    "scored_cids":  set(), # conditionIds already scored (avoid double-count)
    "signals":      [],    # current signal list
    "triggered":    _load(F_TRIGGERED, {}),
    "bought":       _load(F_BOUGHT, {"markets": {}, "spent": 0.0, "day": ""}),
    "settings":     _load(F_SETTINGS, {}),
    "stats": {
        "open_markets": 0, "ledger_size": 0, "top_traders": 0,
        "signals_today": 0, "last_scan": None, "last_ledger": None,
    },
}

# ── HTTP ──────────────────────────────────────────────────────────────────────
_sess = requests.Session()
_sess.headers["User-Agent"] = "PMB/3.0"

def _get(url, params=None):
    for attempt in range(3):
        try:
            r = _sess.get(url, params=params, timeout=12)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == 2:
                log.warning(f"GET failed {url} {params}: {e}")
                return None
            time.sleep(1.5)

def _tg(msg: str, chat_id: str = None):
    if not TELEGRAM_TOKEN: return
    cid = chat_id or TELEGRAM_CHAT_ID
    if not cid: return
    try:
        _sess.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": cid, "text": msg, "parse_mode": "HTML"},
            timeout=10,
        )
    except Exception as e: log.warning(f"Telegram error: {e}")

# ── Step 1: Open markets ──────────────────────────────────────────────────────
def discover_open_markets() -> dict:
    found = {}
    for s in CANDLE_SERIES:
        data = _get(f"{GAMMA_API}/events", params={
            "seriesSlug": s["slug"], "closed": "false",
            "order": "id", "ascending": "false", "limit": 5,
        })
        time.sleep(REQUEST_DELAY_S)
        if not data: continue
        for ev in (data if isinstance(data, list) else []):
            for m in ev.get("markets", []):
                cid = m.get("conditionId")
                if not cid: continue
                try:
                    outcomes  = json.loads(m.get("outcomes",  '["Up","Down"]'))
                    token_ids = json.loads(m.get("clobTokenIds", "[]"))
                except Exception:
                    outcomes, token_ids = ["Up","Down"], []
                found[cid] = {
                    "conditionId": cid,
                    "title":    ev.get("title", m.get("question","")),
                    "asset":    s["asset"],  "window":   s["window"],
                    "outcomes": outcomes,    "token_ids": token_ids,
                    "end_date": m.get("endDate",""),
                }
    log.info(f"Open markets: {len(found)}")
    return found

# ── Step 2: Ledger from closed markets ────────────────────────────────────────
def _resolved_outcome(market: dict):
    try:
        outcomes = json.loads(market["outcomes"]) if isinstance(market.get("outcomes"), str) else market.get("outcomes", ["Up","Down"])
        prices   = json.loads(market["outcomePrices"]) if isinstance(market.get("outcomePrices"), str) else market.get("outcomePrices", [])
        if not prices: return None
        pf = [float(p) for p in prices]
        mx = max(pf)
        if mx >= 0.99:
            return outcomes[pf.index(mx)]
    except Exception: pass
    return None

def rebuild_ledger():
    ledger  = state["ledger"]
    scored  = state["scored_cids"]
    new_n   = 0

    for s in CANDLE_SERIES:
        data = _get(f"{GAMMA_API}/events", params={
            "seriesSlug": s["slug"], "closed": "true",
            "order": "id", "ascending": "false", "limit": CLOSED_MKTS_N,
        })
        time.sleep(REQUEST_DELAY_S)
        if not data: continue

        for ev in (data if isinstance(data, list) else []):
            for m in ev.get("markets", []):
                cid = m.get("conditionId")
                if not cid or cid in scored: continue

                winner = _resolved_outcome(m)
                if not winner:
                    scored.add(cid)   # mark so we don't keep retrying unresolved
                    continue

                trades = _get(f"{DATA_API}/trades", params={"conditionId": cid, "limit": TRADES_PER_MKT})
                time.sleep(REQUEST_DELAY_S)
                trade_list = (trades if isinstance(trades, list) else (trades or {}).get("data", [])) if trades else []

                for t in trade_list:
                    wallet  = t.get("proxyWallet")
                    side    = (t.get("side") or "").upper()
                    outcome = (t.get("outcome") or "").strip()
                    usd     = float(t.get("size") or 0) * float(t.get("price") or 0)
                    if not wallet or side != "BUY" or usd < MIN_TRADE_USD: continue
                    if wallet not in ledger:
                        ledger[wallet] = {"wins": 0, "losses": 0, "vol_usd": 0.0}
                    ledger[wallet]["vol_usd"] += usd
                    if outcome == winner: ledger[wallet]["wins"]   += 1
                    else:                 ledger[wallet]["losses"] += 1

                scored.add(cid)
                new_n += 1

    # Recompute top traders
    top = set()
    for w, e in ledger.items():
        total = e["wins"] + e["losses"]
        if total >= MIN_SCORED_TRADES and (e["wins"] / total) >= MIN_WIN_RATE:
            top.add(w)
    state["top_traders"] = top

    _save(F_LEDGER, {"ledger": ledger, "scored": list(scored)})
    state["stats"].update({"ledger_size": len(ledger), "top_traders": len(top),
                           "last_ledger": datetime.now(timezone.utc).isoformat()})
    log.info(f"Ledger: {len(ledger)} wallets, {len(scored)} scored (+{new_n}), {len(top)} top traders")

# ── Step 3: Scan trades on open markets ───────────────────────────────────────
def scan_signals() -> list:
    open_markets = state["open_markets"]
    top_traders  = state["top_traders"]
    if not open_markets or not top_traders: return []

    now_ts  = time.time()
    seen    = {}   # "cid:outcome" -> signal dict
    signals = []

    for cid, mkt in open_markets.items():
        trades = _get(f"{DATA_API}/trades", params={"conditionId": cid, "limit": 50})
        time.sleep(REQUEST_DELAY_S)
        if not trades: continue
        trade_list = trades if isinstance(trades, list) else trades.get("data", [])

        for t in trade_list:
            wallet    = t.get("proxyWallet")
            side      = (t.get("side") or "").upper()
            outcome   = (t.get("outcome") or "").strip()
            usd       = float(t.get("size") or 0) * float(t.get("price") or 0)
            price     = float(t.get("price") or 0)
            timestamp = int(t.get("timestamp") or 0)

            if not wallet or side != "BUY": continue
            if wallet not in top_traders: continue
            if usd < MIN_TRADE_USD: continue
            if now_ts - timestamp > MAX_SIGNAL_AGE_S: continue

            key = f"{cid}:{outcome}"
            if key not in seen:
                seen[key] = {
                    "conditionId": cid, "title": mkt["title"],
                    "asset": mkt["asset"], "window": mkt["window"],
                    "outcome": outcome, "traders": [],
                    "end_date": mkt["end_date"], "token_ids": mkt["token_ids"],
                    "outcomes": mkt["outcomes"],
                }
                signals.append(seen[key])

            e     = state["ledger"].get(wallet, {})
            total = e.get("wins",0) + e.get("losses",0)
            seen[key]["traders"].append({
                "wallet":   wallet, "price": price, "usd": round(usd,2),
                "win_rate": round(e["wins"]/total,3) if total>0 else None,
                "ts":       timestamp,
            })

    return [s for s in signals if len(s["traders"]) >= NOTIFY_THRESHOLD]

# ── Alerts ────────────────────────────────────────────────────────────────────
def _fire_alerts(signals: list):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if state["bought"].get("day") != today:
        state["bought"].update({"spent": 0.0, "day": today, "markets": {}})
        _save(F_BOUGHT, state["bought"])

    for sig in signals:
        key = f"{sig['conditionId']}:{sig['outcome']}"
        if state["triggered"].get(key,"")[:10] == today: continue

        traders = sig["traders"]
        avg_px  = sum(t["price"] for t in traders) / len(traders)
        wr_str  = ", ".join(f"{t['win_rate']:.0%}" if t["win_rate"] is not None else "?" for t in traders)

        msg = (
            f"🕯 <b>PMB Signal</b> — {sig['asset']} {sig['window']}min\n"
            f"<b>{sig['outcome']}</b> @ {avg_px:.2f}\n"
            f"📋 {sig['title']}\n"
            f"👥 {len(traders)} top trader(s) | WR: {wr_str}\n"
            f"🔗 https://polymarket.com/event/{sig['conditionId']}"
        )
        _tg(msg)
        state["triggered"][key] = datetime.now(timezone.utc).isoformat()
        _save(F_TRIGGERED, state["triggered"])
        state["stats"]["signals_today"] += 1
        log.info(f"SIGNAL {sig['asset']} {sig['window']}min {sig['outcome']} @ {avg_px:.2f} ({len(traders)} traders)")
        _try_autobuy(sig, avg_px)

def _try_autobuy(sig, avg_px):
    s = state["settings"]
    if not s.get("autobuy_enabled", AUTOBUY_ENABLED): return
    if len(sig["traders"]) < s.get("autobuy_min_top", AUTOBUY_MIN_TOP): return
    if avg_px > s.get("autobuy_max_price", AUTOBUY_MAX_PRICE): return
    size = s.get("autobuy_size_usd", AUTOBUY_SIZE_USD)
    if state["bought"]["spent"] + size > s.get("autobuy_daily_max", AUTOBUY_DAILY_MAX): return
    if sig["conditionId"] in state["bought"]["markets"]: return
    _place_order(sig["conditionId"], sig["outcome"], sig["outcomes"], sig["token_ids"], avg_px, size)

def _place_order(cid, outcome, outcomes, token_ids, price, size_usd):
    if not PRIVATE_KEY: log.warning("No PRIVATE_KEY — skipping"); return
    try:
        from py_clob_client_v2 import ClobClient, OrderArgs, OrderType, Side
        l1    = ClobClient(host=CLOB_HOST, chain_id=137, key=PRIVATE_KEY)
        creds = l1.create_or_derive_api_key(nonce=0) if SIG_TYPE==1 else l1.create_or_derive_api_key()
        kw    = {"funder": FUNDER, "signature_type": 1} if SIG_TYPE==1 and FUNDER else {}
        client = ClobClient(host=CLOB_HOST, chain_id=137, key=PRIVATE_KEY, creds=creds, **kw)
        token_id = token_ids[outcomes.index(outcome)]
        order = client.create_order(OrderArgs(
            token_id=token_id, price=price, size=round(size_usd/price,4),
            side=Side.BUY, order_type=OrderType.GTC,
        ))
        resp = client.post_order(order)
        log.info(f"Order: {outcome} @ {price} ${size_usd} → {resp}")
        state["bought"]["markets"][cid] = {"outcome": outcome, "price": price, "usd": size_usd,
                                            "ts": datetime.now(timezone.utc).isoformat()}
        state["bought"]["spent"] = round(state["bought"]["spent"] + size_usd, 2)
        _save(F_BOUGHT, state["bought"])
        _tg(f"✅ <b>Bought</b> {outcome} @ {price:.2f} (${size_usd})")
    except Exception as e:
        log.error(f"Order error: {e}"); _tg(f"⚠️ Autobuy error: {e}")

# ── Main loop ─────────────────────────────────────────────────────────────────
def main_loop():
    last_ledger = last_markets = 0
    while True:
        if state["settings"].get("paused"):
            time.sleep(POLL_INTERVAL_S); continue
        now = time.time()

        if now - last_ledger > LEDGER_REFRESH_S:
            try: rebuild_ledger()
            except Exception as e: log.error(f"Ledger error: {e}")
            last_ledger = time.time()

        if now - last_markets > MARKET_REFRESH_S:
            try:
                mkts = discover_open_markets()
                with _lock:
                    state["open_markets"] = mkts
                    state["stats"]["open_markets"] = len(mkts)
            except Exception as e: log.error(f"Market error: {e}")
            last_markets = time.time()

        try:
            sigs = scan_signals()
            with _lock: state["signals"] = sigs
            _fire_alerts(sigs)
        except Exception as e: log.error(f"Scan error: {e}")

        state["stats"]["last_scan"] = datetime.now(timezone.utc).isoformat()
        log.info(
            f"Scan | open={state['stats']['open_markets']} "
            f"ledger={state['stats']['ledger_size']} "
            f"top={state['stats']['top_traders']} "
            f"signals={len(state['signals'])}"
        )
        time.sleep(POLL_INTERVAL_S)

# ── Telegram commands ─────────────────────────────────────────────────────────
def _handle_command(text: str, chat_id: str):
    parts = text.strip().split()
    cmd   = parts[0].lower() if parts else ""
    s     = state["settings"]

    def reply(msg): _tg(msg, chat_id)

    if cmd == "/status":
        st = state["stats"]
        reply(
            f"📊 <b>PMB Status</b>\n"
            f"Open markets:  {st['open_markets']}\n"
            f"Ledger:        {st['ledger_size']} wallets\n"
            f"Top traders:   {st['top_traders']}\n"
            f"Signals today: {st['signals_today']}\n"
            f"Spent today:   ${state['bought'].get('spent',0):.2f}\n"
            f"Autobuy:       {'ON' if s.get('autobuy_enabled', AUTOBUY_ENABLED) else 'OFF'}\n"
            f"Last scan:     {st['last_scan']}\n"
            f"Last ledger:   {st['last_ledger']}"
        )
    elif cmd == "/toptraders":
        ledger = state["ledger"]
        rows = sorted(
            [(w, e) for w, e in ledger.items() if w in state["top_traders"]],
            key=lambda x: x[1]["wins"]/(x[1]["wins"]+x[1]["losses"]) if (x[1]["wins"]+x[1]["losses"])>0 else 0,
            reverse=True
        )[:10]
        if not rows: reply("No top traders yet."); return
        lines = ["👑 <b>Top Candle Traders</b>"]
        for i,(w,e) in enumerate(rows,1):
            total = e["wins"]+e["losses"]
            wr = e["wins"]/total if total>0 else 0
            lines.append(f"{i}. <code>{w[:10]}…</code> {wr:.0%} ({e['wins']}W/{e['losses']}L) ${e['vol_usd']:.0f}")
        reply("\n".join(lines))
    elif cmd == "/markets":
        mkts = list(state["open_markets"].values())
        if not mkts: reply("No open markets."); return
        lines = ["🕯 <b>Open Candle Markets</b>"]
        for m in mkts[:12]:
            lines.append(f"• {m['asset']} {m['window']}min — {m['title'][-45:]}")
        reply("\n".join(lines))
    elif cmd == "/signals":
        sigs = state["signals"]
        if not sigs: reply("No live signals right now."); return
        lines = ["🚨 <b>Live Signals</b>"]
        for sig in sigs[:5]:
            avg = sum(t["price"] for t in sig["traders"])/len(sig["traders"])
            lines.append(f"• {sig['asset']} {sig['window']}min → <b>{sig['outcome']}</b> @ {avg:.2f} ({len(sig['traders'])} traders)")
        reply("\n".join(lines))
    elif cmd == "/ledger":
        reply(f"📒 Wallets: {len(state['ledger'])} | Top: {len(state['top_traders'])} | Scored markets: {len(state['scored_cids'])}")
    elif cmd == "/autobuy":
        val = parts[1].lower() if len(parts)>1 else ""
        s["autobuy_enabled"] = (val=="on")
        _save(F_SETTINGS,s); reply(f"Autobuy {'✅ ON' if s['autobuy_enabled'] else '❌ OFF'}")
    elif cmd == "/setsize" and len(parts)>1:
        s["autobuy_size_usd"]=float(parts[1]); _save(F_SETTINGS,s); reply(f"Size → ${s['autobuy_size_usd']}")
    elif cmd == "/setdailylimit" and len(parts)>1:
        s["autobuy_daily_max"]=float(parts[1]); _save(F_SETTINGS,s); reply(f"Daily limit → ${s['autobuy_daily_max']}")
    elif cmd == "/setmaxprice" and len(parts)>1:
        v=float(parts[1]); s["autobuy_max_price"]=v/100 if v>1 else v
        _save(F_SETTINGS,s); reply(f"Max price → {s['autobuy_max_price']:.2f}")
    elif cmd == "/setmintraders" and len(parts)>1:
        s["autobuy_min_top"]=int(parts[1]); _save(F_SETTINGS,s); reply(f"Min traders → {s['autobuy_min_top']}")
    elif cmd == "/setwinrate" and len(parts)>1:
        v=float(parts[1]); s["min_win_rate"]=v/100 if v>1 else v
        _save(F_SETTINGS,s); reply(f"Min win rate → {s['min_win_rate']:.0%}")
    elif cmd == "/pause":
        s["paused"]=True;  _save(F_SETTINGS,s); reply("⏸ Paused")
    elif cmd == "/resume":
        s["paused"]=False; _save(F_SETTINGS,s); reply("▶️ Resumed")
    elif cmd == "/help":
        reply(
            "📖 <b>PMB Commands</b>\n\n"
            "/status — overview\n/toptraders — ranked wallets\n"
            "/markets — open candle markets\n/signals — live signals\n/ledger — ledger stats\n\n"
            "/autobuy on|off\n/setsize [usd]\n/setdailylimit [usd]\n"
            "/setmaxprice [0-100]\n/setmintraders [n]\n/setwinrate [0-100]\n"
            "/pause | /resume"
        )
    else:
        reply("Unknown command — try /help")

# ── Flask ─────────────────────────────────────────────────────────────────────
app = Flask(__name__)

@app.route("/")
def r_health(): return jsonify({"status":"ok","stats":state["stats"]})

@app.route("/signals")
def r_signals():
    out=[]
    for sig in state["signals"]:
        traders=sig["traders"]
        avg=sum(t["price"] for t in traders)/max(len(traders),1)
        out.append({"conditionId":sig["conditionId"],"title":sig["title"],"asset":sig["asset"],
                    "window":sig["window"],"outcome":sig["outcome"],"trader_count":len(traders),
                    "avg_price":round(avg,3),"traders":traders})
    return jsonify(out)

@app.route("/markets")
def r_markets(): return jsonify(list(state["open_markets"].values()))

@app.route("/traders")
def r_traders():
    ledger=state["ledger"]; top=state["top_traders"]
    rows=[]
    for w in top:
        e=ledger.get(w,{}); total=e.get("wins",0)+e.get("losses",0)
        rows.append({"wallet":w,"wins":e.get("wins",0),"losses":e.get("losses",0),
                     "win_rate":round(e["wins"]/total,3) if total>0 else None,
                     "vol_usd":round(e.get("vol_usd",0),2)})
    rows.sort(key=lambda x:x["win_rate"] or 0,reverse=True)
    return jsonify(rows)

@app.route("/stats")
def r_stats(): return jsonify({**state["stats"],"spent_today":state["bought"].get("spent",0)})

@app.route("/telegram", methods=["POST"])
def r_telegram():
    try:
        update  = request.get_json(force=True, silent=True) or {}
        msg     = update.get("message") or update.get("edited_message") or {}
        text    = msg.get("text","").strip()
        chat_id = str(msg.get("chat",{}).get("id",""))
        if text and chat_id:
            if not TELEGRAM_CHAT_ID or chat_id == TELEGRAM_CHAT_ID:
                log.info(f"Telegram: {text!r} from {chat_id}")
                threading.Thread(target=_handle_command, args=(text,chat_id), daemon=True).start()
    except Exception as e: log.warning(f"Webhook error: {e}")
    return "",200

# ── Telegram setup ────────────────────────────────────────────────────────────
def setup_telegram():
    domain = os.getenv("RAILWAY_PUBLIC_DOMAIN","")
    if not TELEGRAM_TOKEN: return
    if domain:
        url = f"https://{domain}/telegram"
        try:
            r = _sess.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setWebhook",
                json={"url": url, "allowed_updates": ["message"]},
                timeout=10,
            )
            result = r.json()
            if result.get("ok"):
                log.info(f"Telegram webhook → {url}")
                return
            log.warning(f"Webhook failed: {result}")
        except Exception as e:
            log.warning(f"Webhook error: {e}")
    # Fallback: long-poll (delete any existing webhook first)
    log.info("Telegram: using long-poll")
    try:
        _sess.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/deleteWebhook", timeout=10)
    except Exception: pass
    threading.Thread(target=_longpoll, daemon=True).start()

def _longpoll():
    offset = None
    while True:
        try:
            params = {"timeout": 30, "allowed_updates": ["message"]}
            if offset: params["offset"] = offset
            r = _sess.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates",
                          params=params, timeout=35)
            for u in r.json().get("result",[]):
                offset = u["update_id"]+1
                msg    = u.get("message",{})
                text   = msg.get("text","").strip()
                cid    = str(msg.get("chat",{}).get("id",""))
                if text and cid:
                    if not TELEGRAM_CHAT_ID or cid == TELEGRAM_CHAT_ID:
                        _handle_command(text, cid)
        except Exception as e: log.warning(f"Long-poll error: {e}")
        time.sleep(1)

# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("PMB v3 starting…")

    saved = _load(F_LEDGER, {})
    state["ledger"]      = saved.get("ledger", {})
    state["scored_cids"] = set(saved.get("scored", []))

    # Rebuild top traders from loaded ledger
    top = set()
    for w, e in state["ledger"].items():
        total = e.get("wins",0)+e.get("losses",0)
        if total >= MIN_SCORED_TRADES and (e["wins"]/total) >= MIN_WIN_RATE:
            top.add(w)
    state["top_traders"] = top
    state["stats"].update({"ledger_size": len(state["ledger"]), "top_traders": len(top)})
    log.info(f"Loaded: {len(state['ledger'])} wallets, {len(state['scored_cids'])} scored, {len(top)} top traders")

    threading.Thread(target=main_loop, daemon=True).start()
    setup_telegram()
    app.run(host="0.0.0.0", port=PORT)
