# 🤖 Polymarket Trading Bot

A copy-trading bot for Polymarket's BTC/ETH 5-minute and 15-minute up/down markets. Tracks profitable traders and mirrors their positions. Controlled entirely via Telegram, deployed on Railway.

---

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Telegram Bot   │────▶│  Trading Engine  │────▶│ Polymarket CLOB │
│  (Control UI)   │◀────│  (bot_engine.py) │◀────│    (Orders)     │
└─────────────────┘     └──────────────────┘     └─────────────────┘
                                │
                         ┌──────▼──────┐
                         │  SQLite DB  │
                         │ (positions, │
                         │  traders)   │
                         └─────────────┘
```

**How it works:**
1. Every 5 minutes: fetches active BTC/ETH short-term markets from Polymarket Gamma API
2. Every 60 seconds: scans recent trades across those markets, finds traders with ≥60% win rate and ≥20 trades
3. If copy trading is enabled: detects when top traders open a new position, mirrors it with your configured stake
4. Monitors positions and notifies you on close with PnL

---

## Prerequisites

- Python 3.11+
- A Polymarket account with USDC on Polygon
- Polymarket API credentials (from polymarket.com/settings)
- A Telegram bot token (from @BotFather)
- A Railway account (railway.app)

---

## Setup

### 1. Clone & install

```bash
git clone <your-repo>
cd polymarket-bot
pip install -r requirements.txt
```

### 2. Get Polymarket API Keys

1. Go to [polymarket.com](https://polymarket.com) → Settings → API
2. Create a new API key → copy Key, Secret, Passphrase
3. Export your wallet private key (the EVM wallet connected to Polymarket)
   - MetaMask: Account Details → Export Private Key

### 3. Create Telegram Bot

1. Message [@BotFather](https://t.me/BotFather) on Telegram
2. `/newbot` → follow prompts → copy the token
3. Get your Telegram user ID: message [@userinfobot](https://t.me/userinfobot)

### 4. Configure environment

```bash
cp .env.example .env
```

Edit `.env`:
```env
TELEGRAM_BOT_TOKEN=1234567890:ABCdef...
ALLOWED_CHAT_IDS=your_telegram_user_id

POLYMARKET_API_KEY=...
POLYMARKET_API_SECRET=...
POLYMARKET_API_PASSPHRASE=...
POLYMARKET_PRIVATE_KEY=0x...
POLYMARKET_WALLET_ADDRESS=0x...

DEFAULT_STAKE_USDC=5.0
MAX_STAKE_USDC=50.0
MAX_OPEN_POSITIONS=5
MIN_TRADER_WIN_RATE=0.60
MIN_TRADER_TRADES=20
```

### 5. Run locally

```bash
python main.py
```

---

## Deploy to Railway

### Option A: GitHub (recommended)

1. Push this repo to GitHub
2. Go to [railway.app](https://railway.app) → New Project → Deploy from GitHub
3. Select your repo
4. Go to **Variables** tab → add all your `.env` values
5. Railway auto-detects Python and deploys

### Option B: Railway CLI

```bash
npm install -g @railway/cli
railway login
railway init
railway up
railway variables set TELEGRAM_BOT_TOKEN=... POLYMARKET_API_KEY=... # etc
```

### Persistent storage on Railway

The bot uses SQLite by default. For Railway, add a **Volume**:
1. Railway dashboard → your service → **Volumes** → Add Volume
2. Mount path: `/data`
3. Update your env: `DATABASE_URL=sqlite+aiosqlite:////data/bot_data.db`

---

## Telegram Commands

| Command | Description |
|---------|-------------|
| `/start` | Main control panel with buttons |
| `/stats` | Performance stats (PnL, win rate, open positions) |
| `/positions` | List all open positions with unrealized PnL |
| `/traders` | Top tracked profitable traders |
| `/markets` | Active BTC/ETH markets being watched |
| `/stake <amount>` | Set stake per trade (e.g. `/stake 10`) |
| `/toggle` | Toggle copy trading on/off |
| `/startbot` | Start the trading engine |
| `/stopbot` | Stop the trading engine |
| `/help` | Show all commands |

---

## Configuration Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `DEFAULT_STAKE_USDC` | `5.0` | USDC per copy trade |
| `MAX_STAKE_USDC` | `50.0` | Max allowed stake (safety limit) |
| `MAX_OPEN_POSITIONS` | `5` | Max concurrent positions |
| `MIN_TRADER_WIN_RATE` | `0.60` | Min win rate to follow (60%) |
| `MIN_TRADER_TRADES` | `20` | Min historical trades to qualify |
| `TOP_TRADERS_TO_TRACK` | `10` | Number of traders to monitor |
| `MARKET_REFRESH_INTERVAL` | `300` | Market refresh in seconds |
| `COPY_TRADE_ENABLED` | `false` | Auto-enable copy trading on start |
| `AUTO_START_BOT` | `true` | Auto-start engine on launch |

---

## Risk Warning

⚠️ **This bot trades real money. Polymarket prediction markets are highly volatile, especially short-term crypto markets.**

- Start with a small stake ($1–$5) to test
- Monitor closely for the first few days
- Past profitable traders don't guarantee future returns
- Keep `MAX_OPEN_POSITIONS` and `MAX_STAKE_USDC` conservative
- Never fund more than you're willing to lose

---

## File Structure

```
polymarket-bot/
├── main.py              # Entry point
├── src/
│   ├── bot_engine.py    # Core trading logic
│   ├── polymarket.py    # Polymarket API client
│   ├── telegram_bot.py  # Telegram interface
│   └── database.py      # SQLAlchemy models
├── requirements.txt
├── railway.toml         # Railway deployment config
├── .env.example
└── README.md
```

---

## Troubleshooting

**Bot doesn't find markets:** Polymarket's short-term crypto markets come and go. Check polymarket.com to confirm 5min/15min markets are currently active.

**Orders failing:** Ensure your wallet has USDC on Polygon, not Ethereum mainnet. Also verify API credentials are correct in the CLOB settings.

**Telegram not responding:** Check `ALLOWED_CHAT_IDS` — your Telegram user ID must be in the list.
