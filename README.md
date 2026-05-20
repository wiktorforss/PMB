# PolySharp 👁 — Polymarket Wallet Intelligence Bot

A Telegram bot that scores Polymarket wallets by **accuracy** (not PnL), identifies the top 3% of "sharp" wallets, and surfaces markets where they independently converge.

---

## How It Works

### Scoring Logic
Wallets are scored like free throws:
- We pull the top 100 wallets from Polymarket's leaderboard (by PnL + volume)
- For each wallet, we fetch their trade history
- We evaluate whether their conviction trades (buying below fair value) resolved in their favour
- Accuracy is measured against a **50% baseline** (random chance on binary markets)
- A **Wilson score** adjusts for sample size — a wallet hitting 70% on 5 trades ranks lower than one hitting 68% on 100 trades
- The top ~3% who consistently beat the baseline are labelled **sharp**

### Signal Logic
A signal fires when:
1. 2+ sharp wallets are independently holding the same side of a market
2. Their combined confidence score exceeds the threshold

One wallet could be wrong. When the sharpest wallets all agree without coordinating, that's a signal.

---

## Commands

| Command | Description |
|---|---|
| `/start` | Main menu |
| `/signals` | Markets where sharp wallets converge |
| `/leaderboard` | Top wallets ranked by accuracy score |
| `/help` | Help text |

---

## Setup

### 1. Create a Telegram Bot
1. Message [@BotFather](https://t.me/botfather) on Telegram
2. Send `/newbot` and follow the prompts
3. Copy your bot token

### 2. Local Development

```bash
# Clone / download the project
cd polymarket-bot

# Install dependencies
pip install -r requirements.txt

# Set your bot token
cp .env.example .env
# Edit .env and add your TELEGRAM_BOT_TOKEN

# Run locally
python bot.py
```

> **Note:** For local development with `.env` files, install `python-dotenv` and add `from dotenv import load_dotenv; load_dotenv()` at the top of `bot.py`.

---

## Deploy to Railway

### 1. Push to GitHub
```bash
git init
git add .
git commit -m "Initial PolySharp bot"
git remote add origin https://github.com/yourusername/polymarket-bot.git
git push -u origin main
```

### 2. Create Railway Project
1. Go to [railway.app](https://railway.app) and sign in
2. Click **New Project → Deploy from GitHub repo**
3. Select your repository
4. Railway auto-detects Python via `nixpacks.toml`

### 3. Set Environment Variables
In your Railway project dashboard:
- Go to your service → **Variables**
- Add: `TELEGRAM_BOT_TOKEN` = `your_bot_token_here`

### 4. Deploy
Railway deploys automatically on every push to `main`. The bot runs 24/7.

---

## Architecture

```
bot.py              — Telegram handlers, command routing
src/
  scorer.py         — Fetches wallet trades, scores by accuracy
  signals.py        — Finds markets with sharp wallet consensus
  formatter.py      — Renders clean Telegram Markdown output
```

### Data Sources (all public, no auth required)
| Endpoint | Used For |
|---|---|
| `data-api.polymarket.com/v1/leaderboard` | Seed wallet list |
| `data-api.polymarket.com/trades` | Trade history per wallet |
| `data-api.polymarket.com/positions` | Current open positions |

### Caching
- Wallet scores are cached for **5 minutes**
- Signals are cached for **3 minutes**

This keeps API calls reasonable while staying fresh.

---

## Improving Accuracy

The current scorer uses a **price-movement heuristic** to approximate win/loss on trades (since Polymarket's public API doesn't expose resolution status directly in trade history). 

To improve this:
1. Cross-reference the `gamma-api.polymarket.com/markets` endpoint to get `resolved` status and `resolutionValue`
2. Match trade `conditionId` to resolved markets
3. Check if the wallet bought YES on a YES-resolved market (or NO on a NO-resolved market)

This would give you exact accuracy numbers instead of heuristics.

---

## Rate Limits

Polymarket's public APIs are unauthenticated but rate-limited. The bot uses:
- `asyncio.Semaphore(5)` — max 5 concurrent requests
- 5-minute cache on wallet scores
- 3-minute cache on signals

If you hit rate limits, increase cache TTLs in `scorer.py` and `signals.py`.
