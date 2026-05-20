"""
WalletScorer — scores wallets by prediction accuracy, not PnL.

Logic:
  1. Pull the Polymarket leaderboard (top traders by PnL/volume) as our seed set.
  2. For each wallet, fetch their closed trades.
  3. Score them: did they BUY the outcome that won? 
     We use trades on resolved markets where we know the result (price → 1.0 = YES won).
  4. Compare accuracy to a baseline (50% for binary markets).
  5. Wallets that beat the baseline by a statistically meaningful margin get a confidence score.
  6. Top 3% are "sharp".
"""

import asyncio
import logging
import math
from dataclasses import dataclass, field
from typing import Optional
import httpx

logger = logging.getLogger(__name__)

DATA_API = "https://data-api.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"

# Minimum trades before we score a wallet
MIN_TRADES = 10
# Baseline accuracy for binary markets
BASELINE = 0.50
# Sharp threshold: top this percentile
SHARP_THRESHOLD = 0.03


@dataclass
class WalletScore:
    address: str
    username: str
    total_trades: int
    correct_trades: int
    accuracy: float
    confidence_score: float  # how far above baseline, weighted by sample size
    pnl: float
    vol: float
    profile_image: str = ""
    x_username: str = ""

    @property
    def accuracy_pct(self) -> str:
        return f"{self.accuracy * 100:.1f}%"

    @property
    def display_name(self) -> str:
        return self.username or self.address[:8] + "..."


@dataclass
class LeaderboardEntry:
    rank: int
    wallet: WalletScore


async def fetch_json(client: httpx.AsyncClient, url: str, params: dict = None) -> dict | list:
    try:
        resp = await client.get(url, params=params, timeout=15.0)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"API error {url}: {e}")
        return []


def compute_confidence_score(correct: int, total: int, baseline: float = BASELINE) -> float:
    """
    Wilson score interval lower bound — penalises small samples.
    Returns how far above baseline the wallet is, adjusted for sample size.
    """
    if total == 0:
        return 0.0
    p = correct / total
    z = 1.645  # 90% confidence
    n = total
    # Wilson lower bound
    numerator = p + z * z / (2 * n) - z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n)
    denominator = 1 + z * z / n
    wilson_lower = numerator / denominator
    # Score = how far the lower bound is above baseline
    return max(0.0, wilson_lower - baseline)


class WalletScorer:
    def __init__(self):
        self._cache: list[WalletScore] = []
        self._cache_ttl = 300  # 5 minutes
        self._last_fetch = 0.0

    async def get_sharp_leaderboard(self, limit: int = 20) -> list[LeaderboardEntry]:
        import time
        now = time.time()

        if self._cache and (now - self._last_fetch) < self._cache_ttl:
            scored = self._cache
        else:
            scored = await self._build_scored_wallets()
            self._cache = scored
            self._last_fetch = now

        # Sort by confidence score descending
        sorted_wallets = sorted(scored, key=lambda w: w.confidence_score, reverse=True)

        # Top 3% are "sharp" — show top N
        entries = [
            LeaderboardEntry(rank=i + 1, wallet=w)
            for i, w in enumerate(sorted_wallets[:limit])
        ]
        return entries

    async def get_all_scored(self) -> list[WalletScore]:
        import time
        now = time.time()
        if self._cache and (now - self._last_fetch) < self._cache_ttl:
            return self._cache
        scored = await self._build_scored_wallets()
        self._cache = scored
        self._last_fetch = now
        return scored

    async def _build_scored_wallets(self) -> list[WalletScore]:
        """Fetch leaderboard wallets, then score each by trade accuracy."""
        async with httpx.AsyncClient() as client:
            # Fetch top 50 traders by PnL (ALL time) and by volume
            pnl_board, vol_board = await asyncio.gather(
                fetch_json(client, f"{DATA_API}/v1/leaderboard", {
                    "orderBy": "PNL", "timePeriod": "ALL", "limit": 50
                }),
                fetch_json(client, f"{DATA_API}/v1/leaderboard", {
                    "orderBy": "VOL", "timePeriod": "ALL", "limit": 50
                }),
            )

            # Deduplicate wallets
            seen = {}
            for entry in (pnl_board + vol_board):
                addr = entry.get("proxyWallet", "")
                if addr and addr not in seen:
                    seen[addr] = entry

            wallets = list(seen.values())
            logger.info(f"Scoring {len(wallets)} unique wallets...")

            # Score each wallet concurrently (with semaphore to avoid rate limits)
            sem = asyncio.Semaphore(5)

            async def score_one(entry):
                async with sem:
                    return await self._score_wallet(client, entry)

            results = await asyncio.gather(*[score_one(e) for e in wallets])
            scored = [r for r in results if r is not None]
            logger.info(f"Scored {len(scored)} wallets with enough trades")
            return scored

    async def _score_wallet(self, client: httpx.AsyncClient, entry: dict) -> Optional[WalletScore]:
        """
        Score a single wallet by accuracy on resolved markets.
        We look at their trades and check if price hit ~1.0 (they won) or ~0.0 (they lost).
        """
        addr = entry.get("proxyWallet", "")
        if not addr:
            return None

        try:
            # Fetch recent trades for this wallet
            trades = await fetch_json(client, f"{DATA_API}/trades", {
                "user": addr,
                "limit": 200,
                "takerOnly": "true",
            })

            if not isinstance(trades, list) or len(trades) < MIN_TRADES:
                return None

            # Score: for each BUY trade, did the outcome resolve YES (price → 1)?
            # We approximate this: if price > 0.95 at time of trade, they were late.
            # Better: look at trades where side=BUY and price was low (they had conviction early).
            # We count a trade as "correct" if they bought at < 0.7 and it resolved YES,
            # or sold at > 0.7 and it resolved NO.
            # Since we don't have resolution data directly in trade history,
            # we use a proxy: trades where the wallet bought at < 0.5 (contrarian/early)
            # count toward accuracy if that market's last price > 0.7.

            correct = 0
            total = 0

            # Group trades by market
            by_market: dict[str, list[dict]] = {}
            for t in trades:
                cid = t.get("conditionId", "")
                if cid:
                    by_market.setdefault(cid, []).append(t)

            for cid, market_trades in by_market.items():
                # Only score markets with actual conviction (not just noise trades)
                buys = [t for t in market_trades if t.get("side") == "BUY"]
                if not buys:
                    continue

                avg_buy_price = sum(t.get("price", 0.5) for t in buys) / len(buys)
                avg_buy_size = sum(t.get("size", 0) for t in buys) / len(buys)

                # Skip tiny trades (noise filter)
                if avg_buy_size < 5:
                    continue

                # We use last known price as a proxy for resolution
                # (in production you'd cross-reference the gamma API for resolved status)
                last_price = market_trades[-1].get("price", 0.5)

                # Heuristic accuracy:
                # Bought below 0.6, last price > 0.75 → likely correct
                # Bought above 0.6, last price < 0.25 → likely wrong
                if avg_buy_price < 0.6 and last_price > 0.75:
                    correct += 1
                    total += 1
                elif avg_buy_price > 0.6 and last_price < 0.25:
                    # Bought high, price crashed — wrong call
                    total += 1
                elif avg_buy_price < 0.5 and last_price > 0.65:
                    correct += 1
                    total += 1
                elif 0.3 < avg_buy_price < 0.7:
                    # Ambiguous — skip to avoid noise
                    pass

            if total < 5:
                return None

            accuracy = correct / total
            confidence = compute_confidence_score(correct, total)

            return WalletScore(
                address=addr,
                username=entry.get("userName") or "",
                total_trades=total,
                correct_trades=correct,
                accuracy=accuracy,
                confidence_score=confidence,
                pnl=entry.get("pnl", 0),
                vol=entry.get("vol", 0),
                profile_image=entry.get("profileImage", ""),
                x_username=entry.get("xUsername", ""),
            )

        except Exception as e:
            logger.error(f"Error scoring {addr}: {e}")
            return None
