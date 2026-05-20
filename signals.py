"""
SignalEngine — surfaces markets where multiple sharp wallets independently agree.

Logic:
  1. Get the sharp wallet list from WalletScorer.
  2. For each sharp wallet, fetch their current open positions.
  3. Find markets where N+ sharp wallets are on the same side.
  4. Weight agreement by each wallet's confidence score.
  5. Return ranked signals.
"""

import asyncio
import logging
from dataclasses import dataclass
from typing import Optional
import httpx

from src.scorer import WalletScorer, WalletScore

logger = logging.getLogger(__name__)

DATA_API = "https://data-api.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com"

MIN_WALLETS_FOR_SIGNAL = 2  # At least this many sharp wallets must agree
MIN_SIGNAL_STRENGTH = 0.05  # Minimum combined confidence score


@dataclass
class Signal:
    market_title: str
    market_slug: str
    outcome: str          # "YES" or "NO"
    avg_price: float      # Average price the sharp wallets hold at
    num_wallets: int      # How many sharp wallets agree
    signal_strength: float  # Combined confidence score
    total_size: float     # Total USDC size across sharp wallets
    wallets: list[str]    # Display names of agreeing wallets
    url: str = ""


async def fetch_json(client: httpx.AsyncClient, url: str, params: dict = None) -> dict | list:
    try:
        resp = await client.get(url, params=params, timeout=15.0)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"API error {url}: {e}")
        return []


class SignalEngine:
    def __init__(self, scorer: WalletScorer):
        self.scorer = scorer
        self._cache: list[Signal] = []
        self._cache_ttl = 180  # 3 minutes
        self._last_fetch = 0.0

    async def get_signals(self, top_n: int = 10) -> list[Signal]:
        import time
        now = time.time()

        if self._cache and (now - self._last_fetch) < self._cache_ttl:
            return self._cache[:top_n]

        signals = await self._build_signals()
        self._cache = signals
        self._last_fetch = now
        return signals[:top_n]

    async def _build_signals(self) -> list[Signal]:
        # Get sharp wallets (top by confidence score)
        scored = await self.scorer.get_all_scored()
        if not scored:
            logger.warning("No scored wallets available")
            return []

        # Sort by confidence, take top 15 sharp wallets
        sharp = sorted(scored, key=lambda w: w.confidence_score, reverse=True)[:15]
        logger.info(f"Building signals from {len(sharp)} sharp wallets")

        async with httpx.AsyncClient() as client:
            sem = asyncio.Semaphore(5)

            async def get_positions(wallet: WalletScore):
                async with sem:
                    return wallet, await self._get_wallet_positions(client, wallet.address)

            results = await asyncio.gather(*[get_positions(w) for w in sharp])

        # Aggregate positions by market+outcome
        # market_key → {outcome → [(wallet, size, price, confidence)]}
        market_data: dict[str, dict] = {}

        for wallet, positions in results:
            for pos in positions:
                cid = pos.get("conditionId", "")
                outcome = pos.get("outcome", "YES")
                size = pos.get("size", 0)
                price = pos.get("avgPrice", 0.5)
                title = pos.get("title", cid[:16] + "...")
                slug = pos.get("slug", "")

                if not cid or size < 10:  # skip tiny positions
                    continue

                key = f"{cid}:{outcome}"
                if key not in market_data:
                    market_data[key] = {
                        "title": title,
                        "slug": slug,
                        "outcome": outcome,
                        "wallets": [],
                    }

                market_data[key]["wallets"].append({
                    "name": wallet.display_name,
                    "size": size,
                    "price": price,
                    "confidence": wallet.confidence_score,
                })

        # Build signals from markets with multiple sharp wallets agreeing
        signals = []
        for key, data in market_data.items():
            wallets = data["wallets"]
            if len(wallets) < MIN_WALLETS_FOR_SIGNAL:
                continue

            total_confidence = sum(w["confidence"] for w in wallets)
            if total_confidence < MIN_SIGNAL_STRENGTH:
                continue

            avg_price = sum(w["price"] * w["size"] for w in wallets) / max(
                sum(w["size"] for w in wallets), 1
            )
            total_size = sum(w["size"] for w in wallets)

            slug = data["slug"]
            signals.append(Signal(
                market_title=data["title"],
                market_slug=slug,
                outcome=data["outcome"],
                avg_price=avg_price,
                num_wallets=len(wallets),
                signal_strength=total_confidence,
                total_size=total_size,
                wallets=[w["name"] for w in wallets],
                url=f"https://polymarket.com/event/{slug}" if slug else "",
            ))

        # Sort by signal strength
        signals.sort(key=lambda s: (s.num_wallets, s.signal_strength), reverse=True)
        logger.info(f"Found {len(signals)} signals")
        return signals

    async def _get_wallet_positions(self, client: httpx.AsyncClient, address: str) -> list[dict]:
        """Fetch current open positions for a wallet."""
        try:
            data = await fetch_json(
                client,
                f"{DATA_API}/positions",
                {"user": address, "sizeThreshold": "0.01", "limit": 100}
            )
            if isinstance(data, list):
                return data
            # Some endpoints return {"data": [...]}
            if isinstance(data, dict):
                return data.get("data", [])
            return []
        except Exception as e:
            logger.error(f"Error fetching positions for {address}: {e}")
            return []
