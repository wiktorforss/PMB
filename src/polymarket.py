"""
Polymarket CLOB API client.
Handles market discovery, order placement, and trader analytics.
"""
import os
import asyncio
import aiohttp
import logging
from typing import Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
DATA_API = "https://data-api.polymarket.com"

# Known market slug patterns for BTC/ETH short-term markets
CRYPTO_MARKET_KEYWORDS = [
    "will-btc-price-be-higher",
    "will-eth-price-be-higher",
    "btc-up-or-down",
    "eth-up-or-down",
    "bitcoin-price",
    "ethereum-price",
]


class PolymarketClient:
    def __init__(self):
        self.api_key = os.getenv("POLYMARKET_API_KEY")
        self.api_secret = os.getenv("POLYMARKET_API_SECRET")
        self.passphrase = os.getenv("POLYMARKET_API_PASSPHRASE")
        self.private_key = os.getenv("POLYMARKET_PRIVATE_KEY")
        self.wallet_address = os.getenv("POLYMARKET_WALLET_ADDRESS", "").lower()
        self.session: Optional[aiohttp.ClientSession] = None
        self._clob_client = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()

    def _get_clob_client(self):
        """Lazy-init the CLOB client for order placement."""
        if self._clob_client is None:
            try:
                from py_clob_client.client import ClobClient
                from py_clob_client.clob_types import ApiCreds
                creds = ApiCreds(
                    api_key=self.api_key,
                    api_secret=self.api_secret,
                    api_passphrase=self.passphrase,
                )
                self._clob_client = ClobClient(
                    host=CLOB_API,
                    chain_id=137,       # Polygon PoS
                    key=self.private_key,
                    creds=creds,
                    signature_type=0,   # EOA (standard wallet)
                )
            except Exception as e:
                logger.error(f"Failed to init CLOB client: {e}")
        return self._clob_client

    # ──────────────────────────────────────────────
    # Market Discovery
    # ──────────────────────────────────────────────

    async def get_active_crypto_markets(self) -> list[dict]:
        """
        Fetch the 4 rolling BTC/ETH 5min & 15min markets using Unix timestamp slugs.

        Slug format: {asset}-updown-{timeframe}-{unix_timestamp}
        e.g. btc-updown-5m-1778627100, eth-updown-15m-1778626800

        The timestamp is the UTC Unix time of the window start,
        floored to 300s (5min) or 900s (15min).
        We fetch current + next window per market to handle boundary timing.
        """
        import time as _time

        now = int(_time.time())
        w5  = (now // 300) * 300   # current 5-min window start
        w15 = (now // 900) * 900   # current 15-min window start

        # (slug, asset, timeframe) for current + next window
        targets = [
            (f"btc-updown-5m-{w5}",        "BTC", "5min"),
            (f"btc-updown-5m-{w5 + 300}",  "BTC", "5min"),
            (f"btc-updown-15m-{w15}",       "BTC", "15min"),
            (f"btc-updown-15m-{w15 + 900}", "BTC", "15min"),
            (f"eth-updown-5m-{w5}",         "ETH", "5min"),
            (f"eth-updown-5m-{w5 + 300}",   "ETH", "5min"),
            (f"eth-updown-15m-{w15}",        "ETH", "15min"),
            (f"eth-updown-15m-{w15 + 900}",  "ETH", "15min"),
        ]

        markets = []
        seen_ids: set = set()

        for slug, asset, timeframe in targets:
            try:
                async with self.session.get(
                    f"{GAMMA_API}/events",
                    params={"slug": slug}
                ) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json()
                    events = data if isinstance(data, list) else [data]

                    for event in events:
                        for market in event.get("markets", []):
                            mid = market.get("id")
                            if not mid or mid in seen_ids:
                                continue
                            seen_ids.add(mid)
                            markets.append({
                                "id": mid,
                                "condition_id": market.get("conditionId"),
                                "question": market.get("question", ""),
                                "slug": slug,
                                "asset": asset,
                                "timeframe": timeframe,
                                "tokens": market.get("tokens", []),
                                "volume": float(event.get("volume") or 0),
                                "liquidity": float(event.get("liquidity") or 0),
                                "end_date": market.get("endDate"),
                            })
            except Exception as e:
                logger.warning(f"Failed to fetch {slug}: {e}")

        if markets:
            logger.info(f"Found {len(markets)} active BTC/ETH short-term markets")
        else:
            logger.warning(f"No markets found — tried slugs like btc-updown-5m-{w5}")

        return markets

    async def get_market_orderbook(self, token_id: str) -> dict:
        """Fetch current orderbook for a token."""
        try:
            async with self.session.get(f"{CLOB_API}/book", params={"token_id": token_id}) as resp:
                if resp.status == 200:
                    return await resp.json()
        except Exception as e:
            logger.error(f"Orderbook fetch error: {e}")
        return {}

    async def get_market_price(self, token_id: str) -> Optional[float]:
        """Get current mid-price for a token."""
        book = await self.get_market_orderbook(token_id)
        try:
            bids = book.get("bids", [])
            asks = book.get("asks", [])
            if bids and asks:
                best_bid = float(bids[0]["price"])
                best_ask = float(asks[0]["price"])
                return (best_bid + best_ask) / 2
            elif bids:
                return float(bids[0]["price"])
            elif asks:
                return float(asks[0]["price"])
        except (KeyError, IndexError, ValueError):
            pass
        return None

    # ──────────────────────────────────────────────
    # Trader Analytics
    # ──────────────────────────────────────────────

    async def get_market_trades(self, market_id: str, limit: int = 200) -> list[dict]:
        """Fetch recent trades for a market."""
        try:
            params = {"market": market_id, "limit": limit}
            async with self.session.get(f"{DATA_API}/trades", params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data if isinstance(data, list) else data.get("data", [])
        except Exception as e:
            logger.error(f"Error fetching trades for {market_id}: {e}")
        return []

    async def get_trader_profile(self, address: str) -> dict:
        """
        Fetch a trader's historical performance from Polymarket data API.
        Returns win_rate, total_trades, total_pnl, etc.
        """
        try:
            async with self.session.get(
                f"{DATA_API}/portfolio",
                params={"user": address.lower()}
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
        except Exception as e:
            logger.error(f"Error fetching trader profile {address}: {e}")
        return {}

    async def get_profitable_traders(
        self,
        market_ids: list[str],
        min_win_rate: float = 0.60,
        min_trades: int = 20,
        top_n: int = 10,
    ) -> list[dict]:
        """
        Analyze recent market trades to find profitable traders.
        Scores traders by win rate × total PnL.
        """
        trader_stats: dict[str, dict] = {}

        for market_id in market_ids:
            trades = await self.get_market_trades(market_id)
            for trade in trades:
                maker = (trade.get("maker_address") or "").lower()
                taker = (trade.get("taker_address") or "").lower()

                for addr in [maker, taker]:
                    if not addr or addr == self.wallet_address:
                        continue
                    if addr not in trader_stats:
                        trader_stats[addr] = {
                            "address": addr,
                            "markets_seen": set(),
                            "trade_count": 0,
                        }
                    trader_stats[addr]["markets_seen"].add(market_id)
                    trader_stats[addr]["trade_count"] += 1

        # Fetch full profiles for traders with enough activity
        enriched = []
        candidates = [
            t for t in trader_stats.values()
            if t["trade_count"] >= max(3, min_trades // 5)
        ]
        # Limit API calls
        candidates = sorted(candidates, key=lambda x: x["trade_count"], reverse=True)[:50]

        await asyncio.gather(*[
            self._enrich_trader(t, enriched, min_win_rate, min_trades)
            for t in candidates
        ])

        # Sort by score
        enriched.sort(key=lambda t: t.get("score", 0), reverse=True)
        return enriched[:top_n]

    async def _enrich_trader(self, trader: dict, result_list: list, min_wr: float, min_trades: int):
        profile = await self.get_trader_profile(trader["address"])
        if not profile:
            return

        total_trades = profile.get("tradesCount", 0) or trader["trade_count"]
        profit = float(profile.get("profit", 0) or 0)
        win_rate = float(profile.get("winRate", 0) or 0)

        if total_trades < min_trades or win_rate < min_wr:
            return

        score = win_rate * max(profit, 0.01)
        result_list.append({
            "address": trader["address"],
            "win_rate": win_rate,
            "total_trades": total_trades,
            "total_pnl": profit,
            "score": score,
        })

    async def get_trader_open_positions(self, address: str, market_ids: list[str]) -> list[dict]:
        """Check if a tracked trader has open positions in our markets."""
        positions = []
        try:
            async with self.session.get(
                f"{DATA_API}/positions",
                params={"user": address.lower(), "active": "true"}
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    all_positions = data if isinstance(data, list) else data.get("data", [])
                    for pos in all_positions:
                        if pos.get("market") in market_ids:
                            positions.append(pos)
        except Exception as e:
            logger.error(f"Error fetching positions for {address}: {e}")
        return positions

    # ──────────────────────────────────────────────
    # Order Placement
    # ──────────────────────────────────────────────

    async def place_market_order(
        self,
        token_id: str,
        side: str,           # "BUY" or "SELL"
        amount_usdc: float,
        price: float,
    ) -> Optional[str]:
        """
        Place a market order via CLOB. Returns order ID or None.
        side: BUY = buying Yes shares, SELL = selling
        """
        try:
            client = self._get_clob_client()
            if not client:
                logger.error("CLOB client not initialized")
                return None

            from py_clob_client.clob_types import MarketOrderArgs, OrderType
            from py_clob_client.order_builder.constants import BUY, SELL

            clob_side = BUY if side == "BUY" else SELL

            order_args = MarketOrderArgs(
                token_id=token_id,
                amount=amount_usdc,
                side=clob_side,
                price=price,
                order_type=OrderType.FOK,
            )
            signed_order = client.create_market_order(order_args)
            response = client.post_order(signed_order, OrderType.FOK)

            order_id = response.get("orderID") or response.get("id")
            logger.info(f"Order placed: {order_id} | {side} {amount_usdc} USDC @ {price}")
            return order_id

        except Exception as e:
            logger.error(f"Order placement failed: {e}", exc_info=True)
            return None

    async def get_balance(self) -> float:
        """Get USDC balance on Polymarket."""
        try:
            client = self._get_clob_client()
            if client:
                from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
                params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
                result = client.get_balance_allowance(params)
                # Returns balance in USDC (6 decimals on Polygon)
                raw = result.get("balance", 0)
                return float(raw) / 1e6
        except Exception as e:
            logger.error(f"Balance check failed: {e}")
        return 0.0
