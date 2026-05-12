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

    async def get_market_trades(self, condition_id: str, limit: int = 500) -> list[dict]:
        """
        Fetch recent trades for a market via the CLOB API.
        Uses conditionId (0x...) as the market identifier.
        Response fields: maker, taker, price, size, side, timestamp
        """
        try:
            # CLOB /trades endpoint - uses conditionId
            params = {"market": condition_id, "limit": limit}
            async with self.session.get(f"{CLOB_API}/trades", params=params) as resp:
                logger.debug(f"CLOB /trades status={resp.status} market={condition_id[:16]}...")
                if resp.status == 200:
                    data = await resp.json()
                    trades = data if isinstance(data, list) else data.get("data", [])
                    logger.debug(f"  Got {len(trades)} trades, sample keys: {list(trades[0].keys()) if trades else 'none'}")
                    return trades
                else:
                    body = await resp.text()
                    logger.warning(f"CLOB /trades {resp.status}: {body[:100]}")
        except Exception as e:
            logger.error(f"Error fetching trades for {condition_id}: {e}")
        return []

    async def get_trader_pnl(self, address: str) -> dict:
        """
        Fetch a trader's P&L stats from the Data API /activity endpoint.
        Computes win_rate and total_pnl from their resolved trades.
        """
        try:
            # Data API /activity - returns per-trade history for a user
            params = {
                "user": address.lower(),
                "type": "TRADE",
                "limit": 500,
            }
            async with self.session.get(f"{DATA_API}/activity", params=params) as resp:
                logger.debug(f"DATA /activity status={resp.status} user={address[:10]}...")
                if resp.status != 200:
                    body = await resp.text()
                    logger.debug(f"  Activity {resp.status}: {body[:80]}")
                    return {}
                data = await resp.json()
                trades = data if isinstance(data, list) else data.get("data", [])

            if not trades:
                return {}

            # Log field names from first trade so we can debug
            if trades:
                logger.debug(f"  Activity fields: {list(trades[0].keys())}")

            # Calculate win rate from resolved positions
            # Each trade has: side (BUY/SELL), price, size, outcome (Win/Lose or similar)
            wins = losses = 0
            total_pnl = 0.0
            for t in trades:
                # Try multiple possible field names from different API versions
                outcome = (t.get("outcome") or t.get("result") or "").upper()
                cash_pnl = float(t.get("cashPnl") or t.get("pnl") or t.get("profit") or 0)
                total_pnl += cash_pnl
                if outcome in ("WIN", "YES", "UP", "CORRECT"):
                    wins += 1
                elif outcome in ("LOSE", "LOSS", "NO", "DOWN", "INCORRECT"):
                    losses += 1

            total = wins + losses
            win_rate = wins / total if total > 0 else 0.0
            return {
                "win_rate": win_rate,
                "total_trades": len(trades),
                "resolved_trades": total,
                "total_pnl": total_pnl,
            }
        except Exception as e:
            logger.error(f"Error fetching trader activity {address}: {e}")
        return {}

    async def get_profitable_traders(
        self,
        market_ids: list[str],
        condition_ids: list[str],
        min_win_rate: float = 0.60,
        min_trades: int = 10,
        top_n: int = 10,
    ) -> list[dict]:
        """
        Scan recent trades across our 4 markets to find profitable traders.

        Step 1: collect unique wallet addresses from trade history
        Step 2: fetch activity history for top traders by volume
        Step 3: filter by win rate and min trades, score by win_rate * pnl
        """
        trader_volume: dict[str, float] = {}
        trader_trade_count: dict[str, int] = {}

        # Fetch trades using conditionId (required by CLOB API)
        ids_to_scan = condition_ids[:4] if condition_ids else market_ids[:4]
        logger.info(f"Scanning trades for {len(ids_to_scan)} markets...")

        for cid in ids_to_scan:
            trades = await self.get_market_trades(cid)
            logger.info(f"  conditionId {cid[:16]}...: {len(trades)} trades")

            for trade in trades:
                # CLOB API uses "maker" and "taker" fields
                for addr_field in ["maker", "taker", "proxyWallet", "trader"]:
                    addr = (trade.get(addr_field) or "").lower()
                    if addr and addr != self.wallet_address and len(addr) == 42:
                        size = float(trade.get("size") or trade.get("amount") or 0)
                        trader_volume[addr] = trader_volume.get(addr, 0) + size
                        trader_trade_count[addr] = trader_trade_count.get(addr, 0) + 1

        logger.info(f"Found {len(trader_volume)} unique traders across all markets")

        if not trader_volume:
            logger.warning("No traders found — trades API may use different field names or markets are too new")
            return []

        # Take top 30 by volume for profile enrichment
        top_addrs = sorted(trader_volume, key=lambda a: trader_volume[a], reverse=True)[:30]
        logger.info(f"Fetching activity for top {len(top_addrs)} traders by volume...")

        enriched = []
        for addr in top_addrs:
            stats = await self.get_trader_pnl(addr)
            if not stats:
                continue

            wr = stats.get("win_rate", 0)
            resolved = stats.get("resolved_trades", 0)
            pnl = stats.get("total_pnl", 0)
            n_trades = stats.get("total_trades", trader_trade_count.get(addr, 0))

            logger.debug(
                f"  {addr[:10]}... trades={n_trades} resolved={resolved} "
                f"wr={wr:.1%} pnl=${pnl:.2f}"
            )

            if resolved < min_trades or wr < min_win_rate:
                continue

            score = wr * max(pnl, 0.01)
            enriched.append({
                "address": addr,
                "win_rate": wr,
                "total_trades": n_trades,
                "total_pnl": pnl,
                "score": score,
            })

        enriched.sort(key=lambda t: t["score"], reverse=True)
        logger.info(f"Found {len(enriched)} traders meeting criteria (wr>={min_win_rate:.0%}, trades>={min_trades})")
        return enriched[:top_n]

    async def get_trader_open_positions(self, address: str, condition_ids: list[str]) -> list[dict]:
        """Check if a tracked trader has open positions in our markets."""
        positions = []
        try:
            params = {"user": address.lower(), "sizeThreshold": "0.01"}
            async with self.session.get(f"{DATA_API}/positions", params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    all_pos = data if isinstance(data, list) else data.get("data", [])
                    for pos in all_pos:
                        cid = pos.get("conditionId") or pos.get("market") or ""
                        if cid in condition_ids:
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
