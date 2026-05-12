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
    # ──────────────────────────────────────────────
    # ──────────────────────────────────────────────
    # Trader Analytics
    # ──────────────────────────────────────────────

    async def get_market_trades(self, condition_ids: list[str], limit: int = 500) -> list[dict]:
        """
        GET https://data-api.polymarket.com/trades
        Params: market (comma-separated conditionIds), takerOnly=false, limit
        Response: [{proxyWallet, side, price, size, conditionId, outcome, ...}]
        Public endpoint, no auth required.
        """
        try:
            params = {
                "market": ",".join(condition_ids),
                "takerOnly": "false",
                "limit": limit,
            }
            async with self.session.get(f"{DATA_API}/trades", params=params) as resp:
                logger.info(f"DATA /trades status={resp.status} markets={len(condition_ids)}")
                if resp.status == 200:
                    data = await resp.json()
                    trades = data if isinstance(data, list) else data.get("data", [])
                    if trades:
                        logger.info(f"  Got {len(trades)} trades, keys: {list(trades[0].keys())[:8]}")
                    else:
                        logger.info("  Got 0 trades (markets may be too new)")
                    return trades
                else:
                    body = await resp.text()
                    logger.warning(f"DATA /trades {resp.status}: {body[:150]}")
        except Exception as e:
            logger.error(f"Error fetching trades: {e}")
        return []

    async def get_trader_activity(self, address: str, limit: int = 200) -> list[dict]:
        """
        GET https://data-api.polymarket.com/trades?user={address}
        Returns all trades for a specific user across all markets.
        Used to compute their overall win rate and PnL.
        """
        try:
            params = {"user": address.lower(), "takerOnly": "false", "limit": limit}
            async with self.session.get(f"{DATA_API}/trades", params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data if isinstance(data, list) else data.get("data", [])
                else:
                    body = await resp.text()
                    logger.debug(f"Activity {resp.status} for {address[:10]}: {body[:80]}")
        except Exception as e:
            logger.error(f"Error fetching activity for {address}: {e}")
        return []

    async def get_profitable_traders(
        self,
        market_ids: list[str],
        condition_ids: list[str],
        min_win_rate: float = 0.55,
        min_trades: int = 5,
        top_n: int = 10,
    ) -> list[dict]:
        """
        Find profitable traders in our 4 markets.
        1. Fetch all trades for these markets via DATA /trades
        2. Rank traders by volume
        3. Fetch each trader's full history to compute win rate
        """
        ids = condition_ids[:4] if condition_ids else market_ids[:4]
        logger.info(f"Fetching market trades for {len(ids)} conditionIds...")

        all_trades = await self.get_market_trades(ids)
        if not all_trades:
            logger.warning("No trades returned — markets may be too new or conditionIds wrong")
            return []

        # Aggregate by trader
        trader_volume: dict[str, float] = {}
        trader_count: dict[str, int] = {}
        for trade in all_trades:
            addr = (trade.get("proxyWallet") or "").lower()
            if not addr or addr == self.wallet_address or len(addr) != 42:
                continue
            size = float(trade.get("size") or 0)
            trader_volume[addr] = trader_volume.get(addr, 0) + size
            trader_count[addr] = trader_count.get(addr, 0) + 1

        logger.info(f"Found {len(trader_volume)} unique traders")
        if not trader_volume:
            return []

        # Enrich top traders
        top_addrs = sorted(trader_volume, key=lambda a: trader_volume[a], reverse=True)[:30]
        logger.info(f"Fetching full history for top {len(top_addrs)} traders...")

        enriched = []
        logged_keys = False
        for addr in top_addrs:
            history = await self.get_trader_activity(addr)
            if not history:
                continue

            if not logged_keys:
                logger.info(f"Trade history keys: {list(history[0].keys())[:10]}")
                logged_keys = True

            # Win rate: a resolved trade has outcome set (Up/Down/Yes/No)
            # PnL: not directly in trade records, use price * size heuristic
            # For Up outcome BUY at price p -> win if resolved Up (price->1), lose if Down (price->0)
            wins = losses = 0
            for t in history:
                outcome = (t.get("outcome") or "").strip()
                side = (t.get("side") or "").upper()
                price = float(t.get("price") or 0)
                # A resolved trade will have outcome "Up" or "Down"
                # If we bought Up and outcome is Up -> win
                # If we bought Up and outcome is Down -> loss
                if outcome and side == "BUY":
                    if outcome.lower() in ("up", "yes", "win"):
                        wins += 1
                    elif outcome.lower() in ("down", "no", "lose", "loss"):
                        losses += 1

            resolved = wins + losses
            win_rate = wins / resolved if resolved > 0 else 0.0
            # Estimate PnL: winners pay out ~1 USDC/share, losers lose stake
            pnl_estimate = wins * 0.5 - losses * 0.5  # rough

            logger.debug(
                f"  {addr[:10]}... in_market={trader_count.get(addr,0)} "
                f"history={len(history)} wins={wins} losses={losses} wr={win_rate:.1%}"
            )

            if resolved < min_trades or win_rate < min_win_rate:
                continue

            enriched.append({
                "address": addr,
                "win_rate": win_rate,
                "total_trades": len(history),
                "total_pnl": pnl_estimate,
                "score": win_rate * max(pnl_estimate, 0.01),
            })

        enriched.sort(key=lambda t: t["score"], reverse=True)
        logger.info(f"Qualified traders: {len(enriched)}")
        return enriched[:top_n]

    async def get_trader_open_positions(self, address: str, condition_ids: list[str]) -> list[dict]:
        """
        GET https://data-api.polymarket.com/positions?user={address}
        Returns trader's current open positions, filtered to our 4 markets.
        Response fields: conditionId, outcome, size, price, currentPrice, ...
        """
        positions = []
        try:
            params = {"user": address.lower(), "sizeThreshold": "0.01"}
            async with self.session.get(f"{DATA_API}/positions", params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    all_pos = data if isinstance(data, list) else data.get("data", [])
                    if all_pos:
                        logger.debug(f"Positions keys: {list(all_pos[0].keys())[:10]}")
                    for pos in all_pos:
                        cid = pos.get("conditionId") or pos.get("market") or ""
                        size = float(pos.get("size") or 0)
                        if cid in condition_ids and size > 0:
                            positions.append(pos)
                else:
                    body = await resp.text()
                    logger.debug(f"Positions {resp.status} for {address[:10]}: {body[:80]}")
        except Exception as e:
            logger.error(f"Error fetching positions for {address}: {e}")
        return positions

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
