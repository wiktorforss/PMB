"""
Core trading bot engine.
Polls markets, tracks profitable traders, and copies their trades.
"""
import os
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from .database import (
    AsyncSessionLocal, Position, TrackedTrader,
    TraderPosition, BotSettings
)
from .polymarket import PolymarketClient

logger = logging.getLogger(__name__)


class TradingBot:
    def __init__(self):
        self.running = False
        self.copy_trade_enabled = os.getenv("COPY_TRADE_ENABLED", "false").lower() == "true"
        self.stake_usdc = float(os.getenv("DEFAULT_STAKE_USDC", "5.0"))
        self.max_stake_usdc = float(os.getenv("MAX_STAKE_USDC", "50.0"))
        self.max_open_positions = int(os.getenv("MAX_OPEN_POSITIONS", "5"))
        self.min_win_rate = float(os.getenv("MIN_TRADER_WIN_RATE", "0.55"))
        self.min_trader_trades = int(os.getenv("MIN_TRADER_TRADES", "5"))
        self.top_traders_n = int(os.getenv("TOP_TRADERS_TO_TRACK", "10"))

        self._active_markets: list[dict] = []
        self._tracked_traders: list[dict] = []
        self._last_market_refresh: Optional[datetime] = None
        self._market_refresh_interval = int(os.getenv("MARKET_REFRESH_INTERVAL", "300"))
        self._status_callbacks: list = []  # Functions to call with status updates

        self._tasks: list[asyncio.Task] = []

    # ──────────────────────────────────────────────
    # Lifecycle
    # ──────────────────────────────────────────────

    async def start(self):
        if self.running:
            return
        self.running = True
        await self._load_settings()
        logger.info("🤖 Trading bot starting...")

        self._tasks = [
            asyncio.create_task(self._market_refresh_loop()),
            asyncio.create_task(self._trader_scan_loop()),
            asyncio.create_task(self._position_monitor_loop()),
        ]
        logger.info("✅ Bot started")

    async def stop(self):
        self.running = False
        for task in self._tasks:
            task.cancel()
        self._tasks = []
        logger.info("🛑 Bot stopped")

    # ──────────────────────────────────────────────
    # Main Loops
    # ──────────────────────────────────────────────

    async def _market_refresh_loop(self):
        """Periodically refresh the list of active markets."""
        while self.running:
            try:
                await self._refresh_markets()
            except Exception as e:
                logger.error(f"Market refresh error: {e}", exc_info=True)
            await asyncio.sleep(self._market_refresh_interval)

    async def _trader_scan_loop(self):
        """
        Two separate jobs:
        1. Every 60s  — refresh the list of profitable traders (slow, many API calls)
        2. Every 5s   — poll the live trade feed and copy instantly when a tracked
                        trader makes a new BUY (must be fast for 5-min markets)
        """
        await asyncio.sleep(15)  # Wait for market refresh first

        # Start the fast copy-watcher as a separate concurrent task
        asyncio.create_task(self._copy_watcher_loop())

        while self.running:
            try:
                await self._scan_traders()
            except Exception as e:
                logger.error(f"Trader scan error: {e}", exc_info=True)
            await asyncio.sleep(60)

    async def _copy_watcher_loop(self):
        """
        Polls DATA_API /trades every 5 seconds.
        When a tracked trader places a new BUY in one of our 4 markets,
        immediately mirrors it. Uses a seen-set of transactionHashes to
        avoid double-copying.
        """
        await asyncio.sleep(20)  # Let first trader scan complete
        logger.info("🔍 Copy watcher started (polling every 5s)")

        seen_tx: set[str] = set()
        # Pre-fill seen_tx with existing trades so we don't copy historical ones
        seen_tx = await self._init_seen_trades()

        while self.running:
            try:
                if self.copy_trade_enabled and self._tracked_traders:
                    await self._check_new_trades(seen_tx)
            except Exception as e:
                logger.error(f"Copy watcher error: {e}", exc_info=True)
            await asyncio.sleep(5)

    def _trade_key(self, trade: dict) -> str:
        """
        Unique identifier for a trade. The API has no transactionHash in the
        list endpoint, so we use timestamp + wallet + asset as a composite key.
        """
        return f"{trade.get('timestamp','')}:{trade.get('proxyWallet','')}:{trade.get('asset','')}:{trade.get('side','')}"

    async def _init_seen_trades(self) -> set:
        """Fetch current trades to use as baseline — we only copy NEW trades after startup."""
        seen = set()
        try:
            condition_ids = self.get_condition_ids()
            if not condition_ids:
                return seen
            async with PolymarketClient() as client:
                trades = await client.get_market_trades(condition_ids[:4])
                for t in trades:
                    seen.add(self._trade_key(t))
            logger.info(f"Copy watcher baseline: {len(seen)} existing trades (will skip these)")
        except Exception as e:
            logger.error(f"Error initialising seen trades: {e}")
        return seen

    async def _check_new_trades(self, seen_tx: set):
        """
        Fetch latest trades for our markets. For each new BUY by a tracked trader,
        immediately place a copy order.
        """
        condition_ids = self.get_condition_ids()
        if not condition_ids:
            return

        tracked_addrs = {t["address"].lower() for t in self._tracked_traders}
        open_count = await self._count_open_positions()

        async with PolymarketClient() as client:
            trades = await client.get_market_trades(condition_ids[:4], limit=50)

            for trade in trades:
                key = self._trade_key(trade)
                if key in seen_tx:
                    continue
                seen_tx.add(key)  # Mark as seen regardless of whether we copy

                side = (trade.get("side") or "").upper()
                trader_addr = (trade.get("proxyWallet") or "").lower()

                # Log when we see a tracked trader (any side) so we know matching works
                if trader_addr in tracked_addrs:
                    logger.info(f"👀 Tracked trader seen: {trader_addr[:10]}... {side} in {trade.get('conditionId','')[:12]}...")

                if side != "BUY":
                    continue

                if trader_addr not in tracked_addrs:
                    continue

                if open_count >= self.max_open_positions:
                    logger.info("Max positions reached, skipping copy")
                    break

                logger.info(f"  ➡️ BUY from tracked trader — checking market/outcome/price...")

                # Find which market this trade is in
                condition_id = trade.get("conditionId") or ""
                market_info = next(
                    (m for m in self._active_markets if m.get("condition_id") == condition_id), None
                )
                if not market_info:
                    logger.warning(f"  ❌ conditionId {condition_id[:16]}... not in active markets")
                    logger.warning(f"     Active conditionIds: {[m.get('condition_id','')[:16] for m in self._active_markets]}")
                    continue

                logger.info(f"  ✅ Market found: {market_info['asset']} {market_info['timeframe']}")

                # Get outcome from the asset field — for these markets asset IS the token_id
                asset_id = trade.get("asset") or ""
                outcome, token_id = self._resolve_outcome_from_asset(market_info, asset_id)
                if not outcome or not token_id:
                    tokens = [(t.get("outcome"), t.get("token_id","")[:16]) for t in market_info.get("tokens", [])]
                    logger.warning(f"  ❌ asset {asset_id[:16]}... not matched. Market tokens: {tokens}")
                    continue

                logger.info(f"  ✅ Outcome: {outcome} | token: {token_id[:16]}...")

                # Check we haven't already copied this trader in this market window
                already = await self._already_copied(trader_addr, condition_id, outcome)
                if already:
                    logger.info(f"  ⏭️ Already copied this position, skipping")
                    continue

                # Get trader info for logging
                trader_info = next(
                    (t for t in self._tracked_traders if t["address"].lower() == trader_addr), {}
                )

                # Get live price
                current_price = await client.get_market_price(token_id)
                if not current_price or current_price <= 0 or current_price >= 1:
                    logger.warning(f"Bad price {current_price} for {outcome}, skipping")
                    continue

                direction = outcome.upper()
                logger.info(
                    f"🎯 New trade detected! {market_info['asset']} {direction} "
                    f"by {trader_addr[:10]}... (WR:{trader_info.get('win_rate', 0):.0%}) "
                    f"— placing copy @ {current_price:.3f}"
                )

                order_id = await client.place_market_order(
                    token_id=token_id,
                    side="BUY",
                    amount_usdc=self.stake_usdc,
                    price=current_price,
                )

                shares = self.stake_usdc / current_price if current_price > 0 else 0
                await self._record_position(
                    market_info=market_info,
                    outcome=outcome,
                    direction=direction,
                    stake=self.stake_usdc,
                    shares=shares,
                    price=current_price,
                    order_id=order_id,
                    copied_from=trader_addr,
                )

                if order_id:
                    open_count += 1
                    await self._notify(
                        f"📋 <b>Copy Trade Executed</b>\n"
                        f"{market_info['asset']} {direction} ({market_info['timeframe']})\n"
                        f"Price: {current_price:.3f} | Stake: ${self.stake_usdc:.2f} USDC\n"
                        f"Copying: <code>{trader_addr[:12]}...</code> (WR: {trader_info.get('win_rate', 0):.0%})\n"
                        f"Order: <code>{str(order_id)[:20]}</code>"
                    )
                else:
                    await self._notify(
                        f"⚠️ <b>Copy Trade Failed</b>\n"
                        f"{market_info['asset']} {direction} — order returned no ID\n"
                        f"Check CLOB credentials in Railway env vars."
                    )

    async def _position_monitor_loop(self):
        """Monitor open positions and update PnL."""
        await asyncio.sleep(30)
        while self.running:
            try:
                await self._update_positions()
            except Exception as e:
                logger.error(f"Position monitor error: {e}", exc_info=True)
            await asyncio.sleep(30)

    # ──────────────────────────────────────────────
    # Market Management
    # ──────────────────────────────────────────────

    async def _refresh_markets(self):
        async with PolymarketClient() as client:
            markets = await client.get_active_crypto_markets()
            self._active_markets = markets
            self._last_market_refresh = datetime.utcnow()
            logger.info(f"Refreshed {len(markets)} markets")

    def get_market_ids(self) -> list[str]:
        return [m["id"] for m in self._active_markets if m.get("id")]

    def get_condition_ids(self) -> list[str]:
        return [m["condition_id"] for m in self._active_markets if m.get("condition_id")]

    def get_markets_summary(self) -> str:
        if not self._active_markets:
            return "No active markets loaded yet."
        lines = []
        for m in self._active_markets[:10]:
            lines.append(
                f"• [{m['asset']} {m['timeframe']}] {m['question'][:60]}... "
                f"Vol: ${m['volume']:,.0f}"
            )
        return "\n".join(lines)

    # ──────────────────────────────────────────────
    # Trader Tracking
    # ──────────────────────────────────────────────

    async def _scan_traders(self):
        if not self._active_markets:
            logger.info("Trader scan skipped — no markets loaded yet")
            return

        market_ids = self.get_market_ids()
        condition_ids = self.get_condition_ids()
        logger.info(f"Starting trader scan: {len(market_ids)} markets, {len(condition_ids)} conditionIds")

        async with PolymarketClient() as client:
            profitable = await client.get_profitable_traders(
                market_ids=market_ids,
                condition_ids=condition_ids,
                min_win_rate=self.min_win_rate,
                min_trades=self.min_trader_trades,
                top_n=self.top_traders_n,
            )

        self._tracked_traders = profitable
        await self._upsert_traders(profitable)
        logger.info(
            f"Tracking {len(profitable)} profitable traders | "
            f"Copy trading: {'ENABLED ✅' if self.copy_trade_enabled else 'DISABLED ❌ (use /toggle to enable)'}"
        )

        # Copy trading is handled by _copy_watcher_loop (polls every 5s)
        # which reacts to new trades in real-time instead of checking positions

    async def _upsert_traders(self, traders: list[dict]):
        async with AsyncSessionLocal() as session:
            for t in traders:
                existing = await session.execute(
                    select(TrackedTrader).where(TrackedTrader.address == t["address"])
                )
                obj = existing.scalar_one_or_none()
                if obj:
                    obj.win_rate = t["win_rate"]
                    obj.total_trades = t["total_trades"]
                    obj.total_pnl = t["total_pnl"]
                    obj.last_seen_at = datetime.utcnow()
                else:
                    session.add(TrackedTrader(
                        address=t["address"],
                        win_rate=t["win_rate"],
                        total_trades=t["total_trades"],
                        total_pnl=t["total_pnl"],
                        avg_stake=0,
                        last_seen_at=datetime.utcnow(),
                    ))
            await session.commit()

    # ──────────────────────────────────────────────
    # Copy Trading (handled by _copy_watcher_loop)
    # ──────────────────────────────────────────────

    def _get_token_id(self, market_info: dict, outcome: str) -> Optional[str]:
        tokens = market_info.get("tokens", [])
        for token in tokens:
            if (token.get("outcome") or "").lower() == outcome.lower():
                return token.get("token_id")
        return None

    def _resolve_outcome_from_asset(self, market_info: dict, asset_id: str) -> tuple[str, str]:
        """
        Given a trade's asset field (which is the token_id on Polymarket),
        find the matching outcome name ("Up"/"Down") and token_id.
        Returns (outcome, token_id) or ("", "") if not found.
        """
        tokens = market_info.get("tokens", [])
        for token in tokens:
            tid = token.get("token_id") or ""
            if tid == asset_id:
                return token.get("outcome", ""), tid
        return "", ""

    async def _already_copied(self, trader_addr: str, condition_id: str, outcome: str) -> bool:
        """Check if we already have an open copy of this trader's position in this market."""
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(Position).where(
                    Position.copied_from == trader_addr,
                    Position.market_id == condition_id,
                    Position.outcome == outcome,
                    Position.status == "OPEN",
                )
            )
            return result.scalar_one_or_none() is not None

    async def _record_position(self, market_info, outcome, direction, stake, shares, price, order_id, copied_from=None):
        async with AsyncSessionLocal() as session:
            pos = Position(
                market_id=market_info["id"],
                market_question=market_info.get("question", ""),
                outcome=outcome,
                direction=direction,
                asset=market_info.get("asset", ""),
                timeframe=market_info.get("timeframe", ""),
                stake_usdc=stake,
                shares=shares,
                entry_price=price,
                current_price=price,
                status="OPEN",
                copied_from=copied_from,
                order_id=order_id,
                opened_at=datetime.utcnow(),
            )
            session.add(pos)
            await session.commit()

    # ──────────────────────────────────────────────
    # Position Monitoring
    # ──────────────────────────────────────────────

    async def _update_positions(self):
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(Position).where(Position.status == "OPEN")
            )
            positions = result.scalars().all()

        if not positions:
            return

        async with PolymarketClient() as client:
            for pos in positions:
                market = next(
                    (m for m in self._active_markets
                     if m.get("condition_id") == pos.market_id or m.get("id") == pos.market_id), None
                )
                if not market:
                    continue

                token_id = self._get_token_id(market, pos.outcome)
                if not token_id:
                    continue

                current_price = await client.get_market_price(token_id)
                if current_price is None:
                    continue

                # Check if market has ended (price near 0 or 1)
                if current_price >= 0.95 or current_price <= 0.05:
                    # Market resolved
                    pnl = (current_price * pos.shares) - pos.stake_usdc
                    async with AsyncSessionLocal() as session:
                        await session.execute(
                            update(Position)
                            .where(Position.id == pos.id)
                            .values(
                                status="CLOSED",
                                exit_price=current_price,
                                current_price=current_price,
                                pnl_usdc=pnl,
                                closed_at=datetime.utcnow(),
                            )
                        )
                        await session.commit()

                    emoji = "✅" if pnl > 0 else "❌"
                    sign = "+" if pnl > 0 else ""
                    await self._notify(
                        f"{emoji} <b>Position Closed</b>\n"
                        f"{pos.asset} {pos.direction} ({pos.timeframe})\n"
                        f"PnL: {sign}{pnl:.2f} USDC ({sign}{(pnl/pos.stake_usdc*100):.1f}%)"
                    )
                else:
                    # Update current price
                    async with AsyncSessionLocal() as session:
                        await session.execute(
                            update(Position)
                            .where(Position.id == pos.id)
                            .values(current_price=current_price)
                        )
                        await session.commit()

    # ──────────────────────────────────────────────
    # Stats & Settings
    # ──────────────────────────────────────────────

    async def _count_open_positions(self) -> int:
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(Position).where(Position.status == "OPEN")
            )
            return len(result.scalars().all())

    async def get_stats(self) -> dict:
        async with AsyncSessionLocal() as session:
            open_res = await session.execute(
                select(Position).where(Position.status == "OPEN")
            )
            open_positions = open_res.scalars().all()

            closed_res = await session.execute(
                select(Position).where(Position.status == "CLOSED")
            )
            closed_positions = closed_res.scalars().all()

        total_pnl = sum(p.pnl_usdc or 0 for p in closed_positions)
        winning = sum(1 for p in closed_positions if (p.pnl_usdc or 0) > 0)
        win_rate = (winning / len(closed_positions)) if closed_positions else 0

        unrealized = sum(
            ((p.current_price or p.entry_price) * p.shares) - p.stake_usdc
            for p in open_positions
        )

        return {
            "open_positions": len(open_positions),
            "closed_positions": len(closed_positions),
            "total_pnl": total_pnl,
            "unrealized_pnl": unrealized,
            "win_rate": win_rate,
            "tracked_traders": len(self._tracked_traders),
            "active_markets": len(self._active_markets),
            "copy_trade_enabled": self.copy_trade_enabled,
            "stake_usdc": self.stake_usdc,
        }

    async def get_open_positions(self) -> list[Position]:
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(Position).where(Position.status == "OPEN")
                .order_by(Position.opened_at.desc())
            )
            return result.scalars().all()

    async def get_recent_closed(self, limit: int = 5) -> list[Position]:
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(Position).where(Position.status == "CLOSED")
                .order_by(Position.closed_at.desc())
                .limit(limit)
            )
            return result.scalars().all()

    async def get_tracked_traders(self) -> list[TrackedTrader]:
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(TrackedTrader)
                .where(TrackedTrader.is_active == True)
                .order_by(TrackedTrader.win_rate.desc())
            )
            return result.scalars().all()

    async def set_stake(self, amount: float) -> bool:
        if amount <= 0 or amount > self.max_stake_usdc:
            return False
        self.stake_usdc = amount
        await self._save_setting("stake_usdc", str(amount))
        return True

    async def toggle_copy_trading(self) -> bool:
        self.copy_trade_enabled = not self.copy_trade_enabled
        await self._save_setting("copy_trade_enabled", str(self.copy_trade_enabled))
        return self.copy_trade_enabled

    async def _save_setting(self, key: str, value: str):
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(BotSettings).where(BotSettings.key == key)
            )
            obj = result.scalar_one_or_none()
            if obj:
                obj.value = value
                obj.updated_at = datetime.utcnow()
            else:
                session.add(BotSettings(key=key, value=value))
            await session.commit()

    async def _load_settings(self):
        async with AsyncSessionLocal() as session:
            result = await session.execute(select(BotSettings))
            settings = {s.key: s.value for s in result.scalars().all()}

        if "stake_usdc" in settings:
            self.stake_usdc = float(settings["stake_usdc"])
        if "copy_trade_enabled" in settings:
            self.copy_trade_enabled = settings["copy_trade_enabled"].lower() == "true"

    # ──────────────────────────────────────────────
    # Notifications
    # ──────────────────────────────────────────────

    def add_status_callback(self, fn):
        self._status_callbacks.append(fn)

    async def _notify(self, message: str):
        for cb in self._status_callbacks:
            try:
                await cb(message)
            except Exception as e:
                logger.error(f"Notification callback error: {e}")
