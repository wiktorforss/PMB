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
        """Periodically scan for profitable traders and copy their positions."""
        await asyncio.sleep(15)  # Wait for market refresh first
        while self.running:
            try:
                await self._scan_traders()
            except Exception as e:
                logger.error(f"Trader scan error: {e}", exc_info=True)
            await asyncio.sleep(60)  # Scan every minute

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
        logger.info(f"Tracking {len(profitable)} profitable traders")

        if self.copy_trade_enabled:
            await self._copy_trader_positions(profitable)

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
    # Copy Trading
    # ──────────────────────────────────────────────

    async def _copy_trader_positions(self, traders: list[dict]):
        """Check if tracked traders have new positions and copy them."""
        open_count = await self._count_open_positions()
        if open_count >= self.max_open_positions:
            logger.info(f"Max positions ({self.max_open_positions}) reached, skipping copy")
            return

        market_ids = self.get_market_ids()

        async with PolymarketClient() as client:
            for trader in traders:
                if open_count >= self.max_open_positions:
                    break

                positions = await client.get_trader_open_positions(
                    trader["address"], market_ids
                )
                for pos in positions:
                    market_id = pos.get("market")
                    outcome = pos.get("outcome")
                    price = float(pos.get("price", 0))
                    size = float(pos.get("size", 0))

                    if not market_id or not outcome or price <= 0:
                        continue

                    # Check if we already copied this
                    already_copied = await self._already_copied(
                        trader["address"], market_id, outcome
                    )
                    if already_copied:
                        continue

                    # Find market info
                    market_info = next(
                        (m for m in self._active_markets if m["id"] == market_id), None
                    )
                    if not market_info:
                        continue

                    # Find the token ID for this outcome
                    token_id = self._get_token_id(market_info, outcome)
                    if not token_id:
                        continue

                    # Place the copy trade
                    order_id = await client.place_market_order(
                        token_id=token_id,
                        side="BUY",
                        amount_usdc=self.stake_usdc,
                        price=price,
                    )

                    shares = self.stake_usdc / price if price > 0 else 0
                    direction = "UP" if "higher" in (market_info.get("question") or "").lower() else "DOWN"

                    await self._record_position(
                        market_info=market_info,
                        outcome=outcome,
                        direction=direction,
                        stake=self.stake_usdc,
                        shares=shares,
                        price=price,
                        order_id=order_id,
                        copied_from=trader["address"],
                    )

                    if order_id:
                        open_count += 1
                        logger.info(
                            f"✅ Copied trade: {market_info['asset']} {direction} "
                            f"from {trader['address'][:8]}... | ${self.stake_usdc}"
                        )
                        await self._notify(
                            f"📋 *Copy Trade Executed*\n"
                            f"Market: {market_info['question'][:50]}...\n"
                            f"Direction: {direction} | Stake: ${self.stake_usdc}\n"
                            f"Copied from: `{trader['address'][:10]}...`\n"
                            f"Win Rate: {trader['win_rate']:.1%}"
                        )

    def _get_token_id(self, market_info: dict, outcome: str) -> Optional[str]:
        tokens = market_info.get("tokens", [])
        for token in tokens:
            if (token.get("outcome") or "").lower() == outcome.lower():
                return token.get("token_id")
        return None

    async def _already_copied(self, trader_addr: str, market_id: str, outcome: str) -> bool:
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(Position).where(
                    Position.copied_from == trader_addr,
                    Position.market_id == market_id,
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
                    (m for m in self._active_markets if m["id"] == pos.market_id), None
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
                    await self._notify(
                        f"{emoji} *Position Closed*\n"
                        f"{pos.asset} {pos.direction} ({pos.timeframe})\n"
                        f"PnL: {'+'if pnl>0 else ''}{pnl:.2f} USDC\n"
                        f"({'+' if pnl>0 else ''}{(pnl/pos.stake_usdc*100):.1f}%)"
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
