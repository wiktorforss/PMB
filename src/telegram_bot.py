"""
Telegram bot interface for controlling the trading bot.
"""
import os
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ContextTypes, MessageHandler, filters
)
from telegram.constants import ParseMode

from .bot_engine import TradingBot

logger = logging.getLogger(__name__)

ALLOWED_IDS = set(
    int(x.strip())
    for x in os.getenv("ALLOWED_CHAT_IDS", "").split(",")
    if x.strip().isdigit()
)


def auth_required(func):
    """Decorator to restrict commands to allowed users."""
    async def wrapper(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if ALLOWED_IDS and user_id not in ALLOWED_IDS:
            await update.message.reply_text("⛔ Unauthorized.")
            return
        return await func(self, update, context)
    return wrapper


def build_main_keyboard(bot: TradingBot) -> InlineKeyboardMarkup:
    copy_label = "🟢 Copy Trading: ON" if bot.copy_trade_enabled else "🔴 Copy Trading: OFF"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Stats", callback_data="stats"),
         InlineKeyboardButton("📋 Positions", callback_data="positions")],
        [InlineKeyboardButton("👥 Traders", callback_data="traders"),
         InlineKeyboardButton("🏦 Markets", callback_data="markets")],
        [InlineKeyboardButton(copy_label, callback_data="toggle_copy")],
        [InlineKeyboardButton("💰 Set Stake", callback_data="set_stake"),
         InlineKeyboardButton("📜 History", callback_data="history")],
        [InlineKeyboardButton("▶️ Start Bot", callback_data="start_bot"),
         InlineKeyboardButton("⏹ Stop Bot", callback_data="stop_bot")],
    ])


class TelegramBot:
    def __init__(self, trading_bot: TradingBot):
        self.trading_bot = trading_bot
        self.token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.app: Application = None
        self._awaiting_stake: set[int] = set()  # Users mid-stake-set flow

    async def start(self):
        if not self.token:
            raise ValueError("TELEGRAM_BOT_TOKEN not set")

        self.app = Application.builder().token(self.token).build()

        # Register handlers
        self.app.add_handler(CommandHandler("start", self.cmd_start))
        self.app.add_handler(CommandHandler("help", self.cmd_help))
        self.app.add_handler(CommandHandler("stats", self.cmd_stats))
        self.app.add_handler(CommandHandler("positions", self.cmd_positions))
        self.app.add_handler(CommandHandler("traders", self.cmd_traders))
        self.app.add_handler(CommandHandler("markets", self.cmd_markets))
        self.app.add_handler(CommandHandler("stake", self.cmd_stake))
        self.app.add_handler(CommandHandler("toggle", self.cmd_toggle_copy))
        self.app.add_handler(CommandHandler("startbot", self.cmd_start_bot))
        self.app.add_handler(CommandHandler("stopbot", self.cmd_stop_bot))
        self.app.add_handler(CallbackQueryHandler(self.handle_callback))
        self.app.add_handler(
            MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_text)
        )

        # Register notification callback
        self.trading_bot.add_status_callback(self.broadcast_message)

        await self.app.initialize()
        # Kill any existing webhook and wait for old polling instance to die.
        # Railway overlaps old+new containers during deploy, so we retry
        # with backoff until Telegram stops reporting a conflict.
        import asyncio as _asyncio
        await self.app.bot.delete_webhook(drop_pending_updates=True)
        await self.app.start()

        for attempt in range(12):  # up to ~60s of retries
            try:
                await self.app.updater.start_polling(
                    drop_pending_updates=True,
                    allowed_updates=["message", "callback_query"],
                )
                break  # success
            except Exception as e:
                if "conflict" in str(e).lower() and attempt < 11:
                    wait = 5 * (attempt + 1)
                    logger.warning(f"Telegram conflict (attempt {attempt+1}/12), retrying in {wait}s...")
                    await _asyncio.sleep(wait)
                else:
                    raise

        logger.info("🤖 Telegram bot started")

    async def stop(self):
        if self.app:
            await self.app.updater.stop()
            await self.app.stop()
            await self.app.shutdown()

    async def broadcast_message(self, text: str):
        """Send a message to all allowed users."""
        if not self.app:
            return
        for chat_id in ALLOWED_IDS:
            try:
                await self.app.bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    parse_mode=ParseMode.MARKDOWN,
                )
            except Exception as e:
                logger.error(f"Broadcast failed for {chat_id}: {e}")

    # ──────────────────────────────────────────────
    # Commands
    # ──────────────────────────────────────────────

    @auth_required
    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        bot_status = "🟢 Running" if self.trading_bot.running else "🔴 Stopped"
        copy_status = "🟢 ON" if self.trading_bot.copy_trade_enabled else "🔴 OFF"
        text = (
            f"🤖 *Polymarket Trading Bot*\n\n"
            f"Status: {bot_status}\n"
            f"Copy Trading: {copy_status}\n"
            f"Stake: ${self.trading_bot.stake_usdc:.2f} USDC\n\n"
            f"Use the buttons below to control your bot."
        )
        await update.message.reply_text(
            text,
            parse_mode=ParseMode.MARKDOWN,
            reply_markup=build_main_keyboard(self.trading_bot)
        )

    @auth_required
    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        help_text = (
            "📖 *Available Commands*\n\n"
            "/start — Main control panel\n"
            "/stats — Performance statistics\n"
            "/positions — Open positions\n"
            "/traders — Tracked profitable traders\n"
            "/markets — Active BTC/ETH markets\n"
            "/stake `<amount>` — Set stake in USDC (e.g. `/stake 10`)\n"
            "/toggle — Toggle copy trading on/off\n"
            "/startbot — Start the trading bot\n"
            "/stopbot — Stop the trading bot\n"
        )
        await update.message.reply_text(help_text, parse_mode=ParseMode.MARKDOWN)

    @auth_required
    async def cmd_stats(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        stats = await self.trading_bot.get_stats()
        pnl_emoji = "📈" if stats["total_pnl"] >= 0 else "📉"
        text = (
            f"📊 *Bot Statistics*\n\n"
            f"🔓 Open Positions: {stats['open_positions']}/{self.trading_bot.max_open_positions}\n"
            f"✅ Closed Trades: {stats['closed_positions']}\n"
            f"🎯 Win Rate: {stats['win_rate']:.1%}\n\n"
            f"{pnl_emoji} Realized PnL: ${stats['total_pnl']:+.2f} USDC\n"
            f"💫 Unrealized PnL: ${stats['unrealized_pnl']:+.2f} USDC\n\n"
            f"👥 Tracked Traders: {stats['tracked_traders']}\n"
            f"🏦 Active Markets: {stats['active_markets']}\n"
            f"💰 Stake per Trade: ${stats['stake_usdc']:.2f}\n"
            f"📋 Copy Trading: {'ON ✅' if stats['copy_trade_enabled'] else 'OFF ❌'}"
        )
        await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

    @auth_required
    async def cmd_positions(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        positions = await self.trading_bot.get_open_positions()
        if not positions:
            await update.message.reply_text("No open positions.")
            return

        lines = ["📋 *Open Positions*\n"]
        for p in positions:
            current = p.current_price or p.entry_price
            unrealized = (current * p.shares) - p.stake_usdc
            direction_emoji = "🟢" if p.direction == "UP" else "🔴"
            lines.append(
                f"{direction_emoji} *{p.asset} {p.direction}* ({p.timeframe})\n"
                f"  Entry: {p.entry_price:.3f} → Now: {current:.3f}\n"
                f"  Stake: ${p.stake_usdc:.2f} | PnL: ${unrealized:+.2f}\n"
                f"  {'📋 Copied' if p.copied_from else '🖐 Manual'}"
            )

        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

    @auth_required
    async def cmd_traders(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        traders = await self.trading_bot.get_tracked_traders()
        if not traders:
            await update.message.reply_text(
                "No traders tracked yet. Bot needs to run a scan first."
            )
            return

        lines = [f"👥 *Top {len(traders)} Profitable Traders*\n"]
        for i, t in enumerate(traders[:10], 1):
            lines.append(
                f"{i}. `{t.address[:8]}...{t.address[-4:]}`\n"
                f"   WR: {t.win_rate:.1%} | Trades: {t.total_trades} | PnL: ${t.total_pnl:+.0f}"
            )

        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

    @auth_required
    async def cmd_markets(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        summary = self.trading_bot.get_markets_summary()
        await update.message.reply_text(
            f"🏦 *Active Crypto Markets*\n\n{summary}",
            parse_mode=ParseMode.MARKDOWN
        )

    @auth_required
    async def cmd_stake(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if context.args:
            try:
                amount = float(context.args[0])
                success = await self.trading_bot.set_stake(amount)
                if success:
                    await update.message.reply_text(
                        f"✅ Stake set to ${amount:.2f} USDC per trade."
                    )
                else:
                    await update.message.reply_text(
                        f"❌ Invalid amount. Must be between $0.01 and ${self.trading_bot.max_stake_usdc}."
                    )
            except ValueError:
                await update.message.reply_text("❌ Invalid amount. Use `/stake 10` for $10.")
        else:
            self._awaiting_stake.add(update.effective_user.id)
            await update.message.reply_text(
                f"💰 Current stake: ${self.trading_bot.stake_usdc:.2f} USDC\n"
                f"Enter new stake amount (max ${self.trading_bot.max_stake_usdc}):"
            )

    @auth_required
    async def cmd_toggle_copy(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        enabled = await self.trading_bot.toggle_copy_trading()
        status = "✅ ENABLED" if enabled else "❌ DISABLED"
        await update.message.reply_text(f"Copy trading is now {status}.")

    @auth_required
    async def cmd_start_bot(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if self.trading_bot.running:
            await update.message.reply_text("Bot is already running! ✅")
            return
        await self.trading_bot.start()
        await update.message.reply_text("🚀 Bot started! Scanning markets and traders...")

    @auth_required
    async def cmd_stop_bot(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not self.trading_bot.running:
            await update.message.reply_text("Bot is already stopped.")
            return
        await self.trading_bot.stop()
        await update.message.reply_text("⏹ Bot stopped.")

    # ──────────────────────────────────────────────
    # Callback Handlers
    # ──────────────────────────────────────────────

    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()

        user_id = query.from_user.id
        if ALLOWED_IDS and user_id not in ALLOWED_IDS:
            return

        data = query.data

        try:
            if data == "stats":
                stats = await self.trading_bot.get_stats()
                pnl_emoji = "📈" if stats["total_pnl"] >= 0 else "📉"
                lines = [
                    "📊 *Statistics*\n",
                    f"Open: {stats['open_positions']} | Closed: {stats['closed_positions']}",
                    f"Win Rate: {stats['win_rate']:.1%}",
                    f"{pnl_emoji} Realized: ${stats['total_pnl']:+.2f}",
                    f"💫 Unrealized: ${stats['unrealized_pnl']:+.2f}",
                    f"Stake: ${stats['stake_usdc']}",
                ]
                text = "\n".join(lines)
                await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN,
                                              reply_markup=build_main_keyboard(self.trading_bot))

            elif data == "positions":
                positions = await self.trading_bot.get_open_positions()
                if not positions:
                    text = "No open positions."
                else:
                    lines = [f"📋 *{len(positions)} Open Position(s)*\n"]
                    for p in positions:
                        current = p.current_price or p.entry_price
                        pnl = (current * p.shares) - p.stake_usdc
                        emoji = "🟢" if p.direction == "UP" else "🔴"
                        lines.append(f"{emoji} {p.asset} {p.direction} | ${pnl:+.2f}")
                    text = "\n".join(lines)
                await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN,
                                              reply_markup=build_main_keyboard(self.trading_bot))

            elif data == "traders":
                traders = await self.trading_bot.get_tracked_traders()
                if not traders:
                    text = "No traders tracked yet."
                else:
                    lines = [f"👥 *{len(traders)} Tracked Traders*\n"]
                    for t in traders[:5]:
                        lines.append(f"`{t.address[:8]}...` WR:{t.win_rate:.0%} PnL:${t.total_pnl:+.0f}")
                    text = "\n".join(lines)
                await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN,
                                              reply_markup=build_main_keyboard(self.trading_bot))

            elif data == "markets":
                summary = self.trading_bot.get_markets_summary()
                text = ("🏦 *Markets*\n\n" + summary) if summary else "No markets loaded yet."
                await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN,
                                              reply_markup=build_main_keyboard(self.trading_bot))

            elif data == "toggle_copy":
                enabled = await self.trading_bot.toggle_copy_trading()
                status = "✅ ENABLED" if enabled else "❌ DISABLED"
                await query.edit_message_text(
                    "Copy trading " + status,
                    reply_markup=build_main_keyboard(self.trading_bot)
                )

            elif data == "set_stake":
                self._awaiting_stake.add(user_id)
                await query.edit_message_text(
                    "💰 Enter stake amount in USDC\n"
                    f"(current: ${self.trading_bot.stake_usdc}, max: ${self.trading_bot.max_stake_usdc}):"
                )

            elif data == "history":
                closed = await self.trading_bot.get_recent_closed(5)
                if not closed:
                    text = "No closed trades yet."
                else:
                    lines = ["📜 *Recent Trades*\n"]
                    for p in closed:
                        emoji = "✅" if (p.pnl_usdc or 0) > 0 else "❌"
                        lines.append(f"{emoji} {p.asset} {p.direction} ({p.timeframe})")
                        lines.append(f"   PnL: ${p.pnl_usdc:+.2f}")
                    text = "\n".join(lines)
                await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN,
                                              reply_markup=build_main_keyboard(self.trading_bot))

            elif data == "start_bot":
                if not self.trading_bot.running:
                    await self.trading_bot.start()
                    await query.edit_message_text("🚀 Bot started!",
                                                  reply_markup=build_main_keyboard(self.trading_bot))
                else:
                    await query.answer("Bot is already running!", show_alert=True)

            elif data == "stop_bot":
                if self.trading_bot.running:
                    await self.trading_bot.stop()
                    await query.edit_message_text("⏹ Bot stopped.",
                                                  reply_markup=build_main_keyboard(self.trading_bot))
                else:
                    await query.answer("Bot is already stopped!", show_alert=True)

        except Exception as e:
            err = str(e).lower()
            if "not modified" not in err and "query is too old" not in err:
                logger.error(f"Callback handler error: {e}")

    async def handle_text(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if ALLOWED_IDS and user_id not in ALLOWED_IDS:
            return

        if user_id in self._awaiting_stake:
            try:
                amount = float(update.message.text.strip().replace("$", ""))
                success = await self.trading_bot.set_stake(amount)
                self._awaiting_stake.discard(user_id)
                if success:
                    await update.message.reply_text(
                        f"✅ Stake updated to ${amount:.2f} USDC",
                        reply_markup=build_main_keyboard(self.trading_bot)
                    )
                else:
                    await update.message.reply_text(
                        f"❌ Must be between $0.01 and ${self.trading_bot.max_stake_usdc}"
                    )
            except ValueError:
                await update.message.reply_text("❌ Enter a valid number like `5` or `10.50`")
