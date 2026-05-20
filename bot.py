import logging
import os
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)
from src.scorer import WalletScorer
from src.signals import SignalEngine
from src.formatter import format_leaderboard, format_signals

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

scorer = WalletScorer()
signal_engine = SignalEngine(scorer)


def main_menu_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📡 Top Signals", callback_data="signals"),
            InlineKeyboardButton("🏆 Leaderboard", callback_data="leaderboard"),
        ],
        [
            InlineKeyboardButton("🔄 Refresh", callback_data="refresh"),
        ]
    ])


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    welcome = (
        "👁 *PolySharp* — Wallet Intelligence for Polymarket\n\n"
        "I track the top 3% of wallets that beat random chance, "
        "then surface markets where they all agree.\n\n"
        "Choose an option:"
    )
    await update.message.reply_text(
        welcome,
        parse_mode="Markdown",
        reply_markup=main_menu_keyboard()
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "*Commands*\n\n"
        "/start — Main menu\n"
        "/signals — Markets where sharp wallets converge\n"
        "/leaderboard — Top sharp wallets ranked by accuracy\n"
        "/help — This message\n\n"
        "*How scoring works*\n"
        "Wallets are scored like free throws — we expect ~50% accuracy on binary markets. "
        "Wallets beating the baseline consistently get a confidence score. "
        "Only the top 3% make the leaderboard. "
        "A signal fires when multiple top wallets independently land on the same side."
    )
    await update.message.reply_text(text, parse_mode="Markdown")


async def signals_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message or update.callback_query.message
    await msg.reply_text("🔍 Scanning sharp wallet positions...", parse_mode="Markdown")

    try:
        signals = await signal_engine.get_signals()
        text = format_signals(signals)
    except Exception as e:
        logger.error(f"Error fetching signals: {e}")
        text = "⚠️ Could not fetch signals right now. Try again in a moment."

    await msg.reply_text(text, parse_mode="Markdown", reply_markup=main_menu_keyboard())


async def leaderboard_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message or update.callback_query.message
    await msg.reply_text("📊 Loading sharp wallet rankings...", parse_mode="Markdown")

    try:
        leaderboard = await scorer.get_sharp_leaderboard()
        text = format_leaderboard(leaderboard)
    except Exception as e:
        logger.error(f"Error fetching leaderboard: {e}")
        text = "⚠️ Could not fetch leaderboard right now. Try again in a moment."

    await msg.reply_text(text, parse_mode="Markdown", reply_markup=main_menu_keyboard())


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "signals":
        await query.message.reply_text("🔍 Scanning sharp wallet positions...")
        try:
            signals = await signal_engine.get_signals()
            text = format_signals(signals)
        except Exception as e:
            logger.error(f"Error: {e}")
            text = "⚠️ Could not fetch signals. Try again shortly."
        await query.message.reply_text(text, parse_mode="Markdown", reply_markup=main_menu_keyboard())

    elif query.data == "leaderboard":
        await query.message.reply_text("📊 Loading sharp wallet rankings...")
        try:
            leaderboard = await scorer.get_sharp_leaderboard()
            text = format_leaderboard(leaderboard)
        except Exception as e:
            logger.error(f"Error: {e}")
            text = "⚠️ Could not fetch leaderboard. Try again shortly."
        await query.message.reply_text(text, parse_mode="Markdown", reply_markup=main_menu_keyboard())

    elif query.data == "refresh":
        await query.edit_message_text(
            "👁 *PolySharp* — Wallet Intelligence for Polymarket\n\n"
            "I track the top 3% of wallets that beat random chance, "
            "then surface markets where they all agree.\n\n"
            "Choose an option:",
            parse_mode="Markdown",
            reply_markup=main_menu_keyboard()
        )


def main():
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        raise ValueError("TELEGRAM_BOT_TOKEN environment variable not set")

    app = Application.builder().token(token).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("signals", signals_cmd))
    app.add_handler(CommandHandler("leaderboard", leaderboard_cmd))
    app.add_handler(CallbackQueryHandler(button_handler))

    logger.info("PolySharp bot starting...")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
