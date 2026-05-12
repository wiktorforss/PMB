"""
Polymarket Trading Bot — Main Entry Point
Starts the trading engine and Telegram bot together.
"""
import asyncio
import logging
import os
from dotenv import load_dotenv

load_dotenv()

# Configure logging
log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=getattr(logging, log_level, logging.INFO),
)
logger = logging.getLogger(__name__)

from src.database import init_db
from src.bot_engine import TradingBot
from src.telegram_bot import TelegramBot


async def main():
    logger.info("🚀 Polymarket Bot starting up...")

    # Initialize database
    await init_db()
    logger.info("✅ Database initialized")

    # Create bot instances
    trading_bot = TradingBot()
    telegram_bot = TelegramBot(trading_bot)

    # Start Telegram bot
    await telegram_bot.start()
    logger.info("✅ Telegram bot started")

    # Auto-start trading engine on launch
    auto_start = os.getenv("AUTO_START_BOT", "true").lower() == "true"
    if auto_start:
        await trading_bot.start()
        logger.info("✅ Trading engine started")
    else:
        logger.info("ℹ️  Trading engine NOT auto-started (AUTO_START_BOT=false). Use /startbot in Telegram.")

    # Keep running
    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down...")
    finally:
        await trading_bot.stop()
        await telegram_bot.stop()
        logger.info("👋 Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
