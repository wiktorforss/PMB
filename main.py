"""
Polymarket Trading Bot — Main Entry Point
Starts the trading engine and Telegram bot together.
A minimal HTTP health server runs on $PORT so Railway stays happy.
"""
import asyncio
import logging
import os
from aiohttp import web
from dotenv import load_dotenv

load_dotenv()

log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=getattr(logging, log_level, logging.INFO),
)
logger = logging.getLogger(__name__)

from src.database import init_db
from src.bot_engine import TradingBot
from src.telegram_bot import TelegramBot


async def health_handler(request):
    return web.Response(text="OK")


async def start_health_server():
    """Tiny HTTP server so Railway's healthcheck passes."""
    port = int(os.getenv("PORT", "8080"))
    app = web.Application()
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"✅ Health server listening on port {port}")
    return runner


async def main():
    logger.info("🚀 Polymarket Bot starting up...")

    await init_db()
    logger.info("✅ Database initialized")

    trading_bot = TradingBot()
    telegram_bot = TelegramBot(trading_bot)

    health_runner = await start_health_server()

    await telegram_bot.start()
    logger.info("✅ Telegram bot started")

    auto_start = os.getenv("AUTO_START_BOT", "true").lower() == "true"
    if auto_start:
        await trading_bot.start()
        logger.info("✅ Trading engine started")
    else:
        logger.info("ℹ️  Trading engine not auto-started. Use /startbot in Telegram.")

    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down...")
    finally:
        await trading_bot.stop()
        await telegram_bot.stop()
        await health_runner.cleanup()
        logger.info("👋 Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
