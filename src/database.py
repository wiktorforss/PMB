"""
Database models for tracking positions, traders, and bot state.
"""
from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from datetime import datetime
import os


DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///bot_data.db")

engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class Position(Base):
    __tablename__ = "positions"

    id = Column(Integer, primary_key=True)
    market_id = Column(String, nullable=False)
    market_question = Column(String)
    outcome = Column(String)          # "Yes" or "No"
    direction = Column(String)        # "UP" or "DOWN"
    asset = Column(String)            # "BTC" or "ETH"
    timeframe = Column(String)        # "5min" or "15min"
    stake_usdc = Column(Float)
    shares = Column(Float)
    entry_price = Column(Float)
    current_price = Column(Float)
    exit_price = Column(Float, nullable=True)
    pnl_usdc = Column(Float, nullable=True)
    status = Column(String, default="OPEN")   # OPEN, CLOSED, CANCELLED
    copied_from = Column(String, nullable=True)  # trader address
    order_id = Column(String, nullable=True)
    opened_at = Column(DateTime, default=datetime.utcnow)
    closed_at = Column(DateTime, nullable=True)
    notes = Column(Text, nullable=True)


class TrackedTrader(Base):
    __tablename__ = "tracked_traders"

    id = Column(Integer, primary_key=True)
    address = Column(String, unique=True, nullable=False)
    win_rate = Column(Float)
    total_trades = Column(Integer)
    total_pnl = Column(Float)
    avg_stake = Column(Float)
    last_seen_at = Column(DateTime)
    is_active = Column(Boolean, default=True)
    added_at = Column(DateTime, default=datetime.utcnow)


class TraderPosition(Base):
    __tablename__ = "trader_positions"

    id = Column(Integer, primary_key=True)
    trader_address = Column(String, nullable=False)
    market_id = Column(String, nullable=False)
    outcome = Column(String)
    shares = Column(Float)
    avg_price = Column(Float)
    detected_at = Column(DateTime, default=datetime.utcnow)
    copied = Column(Boolean, default=False)


class BotSettings(Base):
    __tablename__ = "bot_settings"

    id = Column(Integer, primary_key=True)
    key = Column(String, unique=True, nullable=False)
    value = Column(String)
    updated_at = Column(DateTime, default=datetime.utcnow)


async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_session():
    async with AsyncSessionLocal() as session:
        yield session
