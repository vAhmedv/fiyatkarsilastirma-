"""
Async Database Layer for Fiyat Takip v8.0
Uses aiosqlite with SQLAlchemy async engine
"""
from sqlmodel import SQLModel
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from contextlib import asynccontextmanager
import logging

logger = logging.getLogger("FiyatTakip.Database")

# Async SQLite connection
SQLITE_FILE = "prices.db"
SQLITE_URL = f"sqlite+aiosqlite:///{SQLITE_FILE}"

# Create async engine (aiosqlite handles thread safety internally)
engine = create_async_engine(
    SQLITE_URL,
    echo=False,
    future=True
)

# Async session factory
async_session_factory = sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False
)


async def init_db():
    """Initialize database tables."""
    async with engine.begin() as conn:
        # Enable WAL mode and foreign keys (use text() for raw SQL)
        await conn.execute(text("PRAGMA journal_mode=WAL"))
        await conn.execute(text("PRAGMA foreign_keys=ON"))
        # Create all tables
        await conn.run_sync(SQLModel.metadata.create_all)
    logger.info("✅ Veritabanı başlatıldı (WAL mode, FK enabled)")


async def get_session() -> AsyncSession:
    """FastAPI dependency for async database sessions."""
    async with async_session_factory() as session:
        try:
            yield session
        except Exception as e:
            await session.rollback()
            logger.error(f"Database session error: {e}", exc_info=True)
            raise
        finally:
            await session.close()


@asynccontextmanager
async def get_db_context():
    """Context manager for database sessions outside FastAPI routes."""
    async with async_session_factory() as session:
        try:
            yield session
            await session.commit()
        except Exception as e:
            await session.rollback()
            logger.error(f"Database context error: {e}", exc_info=True)
            raise
        finally:
            await session.close()


async def close_db():
    """Cleanup database connections."""
    await engine.dispose()
    logger.info("🛑 Veritabanı bağlantısı kapatıldı")
