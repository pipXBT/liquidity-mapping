"""Database engine configuration."""

from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from src.db.models import Base

_engine: AsyncEngine | None = None


def get_engine(db_path: Path | None = None) -> AsyncEngine:
    """Get or create the async database engine.

    Args:
        db_path: Path to SQLite database file. Defaults to liquidity.db in cwd.

    Returns:
        AsyncEngine instance
    """
    global _engine
    if _engine is None:
        if db_path is None:
            db_path = Path.cwd() / "liquidity.db"
        _engine = create_async_engine(
            f"sqlite+aiosqlite:///{db_path}",
            echo=False,
        )
    return _engine


async def init_db(engine: AsyncEngine | None = None) -> None:
    """Initialize database tables.

    Args:
        engine: Optional engine to use. Defaults to global engine.
    """
    if engine is None:
        engine = get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def close_db() -> None:
    """Close the database engine."""
    global _engine
    if _engine is not None:
        await _engine.dispose()
        _engine = None
