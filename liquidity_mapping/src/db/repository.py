"""Repository for storing and retrieving market data."""

from datetime import datetime

from sqlalchemy import select, func
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.connectors.base import FundingRate, Kline, MarketType, OpenInterest
from src.db.engine import get_engine
from src.db.models import FundingRateModel, KlineModel, OpenInterestModel


class Repository:
    """Data access layer for klines and open interest."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession] | None = None):
        """Initialize repository.

        Args:
            session_factory: Optional session factory. Creates one if not provided.
        """
        if session_factory is None:
            engine = get_engine()
            session_factory = async_sessionmaker(engine, expire_on_commit=False)
        self._session_factory = session_factory

    async def upsert_klines(self, klines: list[Kline]) -> int:
        """Insert or update klines with deduplication.

        Args:
            klines: List of Kline objects to store

        Returns:
            Number of rows affected
        """
        if not klines:
            return 0

        async with self._session_factory() as session:
            rows = [
                {
                    "exchange": k.exchange,
                    "market_type": k.market_type.value,
                    "symbol": k.symbol,
                    "interval": k.interval,
                    "open_time": k.open_time,
                    "open": k.open,
                    "high": k.high,
                    "low": k.low,
                    "close": k.close,
                    "volume": k.volume,
                    "quote_volume": k.quote_volume,
                }
                for k in klines
            ]
            stmt = insert(KlineModel).values(rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=["exchange", "market_type", "symbol", "interval", "open_time"],
                set_={
                    "open": stmt.excluded.open,
                    "high": stmt.excluded.high,
                    "low": stmt.excluded.low,
                    "close": stmt.excluded.close,
                    "volume": stmt.excluded.volume,
                    "quote_volume": stmt.excluded.quote_volume,
                },
            )
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount

    async def upsert_open_interest(self, oi_data: list[OpenInterest]) -> int:
        """Insert or update open interest with deduplication.

        Args:
            oi_data: List of OpenInterest objects to store

        Returns:
            Number of rows affected
        """
        if not oi_data:
            return 0

        async with self._session_factory() as session:
            rows = [
                {
                    "exchange": oi.exchange,
                    "symbol": oi.symbol,
                    "timestamp": oi.timestamp,
                    "open_interest": oi.open_interest,
                    "open_interest_value": oi.open_interest_value,
                }
                for oi in oi_data
            ]
            stmt = insert(OpenInterestModel).values(rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=["exchange", "symbol", "timestamp"],
                set_={
                    "open_interest": stmt.excluded.open_interest,
                    "open_interest_value": stmt.excluded.open_interest_value,
                },
            )
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount

    async def get_klines(
        self,
        symbol: str,
        interval: str = "1h",
        exchange: str | None = None,
        market_type: MarketType | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> list[KlineModel]:
        """Retrieve klines from database.

        Args:
            symbol: Trading pair symbol
            interval: Kline interval
            exchange: Optional exchange filter
            market_type: Optional market type filter
            start_time: Optional start time filter
            end_time: Optional end time filter

        Returns:
            List of KlineModel objects
        """
        async with self._session_factory() as session:
            stmt = select(KlineModel).where(
                KlineModel.symbol == symbol,
                KlineModel.interval == interval,
            )
            if exchange:
                stmt = stmt.where(KlineModel.exchange == exchange)
            if market_type:
                stmt = stmt.where(KlineModel.market_type == market_type.value)
            if start_time:
                stmt = stmt.where(KlineModel.open_time >= start_time)
            if end_time:
                stmt = stmt.where(KlineModel.open_time <= end_time)
            stmt = stmt.order_by(KlineModel.open_time)
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def get_open_interest(
        self,
        symbol: str,
        exchange: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> list[OpenInterestModel]:
        """Retrieve open interest from database.

        Args:
            symbol: Trading pair symbol
            exchange: Optional exchange filter
            start_time: Optional start time filter
            end_time: Optional end time filter

        Returns:
            List of OpenInterestModel objects
        """
        async with self._session_factory() as session:
            stmt = select(OpenInterestModel).where(OpenInterestModel.symbol == symbol)
            if exchange:
                stmt = stmt.where(OpenInterestModel.exchange == exchange)
            if start_time:
                stmt = stmt.where(OpenInterestModel.timestamp >= start_time)
            if end_time:
                stmt = stmt.where(OpenInterestModel.timestamp <= end_time)
            stmt = stmt.order_by(OpenInterestModel.timestamp)
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def get_available_date_range(
        self, symbol: str
    ) -> tuple[datetime | None, datetime | None, dict[str, bool]]:
        """Get the available date range for a symbol.

        Args:
            symbol: Trading pair symbol

        Returns:
            Tuple of (earliest_date, latest_date, exchange_availability)
        """
        async with self._session_factory() as session:
            # Get date range
            stmt = select(
                func.min(KlineModel.open_time),
                func.max(KlineModel.open_time),
            ).where(KlineModel.symbol == symbol)
            result = await session.execute(stmt)
            row = result.one()
            earliest, latest = row[0], row[1]

            # Get exchange availability
            stmt = select(KlineModel.exchange).where(
                KlineModel.symbol == symbol
            ).distinct()
            result = await session.execute(stmt)
            exchanges = {row[0]: True for row in result.all()}

            return earliest, latest, exchanges

    async def get_exchange_date_ranges(
        self, symbol: str
    ) -> dict[str, tuple[datetime | None, datetime | None]]:
        """Get date ranges per exchange for a symbol.

        Args:
            symbol: Trading pair symbol

        Returns:
            Dict of exchange -> (earliest_date, latest_date)
        """
        async with self._session_factory() as session:
            stmt = select(
                KlineModel.exchange,
                func.min(KlineModel.open_time),
                func.max(KlineModel.open_time),
            ).where(KlineModel.symbol == symbol).group_by(KlineModel.exchange)
            result = await session.execute(stmt)
            return {row[0]: (row[1], row[2]) for row in result.all()}

    async def get_kline_count(self, symbol: str) -> int:
        """Get total kline count for a symbol.

        Args:
            symbol: Trading pair symbol

        Returns:
            Count of klines
        """
        async with self._session_factory() as session:
            stmt = select(func.count()).select_from(KlineModel).where(
                KlineModel.symbol == symbol
            )
            result = await session.execute(stmt)
            return result.scalar() or 0

    async def get_oi_count(self, symbol: str) -> int:
        """Get total OI snapshot count for a symbol.

        Args:
            symbol: Trading pair symbol

        Returns:
            Count of OI snapshots
        """
        async with self._session_factory() as session:
            stmt = select(func.count()).select_from(OpenInterestModel).where(
                OpenInterestModel.symbol == symbol
            )
            result = await session.execute(stmt)
            return result.scalar() or 0

    async def upsert_funding_rates(self, funding_data: list[FundingRate]) -> int:
        """Insert or update funding rates with deduplication.

        Args:
            funding_data: List of FundingRate objects to store

        Returns:
            Number of rows affected
        """
        if not funding_data:
            return 0

        async with self._session_factory() as session:
            rows = [
                {
                    "exchange": fr.exchange,
                    "symbol": fr.symbol,
                    "funding_time": fr.funding_time,
                    "funding_rate": fr.funding_rate,
                }
                for fr in funding_data
            ]
            stmt = insert(FundingRateModel).values(rows)
            stmt = stmt.on_conflict_do_update(
                index_elements=["exchange", "symbol", "funding_time"],
                set_={
                    "funding_rate": stmt.excluded.funding_rate,
                },
            )
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount

    async def get_funding_rates(
        self,
        symbol: str,
        exchange: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> list[FundingRateModel]:
        """Retrieve funding rates from database.

        Args:
            symbol: Trading pair symbol
            exchange: Optional exchange filter
            start_time: Optional start time filter
            end_time: Optional end time filter

        Returns:
            List of FundingRateModel objects
        """
        async with self._session_factory() as session:
            stmt = select(FundingRateModel).where(FundingRateModel.symbol == symbol)
            if exchange:
                stmt = stmt.where(FundingRateModel.exchange == exchange)
            if start_time:
                stmt = stmt.where(FundingRateModel.funding_time >= start_time)
            if end_time:
                stmt = stmt.where(FundingRateModel.funding_time <= end_time)
            stmt = stmt.order_by(FundingRateModel.funding_time)
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def get_funding_count(self, symbol: str) -> int:
        """Get total funding rate count for a symbol.

        Args:
            symbol: Trading pair symbol

        Returns:
            Count of funding rate records
        """
        async with self._session_factory() as session:
            stmt = select(func.count()).select_from(FundingRateModel).where(
                FundingRateModel.symbol == symbol
            )
            result = await session.execute(stmt)
            return result.scalar() or 0
