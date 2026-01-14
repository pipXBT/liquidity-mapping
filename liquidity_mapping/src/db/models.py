"""SQLAlchemy models for kline and open interest data."""

from datetime import datetime

from sqlalchemy import DateTime, Float, Index, String, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base class for all models."""

    pass


class KlineModel(Base):
    """Kline (candlestick) data model."""

    __tablename__ = "klines"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    exchange: Mapped[str] = mapped_column(String(20), nullable=False)
    market_type: Mapped[str] = mapped_column(String(10), nullable=False)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False)
    interval: Mapped[str] = mapped_column(String(5), nullable=False)
    open_time: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    open: Mapped[float] = mapped_column(Float, nullable=False)
    high: Mapped[float] = mapped_column(Float, nullable=False)
    low: Mapped[float] = mapped_column(Float, nullable=False)
    close: Mapped[float] = mapped_column(Float, nullable=False)
    volume: Mapped[float] = mapped_column(Float, nullable=False)
    quote_volume: Mapped[float] = mapped_column(Float, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "exchange", "market_type", "symbol", "interval", "open_time",
            name="uq_kline"
        ),
        Index("ix_kline_lookup", "symbol", "exchange", "market_type", "interval", "open_time"),
    )


class OpenInterestModel(Base):
    """Open interest snapshot model."""

    __tablename__ = "open_interest"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    exchange: Mapped[str] = mapped_column(String(20), nullable=False)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False)
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    open_interest: Mapped[float] = mapped_column(Float, nullable=False)
    open_interest_value: Mapped[float] = mapped_column(Float, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "exchange", "symbol", "timestamp",
            name="uq_open_interest"
        ),
        Index("ix_oi_lookup", "symbol", "exchange", "timestamp"),
    )


class FundingRateModel(Base):
    """Funding rate data model for perpetual contracts."""

    __tablename__ = "funding_rates"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    exchange: Mapped[str] = mapped_column(String(20), nullable=False)
    symbol: Mapped[str] = mapped_column(String(20), nullable=False)
    funding_time: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    funding_rate: Mapped[float] = mapped_column(Float, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "exchange", "symbol", "funding_time",
            name="uq_funding_rate"
        ),
        Index("ix_funding_lookup", "symbol", "exchange", "funding_time"),
    )
