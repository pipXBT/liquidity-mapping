"""Base connector interface for exchange data fetching."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import AsyncIterator


class MarketType(str, Enum):
    """Market type enum."""

    SPOT = "spot"
    PERP = "perp"


@dataclass
class Kline:
    """Candlestick data."""

    exchange: str
    market_type: MarketType
    symbol: str
    interval: str
    open_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float


@dataclass
class OpenInterest:
    """Open interest snapshot."""

    exchange: str
    symbol: str
    timestamp: datetime
    open_interest: float
    open_interest_value: float


@dataclass
class FundingRate:
    """Funding rate data for perpetual contracts."""

    exchange: str
    symbol: str
    funding_time: datetime
    funding_rate: float  # As decimal (e.g., 0.0001 = 0.01%)


class ExchangeConnector(ABC):
    """Abstract base class for exchange connectors."""

    name: str

    @abstractmethod
    async def fetch_klines(
        self,
        symbol: str,
        interval: str,
        market_type: MarketType,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> AsyncIterator[Kline]:
        """Fetch historical klines with pagination.

        Args:
            symbol: Trading pair symbol (e.g., COAIUSDT)
            interval: Kline interval (1h, 4h, 1d)
            market_type: Spot or perpetual
            start_time: Start of range (None = earliest available)
            end_time: End of range (None = now)

        Yields:
            Kline objects in chronological order
        """
        ...

    @abstractmethod
    async def fetch_open_interest_history(
        self,
        symbol: str,
        interval: str = "1h",
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> AsyncIterator[OpenInterest]:
        """Fetch historical open interest data.

        Args:
            symbol: Trading pair symbol (e.g., COAIUSDT)
            interval: Data interval (default 1h)
            start_time: Start of range (None = earliest available)
            end_time: End of range (None = now)

        Yields:
            OpenInterest objects in chronological order
        """
        ...

    @abstractmethod
    async def fetch_funding_history(
        self,
        symbol: str,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> AsyncIterator[FundingRate]:
        """Fetch historical funding rate data.

        Args:
            symbol: Trading pair symbol (e.g., COAIUSDT)
            start_time: Start of range (None = earliest available)
            end_time: End of range (None = now)

        Yields:
            FundingRate objects in chronological order
        """
        ...

    @abstractmethod
    async def get_symbol(self, base_asset: str) -> str | None:
        """Get the full trading symbol for a base asset.

        Args:
            base_asset: Base asset name (e.g., COAI)

        Returns:
            Full symbol (e.g., COAIUSDT) or None if not found
        """
        ...

    async def close(self) -> None:
        """Clean up resources."""
        pass
