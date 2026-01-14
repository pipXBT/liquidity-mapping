"""Binance exchange connector for spot and perpetual futures."""

import asyncio
from datetime import datetime, timezone
from typing import AsyncIterator

import httpx

from src.connectors.base import ExchangeConnector, FundingRate, Kline, MarketType, OpenInterest


class BinanceConnector(ExchangeConnector):
    """Binance spot and perpetual futures connector."""

    name = "binance"

    SPOT_BASE_URL = "https://api.binance.com"
    FUTURES_BASE_URL = "https://fapi.binance.com"
    DATA_BASE_URL = "https://fapi.binance.com"

    KLINE_LIMIT = 1000
    OI_LIMIT = 500

    def __init__(self):
        """Initialize Binance connector."""
        self._client = httpx.AsyncClient(timeout=30.0)
        self._rate_limit_delay = 0.1  # 100ms between requests

    async def close(self) -> None:
        """Close HTTP client."""
        await self._client.aclose()

    async def get_symbol(self, base_asset: str) -> str | None:
        """Get trading symbol for base asset.

        Args:
            base_asset: Base asset (e.g., COAI)

        Returns:
            Full symbol (e.g., COAIUSDT) or None
        """
        symbol = f"{base_asset.upper()}USDT"
        # Check spot first
        try:
            resp = await self._client.get(
                f"{self.SPOT_BASE_URL}/api/v3/ticker/price",
                params={"symbol": symbol},
            )
            if resp.status_code == 200:
                return symbol
        except httpx.HTTPError:
            pass
        # Check futures
        try:
            resp = await self._client.get(
                f"{self.FUTURES_BASE_URL}/fapi/v1/ticker/price",
                params={"symbol": symbol},
            )
            if resp.status_code == 200:
                return symbol
        except httpx.HTTPError:
            pass
        return None

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
            symbol: Trading pair symbol
            interval: Kline interval (1h, 4h, 1d)
            market_type: Spot or perpetual
            start_time: Start of range
            end_time: End of range

        Yields:
            Kline objects
        """
        base_url = (
            self.SPOT_BASE_URL if market_type == MarketType.SPOT else self.FUTURES_BASE_URL
        )
        endpoint = "/api/v3/klines" if market_type == MarketType.SPOT else "/fapi/v1/klines"

        params: dict = {
            "symbol": symbol,
            "interval": interval,
            "limit": self.KLINE_LIMIT,
        }
        if end_time:
            params["endTime"] = int(end_time.timestamp() * 1000)

        # Paginate backwards to get all historical data (like ByBit)
        all_klines = []

        while True:
            await asyncio.sleep(self._rate_limit_delay)
            try:
                resp = await self._client.get(f"{base_url}{endpoint}", params=params)
                resp.raise_for_status()
                data = resp.json()
            except httpx.HTTPError as e:
                raise RuntimeError(f"Binance API error: {e}") from e

            if not data:
                break

            for candle in data:
                open_time = datetime.fromtimestamp(candle[0] / 1000, tz=timezone.utc)
                # Check if we've gone before start_time
                if start_time and open_time < start_time:
                    continue
                all_klines.append(
                    Kline(
                        exchange=self.name,
                        market_type=market_type,
                        symbol=symbol,
                        interval=interval,
                        open_time=open_time,
                        open=float(candle[1]),
                        high=float(candle[2]),
                        low=float(candle[3]),
                        close=float(candle[4]),
                        volume=float(candle[5]),
                        quote_volume=float(candle[7]),
                    )
                )

            # Check if we got less than limit (no more data)
            if len(data) < self.KLINE_LIMIT:
                break

            # Move end time backwards for next page
            earliest_time = data[0][0]
            if start_time and earliest_time <= int(start_time.timestamp() * 1000):
                break
            params["endTime"] = earliest_time - 1

        # Yield in chronological order
        for kline in sorted(all_klines, key=lambda k: k.open_time):
            yield kline

    async def fetch_open_interest_history(
        self,
        symbol: str,
        interval: str = "1h",
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> AsyncIterator[OpenInterest]:
        """Fetch historical open interest data.

        Args:
            symbol: Trading pair symbol
            interval: Data interval
            start_time: Start of range
            end_time: End of range

        Yields:
            OpenInterest objects
        """
        # Map interval to Binance's period format
        period_map = {"1h": "1h", "4h": "4h", "1d": "1d"}
        period = period_map.get(interval, "1h")

        params: dict = {
            "symbol": symbol,
            "period": period,
            "limit": self.OI_LIMIT,
        }
        if start_time:
            params["startTime"] = int(start_time.timestamp() * 1000)
        if end_time:
            params["endTime"] = int(end_time.timestamp() * 1000)

        while True:
            await asyncio.sleep(self._rate_limit_delay)
            try:
                resp = await self._client.get(
                    f"{self.DATA_BASE_URL}/futures/data/openInterestHist",
                    params=params,
                )
                resp.raise_for_status()
                data = resp.json()
            except httpx.HTTPError as e:
                raise RuntimeError(f"Binance OI API error: {e}") from e

            if not data:
                break

            for item in data:
                yield OpenInterest(
                    exchange=self.name,
                    symbol=symbol,
                    timestamp=datetime.fromtimestamp(item["timestamp"] / 1000, tz=timezone.utc),
                    open_interest=float(item["sumOpenInterest"]),
                    open_interest_value=float(item["sumOpenInterestValue"]),
                )

            # Check if we got less than limit
            if len(data) < self.OI_LIMIT:
                break

            # Move start time forward
            last_time = data[-1]["timestamp"]
            params["startTime"] = last_time + 1

    async def fetch_funding_history(
        self,
        symbol: str,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> AsyncIterator[FundingRate]:
        """Fetch historical funding rate data.

        Args:
            symbol: Trading pair symbol
            start_time: Start of range
            end_time: End of range

        Yields:
            FundingRate objects in chronological order
        """
        params: dict = {
            "symbol": symbol,
            "limit": self.KLINE_LIMIT,  # Max 1000
        }
        if start_time:
            params["startTime"] = int(start_time.timestamp() * 1000)
        if end_time:
            params["endTime"] = int(end_time.timestamp() * 1000)

        while True:
            await asyncio.sleep(self._rate_limit_delay)
            try:
                resp = await self._client.get(
                    f"{self.FUTURES_BASE_URL}/fapi/v1/fundingRate",
                    params=params,
                )
                resp.raise_for_status()
                data = resp.json()
            except httpx.HTTPError as e:
                raise RuntimeError(f"Binance funding API error: {e}") from e

            if not data:
                break

            for item in data:
                yield FundingRate(
                    exchange=self.name,
                    symbol=symbol,
                    funding_time=datetime.fromtimestamp(
                        item["fundingTime"] / 1000, tz=timezone.utc
                    ),
                    funding_rate=float(item["fundingRate"]),
                )

            # Check if we got less than limit
            if len(data) < self.KLINE_LIMIT:
                break

            # Move start time forward for next page
            last_time = data[-1]["fundingTime"]
            params["startTime"] = last_time + 1
