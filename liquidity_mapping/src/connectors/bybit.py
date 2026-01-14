"""ByBit exchange connector for spot and perpetual futures."""

import asyncio
from datetime import datetime, timezone
from typing import AsyncIterator

import httpx

from src.connectors.base import ExchangeConnector, FundingRate, Kline, MarketType, OpenInterest


class BybitConnector(ExchangeConnector):
    """ByBit spot and perpetual futures connector."""

    name = "bybit"

    BASE_URL = "https://api.bybit.com"
    KLINE_LIMIT = 1000
    OI_LIMIT = 200

    def __init__(self):
        """Initialize ByBit connector."""
        self._client = httpx.AsyncClient(timeout=30.0)
        self._rate_limit_delay = 0.1

    async def close(self) -> None:
        """Close HTTP client."""
        await self._client.aclose()

    async def get_symbol(self, base_asset: str) -> str | None:
        """Get trading symbol for base asset.

        Args:
            base_asset: Base asset (e.g., COAI)

        Returns:
            Full symbol or None
        """
        symbol = f"{base_asset.upper()}USDT"
        # Check both spot and linear (perp) markets
        for category in ["spot", "linear"]:
            try:
                resp = await self._client.get(
                    f"{self.BASE_URL}/v5/market/tickers",
                    params={"category": category, "symbol": symbol},
                )
                data = resp.json()
                if data.get("retCode") == 0 and data.get("result", {}).get("list"):
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
            interval: Kline interval (1h -> 60, 4h -> 240, 1d -> D)
            market_type: Spot or perpetual
            start_time: Start of range
            end_time: End of range

        Yields:
            Kline objects
        """
        # Map interval to ByBit format
        interval_map = {"1h": "60", "4h": "240", "1d": "D"}
        bybit_interval = interval_map.get(interval, "60")

        category = "spot" if market_type == MarketType.SPOT else "linear"

        params: dict = {
            "category": category,
            "symbol": symbol,
            "interval": bybit_interval,
            "limit": self.KLINE_LIMIT,
        }
        if end_time:
            params["end"] = int(end_time.timestamp() * 1000)

        # ByBit returns data in reverse chronological order, so we paginate backwards
        all_klines = []

        while True:
            await asyncio.sleep(self._rate_limit_delay)
            try:
                resp = await self._client.get(
                    f"{self.BASE_URL}/v5/market/kline",
                    params=params,
                )
                data = resp.json()
                if data.get("retCode") != 0:
                    raise RuntimeError(f"ByBit API error: {data.get('retMsg')}")
                kline_list = data.get("result", {}).get("list", [])
            except httpx.HTTPError as e:
                raise RuntimeError(f"ByBit API error: {e}") from e

            if not kline_list:
                break

            for candle in kline_list:
                open_time = datetime.fromtimestamp(int(candle[0]) / 1000, tz=timezone.utc)
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
                        quote_volume=float(candle[6]),
                    )
                )

            if len(kline_list) < self.KLINE_LIMIT:
                break

            # Move end time backwards for next page
            earliest_time = int(kline_list[-1][0])
            if start_time and earliest_time <= int(start_time.timestamp() * 1000):
                break
            params["end"] = earliest_time - 1

        # Yield in chronological order
        for kline in reversed(all_klines):
            yield kline

    async def fetch_open_interest_history(
        self,
        symbol: str,
        interval: str = "1h",
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> AsyncIterator[OpenInterest]:
        """Fetch historical open interest data.

        Note: ByBit v5 API provides current OI, not historical.
        We'll fetch current and store incrementally.

        Args:
            symbol: Trading pair symbol
            interval: Data interval (not used for ByBit current OI)
            start_time: Start of range
            end_time: End of range

        Yields:
            OpenInterest objects
        """
        # ByBit doesn't have historical OI API like Binance
        # We can only get current OI snapshot
        # For historical data, you'd need to collect over time
        await asyncio.sleep(self._rate_limit_delay)
        try:
            resp = await self._client.get(
                f"{self.BASE_URL}/v5/market/open-interest",
                params={
                    "category": "linear",
                    "symbol": symbol,
                    "intervalTime": "1h",
                    "limit": self.OI_LIMIT,
                },
            )
            data = resp.json()
            if data.get("retCode") != 0:
                raise RuntimeError(f"ByBit OI API error: {data.get('retMsg')}")

            oi_list = data.get("result", {}).get("list", [])
            for item in reversed(oi_list):  # Chronological order
                ts = datetime.fromtimestamp(int(item["timestamp"]) / 1000, tz=timezone.utc)
                if start_time and ts < start_time:
                    continue
                if end_time and ts > end_time:
                    continue
                yield OpenInterest(
                    exchange=self.name,
                    symbol=symbol,
                    timestamp=ts,
                    open_interest=float(item["openInterest"]),
                    open_interest_value=0.0,  # ByBit doesn't provide OI value directly
                )
        except httpx.HTTPError as e:
            raise RuntimeError(f"ByBit OI API error: {e}") from e

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
            "category": "linear",
            "symbol": symbol,
            "limit": self.OI_LIMIT,  # Max 200
        }
        if start_time:
            params["startTime"] = int(start_time.timestamp() * 1000)
        if end_time:
            params["endTime"] = int(end_time.timestamp() * 1000)

        all_funding = []

        while True:
            await asyncio.sleep(self._rate_limit_delay)
            try:
                resp = await self._client.get(
                    f"{self.BASE_URL}/v5/market/funding/history",
                    params=params,
                )
                data = resp.json()
                if data.get("retCode") != 0:
                    raise RuntimeError(f"ByBit funding API error: {data.get('retMsg')}")
                funding_list = data.get("result", {}).get("list", [])
            except httpx.HTTPError as e:
                raise RuntimeError(f"ByBit funding API error: {e}") from e

            if not funding_list:
                break

            for item in funding_list:
                ts = datetime.fromtimestamp(
                    int(item["fundingRateTimestamp"]) / 1000, tz=timezone.utc
                )
                if start_time and ts < start_time:
                    continue
                all_funding.append(
                    FundingRate(
                        exchange=self.name,
                        symbol=symbol,
                        funding_time=ts,
                        funding_rate=float(item["fundingRate"]),
                    )
                )

            if len(funding_list) < self.OI_LIMIT:
                break

            # Move end time backwards for next page (ByBit returns newest first)
            earliest_time = int(funding_list[-1]["fundingRateTimestamp"])
            if start_time and earliest_time <= int(start_time.timestamp() * 1000):
                break
            params["endTime"] = earliest_time - 1

        # Yield in chronological order
        for fr in reversed(all_funding):
            yield fr
