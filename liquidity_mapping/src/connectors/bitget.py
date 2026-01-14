"""BitGet exchange connector for spot and perpetual futures."""

import asyncio
from datetime import datetime, timezone
from typing import AsyncIterator

import httpx

from src.connectors.base import ExchangeConnector, FundingRate, Kline, MarketType, OpenInterest


class BitgetConnector(ExchangeConnector):
    """BitGet spot and perpetual futures connector."""

    name = "bitget"

    SPOT_BASE_URL = "https://api.bitget.com"
    FUTURES_BASE_URL = "https://api.bitget.com"
    KLINE_LIMIT = 1000
    OI_LIMIT = 100

    def __init__(self):
        """Initialize BitGet connector."""
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
        # Check spot first
        try:
            resp = await self._client.get(
                f"{self.SPOT_BASE_URL}/api/v2/spot/market/tickers",
                params={"symbol": symbol},
            )
            data = resp.json()
            if data.get("code") == "00000" and data.get("data"):
                return symbol
        except httpx.HTTPError:
            pass
        # Check futures
        try:
            resp = await self._client.get(
                f"{self.FUTURES_BASE_URL}/api/v2/mix/market/ticker",
                params={"symbol": symbol, "productType": "USDT-FUTURES"},
            )
            data = resp.json()
            if data.get("code") == "00000" and data.get("data"):
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
            interval: Kline interval (1h, 4h, 1d -> 1H, 4H, 1D)
            market_type: Spot or perpetual
            start_time: Start of range
            end_time: End of range

        Yields:
            Kline objects
        """
        # Map interval to BitGet format
        interval_map = {"1h": "1H", "4h": "4H", "1d": "1D"}
        bitget_interval = interval_map.get(interval, "1H")

        if market_type == MarketType.SPOT:
            endpoint = "/api/v2/spot/market/candles"
            params: dict = {
                "symbol": symbol,
                "granularity": bitget_interval,
                "limit": str(self.KLINE_LIMIT),
            }
        else:
            # For futures, use history-candles endpoint which supports full pagination
            endpoint = "/api/v2/mix/market/history-candles"
            params = {
                "symbol": symbol,
                "productType": "USDT-FUTURES",
                "granularity": bitget_interval,
                "limit": "200",  # history-candles works best with 200
            }

        if end_time:
            params["endTime"] = str(int(end_time.timestamp() * 1000))

        all_klines = []

        while True:
            await asyncio.sleep(self._rate_limit_delay)
            try:
                resp = await self._client.get(
                    f"{self.SPOT_BASE_URL}{endpoint}",
                    params=params,
                )
                data = resp.json()
                if data.get("code") != "00000":
                    raise RuntimeError(f"BitGet API error: {data.get('msg')}")
                kline_list = data.get("data", [])
            except httpx.HTTPError as e:
                raise RuntimeError(f"BitGet API error: {e}") from e

            if not kline_list:
                break

            for candle in kline_list:
                # BitGet format: [timestamp, open, high, low, close, volume, quoteVolume]
                open_time = datetime.fromtimestamp(int(candle[0]) / 1000, tz=timezone.utc)
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
                        quote_volume=float(candle[6]) if len(candle) > 6 else 0.0,
                    )
                )

            # Check if we got a full page (200 for futures, KLINE_LIMIT for spot)
            page_limit = 200 if market_type != MarketType.SPOT else self.KLINE_LIMIT
            if len(kline_list) < page_limit:
                break

            # Move end time backwards (Bitget returns chronological order, so [0] is oldest)
            earliest_time = int(kline_list[0][0])
            if start_time and earliest_time <= int(start_time.timestamp() * 1000):
                break
            params["endTime"] = str(earliest_time - 1)

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
        """Fetch open interest data.

        Note: BitGet provides current OI, limited historical.

        Args:
            symbol: Trading pair symbol
            interval: Data interval
            start_time: Start of range
            end_time: End of range

        Yields:
            OpenInterest objects
        """
        await asyncio.sleep(self._rate_limit_delay)
        try:
            resp = await self._client.get(
                f"{self.FUTURES_BASE_URL}/api/v2/mix/market/open-interest",
                params={
                    "symbol": symbol,
                    "productType": "USDT-FUTURES",
                },
            )
            data = resp.json()
            if data.get("code") != "00000":
                raise RuntimeError(f"BitGet OI API error: {data.get('msg')}")

            oi_data = data.get("data", {})
            oi_list = oi_data.get("openInterestList", [])
            if oi_list:
                item = oi_list[0]
                yield OpenInterest(
                    exchange=self.name,
                    symbol=symbol,
                    timestamp=datetime.now(tz=timezone.utc),
                    open_interest=float(item.get("size", 0)),
                    open_interest_value=0.0,  # BitGet doesn't provide USD value
                )
        except httpx.HTTPError as e:
            raise RuntimeError(f"BitGet OI API error: {e}") from e

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
        all_funding = []
        page_no = 1
        page_size = 100  # Max 100

        while True:
            await asyncio.sleep(self._rate_limit_delay)
            try:
                resp = await self._client.get(
                    f"{self.FUTURES_BASE_URL}/api/v2/mix/market/history-fund-rate",
                    params={
                        "symbol": symbol,
                        "productType": "USDT-FUTURES",
                        "pageSize": str(page_size),
                        "pageNo": str(page_no),
                    },
                )
                data = resp.json()
                if data.get("code") != "00000":
                    raise RuntimeError(f"BitGet funding API error: {data.get('msg')}")
                funding_list = data.get("data", [])
            except httpx.HTTPError as e:
                raise RuntimeError(f"BitGet funding API error: {e}") from e

            if not funding_list:
                break

            for item in funding_list:
                ts = datetime.fromtimestamp(
                    int(item["fundingTime"]) / 1000, tz=timezone.utc
                )
                # Apply time filters
                if start_time and ts < start_time:
                    continue
                if end_time and ts > end_time:
                    continue
                all_funding.append(
                    FundingRate(
                        exchange=self.name,
                        symbol=symbol,
                        funding_time=ts,
                        funding_rate=float(item["fundingRate"]),
                    )
                )

            if len(funding_list) < page_size:
                break

            page_no += 1

        # Sort and yield in chronological order
        for fr in sorted(all_funding, key=lambda x: x.funding_time):
            yield fr
