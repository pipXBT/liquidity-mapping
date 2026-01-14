"""Delta calculations for price, volume, and OI across timeframes."""

from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd

from src.db.models import FundingRateModel, KlineModel, OpenInterestModel


@dataclass
class TimeframeDelta:
    """Delta values for a single timeframe."""

    timeframe: str  # e.g., "1h", "4h", "12h", "24h"
    price_start: float
    price_end: float
    price_delta: float
    price_delta_pct: float
    volume_total: float
    oi_start: float | None
    oi_end: float | None
    oi_delta: float | None
    vwap: float


@dataclass
class ExchangeAnalysis:
    """Analysis results for a single exchange/market."""

    exchange: str
    market_type: str
    timeframe_deltas: list[TimeframeDelta]


@dataclass
class AnalysisResult:
    """Complete analysis result for a symbol."""

    symbol: str
    start_time: datetime
    end_time: datetime
    exchange_analyses: list[ExchangeAnalysis]
    raw_klines: pd.DataFrame
    raw_oi: pd.DataFrame
    raw_funding: pd.DataFrame | None = None


def calculate_deltas(
    klines: list[KlineModel],
    oi_data: list[OpenInterestModel],
    start_time: datetime,
    end_time: datetime,
    timeframes: list[str] | None = None,
    funding_data: list[FundingRateModel] | None = None,
) -> AnalysisResult:
    """Calculate delta metrics across timeframes.

    Args:
        klines: List of kline data from database
        oi_data: List of OI data from database
        start_time: Analysis start time
        end_time: Analysis end time
        timeframes: Timeframes to calculate (default: 1h, 4h, 12h, 24h)
        funding_data: List of funding rate data from database

    Returns:
        AnalysisResult with all calculations
    """
    if timeframes is None:
        timeframes = ["1h", "4h", "12h", "24h"]

    # Convert to DataFrames
    kline_df = pd.DataFrame([
        {
            "exchange": k.exchange,
            "market_type": k.market_type,
            "open_time": k.open_time,
            "open": k.open,
            "high": k.high,
            "low": k.low,
            "close": k.close,
            "volume": k.volume,
            "quote_volume": k.quote_volume,
        }
        for k in klines
    ]) if klines else pd.DataFrame()

    oi_df = pd.DataFrame([
        {
            "exchange": o.exchange,
            "timestamp": o.timestamp,
            "open_interest": o.open_interest,
            "open_interest_value": o.open_interest_value,
        }
        for o in oi_data
    ]) if oi_data else pd.DataFrame()

    funding_df = pd.DataFrame([
        {
            "exchange": f.exchange,
            "funding_time": f.funding_time,
            "funding_rate": f.funding_rate,
        }
        for f in funding_data
    ]) if funding_data else None

    exchange_analyses = []

    if not kline_df.empty:
        # Group by exchange and market type
        for (exchange, market_type), group in kline_df.groupby(["exchange", "market_type"]):
            group = group.sort_values("open_time")

            # Get OI for this exchange
            exchange_oi = oi_df[oi_df["exchange"] == exchange] if not oi_df.empty else pd.DataFrame()

            timeframe_deltas = []
            for tf in timeframes:
                delta = _calculate_timeframe_delta(
                    group, exchange_oi, tf, start_time, end_time
                )
                if delta:
                    timeframe_deltas.append(delta)

            if timeframe_deltas:
                exchange_analyses.append(ExchangeAnalysis(
                    exchange=str(exchange),
                    market_type=str(market_type),
                    timeframe_deltas=timeframe_deltas,
                ))

    # Get symbol from first kline
    symbol = klines[0].symbol if klines else ""

    return AnalysisResult(
        symbol=symbol,
        start_time=start_time,
        end_time=end_time,
        exchange_analyses=exchange_analyses,
        raw_klines=kline_df,
        raw_oi=oi_df,
        raw_funding=funding_df,
    )


def _calculate_timeframe_delta(
    kline_df: pd.DataFrame,
    oi_df: pd.DataFrame,
    timeframe: str,
    start_time: datetime,
    end_time: datetime,
) -> TimeframeDelta | None:
    """Calculate delta for a single timeframe.

    Args:
        kline_df: Kline DataFrame for one exchange/market
        oi_df: OI DataFrame for one exchange
        timeframe: Timeframe string (1h, 4h, 12h, 24h)
        start_time: Analysis start
        end_time: Analysis end

    Returns:
        TimeframeDelta or None if insufficient data
    """
    # Parse timeframe to hours
    hours = _parse_timeframe(timeframe)
    tf_duration = timedelta(hours=hours)

    # Filter to timeframe window from end
    tf_start = end_time - tf_duration
    if tf_start < start_time:
        tf_start = start_time

    mask = (kline_df["open_time"] >= tf_start) & (kline_df["open_time"] <= end_time)
    tf_klines = kline_df[mask].sort_values("open_time")

    if tf_klines.empty:
        return None

    # Price delta
    price_start = tf_klines.iloc[0]["open"]
    price_end = tf_klines.iloc[-1]["close"]
    price_delta = price_end - price_start
    price_delta_pct = (price_delta / price_start * 100) if price_start != 0 else 0

    # Volume total
    volume_total = tf_klines["volume"].sum()

    # VWAP
    typical_price = (tf_klines["high"] + tf_klines["low"] + tf_klines["close"]) / 3
    vwap = (typical_price * tf_klines["volume"]).sum() / volume_total if volume_total > 0 else 0

    # OI delta
    oi_start = None
    oi_end = None
    oi_delta = None

    if not oi_df.empty:
        oi_mask = (oi_df["timestamp"] >= tf_start) & (oi_df["timestamp"] <= end_time)
        tf_oi = oi_df[oi_mask].sort_values("timestamp")
        if not tf_oi.empty:
            oi_start = tf_oi.iloc[0]["open_interest"]
            oi_end = tf_oi.iloc[-1]["open_interest"]
            oi_delta = oi_end - oi_start

    return TimeframeDelta(
        timeframe=timeframe,
        price_start=price_start,
        price_end=price_end,
        price_delta=price_delta,
        price_delta_pct=price_delta_pct,
        volume_total=volume_total,
        oi_start=oi_start,
        oi_end=oi_end,
        oi_delta=oi_delta,
        vwap=vwap,
    )


def _parse_timeframe(tf: str) -> int:
    """Parse timeframe string to hours.

    Args:
        tf: Timeframe string (1h, 4h, 12h, 24h)

    Returns:
        Number of hours
    """
    mapping = {
        "1h": 1,
        "4h": 4,
        "12h": 12,
        "24h": 24,
    }
    return mapping.get(tf, 1)


def calculate_aggregated_deltas(
    exchange_analyses: list[ExchangeAnalysis],
    market_type: str,
) -> list[TimeframeDelta] | None:
    """Calculate aggregated metrics across all exchanges for a market type.

    Uses volume-weighted average for price metrics.

    Args:
        exchange_analyses: List of ExchangeAnalysis objects
        market_type: Market type to aggregate (spot or perp)

    Returns:
        List of aggregated TimeframeDelta objects or None
    """
    # Filter to matching market type
    matching = [ea for ea in exchange_analyses if ea.market_type == market_type]
    if not matching:
        return None

    timeframes = ["1h", "4h", "12h", "24h"]
    aggregated = []

    for tf in timeframes:
        # Collect deltas for this timeframe from all exchanges
        tf_deltas = []
        for ea in matching:
            for d in ea.timeframe_deltas:
                if d.timeframe == tf:
                    tf_deltas.append(d)

        if not tf_deltas:
            continue

        # Volume-weighted price delta
        total_vol = sum(d.volume_total for d in tf_deltas)
        if total_vol > 0:
            weighted_price_pct = sum(d.price_delta_pct * d.volume_total for d in tf_deltas) / total_vol
            weighted_vwap = sum(d.vwap * d.volume_total for d in tf_deltas) / total_vol
        else:
            weighted_price_pct = sum(d.price_delta_pct for d in tf_deltas) / len(tf_deltas)
            weighted_vwap = sum(d.vwap for d in tf_deltas) / len(tf_deltas)

        # Sum OI deltas
        oi_deltas = [d.oi_delta for d in tf_deltas if d.oi_delta is not None]
        total_oi_delta = sum(oi_deltas) if oi_deltas else None

        aggregated.append(TimeframeDelta(
            timeframe=tf,
            price_start=0,  # Not meaningful for aggregate
            price_end=0,
            price_delta=0,
            price_delta_pct=weighted_price_pct,
            volume_total=total_vol,
            oi_start=None,
            oi_end=None,
            oi_delta=total_oi_delta,
            vwap=weighted_vwap,
        ))

    return aggregated if aggregated else None
