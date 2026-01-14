"""VWAP calculation utilities."""

import pandas as pd

from src.db.models import KlineModel


def calculate_vwap(klines: list[KlineModel]) -> float:
    """Calculate Volume Weighted Average Price.

    VWAP = Σ(Typical Price × Volume) / Σ(Volume)
    where Typical Price = (High + Low + Close) / 3

    Args:
        klines: List of kline data

    Returns:
        VWAP value, or 0 if no data
    """
    if not klines:
        return 0.0

    df = pd.DataFrame([
        {
            "high": k.high,
            "low": k.low,
            "close": k.close,
            "volume": k.volume,
        }
        for k in klines
    ])

    if df.empty or df["volume"].sum() == 0:
        return 0.0

    typical_price = (df["high"] + df["low"] + df["close"]) / 3
    return float((typical_price * df["volume"]).sum() / df["volume"].sum())


def calculate_rolling_vwap(
    klines: list[KlineModel],
    window_hours: int = 24,
) -> pd.DataFrame:
    """Calculate rolling VWAP over a time window.

    Args:
        klines: List of kline data
        window_hours: Rolling window size in hours

    Returns:
        DataFrame with open_time and rolling_vwap columns
    """
    if not klines:
        return pd.DataFrame(columns=["open_time", "rolling_vwap"])

    df = pd.DataFrame([
        {
            "open_time": k.open_time,
            "high": k.high,
            "low": k.low,
            "close": k.close,
            "volume": k.volume,
        }
        for k in klines
    ]).sort_values("open_time")

    if df.empty:
        return pd.DataFrame(columns=["open_time", "rolling_vwap"])

    # Calculate typical price
    df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
    df["tp_volume"] = df["typical_price"] * df["volume"]

    # Rolling sum (assuming hourly data, window = window_hours candles)
    df["rolling_tp_vol"] = df["tp_volume"].rolling(window=window_hours, min_periods=1).sum()
    df["rolling_vol"] = df["volume"].rolling(window=window_hours, min_periods=1).sum()
    df["rolling_vwap"] = df["rolling_tp_vol"] / df["rolling_vol"]

    return df[["open_time", "rolling_vwap"]].copy()
