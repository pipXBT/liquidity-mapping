"""Funding rate analysis utilities."""

import pandas as pd

from src.db.models import FundingRateModel


def calculate_rolling_avg_funding(
    funding_rates: list[FundingRateModel],
    window_periods: int = 3,
) -> pd.DataFrame:
    """Calculate rolling average funding rate over N funding periods.

    Args:
        funding_rates: List of funding rate data from database
        window_periods: Number of funding periods for rolling average (default 3 = 24h)

    Returns:
        DataFrame with funding_time, rolling_avg_rate, and annualized_rate columns
    """
    if not funding_rates:
        return pd.DataFrame(columns=["funding_time", "rolling_avg_rate", "annualized_rate"])

    df = pd.DataFrame([
        {
            "exchange": fr.exchange,
            "funding_time": fr.funding_time,
            "funding_rate": fr.funding_rate,
        }
        for fr in funding_rates
    ]).sort_values("funding_time")

    if df.empty:
        return pd.DataFrame(columns=["funding_time", "rolling_avg_rate", "annualized_rate"])

    # Calculate rolling average across all exchanges per timestamp
    # First, pivot to get funding rate per exchange at each timestamp
    pivot_df = df.pivot_table(
        index="funding_time",
        columns="exchange",
        values="funding_rate",
        aggfunc="mean",
    )

    # Average across exchanges for each timestamp
    avg_rate = pivot_df.mean(axis=1)

    # Apply rolling average
    rolling_avg = avg_rate.rolling(window=window_periods, min_periods=1).mean()

    # Calculate annualized rate: rate * 3 (per day) * 365 (per year) * 100 (percentage)
    annualized = rolling_avg * 3 * 365 * 100

    result = pd.DataFrame({
        "funding_time": rolling_avg.index,
        "rolling_avg_rate": rolling_avg.values,
        "annualized_rate": annualized.values,
    })

    return result


def get_latest_funding_stats(
    funding_rates: list[FundingRateModel],
    window_periods: int = 3,
) -> dict:
    """Get funding statistics averaged across the entire date range.

    Args:
        funding_rates: List of funding rate data
        window_periods: Window for rolling average (unused, kept for compatibility)

    Returns:
        Dict with avg_rate, annualized_rate, and per-exchange rates
    """
    if not funding_rates:
        return {
            "avg_rate": None,
            "annualized_rate": None,
            "per_exchange": {},
        }

    df = pd.DataFrame([
        {
            "exchange": fr.exchange,
            "funding_time": fr.funding_time,
            "funding_rate": fr.funding_rate,
        }
        for fr in funding_rates
    ]).sort_values("funding_time")

    # Get latest rate per exchange
    latest_per_exchange = {}
    for exchange in df["exchange"].unique():
        exchange_df = df[df["exchange"] == exchange]
        if not exchange_df.empty:
            latest = exchange_df.iloc[-1]
            latest_per_exchange[exchange] = {
                "rate": latest["funding_rate"],
                "time": latest["funding_time"],
            }

    # Calculate average across entire range
    # First average across exchanges per timestamp, then average all timestamps
    pivot_df = df.pivot_table(
        index="funding_time",
        columns="exchange",
        values="funding_rate",
        aggfunc="mean",
    )

    # Average across exchanges for each timestamp, then average all timestamps
    avg_rate_per_timestamp = pivot_df.mean(axis=1)
    overall_avg_rate = avg_rate_per_timestamp.mean()

    # Annualized: rate * 3 (per day) * 365 (per year) * 100 (percentage)
    annualized_rate = overall_avg_rate * 3 * 365 * 100

    return {
        "avg_rate": overall_avg_rate,
        "annualized_rate": annualized_rate,
        "per_exchange": latest_per_exchange,
    }
