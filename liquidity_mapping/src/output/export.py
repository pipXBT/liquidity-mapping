"""Export analysis results to CSV and JSON."""

import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from src.analysis.calculator import AnalysisResult


def export_csv(result: AnalysisResult, output_dir: Path | None = None) -> Path:
    """Export analysis results to CSV files.

    Creates two files:
    - {symbol}_klines_{timestamp}.csv - Raw kline data
    - {symbol}_analysis_{timestamp}.csv - Calculated deltas

    Args:
        result: AnalysisResult from calculator
        output_dir: Output directory (defaults to cwd)

    Returns:
        Path to the analysis CSV file
    """
    if output_dir is None:
        output_dir = Path.cwd()
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Export raw klines
    if not result.raw_klines.empty:
        kline_path = output_dir / f"{result.symbol}_klines_{timestamp}.csv"
        result.raw_klines.to_csv(kline_path, index=False)

    # Export analysis summary
    analysis_rows = []
    for exchange_analysis in result.exchange_analyses:
        for delta in exchange_analysis.timeframe_deltas:
            analysis_rows.append({
                "symbol": result.symbol,
                "exchange": exchange_analysis.exchange,
                "market_type": exchange_analysis.market_type,
                "timeframe": delta.timeframe,
                "price_start": delta.price_start,
                "price_end": delta.price_end,
                "price_delta": delta.price_delta,
                "price_delta_pct": delta.price_delta_pct,
                "volume_total": delta.volume_total,
                "oi_start": delta.oi_start,
                "oi_end": delta.oi_end,
                "oi_delta": delta.oi_delta,
                "vwap": delta.vwap,
            })

    analysis_df = pd.DataFrame(analysis_rows)
    analysis_path = output_dir / f"{result.symbol}_analysis_{timestamp}.csv"
    analysis_df.to_csv(analysis_path, index=False)

    return analysis_path


def export_analysis_range_csv(result: AnalysisResult, output_dir: Path | None = None) -> Path:
    """Export 1H analysis range data to CSV with Price, VWAP, Funding, Volume, Volume $USD.

    Args:
        result: AnalysisResult from calculator
        output_dir: Output directory (defaults to cwd)

    Returns:
        Path to the CSV file
    """
    if output_dir is None:
        output_dir = Path.cwd()
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if result.raw_klines.empty:
        # Create empty file if no data
        csv_path = output_dir / f"{result.symbol}_1h_analysis_{timestamp}.csv"
        pd.DataFrame().to_csv(csv_path, index=False)
        return csv_path

    # Build the 1H data export
    rows = []
    klines_df = result.raw_klines.copy()
    funding_df = result.raw_funding

    # Sort klines by time
    klines_df = klines_df.sort_values("open_time")

    # Group by exchange and market type
    for (exchange, market_type), group in klines_df.groupby(["exchange", "market_type"]):
        group = group.sort_values("open_time")

        # Get funding data for this exchange
        exchange_funding = None
        if funding_df is not None and not funding_df.empty:
            exchange_funding = funding_df[funding_df["exchange"] == exchange].copy()
            if not exchange_funding.empty:
                exchange_funding = exchange_funding.sort_values("funding_time")

        for _, row in group.iterrows():
            # Calculate VWAP for this candle (typical price)
            typical_price = (row["high"] + row["low"] + row["close"]) / 3
            vwap = typical_price  # Single candle VWAP is just the typical price

            # Find funding rate for this hour (funding is typically every 8h, so get nearest)
            funding_rate = None
            if exchange_funding is not None and not exchange_funding.empty:
                # Find funding rate that applies to this hour
                open_time = row["open_time"]
                # Get the most recent funding rate at or before this time
                prior_funding = exchange_funding[exchange_funding["funding_time"] <= open_time]
                if not prior_funding.empty:
                    funding_rate = prior_funding.iloc[-1]["funding_rate"]

            rows.append({
                "timestamp": row["open_time"],
                "exchange": exchange,
                "market_type": market_type,
                "price": row["close"],
                "vwap": round(vwap, 6),
                "funding": funding_rate,
                "volume": row["volume"],
                "volume_usd": row["quote_volume"],
            })

    # Create DataFrame and export
    export_df = pd.DataFrame(rows)
    export_df = export_df.sort_values(["timestamp", "exchange", "market_type"])

    csv_path = output_dir / f"{result.symbol}_1h_analysis_{timestamp}.csv"
    export_df.to_csv(csv_path, index=False)

    return csv_path


def export_json(result: AnalysisResult, output_dir: Path | None = None) -> Path:
    """Export analysis results to JSON file.

    Args:
        result: AnalysisResult from calculator
        output_dir: Output directory (defaults to cwd)

    Returns:
        Path to the JSON file
    """
    if output_dir is None:
        output_dir = Path.cwd()
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    output = {
        "symbol": result.symbol,
        "analysis_period": {
            "start": result.start_time.isoformat(),
            "end": result.end_time.isoformat(),
        },
        "exchanges": [],
    }

    for exchange_analysis in result.exchange_analyses:
        exchange_data = {
            "exchange": exchange_analysis.exchange,
            "market_type": exchange_analysis.market_type,
            "timeframes": [],
        }

        for delta in exchange_analysis.timeframe_deltas:
            exchange_data["timeframes"].append({
                "timeframe": delta.timeframe,
                "price": {
                    "start": delta.price_start,
                    "end": delta.price_end,
                    "delta": delta.price_delta,
                    "delta_pct": delta.price_delta_pct,
                },
                "volume_total": delta.volume_total,
                "open_interest": {
                    "start": delta.oi_start,
                    "end": delta.oi_end,
                    "delta": delta.oi_delta,
                } if delta.oi_start is not None else None,
                "vwap": delta.vwap,
            })

        output["exchanges"].append(exchange_data)

    # Add raw data summary
    output["raw_data"] = {
        "kline_count": len(result.raw_klines) if not result.raw_klines.empty else 0,
        "oi_count": len(result.raw_oi) if not result.raw_oi.empty else 0,
    }

    json_path = output_dir / f"{result.symbol}_analysis_{timestamp}.json"
    with open(json_path, "w") as f:
        json.dump(output, f, indent=2, default=str)

    return json_path
