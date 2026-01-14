"""Terminal output using rich tables."""

import pandas as pd
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from src.analysis.calculator import AnalysisResult, calculate_aggregated_deltas
from src.output.plots import display_price_volume_plot


console = Console()


def display_analysis(result: AnalysisResult) -> None:
    """Display analysis results in terminal.

    Args:
        result: AnalysisResult from calculator
    """
    # Header
    console.print()
    console.print(Panel(
        f"[bold cyan]{result.symbol}[/bold cyan] Analysis: "
        f"{result.start_time.strftime('%Y-%m-%d %H:%M')} to "
        f"{result.end_time.strftime('%Y-%m-%d %H:%M')}",
        title="Liquidity Analysis",
        border_style="cyan",
    ))

    if not result.exchange_analyses:
        console.print("[yellow]No data available for analysis[/yellow]")
        return

    # Display each exchange analysis
    for analysis in result.exchange_analyses:
        _display_exchange_analysis(analysis)

    # Display aggregated analysis (after individual exchanges)
    market_types = set(a.market_type for a in result.exchange_analyses)
    for market_type in sorted(market_types):
        # Only show aggregate if more than 1 exchange has data for this type
        exchanges_with_type = [a for a in result.exchange_analyses if a.market_type == market_type]
        if len(exchanges_with_type) > 1:
            _display_aggregated_analysis(result.exchange_analyses, market_type)

    # Display raw data summary
    _display_raw_data_summary(result)

    # Display price/volume time series plot
    display_price_volume_plot(result, console)


def _display_exchange_analysis(analysis) -> None:
    """Display analysis for a single exchange/market.

    Args:
        analysis: ExchangeAnalysis object
    """
    title = f"{analysis.exchange.upper()} {analysis.market_type.upper()}"
    console.print()
    console.print(f"[bold green]{title}[/bold green]")

    # Create delta table
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Metric", style="cyan")

    # Add timeframe columns
    for delta in analysis.timeframe_deltas:
        table.add_column(f"{delta.timeframe} Δ", justify="right")

    # Price row
    price_cells = []
    for delta in analysis.timeframe_deltas:
        sign = "+" if delta.price_delta_pct >= 0 else ""
        color = "green" if delta.price_delta_pct >= 0 else "red"
        price_cells.append(f"[{color}]{sign}{delta.price_delta_pct:.2f}%[/{color}]")
    table.add_row("Price", *price_cells)

    # Volume row
    volume_cells = []
    for delta in analysis.timeframe_deltas:
        volume_cells.append(_format_number(delta.volume_total))
    table.add_row("Volume", *volume_cells)

    # OI row (if available)
    oi_cells = []
    has_oi = any(d.oi_delta is not None for d in analysis.timeframe_deltas)
    if has_oi:
        for delta in analysis.timeframe_deltas:
            if delta.oi_delta is not None:
                sign = "+" if delta.oi_delta >= 0 else ""
                color = "green" if delta.oi_delta >= 0 else "red"
                oi_cells.append(f"[{color}]{sign}{_format_number(delta.oi_delta)}[/{color}]")
            else:
                oi_cells.append("-")
        table.add_row("OI", *oi_cells)

    # VWAP row
    vwap_cells = []
    for delta in analysis.timeframe_deltas:
        vwap_cells.append(f"${delta.vwap:.6f}")
    table.add_row("VWAP", *vwap_cells)

    console.print(table)


def _display_aggregated_analysis(
    exchange_analyses: list,
    market_type: str,
) -> None:
    """Display aggregated analysis across all exchanges for a market type.

    Args:
        exchange_analyses: List of ExchangeAnalysis objects
        market_type: Market type to aggregate
    """
    agg_deltas = calculate_aggregated_deltas(exchange_analyses, market_type)
    if not agg_deltas:
        return

    title = f"ALL EXCHANGES {market_type.upper()} (aggregated)"
    console.print()
    console.print(f"[bold magenta]{title}[/bold magenta]")

    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Metric", style="cyan")

    for delta in agg_deltas:
        table.add_column(f"{delta.timeframe} Δ", justify="right")

    # Price row (volume-weighted)
    price_cells = []
    for delta in agg_deltas:
        sign = "+" if delta.price_delta_pct >= 0 else ""
        color = "green" if delta.price_delta_pct >= 0 else "red"
        price_cells.append(f"[{color}]{sign}{delta.price_delta_pct:.2f}%[/{color}]")
    table.add_row("Price (VWAP)", *price_cells)

    # Volume row (total)
    volume_cells = [_format_number(d.volume_total) for d in agg_deltas]
    table.add_row("Volume", *volume_cells)

    # OI row (total)
    has_oi = any(d.oi_delta is not None for d in agg_deltas)
    if has_oi:
        oi_cells = []
        for delta in agg_deltas:
            if delta.oi_delta is not None:
                sign = "+" if delta.oi_delta >= 0 else ""
                color = "green" if delta.oi_delta >= 0 else "red"
                oi_cells.append(f"[{color}]{sign}{_format_number(delta.oi_delta)}[/{color}]")
            else:
                oi_cells.append("-")
        table.add_row("OI", *oi_cells)

    # VWAP row
    vwap_cells = [f"${d.vwap:.6f}" for d in agg_deltas]
    table.add_row("VWAP", *vwap_cells)

    console.print(table)


def _display_raw_data_summary(result: AnalysisResult) -> None:
    """Display raw data summary table aggregated by day.

    Args:
        result: AnalysisResult object
    """
    if result.raw_klines.empty:
        return

    console.print()
    console.print("[bold yellow]RAW DATA (daily)[/bold yellow]")

    df = result.raw_klines.copy()
    df["date"] = df["open_time"].dt.date

    # Calculate typical price and VWAP components
    df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
    df["tp_volume"] = df["typical_price"] * df["volume"]

    # Aggregate by date, exchange, market_type
    daily = df.groupby(["date", "exchange", "market_type"]).agg({
        "volume": "sum",           # Total daily volume (tokens)
        "quote_volume": "sum",     # Total daily volume (USD)
        "tp_volume": "sum",        # Sum of typical_price * volume for VWAP
    }).reset_index()

    # Calculate VWAP
    daily["vwap"] = daily["tp_volume"] / daily["volume"]
    daily = daily.sort_values("date")

    # Get OI data by date and exchange
    oi_df = result.raw_oi.copy()
    if not oi_df.empty:
        oi_df["date"] = oi_df["timestamp"].dt.date
        oi_daily = oi_df.groupby(["date", "exchange"]).agg({
            "open_interest": "last"  # End of day OI per exchange
        }).reset_index()
        daily = daily.merge(oi_daily, on=["date", "exchange"], how="left")
    else:
        daily["open_interest"] = None

    # Build table with new columns
    table = Table(show_header=True, header_style="bold")
    table.add_column("Date", style="dim")
    table.add_column("Exchange")
    table.add_column("Type")
    table.add_column("VWAP", justify="right")
    table.add_column("Volume", justify="right")
    table.add_column("Volume USD", justify="right")
    table.add_column("OI", justify="right")

    # Iterate by date and add totals for each day
    dates = daily["date"].unique()
    for date in dates:
        day_rows = daily[daily["date"] == date]

        # Display each exchange row for this day
        for row in day_rows.itertuples():
            oi_str = _format_number(row.open_interest) if pd.notna(row.open_interest) else "-"
            table.add_row(
                str(row.date),
                row.exchange,
                row.market_type,
                f"${row.vwap:.6f}",
                _format_number(row.volume),
                _format_number(row.quote_volume),
                oi_str,
            )

        # Add daily total row
        total_volume = day_rows["volume"].sum()
        total_quote = day_rows["quote_volume"].sum()
        total_tp_volume = day_rows["tp_volume"].sum()
        total_vwap = total_tp_volume / total_volume if total_volume > 0 else 0
        total_oi = day_rows["open_interest"].dropna().sum()
        total_oi_str = f"[bold yellow]{_format_number(total_oi)}[/bold yellow]" if total_oi > 0 else "-"
        table.add_row(
            str(date),
            "[bold yellow]TOTAL[/bold yellow]",
            "-",
            f"[bold yellow]${total_vwap:.6f}[/bold yellow]",
            f"[bold yellow]{_format_number(total_volume)}[/bold yellow]",
            f"[bold yellow]{_format_number(total_quote)}[/bold yellow]",
            total_oi_str,
        )

    console.print(table)

    # Print totals
    total_volume = daily["volume"].sum()
    total_volume_usd = daily["quote_volume"].sum()
    oi_values = daily["open_interest"].dropna()
    latest_oi = oi_values.iloc[-1] if not oi_values.empty else None

    console.print()
    console.print("[bold cyan]TOTALS[/bold cyan]")
    console.print(f"  Total Volume:     {_format_number(total_volume)}")
    console.print(f"  Total Volume USD: ${_format_number(total_volume_usd)}")
    if latest_oi is not None:
        console.print(f"  Latest OI:        {_format_number(latest_oi)}")


def _format_number(value: float) -> str:
    """Format large numbers with K/M/B suffixes.

    Args:
        value: Number to format

    Returns:
        Formatted string
    """
    abs_value = abs(value)
    if abs_value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.2f}B"
    elif abs_value >= 1_000_000:
        return f"{value / 1_000_000:.2f}M"
    elif abs_value >= 1_000:
        return f"{value / 1_000:.2f}K"
    else:
        return f"{value:.2f}"


def display_data_summary(
    symbol: str,
    kline_count: int,
    oi_count: int,
    earliest: str | None,
    latest: str | None,
    exchanges: dict[str, bool],
    exchange_date_ranges: dict[str, tuple[str, str]] | None = None,
    funding_count: int = 0,
) -> None:
    """Display summary of fetched data.

    Args:
        symbol: Trading symbol
        kline_count: Number of klines stored
        oi_count: Number of OI snapshots stored
        earliest: Earliest data date
        latest: Latest data date
        exchanges: Exchange availability dict
        exchange_date_ranges: Optional per-exchange date ranges
        funding_count: Number of funding rate records stored
    """
    console.print()
    console.print(Panel(
        f"[bold]{symbol}[/bold] Data Summary",
        border_style="green",
    ))

    console.print(f"  Klines stored: [cyan]{kline_count:,}[/cyan]")
    console.print(f"  OI snapshots: [cyan]{oi_count:,}[/cyan]")
    console.print(f"  Funding rates: [cyan]{funding_count:,}[/cyan]")

    if earliest and latest:
        console.print(f"  Date range: [yellow]{earliest}[/yellow] to [yellow]{latest}[/yellow]")

    exchange_str = "  ".join(
        f"[green]{ex} ✓[/green]" if avail else f"[red]{ex} ✗[/red]"
        for ex, avail in exchanges.items()
    )
    console.print(f"  Exchanges: {exchange_str}")

    # Show per-exchange date ranges if available
    if exchange_date_ranges:
        console.print()
        console.print("  [dim]Per-exchange data availability:[/dim]")
        for ex, (ex_earliest, ex_latest) in sorted(exchange_date_ranges.items()):
            console.print(f"    {ex}: [dim]{ex_earliest}[/dim] to [dim]{ex_latest}[/dim]")


def display_funding_stats(funding_stats: dict) -> None:
    """Display funding rate statistics.

    Args:
        funding_stats: Dict with funding statistics from get_latest_funding_stats()
    """
    console.print()
    console.print(Panel(
        "[bold cyan]FUNDING RATES[/bold cyan]",
        border_style="cyan",
    ))

    avg_rate = funding_stats.get("avg_rate")
    annualized = funding_stats.get("annualized_rate")

    if avg_rate is not None:
        # Color based on positive/negative
        rate_pct = avg_rate * 100
        color = "green" if avg_rate >= 0 else "red"
        sign = "+" if avg_rate >= 0 else ""

        console.print(f"  Avg Funding (full range): [{color}]{sign}{rate_pct:.4f}%[/{color}]")

        if annualized is not None:
            ann_color = "green" if annualized >= 0 else "red"
            ann_sign = "+" if annualized >= 0 else ""
            console.print(f"  Annualized Rate:          [{ann_color}]{ann_sign}{annualized:.2f}%[/{ann_color}]")
    else:
        console.print("  [dim]No funding data available[/dim]")

    # Per-exchange breakdown
    per_exchange = funding_stats.get("per_exchange", {})
    if per_exchange:
        console.print()
        console.print("  [dim]Latest per exchange:[/dim]")

        table = Table(show_header=True, header_style="bold", box=None, padding=(0, 2))
        table.add_column("Exchange", style="dim")
        table.add_column("Rate", justify="right")
        table.add_column("Time", style="dim")

        for exchange, data in sorted(per_exchange.items()):
            rate = data["rate"]
            rate_pct = rate * 100
            color = "green" if rate >= 0 else "red"
            sign = "+" if rate >= 0 else ""
            time_str = data["time"].strftime("%Y-%m-%d %H:%M") if data["time"] else "-"
            table.add_row(
                exchange,
                f"[{color}]{sign}{rate_pct:.4f}%[/{color}]",
                time_str,
            )

        console.print(table)
