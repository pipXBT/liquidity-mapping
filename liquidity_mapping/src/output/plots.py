"""Terminal plotting for price, VWAP, and volume visualization."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
import plotext as plt
from rich.ansi import AnsiDecoder
from rich.console import Console, ConsoleOptions, Group, RenderResult
from rich.jupyter import JupyterMixin
from rich.panel import Panel

if TYPE_CHECKING:
    from src.analysis.calculator import AnalysisResult
    from src.db.models import FundingRateModel


class PlotextMixin(JupyterMixin):
    """Mixin class to render plotext plots within Rich console."""

    def __init__(self, plot_string: str) -> None:
        """Initialize with pre-rendered plot string.

        Args:
            plot_string: ANSI-encoded plot output from plotext
        """
        self.plot_string = plot_string
        self.decoder = AnsiDecoder()

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:
        """Render the plot for Rich console."""
        yield Group(*self.decoder.decode(self.plot_string))


def prepare_plot_data(raw_klines: pd.DataFrame) -> pd.DataFrame:
    """Prepare hourly data for plotting.

    Calculates VWAP per candle and aggregates by hour across exchanges.

    Args:
        raw_klines: DataFrame with columns: open_time, close, high, low,
                    volume, quote_volume, exchange, market_type

    Returns:
        DataFrame with columns: open_time, close, vwap, volume_usd
        Aggregated across all exchanges/market types per hour.
    """
    if raw_klines.empty:
        return pd.DataFrame(columns=["open_time", "close", "vwap", "volume_usd"])

    df = raw_klines.copy()

    # Calculate typical price (for VWAP) per candle
    df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
    df["tp_volume"] = df["typical_price"] * df["volume"]

    # Aggregate by hour (combine all exchanges/market types)
    hourly = df.groupby("open_time").agg({
        "close": "mean",
        "volume": "sum",
        "quote_volume": "sum",
        "tp_volume": "sum",
    }).reset_index()

    # Calculate aggregate VWAP per hour
    hourly["vwap"] = hourly["tp_volume"] / hourly["volume"]
    hourly["vwap"] = hourly["vwap"].fillna(hourly["close"])

    # Rename for clarity
    hourly = hourly.rename(columns={"quote_volume": "volume_usd"})
    hourly = hourly.sort_values("open_time")

    # Downsample if too many points
    max_points = 100
    if len(hourly) > max_points:
        step = len(hourly) // max_points
        hourly = hourly.iloc[::step].reset_index(drop=True)

    return hourly[["open_time", "close", "vwap", "volume_usd"]]


def _create_single_plot(
    x_indices: list,
    y_data: list,
    tick_indices: list,
    tick_labels: list,
    width: int,
    height: int,
    title: str | None,
    ylabel: str,
    xlabel: str | None,
    label: str,
    color: str,
    plot_type: str = "line",
) -> str:
    """Create a single plot panel.

    Args:
        x_indices: X-axis indices
        y_data: Y-axis data
        tick_indices: X-axis tick positions
        tick_labels: X-axis tick labels
        width: Plot width
        height: Plot height
        title: Plot title (None to skip)
        ylabel: Y-axis label
        xlabel: X-axis label (None to skip)
        label: Data series label
        color: Plot color
        plot_type: "line" or "bar"

    Returns:
        ANSI-encoded plot string
    """
    plt.clf()
    plt.clear_figure()
    plt.clear_data()
    plt.clear_color()
    plt.theme("dark")
    plt.plotsize(width, height)

    if title:
        plt.title(title)

    if plot_type == "bar":
        plt.bar(x_indices, y_data, label=label, color=color, width=0.8)
    else:
        plt.plot(x_indices, y_data, label=label, color=color)

    plt.ylabel(ylabel)
    if xlabel:
        plt.xlabel(xlabel)
    plt.xticks(tick_indices, tick_labels)

    return plt.build()


def _format_axis_value(value: float) -> str:
    """Format large numbers with K/M/B suffixes for axis labels.

    Args:
        value: Number to format

    Returns:
        Formatted string with suffix
    """
    abs_value = abs(value)
    if abs_value >= 1_000_000_000:
        return f"{value / 1_000_000_000:.1f}B"
    elif abs_value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    elif abs_value >= 1_000:
        return f"{value / 1_000:.1f}K"
    else:
        return f"{value:.2f}"


def create_price_volume_plot(
    df: pd.DataFrame,
    width: int = 100,
    height: int = 25,
    title: str = "VWAP & Volume (1H)",
) -> tuple[str, str]:
    """Create two separate plots: VWAP and Volume.

    Args:
        df: DataFrame from prepare_plot_data() with open_time, close, vwap, volume_usd
        width: Plot width in characters
        height: Total height in characters
        title: Plot title

    Returns:
        Tuple of (vwap_plot_string, volume_plot_string)
    """
    if df.empty or len(df) < 2:
        return "", ""

    # Prepare data - use numeric indices for x-axis
    x_indices = list(range(len(df)))
    vwaps = df["vwap"].tolist()
    volumes = df["volume_usd"].tolist()

    # Create date labels for x-axis ticks
    date_labels = [dt.strftime("%d/%m %H:%M") for dt in df["open_time"]]
    num_ticks = min(10, len(x_indices))
    tick_step = max(1, len(x_indices) // num_ticks)
    tick_indices = x_indices[::tick_step]
    tick_labels = date_labels[::tick_step]

    # Calculate heights for each pane
    price_height = int(height * 0.65)
    volume_height = int(height * 0.35)

    # === VWAP Plot ===
    plt.clf()
    plt.clear_figure()
    plt.clear_data()
    plt.clear_color()
    plt.theme("dark")
    plt.plotsize(width, price_height)
    plt.title(title)

    # Plot VWAP as dots
    plt.plot(x_indices, vwaps, label="VWAP", color="cyan", marker="dot")

    # Format y-axis with readable labels
    vwap_min, vwap_max = min(vwaps), max(vwaps)
    vwap_range = vwap_max - vwap_min
    vwap_ticks = [vwap_min + i * vwap_range / 5 for i in range(6)]
    vwap_labels = [f"${_format_axis_value(v)}" for v in vwap_ticks]
    plt.yticks(vwap_ticks, vwap_labels)

    plt.ylabel("VWAP ($)")
    plt.xticks(tick_indices, tick_labels)

    price_plot = plt.build()

    # === Volume Plot ===
    plt.clf()
    plt.clear_figure()
    plt.clear_data()
    plt.clear_color()
    plt.theme("dark")
    plt.plotsize(width, volume_height)

    plt.bar(x_indices, volumes, label="Volume USD", color="green+", width=0.8)

    # Format y-axis with readable labels
    vol_min, vol_max = 0, max(volumes)
    vol_ticks = [i * vol_max / 4 for i in range(5)]
    vol_labels = [f"${_format_axis_value(v)}" for v in vol_ticks]
    plt.yticks(vol_ticks, vol_labels)

    plt.ylabel("Volume ($)")
    plt.xlabel("Time")
    plt.xticks(tick_indices, tick_labels)

    volume_plot = plt.build()

    return price_plot, volume_plot


def display_price_volume_plot(
    result: AnalysisResult,
    console: Console | None = None,
) -> None:
    """Display price/VWAP/volume plot for analysis result.

    Args:
        result: AnalysisResult containing raw_klines DataFrame
        console: Rich Console instance (creates new if None)
    """
    if console is None:
        console = Console()

    if result.raw_klines.empty:
        console.print("[yellow]No data available for plotting[/yellow]")
        return

    # Prepare data
    plot_df = prepare_plot_data(result.raw_klines)

    if plot_df.empty or len(plot_df) < 2:
        console.print("[yellow]Insufficient data points for plotting[/yellow]")
        return

    # Get terminal width for responsive sizing
    terminal_width = console.width or 100
    plot_width = min(terminal_width - 4, 120)
    plot_height = 35

    # Create plot title with date range
    date_range = (
        f"{result.start_time.strftime('%Y-%m-%d %H:%M')} to "
        f"{result.end_time.strftime('%Y-%m-%d %H:%M')}"
    )
    title = f"{result.symbol} VWAP (1H) - {date_range}"

    # Generate plots
    price_plot, volume_plot = create_price_volume_plot(
        df=plot_df,
        width=plot_width,
        height=plot_height,
        title=title,
    )

    if not price_plot or not volume_plot:
        console.print("[yellow]Could not generate plot[/yellow]")
        return

    # Render VWAP panel
    console.print()
    price_renderable = PlotextMixin(price_plot)
    console.print(Panel(
        price_renderable,
        title="[bold cyan]VWAP[/bold cyan]",
        border_style="cyan",
        padding=(0, 1),
    ))

    # Render volume panel
    volume_renderable = PlotextMixin(volume_plot)
    console.print(Panel(
        volume_renderable,
        title="[bold green]Volume (USD)[/bold green]",
        border_style="green",
        padding=(0, 1),
    ))


def prepare_funding_plot_data(
    funding_rates: list[FundingRateModel],
    window_periods: int = 1,
) -> pd.DataFrame:
    """Prepare funding rate data for plotting with rolling average.

    Args:
        funding_rates: List of FundingRateModel objects
        window_periods: Rolling window size (default 1 for 1-period avg)

    Returns:
        DataFrame with funding_time and annualized_rate columns
    """
    if not funding_rates:
        return pd.DataFrame(columns=["funding_time", "annualized_rate"])

    df = pd.DataFrame([
        {
            "exchange": fr.exchange,
            "funding_time": fr.funding_time,
            "funding_rate": fr.funding_rate,
        }
        for fr in funding_rates
    ]).sort_values("funding_time")

    if df.empty:
        return pd.DataFrame(columns=["funding_time", "annualized_rate"])

    # Pivot to get funding rate per exchange at each timestamp
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
        "annualized_rate": annualized.values,
    }).reset_index(drop=True)

    # Downsample if too many points
    max_points = 100
    if len(result) > max_points:
        step = len(result) // max_points
        result = result.iloc[::step].reset_index(drop=True)

    return result


def create_funding_plot(
    df: pd.DataFrame,
    width: int = 100,
    height: int = 20,
    title: str = "Annualized Funding Rate (1H Rolling Avg)",
) -> str:
    """Create a dot plot for annualized funding rate.

    Args:
        df: DataFrame with funding_time and annualized_rate columns
        width: Plot width in characters
        height: Plot height in characters
        title: Plot title

    Returns:
        ANSI-encoded plot string
    """
    if df.empty or len(df) < 2:
        return ""

    # Prepare data
    x_indices = list(range(len(df)))
    rates = df["annualized_rate"].tolist()

    # Create date labels for x-axis ticks
    date_labels = [dt.strftime("%d/%m %H:%M") for dt in df["funding_time"]]
    num_ticks = min(10, len(x_indices))
    tick_step = max(1, len(x_indices) // num_ticks)
    tick_indices = x_indices[::tick_step]
    tick_labels = date_labels[::tick_step]

    # Create plot
    plt.clf()
    plt.clear_figure()
    plt.clear_data()
    plt.clear_color()
    plt.theme("dark")
    plt.plotsize(width, height)
    plt.title(title)

    # Plot as dots - color based on positive/negative
    # Split into positive and negative for different colors
    pos_x = [x for x, r in zip(x_indices, rates) if r >= 0]
    pos_y = [r for r in rates if r >= 0]
    neg_x = [x for x, r in zip(x_indices, rates) if r < 0]
    neg_y = [r for r in rates if r < 0]

    if pos_x:
        plt.scatter(pos_x, pos_y, label="Positive", color="green", marker="dot")
    if neg_x:
        plt.scatter(neg_x, neg_y, label="Negative", color="red", marker="dot")

    # Add zero line
    plt.hline(0, color="white")

    # Format y-axis
    rate_min, rate_max = min(rates), max(rates)
    rate_range = rate_max - rate_min
    if rate_range > 0:
        rate_ticks = [rate_min + i * rate_range / 5 for i in range(6)]
        rate_labels = [f"{r:.1f}%" for r in rate_ticks]
        plt.yticks(rate_ticks, rate_labels)

    plt.ylabel("Annualized Rate (%)")
    plt.xlabel("Time")
    plt.xticks(tick_indices, tick_labels)

    return plt.build()


def display_funding_plot(
    funding_rates: list[FundingRateModel],
    console: Console | None = None,
    symbol: str = "",
) -> None:
    """Display funding rate dot plot.

    Args:
        funding_rates: List of FundingRateModel objects
        console: Rich Console instance (creates new if None)
        symbol: Symbol for title
    """
    if console is None:
        console = Console()

    if not funding_rates:
        console.print("[yellow]No funding data available for plotting[/yellow]")
        return

    # Prepare data with 1-period rolling average (each funding event)
    plot_df = prepare_funding_plot_data(funding_rates, window_periods=1)

    if plot_df.empty or len(plot_df) < 2:
        console.print("[yellow]Insufficient funding data points for plotting[/yellow]")
        return

    # Get terminal width for responsive sizing
    terminal_width = console.width or 100
    plot_width = min(terminal_width - 4, 120)
    plot_height = 20

    # Create title
    title = f"{symbol} Annualized Funding Rate" if symbol else "Annualized Funding Rate"

    # Generate plot
    funding_plot = create_funding_plot(
        df=plot_df,
        width=plot_width,
        height=plot_height,
        title=title,
    )

    if not funding_plot:
        console.print("[yellow]Could not generate funding plot[/yellow]")
        return

    # Render funding panel
    console.print()
    funding_renderable = PlotextMixin(funding_plot)
    console.print(Panel(
        funding_renderable,
        title="[bold magenta]Funding Rate (Annualized)[/bold magenta]",
        border_style="magenta",
        padding=(0, 1),
    ))
