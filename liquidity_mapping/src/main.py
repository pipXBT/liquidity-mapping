"""Main entry point for Liquidity Mapping CLI."""

import asyncio
import sys

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from src.connectors import BinanceConnector, BybitConnector, BitgetConnector
from src.connectors.base import FundingRate, Kline, MarketType, OpenInterest
from src.db import init_db, Repository
from src.analysis import calculate_deltas, AnalysisResult
from src.analysis.funding import get_latest_funding_stats
from src.output import display_analysis, export_csv, export_json, export_analysis_range_csv
from src.output.terminal import display_data_summary, display_funding_stats
from src.output.plots import display_funding_plot
from src import menu


console = Console()


# Global state for current session
class AppState:
    """Application state container."""

    def __init__(self):
        self.current_symbol: str | None = None
        self.last_analysis: AnalysisResult | None = None
        self.repository: Repository | None = None


state = AppState()


CONNECTORS = {
    "binance": BinanceConnector,
    "bybit": BybitConnector,
    "bitget": BitgetConnector,
}


async def fetch_token_data(symbol: str, exchanges: list[str], market_types: list[str]) -> None:
    """Fetch all data for a token from selected exchanges.

    Args:
        symbol: Trading pair symbol (e.g., COAIUSDT)
        exchanges: List of exchange names
        market_types: List of market types (spot, perp)
    """
    repo = state.repository
    if repo is None:
        repo = Repository()
        state.repository = repo

    total_klines = 0
    total_oi = 0
    total_funding = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        for exchange_name in exchanges:
            connector_cls = CONNECTORS.get(exchange_name)
            if not connector_cls:
                continue

            connector = connector_cls()
            try:
                for market_type_str in market_types:
                    market_type = MarketType(market_type_str)
                    task_desc = f"Fetching {exchange_name} {market_type_str}..."
                    task = progress.add_task(task_desc, total=None)

                    # Fetch klines
                    klines: list[Kline] = []
                    kline_success = False
                    try:
                        async for kline in connector.fetch_klines(
                            symbol=symbol,
                            interval="1h",
                            market_type=market_type,
                        ):
                            klines.append(kline)
                            if len(klines) >= 100:
                                await repo.upsert_klines(klines)
                                total_klines += len(klines)
                                klines = []

                        if klines:
                            await repo.upsert_klines(klines)
                            total_klines += len(klines)
                        kline_success = True
                    except Exception as e:
                        console.print(f"[red]✗ {exchange_name} {market_type_str} klines: {e}[/red]")

                    if kline_success:
                        progress.update(task, description=f"[green]✓ {exchange_name} {market_type_str} klines ({len(klines)} saved)[/green]")
                    else:
                        progress.update(task, description=f"[red]✗ {exchange_name} {market_type_str} klines (failed)[/red]")

                # Fetch OI (perpetual only)
                if "perp" in market_types:
                    oi_task = progress.add_task(f"Fetching {exchange_name} OI...", total=None)
                    oi_data: list[OpenInterest] = []
                    oi_success = False
                    try:
                        async for oi in connector.fetch_open_interest_history(symbol=symbol):
                            oi_data.append(oi)
                            if len(oi_data) >= 100:
                                await repo.upsert_open_interest(oi_data)
                                total_oi += len(oi_data)
                                oi_data = []

                        if oi_data:
                            await repo.upsert_open_interest(oi_data)
                            total_oi += len(oi_data)
                        oi_success = True
                    except Exception as e:
                        console.print(f"[red]✗ {exchange_name} OI: {e}[/red]")

                    if oi_success:
                        progress.update(oi_task, description=f"[green]✓ {exchange_name} OI ({total_oi} saved)[/green]")
                    else:
                        progress.update(oi_task, description=f"[red]✗ {exchange_name} OI (failed)[/red]")

                    # Fetch funding rates (perpetual only)
                    funding_task = progress.add_task(f"Fetching {exchange_name} funding rates...", total=None)
                    funding_data: list[FundingRate] = []
                    funding_success = False
                    try:
                        async for fr in connector.fetch_funding_history(symbol=symbol):
                            funding_data.append(fr)
                            if len(funding_data) >= 100:
                                await repo.upsert_funding_rates(funding_data)
                                total_funding += len(funding_data)
                                funding_data = []

                        if funding_data:
                            await repo.upsert_funding_rates(funding_data)
                            total_funding += len(funding_data)
                        funding_success = True
                    except Exception as e:
                        console.print(f"[red]✗ {exchange_name} funding: {e}[/red]")

                    if funding_success:
                        progress.update(funding_task, description=f"[green]✓ {exchange_name} funding ({total_funding} saved)[/green]")
                    else:
                        progress.update(funding_task, description=f"[red]✗ {exchange_name} funding (failed)[/red]")

            finally:
                await connector.close()

    # Display summary
    earliest, latest, exchange_avail = await repo.get_available_date_range(symbol)
    kline_count = await repo.get_kline_count(symbol)
    oi_count = await repo.get_oi_count(symbol)
    funding_count = await repo.get_funding_count(symbol)
    exchange_ranges = await repo.get_exchange_date_ranges(symbol)

    # Format exchange date ranges for display
    formatted_ranges = {
        ex: (
            dates[0].strftime("%Y-%m-%d") if dates[0] else "N/A",
            dates[1].strftime("%Y-%m-%d") if dates[1] else "N/A",
        )
        for ex, dates in exchange_ranges.items()
    }

    display_data_summary(
        symbol=symbol,
        kline_count=kline_count,
        oi_count=oi_count,
        funding_count=funding_count,
        earliest=earliest.strftime("%Y-%m-%d %H:%M") if earliest else None,
        latest=latest.strftime("%Y-%m-%d %H:%M") if latest else None,
        exchanges=exchange_avail,
        exchange_date_ranges=formatted_ranges,
    )

    state.current_symbol = symbol


async def run_analysis() -> AnalysisResult | None:
    """Run analysis on current token data.

    Returns:
        AnalysisResult or None if cancelled/no data
    """
    if not state.current_symbol:
        console.print("[yellow]No token loaded. Please fetch data first.[/yellow]")
        return None

    repo = state.repository
    if repo is None:
        return None

    # Get available date range
    earliest, latest, _ = await repo.get_available_date_range(state.current_symbol)

    if not earliest or not latest:
        console.print("[yellow]No data available for this token.[/yellow]")
        return None

    # Get date range from user
    date_range = await menu.date_range_input(earliest, latest)
    if not date_range:
        return None

    start_time, end_time = date_range

    # Check which exchanges have data in this range
    exchange_ranges = await repo.get_exchange_date_ranges(state.current_symbol)
    missing_exchanges = []
    for ex, (ex_start, ex_end) in exchange_ranges.items():
        if ex_start and ex_start > end_time:
            missing_exchanges.append(f"{ex} (data starts {ex_start.strftime('%Y-%m-%d')})")
        elif ex_end and ex_end < start_time:
            missing_exchanges.append(f"{ex} (data ends {ex_end.strftime('%Y-%m-%d')})")

    if missing_exchanges:
        console.print()
        console.print("[yellow]⚠ Some exchanges have no data for this date range:[/yellow]")
        for ex_msg in missing_exchanges:
            console.print(f"  [dim]• {ex_msg}[/dim]")
        console.print()

    # Fetch data from repository
    klines = await repo.get_klines(
        symbol=state.current_symbol,
        interval="1h",
        start_time=start_time,
        end_time=end_time,
    )
    oi_data = await repo.get_open_interest(
        symbol=state.current_symbol,
        start_time=start_time,
        end_time=end_time,
    )

    if not klines:
        console.print("[yellow]No kline data found for this date range.[/yellow]")
        return None

    # Check OI data availability
    if not oi_data:
        console.print("[dim]Note: No OI data available for this date range. OI will show as '-'.[/dim]")

    # Fetch funding data
    funding_data = await repo.get_funding_rates(
        symbol=state.current_symbol,
        start_time=start_time,
        end_time=end_time,
    )

    # Calculate deltas
    result = calculate_deltas(
        klines=klines,
        oi_data=oi_data,
        start_time=start_time,
        end_time=end_time,
        funding_data=funding_data,
    )

    # Display results
    display_analysis(result)

    # Display funding stats and plot if available
    if funding_data:
        funding_stats = get_latest_funding_stats(funding_data, window_periods=3)
        display_funding_stats(funding_stats)
        display_funding_plot(funding_data, console, symbol=state.current_symbol)
    else:
        console.print("[dim]Note: No funding data available for this date range.[/dim]")

    state.last_analysis = result
    return result


async def do_export() -> None:
    """Export last analysis results."""
    if not state.last_analysis:
        console.print("[yellow]No analysis results to export. Run analysis first.[/yellow]")
        return

    format_choice = await menu.export_format_select()
    if not format_choice:
        return

    if format_choice == "analysis_range":
        csv_path = export_analysis_range_csv(state.last_analysis)
        console.print(f"[green]✓ Exported 1H analysis range to {csv_path}[/green]")

    if format_choice in ("csv", "both"):
        csv_path = export_csv(state.last_analysis)
        console.print(f"[green]✓ Exported to {csv_path}[/green]")

    if format_choice in ("json", "both"):
        json_path = export_json(state.last_analysis)
        console.print(f"[green]✓ Exported to {json_path}[/green]")


async def main_loop() -> None:
    """Main application loop."""
    # Initialize database
    await init_db()
    state.repository = Repository()

    console.print()
    console.print("[bold cyan]╔══════════════════════════════════════╗[/bold cyan]")
    console.print("[bold cyan]║     Liquidity Mapping CLI v0.1.0     ║[/bold cyan]")
    console.print("[bold cyan]╚══════════════════════════════════════╝[/bold cyan]")
    console.print()

    while True:
        action = await menu.main_menu()

        if action == "exit" or action is None:
            console.print("[dim]Goodbye![/dim]")
            break

        elif action == "fetch":
            token = await menu.token_input()
            if not token:
                continue

            # Build symbol
            symbol = f"{token.upper()}USDT"
            console.print(f"[cyan]Looking for {symbol}...[/cyan]")

            exchanges = await menu.exchange_select()
            if not exchanges:
                continue

            market_types = await menu.market_type_select()
            if not market_types:
                continue

            await fetch_token_data(symbol, exchanges, market_types)

        elif action == "analyze":
            result = await run_analysis()
            if result:
                # Post-analysis menu
                while True:
                    next_action = await menu.post_analysis_menu()
                    if next_action == "analyze":
                        result = await run_analysis()
                        if not result:
                            break
                    elif next_action == "export":
                        await do_export()
                    elif next_action == "fetch":
                        break  # Go back to main menu fetch
                    elif next_action in ("main", "exit", None):
                        if next_action == "exit":
                            console.print("[dim]Goodbye![/dim]")
                            return
                        break

        elif action == "export":
            await do_export()


def main():
    """Entry point."""
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        console.print("\n[dim]Interrupted. Goodbye![/dim]")
        sys.exit(0)


if __name__ == "__main__":
    main()
