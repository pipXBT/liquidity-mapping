"""Arrow-key menu system using questionary."""

from datetime import datetime

import questionary
from questionary import Style

# Custom style for the menu
custom_style = Style([
    ("qmark", "fg:cyan bold"),
    ("question", "fg:white bold"),
    ("answer", "fg:green bold"),
    ("pointer", "fg:cyan bold"),
    ("highlighted", "fg:cyan bold"),
    ("selected", "fg:green"),
    ("separator", "fg:gray"),
    ("instruction", "fg:gray"),
])


async def main_menu() -> str:
    """Display main menu and return selected action.

    Returns:
        Selected action string
    """
    return await questionary.select(
        "What would you like to do?",
        choices=[
            questionary.Choice("Fetch token data", value="fetch"),
            questionary.Choice("Analyze date range", value="analyze"),
            questionary.Choice("Export results", value="export"),
            questionary.Choice("Exit", value="exit"),
        ],
        style=custom_style,
    ).ask_async()


async def token_input() -> str | None:
    """Prompt for token symbol.

    Returns:
        Token symbol or None if cancelled
    """
    return await questionary.text(
        "Enter token symbol (e.g., COAI):",
        style=custom_style,
    ).ask_async()


async def exchange_select() -> list[str]:
    """Select exchanges to fetch from.

    Returns:
        List of selected exchange names
    """
    result = await questionary.checkbox(
        "Select exchanges to fetch from:",
        choices=[
            questionary.Choice("Binance", value="binance", checked=True),
            questionary.Choice("ByBit", value="bybit", checked=True),
            questionary.Choice("BitGet", value="bitget", checked=True),
        ],
        style=custom_style,
    ).ask_async()
    return result or []


async def market_type_select() -> list[str]:
    """Select market types to fetch.

    Returns:
        List of selected market types
    """
    result = await questionary.checkbox(
        "Select market types:",
        choices=[
            questionary.Choice("Spot", value="spot", checked=True),
            questionary.Choice("Perpetual", value="perp", checked=True),
        ],
        style=custom_style,
    ).ask_async()
    return result or []


async def date_range_input(
    earliest: datetime | None = None,
    latest: datetime | None = None,
) -> tuple[datetime, datetime] | None:
    """Prompt for date range selection.

    Args:
        earliest: Earliest available date
        latest: Latest available date

    Returns:
        Tuple of (start_date, end_date) or None if cancelled
    """
    hint = ""
    if earliest and latest:
        hint = f" (available: {earliest.strftime('%Y-%m-%d')} to {latest.strftime('%Y-%m-%d')})"

    start_str = await questionary.text(
        f"Start date (YYYY-MM-DD){hint}:",
        style=custom_style,
    ).ask_async()

    if not start_str:
        return None

    end_str = await questionary.text(
        "End date (YYYY-MM-DD):",
        style=custom_style,
    ).ask_async()

    if not end_str:
        return None

    try:
        start_date = datetime.strptime(start_str.strip(), "%Y-%m-%d")
        end_date = datetime.strptime(end_str.strip(), "%Y-%m-%d")
        # Set end date to end of day
        end_date = end_date.replace(hour=23, minute=59, second=59)
        return start_date, end_date
    except ValueError:
        return None


async def export_format_select() -> str:
    """Select export format.

    Returns:
        Selected format (csv, json, both, or analysis_range)
    """
    return await questionary.select(
        "Select export format:",
        choices=[
            questionary.Choice("1H Analysis Range (Price, VWAP, Funding, Volume, Volume $USD)", value="analysis_range"),
            questionary.Choice("CSV (summary + raw klines)", value="csv"),
            questionary.Choice("JSON", value="json"),
            questionary.Choice("Both CSV + JSON", value="both"),
        ],
        style=custom_style,
    ).ask_async()


async def confirm(message: str) -> bool:
    """Display confirmation prompt.

    Args:
        message: Confirmation message

    Returns:
        True if confirmed
    """
    return await questionary.confirm(
        message,
        style=custom_style,
        default=True,
    ).ask_async()


async def post_analysis_menu() -> str:
    """Display menu after analysis is shown.

    Returns:
        Selected action
    """
    return await questionary.select(
        "What would you like to do next?",
        choices=[
            questionary.Choice("Analyze different date range", value="analyze"),
            questionary.Choice("Export these results", value="export"),
            questionary.Choice("Fetch new token", value="fetch"),
            questionary.Choice("Return to main menu", value="main"),
            questionary.Choice("Exit", value="exit"),
        ],
        style=custom_style,
    ).ask_async()
