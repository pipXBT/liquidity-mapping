# Liquidity Mapping

A Python CLI tool for analyzing cryptocurrency liquidity, volume, open interest, and funding rates across multiple exchanges.

## Features

- **Multi-Exchange Support**: Fetch data from Binance, ByBit, and BitGet
- **Market Analysis**: Analyze both spot and perpetual markets
- **Data Metrics**:
  - Price deltas across multiple timeframes (1h, 4h, 12h, 24h)
  - Volume calculations and VWAP (Volume-Weighted Average Price)
  - Open Interest tracking and deltas
  - Funding rate statistics with moving averages
- **Visualizations**: Terminal-based price and volume plots
- **Export**: Save analysis results to CSV or JSON

## Requirements

- Python 3.11+

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/liquidity-mapping.git
cd liquidity-mapping

# Install dependencies
pip install -e .
```

## Usage

Run the CLI tool:

```bash
liqmap
```

The interactive menu will guide you through:

1. Select a token (e.g., COAI)
2. Choose exchanges (Binance, ByBit, BitGet)
3. Select market types (Spot, Perpetual)
4. Fetch and analyze data
5. Export results

## Project Structure

```
liquidity_mapping/
├── src/
│   ├── main.py           # CLI entry point
│   ├── menu.py           # Interactive menu system
│   ├── connectors/       # Exchange API connectors
│   ├── db/               # Database layer (SQLite)
│   ├── analysis/         # Data analysis modules
│   └── output/           # Terminal display and export
├── tests/
├── pyproject.toml
└── README.md
```

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Lint code
ruff check src/
```

## License

MIT License - see [LICENSE](LICENSE) for details.
