# Coinglass Automation

A comprehensive cryptocurrency futures data collection system for the Coinglass API. This tool automates the collection of futures market data including funding rates, open interest, and long/short ratios into a MySQL database.

## Features

- **Optimized Data Collection** with appropriate cadences:
  - **Funding Rate**: 8-hour intervals + 1-hour snapshots
  - **Open Interest**: 1-5 minute intervals (high frequency)
  - **Long/Short Ratio**: 15-60 minute intervals
- **MySQL Database Storage**: Structured storage with OHLC format
- **Simple CLI Interface**: Easy-to-use commands for all operations
- **Continuous Mode**: Automated scheduled collection
- **Historical Data Scraping**: Fetch months of historical data
- **Status Monitoring**: Check ingestion status anytime

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd automation_coinglass
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your settings
```

4. Setup database:
```bash
python main.py --setup
```

## Configuration

Edit `.env` file with your settings:

```env
# Database Configuration
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=your_password_here
DB_NAME=coinglass

# Coinglass API
COINGLASS_API_KEY=your_api_key_here

# Optional: Customize exchanges and symbols (comma-separated)
COINGLASS_EXCHANGES=Binance,OKX,Bybit
COINGLASS_SYMBOLS=BTC,ETH,BNB,SOL,XRP,DOGE,MATIC,ARB,AVAX

# Minimum USD for filtering transactions
MIN_USD=1000
```

## Usage

### Setup Database
Create all required database tables:
```bash
python main.py --setup
```

### Initial Historical Scrape
Fetch historical data for multiple months:
```bash
# Fetch 1 month of historical data (default)
python main.py --initial-scrape

# Fetch 12 months of historical data
python main.py --initial-scrape --months 12

# Fetch 3 months
python main.py --initial-scrape --months 3
```

### Run Specific Pipelines
Run one or more specific data collection pipelines:
```bash
# Run funding rate pipeline only
python main.py funding_rate

# Run multiple pipelines
python main.py funding_rate open_interest

# Run all available pipelines
python main.py funding_rate open_interest long_short_ratio
```

### Continuous Mode
Run automated scheduled data collection:
```bash
python main.py --continuous
```

This will schedule:
- **Funding Rate**: Every 1 hour
- **Open Interest**: Every 5 minutes
- **Long/Short Ratio**: Every 15 minutes

Press `Ctrl+C` to stop continuous mode.

### Check Status
View current ingestion status:
```bash
python main.py --status
```

This shows:
- Total records per data type
- Latest timestamp for each data type
- Database statistics

### Run All Pipelines Once
Run all pipelines one time (useful for manual updates):
```bash
python main.py
```

## Project Structure

```
automation_coinglass/
├── app/
│   ├── core/                   # Core utilities
│   │   ├── config.py          # Configuration management
│   │   └── logging.py         # Logging setup
│   ├── database/               # Database layer
│   │   └── connection.py      # MySQL connection
│   ├── models/                 # Database models
│   │   └── coinglass.py       # Table definitions
│   ├── providers/              # API providers
│   │   └── coinglass/         # Coinglass provider
│   │       ├── client.py      # API client
│   │       └── pipelines/     # Data pipelines
│   │           ├── funding_rate.py
│   │           ├── open_interest.py
│   │           └── long_short_ratio.py
│   ├── repositories/           # Data access layer
│   │   └── coinglass_repository.py
│   ├── services/               # Business logic
│   │   └── coinglass_service.py
│   └── controllers/            # Orchestration
│       └── ingestion_controller.py
├── main.py                     # CLI entry point
├── requirements.txt            # Python dependencies
├── .env.example                # Environment template
└── README.md                   # This file
```

## API Endpoints Supported

### Open Interest
- `/api/futures/open-interest/history` - OHLC History
- `/api/futures/open-interest/aggregated-history` - Aggregated History
- `/api/futures/open-interest/exchange-list` - Exchange List
- `/api/futures/open-interest/exchange-history-chart` - Exchange History Chart

### Funding Rates
- `/api/futures/funding-rate/history` - OHLC History
- `/api/futures/funding-rate/exchange-list` - Exchange List
- `/api/futures/funding-rate/accumulated-exchange-list` - Cumulative Exchange List
- `/api/futures/funding-rate/arbitrage` - Arbitrage Opportunities

### Long/Short Ratios
- `/api/futures/top-long-short-account-ratio/history` - Top Account Ratio History
- `/api/futures/top-long-short-position-ratio/history` - Top Position Ratio History

### Orderbook
- `/api/futures/orderbook/ask-bids-history` - Pair Orderbook Bid&Ask
- `/api/futures/orderbook/aggregated-ask-bids-history` - Aggregated Orderbook Bid&Ask

### Options
- `/api/option/max-pain` - Option Max Pain
- `/api/option/info` - Options Info
- `/api/option/exchange-oi-history` - Exchange Open Interest History
- `/api/option/exchange-vol-history` - Exchange Volume History

## Error Handling

The tool includes comprehensive error handling:

- **API Rate Limiting**: Built-in rate limiting to prevent API abuse
- **Retry Logic**: Automatic retries for transient failures
- **Graceful Degradation**: Individual data type failures don't stop the entire collection
- **Detailed Logging**: Comprehensive logging for debugging and monitoring
- **API Error Handling**: Proper handling of API-specific error codes

## Logging

Logs are written to `logs/app.log` and include:

- Collection start/end times
- API request/response details
- Error messages and stack traces
- Data collection summaries

## Dependencies

- `requests==2.32.5` - HTTP client
- `pymysql==1.1.2` - MySQL database support
- `python-dotenv==1.1.1` - Environment variable management
- `pydantic==2.10.4` - Data validation
- `pydantic-settings==2.7.0` - Settings management

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License.

## Support

For issues and questions, please open an issue on the GitHub repository.