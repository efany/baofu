# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Python-based financial analysis and backtesting system called "baofu" (暴富). It provides tools for:

- **Financial Data Collection**: Web crawlers for funds, stocks, bonds, and forex data from various sources (Eastmoney, Morningstar, CMB, etc.)
- **Data Storage**: MySQL database layer with connection pooling for financial data persistence
- **Backtesting Engine**: Two backtesting systems - a custom implementation and Backtrader-based system for strategy testing
- **Web Dashboard**: Dash-based web interface for portfolio management and analysis
- **Investment Strategies**: Various trading strategies including rebalancing, buy-and-hold, and trigger-based strategies

## Development Commands

### Environment Setup
```bash
# Install dependencies for different modules
pip install -r requirements.txt                    # Main dependencies
pip install -r data_science/requirements.txt       # Backtesting dependencies  
pip install -r web_crawler/requirements.txt        # Web scraping dependencies
```

### Running the Application
```bash
# Start the web dashboard
python task_dash/app.py

# Run backtesting examples
python data_science/examples/backtest_example.py

# Run data crawling examples
python web_crawler/examples/fund_eastmoney_example.py
python web_crawler/examples/cmb_example.py
```

### Database Operations
```bash
# Initialize database schema
python database/database_init.py

# Update financial data
python task_data/update_funds_nav_task.py
python task_data/update_stocks_day_hist_task.py
python task_data/update_bond_rate_task.py
```

## Architecture Overview

### Core Components

- **Database Layer** (`database/`): MySQL connection pooling and data access objects for funds, stocks, bonds, and strategies
- **Web Crawlers** (`web_crawler/`, `task_crawlers/`): Data collection from financial websites with respectful crawling practices
- **Data Processing** (`data_process/`, `data_flow/`): ETL pipelines for financial data normalization and calculation
- **Backtesting Systems**: 
  - Custom backtester (`data_science/src/backtester/`) with pluggable analyzers
  - Backtrader integration (`task_backtrader/`) with custom feeds and strategies
- **Web Interface** (`task_dash/`): Dash application with modular pages and callbacks for portfolio management

### Key Design Patterns

- **Strategy Pattern**: Both backtesting systems use pluggable strategies inheriting from base classes
- **Data Access Layer**: Database operations abstracted through dedicated DB classes (DBFunds, DBStocks, etc.)
- **Trigger System**: Event-driven rebalancing with configurable triggers (date, deviation, sorting-based)
- **Analyzer Pipeline**: Modular analysis components for backtesting results

### Database Schema
The MySQL database contains tables for:
- `funds`, `funds_nav`: Fund information and net asset values
- `stocks`, `stocks_day_hist`: Stock data and historical prices  
- `bond_rate`: Bond yield data
- `forex_day_hist`: Foreign exchange rates
- `strategys`: Strategy configurations and results

### Configuration
- Database connection: Hardcoded in `task_dash/app.py` (host='127.0.0.1', user='baofu', password='TYeKmJPfw2b7kxGK', database='baofu')
- Crawler settings: JSON configs in `config/` directory
- Strategy parameters: Defined in strategy classes and example files

## Important Notes

- The system uses Chinese comments and variable names in many places
- Database credentials are hardcoded in the dashboard app
- Web crawlers include rate limiting and robots.txt compliance
- Backtesting supports both percentage-based and absolute position sizing
- The dashboard provides real-time portfolio analysis and comparison tools