# Stock RaceTrack

My stock research toolset.

> [!WARNING]
> This package is in active develop. User interface might suffer from severe changes.

## Usage

### srt.datasource

> [!NOTE]
> Make sure you have configured your API keys and environment variables before running the downloader. Refer to the [Configuration](#configuration) section for setup instructions.

This downloader can help you update your local db from your data provider efficiently.

The downloader cli provide a default method that fetches all stock's data. If you choose to download all stock's data, the first initialization will takes several hours, but only several seconds taken for daily update after that. 50 GB or more free storage space is recommended.

```bash
srt ds download --biz-key tushare_daily --symbol
s ALL --start-at 2025-01-01:00:00:00 --stop-at 2026-
01-01:00:00:00
```

## Configuration

Every Module's CLI commands or subcommands need configuration come with a subcommand `config` that helps you set variables like API keys, DB connection params and so on.

For example, to set tushare token:

```bash
srt ds config tushare.token "<YOUR_TOKEN_HERE>"
```

## Development Progress

Usability:

- Datasource
  - [x] Data providers
    - [x] tushare (need 5000 pts key)
    - [ ] akshare (any volunteer?)
- Data formats
  - [x] stock OHLCV
- Strategies Designing Framework
  - [ ] Backtesting
  - [ ] Apply strategy on real time
  - [ ] Apply strategy on the whole market.
