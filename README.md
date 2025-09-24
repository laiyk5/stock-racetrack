# Stock RaceTrack

My stock research toolset.

> [!NOTE]
> This package is in active develop. User interface might suffer from severe changes.

## Usage

### srt.downloader

> [!NOTE]
> Make sure you have configured your API keys and environment variables before running the downloader. Refer to the [Configuration](#configuration) section for setup instructions.

This downloader can help you update your local db from your data provider efficiently.

The downloader cli provide a default method that fetches all stock's data. If you choose to download all stock's data, the first initialization will takes several hours, but only several seconds taken for daily update after that. 50 GB or more free storage space is recommended.


```bash
srt download download --biz-key tushare_daily --symbol
s ALL --start-at 2025-01-01:00:00:00 --stop-at 2026-
01-01:00:00:00
```

## Configuration

Every Module's CLI commands or subcommands need configuration come with a subcommand `config` that helps you set variables like API keys, DB connection params and so on.

## Development Progress

Usability:

- [x] Downloader
  - [x] Data providers
    - [x] tushare (need 5000 pts key)
