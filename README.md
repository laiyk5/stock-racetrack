# Stock RaceTrack

My stock research toolset.

> [!WARNING]
> This package is in active develop. User interface might suffer from severe changes.

## Installation

- [Install uv](https://docs.astral.sh/uv/getting-started/installation/)
- create virtual environment and install dependencies by:

```sh
uv sync
```

- check if `srt` is available in your environment:

```sh
srt
```

## Usage

### srt.datasource

> [!NOTE]
> Make sure you have configured your API keys and environment variables before running the downloader. Refer to the [Configuration](#configuration) section for setup instructions.

This downloader can help you update your local db from your data provider efficiently.

The downloader cli provide a default method that fetches all stock's data. If you choose to download all stock's data, the first initialization will takes several hours, but only several seconds taken for daily update after that. 50 GB or more free storage space is recommended.

```bash
srt ds download tushare stock daily --symbols 000001.SZ
```

## Configuration

Every Module's CLI commands or subcommands need configuration come with a subcommand `config` that helps you set variables like API keys, DB connection params and so on.

For example, to set tushare token:

```bash
srt ds config tushare.token "<YOUR_TOKEN_HERE>"
```

For example, to set database token:

```bash
srt ds config database.host localhost
srt ds config database.port 5433
srt ds config database.user example_user
srt ds config database.password example_password
srt ds config database.dbname srt_ds
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
