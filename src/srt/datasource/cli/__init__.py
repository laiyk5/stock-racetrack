import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import click

from srt.datasource import config

logger = logging.getLogger(__name__)


@click.group()
def cli():
    pass


@cli.command()
def delete_by_bizkey():
    from srt.datasource.dbtools import delete_rawdata_by_bizkey

    biz_key = click.prompt("Please enter the biz_key to delete", type=str)
    confirm = click.confirm(
        f"Are you sure you want to delete all records with biz_key = '{biz_key}'? This action cannot be undone.",
        default=False,
    )
    if confirm:
        delete_rawdata_by_bizkey(biz_key)
        click.echo(f"All records with biz_key = '{biz_key}' have been deleted.")
    else:
        click.echo("Operation cancelled.")


@cli.command()
@click.option(
    "--reset-db", is_flag=True, help="Reset the entire database and all tables."
)
@click.option("--reset-tables", is_flag=True, help="Reset all tables in the database.")
def reset_db_or_tables(reset_db, reset_tables):
    from srt.datasource.dbtools import reset_database as _reset_database
    from srt.datasource.dbtools import reset_tables as _reset_tables

    if reset_db:
        confirm = click.confirm(
            "Are you sure you want to reset the entire database? This action cannot be undone.",
            default=False,
        )
        if confirm:
            _reset_database()
            click.echo("Database has been reset.")
        else:
            click.echo("Operation cancelled.")
    elif reset_tables:
        confirm = click.confirm(
            "Are you sure you want to reset all tables in the database? This action cannot be undone.",
            default=False,
        )
        if confirm:
            _reset_tables()
            click.echo("Tables have been reset.")
        else:
            click.echo("Operation cancelled.")
    else:
        click.echo("No action specified. Use --reset-db or --reset-tables.")


@cli.command()
@click.option("--biz-key", prompt="Business Key", help="The business key for the data.")
@click.option(
    "--symbols",
    prompt="Symbols (comma-separated), left empty for all symbols",
    help="Comma-separated list of symbols to update.",
    default="",
)
@click.option(
    "--start-at",
    prompt="Start At (YYYY-MM-DD:hh:mm:ss)",
    help="The start timestamp for the data update in YYYY-MM-DD:hh:mm:ss format.",
    default=lambda: datetime(1980, 1, 1).strftime("%Y-%m-%d:%H:%M:%S"),
)
@click.option(
    "--stop-at",
    prompt="Stop At (YYYY-MM-DD:hh:mm:ss)",
    help="The end timestamp for the data update in YYYY-MM-DD:hh:mm:ss format.",
    default=lambda: datetime.now().strftime("%Y-%m-%d:%H:%M:%S"),
)
def download(biz_key, symbols, start_at, stop_at):
    from srt.datasource.downloader import download

    start_at = set_timezone(start_at)
    stop_at = set_timezone(stop_at)

    symbols = [s.strip() for s in symbols.split(",")] if symbols else []

    download(biz_key, symbols, start_at, stop_at)
    click.echo(f"Data update for biz_key '{biz_key}' completed up to {stop_at}.")


def set_timezone(start_at):
    start_at = start_at.replace(tzinfo=ZoneInfo(config.get("app", "timezone")))
    return start_at


# Get or Set configuration
@cli.command(name="config")
@click.argument("section_option", required=False)
@click.argument(
    "value",
    required=False,
)
def config_(section_option, value):
    import configparser

    from srt.datasource import config as _config
    from srt.datasource import config_dir, config_file

    if value is None:
        if section_option:
            if "." in section_option:
                section, option = section_option.split(".", 1)
            else:
                section, option = "default", section_option
            if _config.has_section(section) and _config.has_option(section, option):
                current_value = _config.get(section, option)
                click.echo(f"{section}.{option} = {current_value}")
            else:
                click.echo(f"Configuration '{section}.{option}' not found.")
        else:
            for section in _config.sections():
                click.echo(f"[{section}]")
                for option in _config.options(section):
                    value = _config.get(section, option)
                    click.echo(f"{option} = {value}")
                click.echo()
    else:
        if section_option:
            if "." in section_option:
                section, option = section_option.split(".", 1)
            else:
                section, option = "default", section_option
            if not _config.has_section(section):
                click.echo(f"Section '{section}' does not exist.")
                return
            if not _config.has_option(section, option):
                click.echo(f"Option '{option}' does not exist in section '{section}'.")
                return

            config_in_file = configparser.ConfigParser()
            if not config_dir.exists():
                config_dir.mkdir(parents=True, exist_ok=True)
            else:
                config_in_file.read(config_file)
            if not config_in_file.has_section(section):
                config_in_file.add_section(section)
            config_in_file.set(section, option, value)
            with open(config_file, "w") as f:
                config_in_file.write(f)

            click.echo(f"Set {section}.{option} = {value} and saved to {config_file}")
        else:
            click.echo(
                "Please specify the configuration option to set in 'section.option' format."
            )


@cli.command()
@click.option(
    "--provider", required=True, type=click.Choice(["tushare"]), help="Data provider"
)
@click.option("--dataset", required=True, type=str, help="")
@click.option("--symbol", required=True, type=str, help="Stock symbol, e.g., 601088.SH")
@click.option(
    "--start-at",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d:%H:%M:%S"]),
    help="Start date in YYYY-MM-DD format",
    default=(datetime.now() - timedelta(days=30)).strftime(
        "%Y-%m-%d:%H:%M:%S"
    ),  # default to last 30 days
)
@click.option(
    "--end-at",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d:%H:%M:%S"]),
    help="End date in YYYY-MM-DD format",
    default=datetime.now().strftime("%Y-%m-%d:%H:%M:%S"),
)
def show(provider, dataset, symbol, start_at, end_at):
    from srt.datasource.datasource import TushareDatasource

    start_at = set_timezone(start_at)
    end_at = set_timezone(end_at)

    ds_map = {
        "tushare": {
            "stock_price_ohlcv_daily": TushareDatasource.get_stock_price_ohlcv_daily
        },
    }

    click.echo(
        f"Fetching data from provider '{provider}', dataset '{dataset}' for symbol '{symbol}' from {start_at} to {end_at}"
    )
    provider_info = ds_map.get(provider)
    if dataset not in provider_info:
        click.echo(f"Dataset '{dataset}' not supported for provider '{provider}'")
        return

    data = provider_info[dataset](symbol, start_at, end_at)
    click.echo(data)
    click.echo("Data fetch completed.")
