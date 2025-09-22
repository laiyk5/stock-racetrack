from datetime import datetime
from email.policy import default
from zoneinfo import ZoneInfo
import click

import srt
from srt.downloader import config

@click.group()
def cli():
    pass


@cli.command()
def delete_by_bizkey():
    from srt.downloader.dbtools import delete_rawdata_by_bizkey

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
    from srt.downloader.dbtools import (
        reset_database as _reset_database,
        reset_tables as _reset_tables,
    )

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
    prompt="Symbols (comma-separated), [ALL] for all symbols",
    help="Comma-separated list of symbols to update.",
    default="ALL",
)
@click.option(
    "--stop-at",
    prompt="Stop At (YYYY-MM-DD)",
    help="The end date for the data update in YYYY-MM-DD format.",
    default=lambda: datetime.now().strftime("%Y-%m-%d"),
)
def update_tushare_data(biz_key, symbols, stop_at):
    from srt.downloader.updaters import (
        TushareDailyUpdaterWithSymbolAndTime,
        get_symbol_list,
    )
    from tushare import pro_api

    available_biz_keys = {
        "tushare_daily": "Daily stock data from Tushare",
        "tushare_fund_daily": "Daily fund data from Tushare",
        "tushare_index_daily": "Daily index data from Tushare",
    }

    if biz_key not in available_biz_keys:
        click.echo(
            f"Invalid biz_key. Available options are: {', '.join(available_biz_keys.keys())}"
        )
        return

    if symbols == "ALL":
        if biz_key == "tushare_daily":
            symbol_list = get_symbol_list("stock")
        elif biz_key == "tushare_fund_daily":
            symbol_list = get_symbol_list("fund")
        elif biz_key == "tushare_index_daily":
            symbol_list = get_symbol_list("index")
    else:
        symbol_list = [s.strip() for s in symbols.split(",") if s.strip()]

    api_methods = {
        "tushare_daily": "daily",
        "tushare_fund_daily": "fund_daily",
        "tushare_index_daily": "index_daily",
    }

    api = pro_api()

    def api_method(*args, **kwargs):
        return api.query(api_methods[biz_key], *args, **kwargs)

    updater = TushareDailyUpdaterWithSymbolAndTime(biz_key, api_method)
    stop_at_date = datetime.strptime(stop_at, "%Y-%m-%d")
    stop_at_date = stop_at_date.replace(tzinfo=ZoneInfo(config.get("app", "timezone")))
    updater.download(symbol_list, stop_at_date)
    click.echo(f"Data update for biz_key '{biz_key}' completed up to {stop_at}.")


# Get or Set configuration
@cli.command(name="config")
@click.argument("section_option", required=False)
@click.argument(
    "value",
    required=False,
)
def config_(section_option, value):
    from srt.downloader import config as _config, config_dir, config_file
    import configparser

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
