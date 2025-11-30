import logging
from typing import Optional

import click

from .config import USER_CONFIG_PATH, get_config

logger = logging.getLogger(__name__)


@click.group()
def cli():
    """Stock Racetrack CLI"""
    pass


@cli.command()
def update():
    """Update the stock data."""

    from srt.datasource.downloader.downloader import Updater
    from srt.datasource.downloader.tushare import (
        TushareStockDailyPriceDownloader,
        TushareStockDownloader,
    )

    config = get_config()

    db_url = config["database"]["url"]
    if db_url == "":
        click.echo(
            "Database URL is not set in the configuration."
            "\nSet it with 'srt-cli config database.url <DATABASE_URL>'."
        )
        return

    from srt.datasource.utils import DB

    db = DB.get_instance(db_url)

    tushare_api_token = config["tushare"]["api_token"]

    if tushare_api_token == "":
        click.echo(
            "Tushare API token is not set in the configuration."
            "\nSet it with 'srt-cli config tushare.api_token <API_TOKEN>'."
        )
        return

    updaters: list[Updater] = [
        TushareStockDownloader(
            session_factory=db.get_session_factory(), api_token=tushare_api_token
        ),
        TushareStockDailyPriceDownloader(
            session_factory=db.get_session_factory(), api_token=tushare_api_token
        ),
    ]

    for updater in updaters:
        click.echo(f"Updating data using {updater.__class__.__name__}...")

        session = db.get_session_factory()()
        try:
            id = updater.get_data_info_id(session)
        except Exception as e:
            click.echo(f"Error getting data info ID: {e}.")
            click.echo(f"Try registering data info...")
            updater.register_data_info(session)
            session.commit()
        finally:
            session.close()

        updater.update()
        click.echo(f"Finished updating data using {updater.__class__.__name__}.")


@cli.command()
@click.argument("key", nargs=1, required=False)
@click.argument("value", nargs=1, required=False)
def config(key: Optional[str] = None, value: Optional[str] = None):
    """
    View or set configuration settings.

    KEY is in the format 'section.option'.

    Key can in the format 'section' to display all options in that section.
    """

    if key is None and value is None:
        config = get_config()
        for section in config.sections():
            click.echo(f"[{section}]")
            for key, value in config.items(section):
                click.echo(f"{key}={value}")
            click.echo("")

    elif key is not None and value is None:
        config = get_config()
        # display the value for the given key.
        # If only section is given, display all options in that section.
        # If both section and option are given, display the value of that option.

        if "." not in key:
            # only section is given
            section = key
            if not config.has_section(section):
                click.echo(f"Section '{section}' does not exist.")
                return
            click.echo(f"[{section}]")
            for key, value in config.items(section):
                click.echo(f"{key}={value}")
            return
        section, option = key.split(".")
        if not config.has_section(section):
            click.echo(f"Section '{section}' does not exist.")
            return
        if not config.has_option(section, option):
            click.echo(f"Option '{option}' does not exist in section '{section}'.")
            return
        value = config.get(section, option)
        click.echo(f"{section}.{option}={value}")
    elif key is not None and value is not None:
        config = get_config()
        try:
            section, option = key.split(".")
            if not config.has_section(section):
                raise ValueError("Section does not exist.")
            config.set(section, option, value)
            config_path = USER_CONFIG_PATH
            config_path.parent.mkdir(parents=True, exist_ok=True)
            with open(config_path, "w") as configfile:
                config.write(configfile)
            click.echo(f"Set '{key}' to '{value}' in configuration.")
        except ValueError:
            click.echo("Invalid key format. Use 'section.argument'.")
