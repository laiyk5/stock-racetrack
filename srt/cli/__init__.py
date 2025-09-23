import configparser
import logging

import click
from rich.logging import RichHandler

from srt.downloader.cli import cli as downloader_cli


@click.group()
@click.option("--debug", is_flag=True, help="Enable debug logging.")
def cli(debug):
    if debug:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[RichHandler(rich_tracebacks=True)],
        )
        logging.getLogger("tushare").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("sqlalchemy").setLevel(logging.WARNING)
        logging.getLogger("click").setLevel(logging.WARNING)
        logging.getLogger("rich").setLevel(logging.WARNING)
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[RichHandler(rich_tracebacks=True)],
        )
        logging.getLogger("tushare").setLevel(logging.ERROR)
        logging.getLogger("urllib3").setLevel(logging.ERROR)
        logging.getLogger("sqlalchemy").setLevel(logging.ERROR)
        logging.getLogger("click").setLevel(logging.ERROR)
        logging.getLogger("rich").setLevel(logging.ERROR)


cli.add_command(downloader_cli, "download")


# Get or Set configuration
@cli.command()
@click.argument("section_option", required=False)
@click.argument(
    "value",
    required=False,
)
def config(section_option, value):
    import srt
    from srt import config as _config
    from srt import config_dir, config_file

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
