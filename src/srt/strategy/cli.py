import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import click

from srt import config

logger = logging.getLogger(__name__)


@click.group()
def cli():
    pass


def set_timezone(dt):
    if dt.tzinfo is None:
        tz = ZoneInfo(config.get("app", "timezone", fallback="Asia/Shanghai"))
        dt = dt.replace(tzinfo=tz)
    return dt


@cli.command(name="backtest")
@click.argument(
    "strategy", required=True, type=click.Choice(["pyramid"], case_sensitive=False)
)
@click.argument("symbol", required=True, type=str)
@click.option(
    "--start-at",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Start date in YYYY-MM-DD format",
    default=(datetime.now() - timedelta(days=5 * 365)).strftime("%Y-%m-%d"),
)
@click.option(
    "--end-at",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="End date in YYYY-MM-DD format",
    default=datetime.now().strftime("%Y-%m-%d"),
)
@click.option("-o", "--optimize", is_flag=True, help="Run optimization")
def backtest_(strategy, symbol, start_at, end_at, optimize):
    logger.info(f"Running pyramid strategy for {symbol} from {start_at} to {end_at}")
    if optimize:
        logger.info("Optimization is enabled.")

    from .pyramid import backtest

    backtest(symbol, start_at, end_at, optimize)
