import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import tenacity
from tushare import pro_api, set_token

from srt.datasource import config

logger = logging.getLogger(__name__)

set_token(config.get("tushare", "token"))
api = pro_api()


@tenacity.retry(
    wait=tenacity.wait_exponential(multiplier=1, min=4, max=60),
    stop=tenacity.stop_after_attempt(5),
    before=tenacity.before_log(logger, logging.DEBUG),
)
def get_symbol_list(symbol_type: str) -> list:
    """
    Fetch the stock list from Tushare and return a list of stock symbols.
    """

    available_symbol_types = {
        "index",
        "fund",
        "stock",
    }

    if symbol_type not in available_symbol_types:
        raise ValueError(
            f"Invalid symbol_type. Available options are: {', '.join(available_symbol_types)}"
        )

    logger.debug("Fetching stock list from Tushare...")
    df = api.query(
        symbol_type + "_basic",
        exchange="",
        list_status="L",
        fields="ts_code,symbol,name,area,industry,list_date",
    )
    logger.debug("Fetched %d stocks from Tushare.", len(df))
    logger.debug("Sample stock list data:\n%s", df.head())
    return df["ts_code"].tolist()
