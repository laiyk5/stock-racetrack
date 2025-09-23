import json
import logging
from abc import abstractmethod
from datetime import date, datetime, timedelta, timezone
from typing import Callable

import pandas as pd
import psycopg
import tenacity

from srt.downloader.tracker import track

from .dbtools import DB_NAME, get_conn_str

logger = logging.getLogger(__name__)

from tushare import pro_api, set_token

api = pro_api()


class DailyUpdaterWithSymbolAndTime:
    """
    An Easy and powerful downloader.
    Download data by symbol if database is too old.
    Download data by date if database is new enough.
    """

    def __init__(self, biz_key: str):
        self.biz_key = biz_key

    @abstractmethod
    def download_by_date(
        self, symbols: list[str], start_at: datetime, stop_at: datetime
    ):
        pass

    @abstractmethod
    def download_by_symbol(self, symbols: list[str], stop_at: datetime):
        pass

    def download(self, symbols: list[str], stop_at: datetime):
        # get latest date from db
        conn_str = get_conn_str(DB_NAME)

        with psycopg.connect(conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT MAX(timestamp) FROM raw_data as rd WHERE rd.biz_key = '{self.biz_key}'"
                )
                latest_date = cur.fetchone()[0]

        if not latest_date:
            # download by symbol
            self.download_by_symbol(symbols, stop_at)
            return
        elif latest_date >= stop_at:
            # nothing to do
            return

        # find out symbols not in db
        with psycopg.connect(conn_str) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT DISTINCT symbol FROM raw_data as rd WHERE rd.biz_key = '{self.biz_key}'"
                )
                existing_symbols = {row[0] for row in cur.fetchall()}

        missing_symbols = set(symbols) - existing_symbols

        if missing_symbols:
            self.download_by_symbol(missing_symbols, latest_date)

        # download the rest by date
        self.download_by_date(symbols, latest_date, stop_at)


from tushare import pro_api, set_token

from . import config
from .dbtools import store_data

set_token(config.get("tushare", "token"))


class TushareDailyUpdaterWithSymbolAndTime(DailyUpdaterWithSymbolAndTime):
    def __init__(self, biz_key, api_method: Callable):
        """
        api_method: The API method to use for downloading data. It should support 'ts_code', 'trade_date', 'start_date', and 'end_date' parameters.
        And the returning dataframe should have 'ts_code' and 'trade_date' columns.
        """
        super().__init__(biz_key)
        self.api = pro_api()
        self.api_method = api_method
        self.tz = timezone(timedelta(hours=8))  # Asia/Shanghai

    def _transform_record(self, row) -> tuple:
        timestamp = datetime.strptime(row["trade_date"], "%Y%m%d")
        timestamp = timestamp.replace(tzinfo=self.tz)
        return (
            self.biz_key,  # biz_key
            row["ts_code"],  # symbol
            timestamp,  # timestamp
            row.to_json(),  # data
        )

    def download_by_date(self, symbols, start_at, stop_at: datetime):
        @tenacity.retry(
            wait=tenacity.wait_exponential(multiplier=1, min=4, max=60),
            stop=tenacity.stop_after_attempt(5),
            reraise=True,
        )
        def fetch(date: datetime):
            date_str = date.strftime("%Y%m%d")
            df = self.api_method(trade_date=date_str)
            return df

        for date in track(
            [start_at + timedelta(n) for n in range((stop_at - start_at).days + 1)]
        ):
            df = fetch(date)
            if df is not None and not df.empty:
                # filter out all symbols not in the list
                df = df[df["ts_code"].isin(symbols)]
                if not df.empty:
                    # transform data into records
                    records = []
                    for row in df.iterrows():
                        record = self._transform_record(row[1])
                        records.append(record)
                    store_data(records)

    def download_by_symbol(self, symbols, stop_at):

        @tenacity.retry(
            wait=tenacity.wait_exponential(multiplier=1, min=4, max=60),
            stop=tenacity.stop_after_attempt(5),
            reraise=True,
        )
        def fetch(symbol: str, start_at: datetime, stop_at: datetime):
            df = self.api_method(
                ts_code=symbol,
                start_date=start_at.strftime("%Y%m%d"),
                end_date=stop_at.strftime("%Y%m%d"),
            )
            return df

        MAX_DATE_RANGE_DAYS = 5000

        for symbol in track(symbols):
            # split the fetch request into smaller chunks if needed
            df_list = []
            start_at = datetime(1980, 1, 1, tzinfo=self.tz)
            while True:
                if (stop_at - start_at).days > MAX_DATE_RANGE_DAYS:
                    chunk_stop_at = start_at + timedelta(MAX_DATE_RANGE_DAYS)
                else:
                    chunk_stop_at = stop_at
                df = fetch(symbol, start_at, chunk_stop_at)
                logger.debug(
                    f"Fetched {len(df) if df is not None else 0} records for {symbol} from {start_at} to {chunk_stop_at}"
                )
                if df is not None and not df.empty:
                    if df.isna().all().all():
                        logger.warning(
                            f"All data is NA for {symbol} from {start_at} to {chunk_stop_at}, skipping."
                        )
                    else:
                        df_list.append(df)
                if chunk_stop_at >= stop_at:
                    break
                start_at = chunk_stop_at + timedelta(1)

            df = pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()

            logger.debug("Sample of fetched data:\n%s", df.head())

            if not df.empty:
                # store data
                records = []
                for row in df.iterrows():
                    record = self._transform_record(row[1])
                    records.append(record)
                store_data(records)


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


if __name__ == "__main__":
    from rich.logging import RichHandler

    from . import logger

    logger.addHandler(RichHandler(level=logging.DEBUG))
    logger.setLevel(logging.DEBUG)
    logger.debug("Logger initialized.")

    api = pro_api()

    stock_list = get_symbol_list("stock")
    TushareDailyUpdaterWithSymbolAndTime(
        biz_key="tushare_daily", api_method=api.daily
    ).download(
        symbols=stock_list,
        stop_at=datetime.strptime("20240101", "%Y%m%d"),
    )
