import logging
from abc import ABC, abstractmethod
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg

logger = logging.getLogger(__name__)

from . import config
from .dbtools import Dataset, Query, get_conn_str


def _set_timezone(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=ZoneInfo(config.get("app", "timezone")))
    return dt.astimezone(ZoneInfo(config.get("app", "timezone")))


class Datasource(ABC):

    @staticmethod
    @abstractmethod
    def get_stock_price_ohlcv_daily(
        symbol: str, start_at: datetime, end_at: datetime
    ) -> pd.DataFrame:
        pass

    @staticmethod
    @abstractmethod
    def get_stock_moneyflow(
        symbol: str, start_at: datetime, end_at: datetime
    ) -> pd.DataFrame:
        pass

    @staticmethod
    @abstractmethod
    def get_stock_basic_daily(
        symbol: str, start_at: datetime, end_at: datetime
    ) -> pd.DataFrame:
        pass


from .downloader import download


def _fetch_data(
    dataset, symbol: str, start_at: datetime, end_at: datetime
) -> pd.DataFrame:
    try:
        download(
            Query(
                dataset,
                [symbol],
                start_at,
                end_at,
            )
        )
    except Exception as e:
        logger.error(f"Error downloading data: {e}", exc_info=True)

    with psycopg.connect(get_conn_str(config.get("database", "dbname"))) as conn:
        with conn.cursor() as cur:
            query = """
            SELECT * FROM raw_data
            WHERE dataset_id = %s AND symbol = %s
            AND tstzrange && tstzrange(%s, %s, '[)')
            ORDER BY tstzrange
            """
            cur.execute(query, (dataset.id(), symbol, start_at, end_at))
            rows = cur.fetchall()
            if not rows:
                raise ValueError(
                    f"No data found for symbol {symbol} between {start_at} and {end_at}"
                )
    return rows


def _setup_dataframe(records: list) -> pd.DataFrame:
    df = pd.DataFrame(records)
    df.set_index("trade_date", inplace=True)
    df.index = pd.to_datetime(df.index, format=r"%Y%m%d")
    df.sort_index(inplace=True)
    return df


class TushareDatasource(Datasource):
    provider = "tushare"

    @staticmethod
    def get_stock_price_ohlcv_daily(
        symbol: str, start_at: datetime, end_at: datetime
    ) -> pd.DataFrame:
        start_at = _set_timezone(start_at)
        end_at = _set_timezone(end_at)

        dataset = Dataset(TushareDatasource.provider, "stock", "daily")
        rows = _fetch_data(dataset, symbol, start_at, end_at)
        records = [
            {
                "trade_date": datetime.strptime(row[4]["trade_date"], r"%Y%m%d"),
                "Open": row[4]["open"],
                "High": row[4]["high"],
                "Low": row[4]["low"],
                "Close": row[4]["close"],
                "Volume": row[4]["vol"],
            }
            for row in rows
        ]

        df = _setup_dataframe(records)

        logger.debug(
            f"Retrieved {len(df)} rows for symbol {symbol} between {start_at} and {end_at}."
            f" Data sample:{df.head()}",
        )

        return df

    @staticmethod
    def get_stock_moneyflow(
        symbol: str,
        start_at: datetime,
        end_at: datetime,
    ):
        start_at = TushareDatasource._set_timezone(start_at)
        end_at = TushareDatasource._set_timezone(end_at)

        dataset = Dataset(TushareDatasource.provider, "stock", "moneyflow")
        rows = _fetch_data(dataset, symbol, start_at, end_at)
        records = [
            {
                "trade_date": datetime.strptime(row[4]["trade_date"], r"%Y%m%d"),
                "elg_buy_vol": row[4]["buy_elg_vol"],
                "elg_sell_vol": row[4]["sell_elg_vol"],
                "lg_buy_vol": row[4]["buy_lg_vol"],
                "lg_sell_vol": row[4]["sell_lg_vol"],
                "md_buy_vol": row[4]["buy_md_vol"],
                "md_sell_vol": row[4]["sell_md_vol"],
                "sm_buy_vol": row[4]["buy_sm_vol"],
                "sm_sell_vol": row[4]["sell_sm_vol"],
            }
            for row in rows
        ]

        df = _setup_dataframe(records)

        logger.debug(
            f"Retrieved {len(df)} rows for symbol {symbol} between {start_at} and {end_at}."
            f" Data sample:{df.head()}",
        )
        return df

    @staticmethod
    def get_stock_basic_daily(
        symbol: str, start_at: datetime, end_at: datetime
    ) -> pd.DataFrame:
        start_at = _set_timezone(start_at)
        end_at = _set_timezone(end_at)

        dataset = Dataset(TushareDatasource.provider, "stock", "daily_basic")
        rows = _fetch_data(dataset, symbol, start_at, end_at)
        records = [
            {
                "trade_date": datetime.strptime(row[4]["trade_date"], r"%Y%m%d"),
                "turnover_rate": row[4]["turnover_rate"],
                "turnover_rate_free": row[4]["turnover_rate_f"],
                "pe": row[4]["pe"],
                "pb": row[4]["pb"],
                "ps": row[4]["ps"],
                "dv": row[4]["dv_ratio"],
                "pe_ttm": row[4]["pe_ttm"],
                # "pb_ttm": row[4]["pb_ttm"],   # pb ttm does not exists.
                "ps_ttm": row[4]["ps_ttm"],
                "dv_ttm": row[4]["dv_ttm"],
                "total_share": row[4]["total_share"],
                "float_share": row[4]["float_share"],
                "free_float_share": row[4]["free_share"],
                "total_mv": row[4]["total_mv"],
                "float_mv": row[4]["circ_mv"],
            }
            for row in rows
        ]

        df = _setup_dataframe(records)

        logger.debug(
            f"Retrieved {len(df)} rows for symbol {symbol} between {start_at} and {end_at}."
            f" Data sample:{df.head()}",
        )

        return df
