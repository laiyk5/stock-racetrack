import logging
from abc import ABC, abstractmethod
from datetime import datetime

import pandas as pd
import psycopg

logger = logging.getLogger(__name__)

from .dbtools import DB_NAME, get_conn_str


class Datasource(ABC):
    @staticmethod
    @abstractmethod
    def get_stock_price_ohlcv_daily(
        symbol: str, start_at: datetime, end_at: datetime
    ) -> pd.DataFrame:
        pass


from .downloader import download


class TushareDatasource(Datasource):

    @staticmethod
    def get_stock_price_ohlcv_daily(
        symbol: str, start_at: datetime, end_at: datetime
    ) -> pd.DataFrame:
        try:
            download("tushare_daily", [symbol], start_at, end_at)
        except Exception as e:
            logger.warning(f"Error downloading data: {e}")

        with psycopg.connect(get_conn_str(DB_NAME)) as conn:
            with conn.cursor() as cur:
                query = """
                SELECT * FROM raw_data
                WHERE biz_key = %s AND symbol = %s
                AND tstzrange && tstzrange(%s, %s, '[)')
                ORDER BY tstzrange
                """
                cur.execute(query, ("tushare_daily", symbol, start_at, end_at))
                rows = cur.fetchall()
                if not rows:
                    raise ValueError(
                        f"No data found for symbol {symbol} between {start_at} and {end_at}"
                    )
                records = [
                    {
                        "trade_date": datetime.strptime(
                            row[4]["trade_date"], r"%Y%m%d"
                        ),
                        "Open": row[4]["open"],
                        "High": row[4]["high"],
                        "Low": row[4]["low"],
                        "Close": row[4]["close"],
                        "Volume": row[4]["vol"],
                    }
                    for row in rows
                ]

                df = pd.DataFrame.from_records(
                    records,
                    columns=["trade_date", "Open", "High", "Low", "Close", "Volume"],
                )

                df.set_index("trade_date", inplace=True)
                df.index = pd.to_datetime(df.index, format=r"%Y%m%d")
                df.sort_index(inplace=True)

                logger.debug(
                    """
                Retrieved %d rows for symbol %s between %s and %s
                Data sample:
                %s
                """,
                    len(df),
                    symbol,
                    start_at,
                    end_at,
                    df.head(),
                )

                return df
