import logging
from abc import abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Callable

import pandas as pd
import tenacity
from tushare import pro_api, set_token

from . import config
from .dbtools import Dataset, Query, RawDataRecord, get_missing_queries, store_data
from .tracker import track
from .utils import get_symbol_list

logger = logging.getLogger(__name__)


class API:
    biz_key: str
    limit_qps: int  # queries per second
    limit_rpq: int  # records per query
    preference: str = "symbol"  # or "time", or "hybrid"
    frequency: timedelta = timedelta(days=1)
    dataset: Dataset
    delay: timedelta = max(frequency, timedelta(seconds=3600))

    @abstractmethod
    def download_on_symbol(
        self, symbol: str, start_at: datetime, stop_at: datetime
    ) -> list[RawDataRecord]:
        """Download data for a single symbol of a time range."""
        ...

    @abstractmethod
    def download_on_time(
        self, symbols: list[str], start_at: datetime, stop_at: datetime
    ) -> list[RawDataRecord]:
        """Download data for multiple symbols of a time range smaller than the frequency."""
        ...


class TushareAPI(API):

    def __init__(
        self,
        api_method: Callable[..., pd.DataFrame],
        dataset: Dataset,
        limit_qps: int = 10,
        limit_rpq: int = 6000,
        preference: str = "hybrid",
        frequency: timedelta = timedelta(days=1),
    ):
        set_token(config.get("tushare", "token"))
        self.dataset = dataset
        self.dataset.provider = "tushare"
        self.limit_qps = limit_qps
        self.limit_rpq = limit_rpq
        self.preference = preference
        self.frequency = frequency

        self.api_method = api_method

    def _transform_record(self, row) -> RawDataRecord:
        timestamp = datetime.strptime(row["trade_date"], "%Y%m%d")
        timestamp = timestamp.replace(
            tzinfo=timezone(timedelta(hours=8))
        )  # Asia/Shanghai
        return RawDataRecord(
            symbol=row["ts_code"],  # symbol
            start_at=timestamp,  # timestamp
            stop_at=timestamp + self.frequency,  # end timestamp
            data=row.to_json(),  # data
        )

    @tenacity.retry(
        wait=tenacity.wait_fixed(1),  # wait 1 second between retries
        stop=tenacity.stop_after_attempt(5),  # stop after 5 attempts
        reraise=True,
    )
    def download_on_time(
        self, symbols: list[str], start_at: datetime, stop_at: datetime
    ):
        logger.debug(
            f"Downloading by time: {symbols[:5]}...({len(symbols) - 5} more), {start_at}, {stop_at}"
        )
        if stop_at - start_at > self.frequency:
            logger.error("Date range too large for time-based download")
            logger.error(
                f"start_at: {start_at}, stop_at: {stop_at}, frequency: {self.frequency}"
            )
            raise ValueError("Date range too large for time-based download")
        df = self.api_method(
            start_date=start_at.strftime("%Y%m%d"),
            end_date=stop_at.strftime("%Y%m%d"),
        )

        records = []
        if df is not None and not df.empty:
            # filter out all symbols not in the list
            df = df[df["ts_code"].isin(symbols)]
            for row in df.iterrows():
                record = self._transform_record(row[1])
                records.append(record)
        return records

    @tenacity.retry(
        wait=tenacity.wait_exponential(multiplier=1, min=4, max=60),
        stop=tenacity.stop_after_attempt(5),
        reraise=True,
    )
    def download_on_symbol(self, symbol: str, start_at: datetime, stop_at: datetime):
        logger.debug(f"Downloading by symbol: {symbol}, {start_at}, {stop_at}")
        df = self.api_method(
            ts_code=symbol,
            start_date=start_at.strftime("%Y%m%d"),
            end_date=stop_at.strftime(
                "%Y%m%d"
            ),  # tushare API is inclusive of end_date, so there's no need to ceiling stop_at.
        )
        records = []
        if df is not None and not df.empty:
            for row in df.iterrows():
                record = self._transform_record(row[1])
                records.append(record)
        return records


def merge_missing_queries(api: API, missing_queries: list[Query]) -> list[Query]:
    """The merge algorithm merge missing queries into larger queries based on api's preference and limit.

    Given a list of missing queries, each query is a Query object with a same dataset.
    Returns a list of merged queries.

    The size of a query is defined as the number of records it will return, which is:
        - For symbol-based query: number of symbols * number of time units (e.g. days) decided by api.frequency
        - For time-based query: number of symbols
    The request size cannot exceed api.limit_rpq.

    If the api supports symbol-based download, it will merge by symbol first, then by time;
    If the api supports time-based download, it will merge by time first, then by symbol;
    If the api supports hybrid download, then compare the above merge results and choose the smaller one.
    """
    if not missing_queries:
        return []

    if api.preference == "symbol":
        return merge_symbols(api, missing_queries)
    elif api.preference == "time":
        return merge_timeranges(api, missing_queries)
    elif api.preference == "hybrid":
        queries_with_symbol_merged = merge_symbols(api, missing_queries)
        queries_with_time_merged = merge_timeranges(api, missing_queries)
        if len(queries_with_symbol_merged) <= len(queries_with_time_merged):
            return queries_with_symbol_merged
        else:
            return queries_with_time_merged
    else:
        raise ValueError(f"Unknown preference: {api.preference}")


def merge_symbols(api: API, missing_queries: list[Query]) -> list[Query]:
    """Divide the time range into chunks of api.frequency and merge the same symbol in the same time chunk.

    Do not merge symbols from different time chunks.
    If merging the next query exceeds api.limit_rpq, start a new merged query.
    """

    chunked_queries = {}
    for q in missing_queries:
        current_start = q.start_at
        while current_start < q.stop_at:
            current_end = min(current_start + api.frequency, q.stop_at)
            key = (q.dataset, current_start, current_end)
            if key not in chunked_queries:
                chunked_queries[key] = []
            chunked_queries[key].append(q.symbols[0])
            current_start = current_end

    merged_queries = []
    current_query = None
    for (dataset, start_at, stop_at), symbols in sorted(chunked_queries.items()):
        if current_query is None:
            current_query = Query(dataset, symbols, start_at, stop_at)
        else:
            # if the same time chunk and not exceeds limit
            estimated_size = len(current_query.symbols + symbols) * (
                (current_query.stop_at - current_query.start_at) // api.frequency
            )
            if (
                dataset == current_query.dataset
                and start_at == current_query.start_at
                and stop_at == current_query.stop_at
                and estimated_size <= api.limit_rpq
            ):
                # merge into current query
                current_query = Query(
                    current_query.dataset,
                    current_query.symbols + symbols,
                    current_query.start_at,
                    current_query.stop_at,
                )
            else:
                # save current query and start a new one
                merged_queries.append(current_query)
                current_query = Query(
                    current_query.dataset,
                    symbols,
                    start_at,
                    stop_at,
                )
    if current_query is not None:
        merged_queries.append(current_query)

    return merged_queries


def merge_timeranges(api: API, missing_queries: list[Query]) -> list[Query]:
    """Merge queries of the same symbol.

    If merging the next query exceeds api.limit_rpq, start a new merged query.
    """
    merged_queries = []
    current_query = None
    for q in sorted(missing_queries, key=lambda x: (x.symbols[0], x.start_at)):
        if current_query is None:
            current_query = q
        else:
            # if the same symbol and not exceeds limit
            estimated_size = (q.stop_at - current_query.start_at) // api.frequency
            if (
                q.dataset == current_query.dataset
                and q.symbols[0] == current_query.symbols[0]
                and estimated_size > api.limit_rpq
            ):
                # merge into current query
                current_query = Query(
                    q.dataset,
                    q.symbols,
                    current_query.start_at,
                    q.stop_at,
                )
            else:
                # save current query and start a new one
                merged_queries.append(current_query)
                current_query = Query(
                    q.dataset,
                    q.symbols,
                    q.start_at,
                    q.stop_at,
                )

    if current_query is not None:
        merged_queries.append(current_query)

    return merged_queries


def _download(api: API, query: Query):
    stop_at = min(datetime.now(tz=query.stop_at.tzinfo), query.stop_at)
    start_at = max(query.start_at, datetime(1989, 1, 1, tzinfo=query.start_at.tzinfo))
    if start_at >= stop_at:
        logger.info("No data to download.")
        return

    logger.debug(
        f"Downloading with query: {query.dataset}, {query.symbols[:5]}...({len(query.symbols) - 5} more), {start_at}, {stop_at}"
    )
    missing_queries = get_missing_queries(query)
    logger.debug(f"Sample missing queries: {missing_queries[:5]}")
    merged_missing_queries = merge_missing_queries(api, missing_queries)
    logger.debug(f"Sample merged missing queries: {merged_missing_queries[:5]}")

    for query in track(merged_missing_queries):
        logger.info(
            f"Downloading data for {len(query.symbols)} symbols from {query.start_at} to {query.stop_at}..."
        )
        if api.preference == "symbol":
            for symbol in query.symbols:
                records = api.download_on_symbol(symbol, query.start_at, query.stop_at)
                store_data(query, records, api.delay)
        elif api.preference == "time":
            records = api.download_on_time(query.symbols, query.start_at, query.stop_at)
            store_data(query, records, api.delay)
        elif api.preference == "hybrid":
            num_symbols = len(query.symbols)
            if num_symbols == 1:
                records = api.download_on_symbol(
                    query.symbols[0], query.start_at, query.stop_at
                )
                store_data(query, records, api.delay)
            elif num_symbols > 1:  # by time
                records = api.download_on_time(
                    query.symbols, query.start_at, query.stop_at
                )
                store_data(query, records, api.delay)
            else:
                raise ValueError("No symbols to download")
        else:
            raise ValueError(f"Unknown preference: {api.preference}")


api = pro_api()
TUSHARE_AVAILABLE_DATASETS = {
    "daily": {
        "method": api.daily,
        "symbol_type": "stock",
    },
    "daily_basic": {
        "method": api.daily_basic,
        "symbol_type": "stock",
    },
    "moneyflow": {
        "method": api.moneyflow,
        "symbol_type": "stock",
    },
}


def tushare_download(query: Query):

    if query.dataset.name not in TUSHARE_AVAILABLE_DATASETS:
        raise ValueError(f"Unknown dataset: {query.dataset.name}")

    dataset_info = TUSHARE_AVAILABLE_DATASETS[query.dataset.name]
    api_method = dataset_info["method"]
    api = TushareAPI(
        api_method=api_method,
        dataset=query.dataset,
        limit_qps=10,
        limit_rpq=6000,
        preference="hybrid",
        frequency=timedelta(days=1),
    )
    if query.symbols == []:
        query.symbols = get_symbol_list(dataset_info["symbol_type"])

    _download(api, query)


AVAILABLE_PROVIDER = {"tushare": tushare_download}


def download(query: Query):

    if query.dataset.provider not in AVAILABLE_PROVIDER:
        raise ValueError(f"Unknown provider: {query.dataset.provider}")

    handler = AVAILABLE_PROVIDER[query.dataset.provider]
    handler(query)
