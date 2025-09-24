import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Callable

import tenacity
from tushare import pro_api, set_token

from srt.downloader.dbtools import get_missing_queries, store_data
from srt.downloader.tracker import track

from . import config

logger = logging.getLogger(__name__)


class API:
    biz_key: str
    limit_qps: int  # queries per second
    limit_rpq: int  # records per query
    preference: str = "symbol"  # or "time", or "hybrid"
    frequency: timedelta = timedelta(days=1)

    @abstractmethod
    def download_on_symbol(
        self, symbol: str, start_at: datetime, stop_at: datetime
    ) -> list[tuple[str, str, datetime, datetime, str]]:
        """Download data for a single symbol of a time range."""
        ...

    @abstractmethod
    def download_on_time(
        self, symbols: list[str], start_at: datetime, stop_at: datetime
    ) -> list[tuple[str, str, datetime, datetime, str]]:
        """Download data for multiple symbols of a time range smaller than the frequency."""
        ...


class TushareAPI(API):

    def __init__(
        self,
        api_method: Callable,
        biz_key: str,
        limit_qps: int = 10,
        limit_rpq: int = 6000,
        preference: str = "hybrid",
        frequency: timedelta = timedelta(days=1),
    ):
        set_token(config.get("tushare", "token"))
        self.biz_key = biz_key
        self.limit_qps = limit_qps
        self.limit_rpq = limit_rpq
        self.preference = preference
        self.frequency = frequency

        self.api_method = api_method

    def _transform_record(self, row) -> tuple[str, str, datetime, datetime, str]:
        timestamp = datetime.strptime(row["trade_date"], "%Y%m%d")
        timestamp = timestamp.replace(
            tzinfo=timezone(timedelta(hours=8))
        )  # Asia/Shanghai
        return (
            self.biz_key,  # biz_key
            row["ts_code"],  # symbol
            timestamp,  # timestamp
            timestamp + self.frequency,  # end timestamp
            row.to_json(),  # data
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
            end_date=stop_at.strftime("%Y%m%d"),
        )
        records = []
        if df is not None and not df.empty:
            for row in df.iterrows():
                record = self._transform_record(row[1])
                records.append(record)
        return records


def merge_missing_queries(
    api: API, missing_queries: list[tuple[str, str, datetime, datetime]]
) -> list[tuple[str, list[str], datetime, datetime]]:
    """The merge algorithm merge missing queries into larger queries based on api's preference and limit.

    Given a list of missing queries, each query is a tuple of (biz_key, symbols, start_at, stop_at) with same biz_key.
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


def merge_symbols(
    api: API, missing_queries: list[tuple[str, str, datetime, datetime]]
) -> list[tuple[str, list[str], datetime, datetime]]:
    """Divide the time range into chunks of api.frequency and merge the same symbol in the same time chunk.

    Do not merge symbols from different time chunks.
    If merging the next query exceeds api.limit_rpq, start a new merged query.
    """

    chuncked_queries = {}
    for biz_key, symbol, start_at, stop_at in missing_queries:
        current_start = start_at
        while current_start < stop_at:
            current_end = min(current_start + api.frequency, stop_at)
            key = (biz_key, current_start, current_end)
            if key not in chuncked_queries:
                chuncked_queries[key] = []
            chuncked_queries[key].append(symbol)
            current_start = current_end

    merged_queries = []
    current_query = None
    for (biz_key, start_at, stop_at), symbols in sorted(chuncked_queries.items()):
        if current_query is None:
            current_query = (biz_key, symbols, start_at, stop_at)
        else:
            current_biz_key, current_symbols, current_start_at, current_stop_at = (
                current_query
            )
            # if the same time chunk and not exceeds limit
            estimated_size = len(current_symbols + symbols) * (
                (current_stop_at - current_start_at) // api.frequency
            )
            if (
                biz_key == current_biz_key
                and start_at == current_start_at
                and stop_at == current_stop_at
                and estimated_size <= api.limit_rpq
            ):
                # merge into current query
                current_query = (
                    current_biz_key,
                    current_symbols + symbols,
                    current_start_at,
                    current_stop_at,
                )
            else:
                # save current query and start a new one
                merged_queries.append(current_query)
                current_query = (biz_key, symbols, start_at, stop_at)
    if current_query is not None:
        merged_queries.append(current_query)

    return merged_queries


def merge_timeranges(
    api: API, missing_queries: list[tuple[str, str, datetime, datetime]]
) -> list[tuple[str, list[str], datetime, datetime]]:
    """Merge queries of the same symbol.

    If merging the next query exceeds api.limit_rpq, start a new merged query.
    """
    merged_queries = []
    current_query = None
    for biz_key, symbol, start_at, stop_at in sorted(missing_queries):
        if current_query is None:
            current_query = (biz_key, symbol, start_at, stop_at)
        else:
            current_biz_key, current_symbol, current_start_at, current_stop_at = (
                current_query
            )
            # if the same symbol and not exceeds limit
            estimated_size = (stop_at - current_start_at) // api.frequency
            if (
                biz_key == current_biz_key
                and symbol == current_symbol
                and estimated_size > api.limit_rpq
            ):
                # merge into current query
                current_query = (
                    current_biz_key,
                    current_symbol,
                    current_start_at,
                    stop_at,
                )
            else:
                # save current query and start a new one
                merged_queries.append(current_query)
                current_query = (biz_key, symbol, start_at, stop_at)
    if current_query is not None:
        merged_queries.append(current_query)

    # convert symbol to list
    merged_queries = [
        (biz_key, [symbol], start_at, stop_at)
        for biz_key, symbol, start_at, stop_at in merged_queries
    ]

    return merged_queries


def download(api: API, symbols: list[str], start_at: datetime, stop_at: datetime):
    stop_at = min(datetime.now(tz=stop_at.tzinfo), stop_at)
    start_at = max(start_at, datetime(1989, 1, 1, tzinfo=start_at.tzinfo))
    if start_at >= stop_at:
        logger.info("No data to download.")
        return
    query = (api.biz_key, symbols, start_at, stop_at)
    logger.debug(
        f"Downloading with query: {api.biz_key}, {symbols[:5]}...({len(symbols) - 5} more), {start_at}, {stop_at}"
    )
    missing_queries = get_missing_queries(query)
    logger.debug(f"Sample missing queries: {missing_queries[:5]}")
    merged_missing_queries = merge_missing_queries(api, missing_queries)
    logger.debug(f"Sample merged missing queries: {merged_missing_queries[:5]}")

    for query in track(merged_missing_queries):
        biz_key, symbols, start_at, stop_at = query
        logger.info(
            f"Downloading data for {len(symbols)} symbols from {start_at} to {stop_at}..."
        )
        if api.preference == "symbol":
            for symbol in symbols:
                records = api.download_on_symbol(symbol, start_at, stop_at)
                store_data(query, records)
        elif api.preference == "time":
            records = api.download_on_time(symbols, start_at, stop_at)
            store_data(query, records)
        elif api.preference == "hybrid":
            num_symbols = len(symbols)
            if num_symbols == 1:
                records = api.download_on_symbol(symbols[0], start_at, stop_at)
                store_data(query, records)
            elif num_symbols > 1:  # by time
                records = api.download_on_time(symbols, start_at, stop_at)
                store_data(query, records)
            else:
                raise ValueError("No symbols to download")
        else:
            raise ValueError(f"Unknown preference: {api.preference}")
