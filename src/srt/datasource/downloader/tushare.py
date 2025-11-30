import logging
import os
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from typing import Any, Callable, Iterable, Optional, Tuple

from cachetools import cached
from sqlalchemy.orm import Session

from srt.datasource.data import Coverage, Exchange, Tradable
from srt.datasource.tables import (
    Base,
    DataInfoTable,
    TradablePriceTable,
    TradableTable,
)

from .downloader import StockDailyPriceDownloader, StockDownloader

logger = logging.getLogger(__name__)

SRT_TS_EXCHANGE_STR_MAPPING = {
    ("CN", "SZSE"): "SZ",
    ("CN", "SSE"): "SH",
    ("CN", "BJE"): "BJ",
}

TS_SRT_ECHANGE_STR_MAPPING = {v: k for k, v in SRT_TS_EXCHANGE_STR_MAPPING.items()}


def parse_ts_code(ts_code: str) -> tuple[Exchange, str]:
    """Map Tushare ts_code to Tradable object."""
    try:
        symbol, exchange_suffix = ts_code.split(".")
        country_code, exchange = TS_SRT_ECHANGE_STR_MAPPING.get(
            exchange_suffix, (None, None)
        )
        if country_code is None or exchange is None:
            raise ValueError(f"Unknown exchange suffix in ts_code: {ts_code}")
        exchange = Exchange(country=country_code, name=exchange)

        return exchange, symbol
    except Exception as e:
        logger.error(f"Failed to parse ts_code: {ts_code}")
        raise e


def tradable_to_ts_code(tradable: Tradable) -> str:
    """Convert Tradable object to Tushare ts_code."""
    exchange_suffix = SRT_TS_EXCHANGE_STR_MAPPING.get(
        (tradable.exchange.country, tradable.exchange.name)
    )
    if exchange_suffix is None:
        raise ValueError(
            f"Unknown exchange for Tushare: {tradable.exchange.to_string()}"
        )
    return f"{tradable.symbol}.{exchange_suffix}"


@cached(cache=defaultdict())
def ts_code_to_tradable_id(session: Session, ts_code: str) -> int | None:
    """Convert Tushare ts_code to Tradable object."""
    exchange, symbol = parse_ts_code(ts_code)

    tradable = (
        session.query(TradableTable)
        .filter_by(exchange=exchange.to_string(), symbol=symbol)
        .one_or_none()
    )

    return tradable.id if tradable is not None else None


def get_missing_coverages(
    session: Session,
    tradables: Iterable[TradableTable],
    start_time: datetime,
    end_time: datetime,
):
    """Get missing coverages for the given tradables between start_time and end_time.

    Return: dict[int, list[Coverage]] where key is tradable_id and value is list of missing coverages.
    """

    coverages: list[TradablePriceTable] = []

    tradables = list(tradables)

    for i in range(0, len(tradables), 100):
        batch = tradables[i : i + 100]
        coverages.extend(
            session.query(TradablePriceTable)
            .filter(
                TradablePriceTable.start_time <= end_time,
                TradablePriceTable.end_time >= start_time,
                TradablePriceTable.tradable_id.in_([tradable.id for tradable in batch]),
            )
            .order_by(
                TradablePriceTable.tradable_id,
                TradablePriceTable.start_time,
            )
            .all()
        )
        logger.debug(f"Fetched {len(coverages)} coverages for batch {i // 100 + 1}")

    coverage_dict: defaultdict[int, list[Coverage]] = defaultdict(list)
    for coverage in coverages:
        key = coverage.tradable_id
        coverage_dict[key].append(
            Coverage(
                start_time=coverage.start_time,
                end_time=coverage.end_time,
            )
        )

    # find missing coverage
    missing_coverages: dict[int, list[Coverage]] = defaultdict(list)

    for tradable in tradables:
        tradable_id: int = tradable.id  # type: ignore

        coverage = coverage_dict[tradable_id]

        current_end = datetime.combine(start_time, datetime.min.time())

        if len(coverage) == 0:
            missing_coverages[tradable_id].append(
                Coverage(
                    start_time=current_end,
                    end_time=datetime.combine(end_time, datetime.max.time()),
                )
            )
            continue

        if coverage[0].start_time > current_end:
            missing_coverages[tradable_id].append(
                Coverage(
                    start_time=current_end,
                    end_time=coverage[0].start_time,
                )
            )
            current_end = coverage[0].end_time

        for cov in coverage:
            if cov.start_time > current_end:
                missing_coverages[tradable_id].append(
                    Coverage(
                        start_time=current_end,
                        end_time=cov.start_time,
                    )
                )
            current_end = cov.end_time

        if coverage[-1].end_time < datetime.combine(end_time, datetime.max.time()):
            missing_coverages[tradable_id].append(
                Coverage(
                    start_time=current_end,
                    end_time=datetime.combine(end_time, datetime.max.time()),
                )
            )

    return missing_coverages


class TushareStockDownloader(StockDownloader):
    def __init__(self, session_factory: Callable[[], Session], api_token: str):
        from tushare import pro_api

        self._api = pro_api(api_token)
        self._session_factory = session_factory

    def update(self):
        """Download stock data from Tushare and update the local database.

        Retrieves basic information for all listed stocks and merges them into
        the database. Existing records are updated, new ones are inserted.
        """
        try:
            df = self._api.stock_basic(
                exchange="",
                list_status="L",
                fields="ts_code,symbol,name,area,industry,list_date",
            )
        except Exception as e:
            logger.error(f"Failed to fetch stock list from Tushare: {e}")
            raise

        session = self._session_factory()
        for _, row in df.iterrows():
            try:
                ts_code = row["ts_code"]
                exchange, symbol = parse_ts_code(ts_code)

                existing = (
                    session.query(TradableTable)
                    .filter_by(exchange=exchange.to_string(), symbol=symbol)
                    .one_or_none()
                )

                if existing is not None and existing.type != "stock":
                    logger.warning(
                        f"Skipping {exchange.to_string()} {symbol}: "
                        f"exists with type '{existing.type}'"
                    )
                    continue

                if existing is not None:
                    # Update existing record
                    existing.alias = row["name"]
                    existing.data_info_id = StockDownloader.get_data_info_id(session)
                else:
                    # Insert new record
                    session.add(
                        TradableTable(
                            exchange=exchange.to_string(),
                            symbol=symbol,
                            alias=row["name"],
                            type="stock",
                            data_info_id=StockDownloader.get_data_info_id(session),
                        )
                    )
                session.commit()

            except Exception as e:
                logger.error(f"Failed to process stock {row['ts_code']}: {e}")
                session.rollback()

        session.close()


class TushareStockDailyPriceDownloader(StockDailyPriceDownloader):
    def __init__(
        self,
        session_factory: Callable[[], Session],
        api_token: str,
    ):
        self._session_factory = session_factory
        self._logger = logging.getLogger(__name__)

        from tushare import pro_api

        self._api = pro_api(api_token)

        self._rate_limit = 60  # Tushare allows 60 requests per minute for free tier
        self._size_limit = 6000  # max 6000 rows per request

    def download(
        self,
        start_time: datetime,
        end_time: datetime,
        tradable_set: Optional[Iterable[Tradable]] = None,
    ):
        """Download daily stock price data from Tushare and update the local database.

        For each stock in the database, fetches daily price data and merges it
        into the database. Existing records are updated, new ones are inserted.
        """
        session = self._session_factory()
        if tradable_set is not None:
            # search them in the database to get full tradable info
            tradables = []
            for tradable in tradable_set:
                record = (
                    session.query(TradableTable)
                    .filter_by(
                        exchange=tradable.exchange.to_string(),
                        symbol=tradable.symbol,
                    )
                    .one_or_none()
                )
                if record is not None:
                    tradables.append(record)
                else:
                    self._logger.warning(
                        f"Tradable {tradable.exchange.to_string()} {tradable.symbol} not found in database."
                    )
        else:
            tradables = session.query(TradableTable).all()

        # analyze each tradable for their missing data
        missing_coverages: dict[int, list[Coverage]] = dict()  # non empty missing only.

        missing_coverages = get_missing_coverages(
            session, tradables, start_time, end_time
        )

        for k, v in list(missing_coverages.items()):
            if len(v) == 0:
                del missing_coverages[k]

        # filter out weekends from missing coverages
        missing_coverages = {
            i: [cov for cov in covs if cov.start_time.weekday() <= 5]
            for i, covs in missing_coverages.items()
        }

        # merge missings tradables while respecting minimum gap and maximum size

        merged_missings: dict[int, list[Coverage]] = defaultdict(list)
        minimum_gap = timedelta(days=30)
        for tradable_id, coverages in missing_coverages.items():
            coverages.sort(key=lambda c: c.start_time)
            merged = []
            current_coverage = None
            for coverage in coverages:
                if current_coverage is None:
                    current_coverage = coverage
                else:
                    if (
                        coverage.start_time - current_coverage.end_time <= minimum_gap
                        and (coverage.end_time - current_coverage.start_time).days
                        <= self._size_limit
                    ):
                        # merge
                        current_coverage.end_time = max(
                            current_coverage.end_time, coverage.end_time
                        )
                    else:
                        merged.append(current_coverage)
                        current_coverage = coverage
            if current_coverage is not None:
                merged.append(current_coverage)
            merged_missings[tradable_id] = merged

        def download_by_tradable(merged_missings: dict[int, list[Coverage]]):
            # should update the session date by date to match the 'Updater.update' function semantic.
            merged_missings = dict(merged_missings)  # make a copy
            queries: list[tuple[int, datetime, datetime]] = (
                []
            )  # list[(tradable_id, start_time, end_time)]
            for tradable_id, missings in merged_missings.items():
                for m in missings:
                    queries.append((tradable_id, m.start_time, m.end_time))
            queries.sort(key=lambda x: x[1])  # sorted by start_time

            records: dict[datetime, list[TradablePriceTable]] = defaultdict(list)

            # Algorithm explaination:
            # iterate through dates between start_time and end_time
            #   for each date, fetch data for all tradables that start_time ealier than current date
            #   since the queries are sorted by start_time, we can just keep a pointer to the current position and move forward as the date increases.
            #   if we meat a tradable whose start_time is later than current date, we stop fetching and write data of current date to database, and move to next date.
            #   This way, we minimize the number of API calls and respect rate limits.

            for date_cursor in (
                start_time + timedelta(days=n)
                for n in range((end_time - start_time).days + 1)
            ):
                batch_queries: list[tuple[int, datetime, datetime]] = []
                while queries and queries[0][1] <= date_cursor:
                    batch_queries.append(queries.pop(0))

                for tradable_id, cov_start, cov_end in batch_queries:
                    try:
                        tradable = (
                            session.query(TradableTable)
                            .filter_by(id=tradable_id)
                            .one_or_none()
                        )

                        if tradable is None:
                            raise ValueError(
                                f"Tradable with id {tradable_id} not found in database."
                            )

                        ts_code = tradable_to_ts_code(tradable.to_tradable())

                        df = self._api.daily(
                            ts_code=ts_code,
                            start_date=cov_start.strftime("%Y%m%d"),
                            end_date=cov_end.strftime("%Y%m%d"),
                        )
                        for _, row in df.iterrows():
                            _start_time = datetime.strptime(
                                str(row["trade_date"]), "%Y%m%d"
                            )
                            _end_time = _start_time + timedelta(days=1)

                            # avoid duplication

                            existing = session.query(TradablePriceTable).filter_by(
                                data_info_id=StockDailyPriceDownloader.get_data_info_id(
                                    session
                                ),
                                tradable_id=tradable_id,
                                end_time=_end_time,
                            )
                            if existing.count() > 0:
                                continue

                            records[_start_time].append(
                                TradablePriceTable(
                                    start_time=_start_time,
                                    end_time=_end_time,
                                    open=row["open"],
                                    high=row["high"],
                                    low=row["low"],
                                    close=row["close"],
                                    volume=row["vol"],
                                    tradable_id=tradable_id,  # type: ignore
                                    data_info_id=StockDailyPriceDownloader.get_data_info_id(
                                        session
                                    ),
                                )
                            )

                        logger.info(
                            f"Successfully fetched prices for {tradable.exchange} {tradable.symbol} "
                            f"from {cov_start} to {cov_end}"
                        )
                    except Exception as e:
                        logger.error(
                            f"Failed to fetch prices for {tradable_id}"
                            f"from {cov_start} to {cov_end}: {e}"
                        )
                        session.rollback()
                        continue

                # write records of current date to database and flush
                session.add_all(records[date_cursor])
                session.commit()
                del records[date_cursor]  # free memory

        def download_by_trade_date(tradables: Iterable[TradableTable]):

            current_date = start_time
            total_days = (end_time - start_time).days + 1
            day_count = 0

            while current_date < end_time:
                # skip weekends
                if current_date.weekday() >= 5:
                    current_date += timedelta(days=1)
                    day_count += 1
                    logger.debug(
                        f"Skipping weekend date {current_date.date()} ({day_count}/{total_days})"
                    )
                    continue
                try:
                    df = self._api.daily(
                        trade_date=current_date.strftime("%Y%m%d"),
                    )
                    if tradables is not None:
                        # filter df to only include needed tradables
                        ts_codes = [
                            tradable_to_ts_code(tradable.to_tradable())
                            for tradable in tradables
                        ]
                        df = df[df["ts_code"].isin(ts_codes)]

                    for _, row in df.iterrows():
                        _start_time = datetime.strptime(
                            str(row["trade_date"]), "%Y%m%d"
                        )
                        _end_time = _start_time + timedelta(days=1)

                        tradable_id = ts_code_to_tradable_id(
                            session, ts_code=row["ts_code"]
                        )

                        existing = session.query(TradablePriceTable).filter_by(
                            tradable_id=tradable_id,
                            end_time=_end_time,
                            data_info_id=StockDailyPriceDownloader.get_data_info_id(
                                session
                            ),
                        )
                        if existing.count() > 0:
                            continue

                        session.add(
                            TradablePriceTable(
                                start_time=_start_time,
                                end_time=_end_time,
                                open=row["open"],
                                high=row["high"],
                                low=row["low"],
                                close=row["close"],
                                volume=row["vol"],
                                tradable_id=tradable_id,  # type: ignore
                                data_info_id=StockDailyPriceDownloader.get_data_info_id(
                                    session
                                ),
                            )
                        )
                    session.commit()
                    logger.info(
                        f"Successfully downloaded prices on {current_date.date()} ({day_count + 1}/{total_days})"
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to download prices on {current_date.date()} ({day_count + 1}/{total_days}): {e}"
                    )
                    session.rollback()

                current_date += timedelta(days=1)
                day_count += 1

        missing_end_time = max([x[-1].end_time for x in merged_missings.values()])
        missing_start_time = min([x[0].start_time for x in merged_missings.values()])

        n_missing_trade_dates = (missing_end_time - missing_start_time).days + 1
        n_tradables = len(merged_missings)

        logger.debug("Number of missing trade dates: {}".format(n_missing_trade_dates))
        logger.debug("Number of tradables related to: {}".format(n_tradables))

        if n_missing_trade_dates < n_tradables:
            # download by trade date
            logger.debug("Downloading by trade date.")
            download_by_trade_date(tradables)
        else:
            # download by tradable
            logger.debug("Downloading by tradable.")
            download_by_tradable(merged_missings)
        session.close()

    def update(self):
        # search for the latest date in the database

        session = self._session_factory()
        latest_record = (
            session.query(TradablePriceTable)
            .order_by(TradablePriceTable.start_time.desc())
            .first()
        )

        if latest_record is not None:
            last_date = latest_record.end_time.date()
            start_date = last_date + timedelta(days=1)
        else:
            start_date = date.today() - timedelta(
                days=10
            )  # default to yesterday if no data

        self.download(
            start_time=datetime.combine(start_date, time.min),
            end_time=datetime.now(),
            tradable_set=None,
        )


if __name__ == "__main__":

    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    db_url = "sqlite:///db/srt_test.db"
    # db_url = "sqlite:///db/srt_test_{}.db".format(datetime.now().strftime("%Y%m%d_%H%M%S"))

    logging.basicConfig(level=logging.DEBUG)

    engine = create_engine(db_url)
    Base.metadata.create_all(engine)

    session_factory = sessionmaker(bind=engine)

    from dotenv import load_dotenv

    load_dotenv()

    # register data info for stock
    # session = session_factory()
    # StockDownloader.register_data_info(session)
    # StockDailyPriceDownloader.register_data_info(session)
    # session.commit()
    # session.close()

    tushare_token = os.getenv("TUSHARE_API_TOKEN", None)

    if tushare_token is None:
        raise KeyError("TUSHARE_API_TOKEN not set in environment variables.")

    # test tushare stock downloader
    stock_downloader = TushareStockDownloader(
        api_token=tushare_token,
        session_factory=session_factory,
    )

    stock_downloader.update()

    # test tushare stock daily price downloader
    stock_price_downloader = TushareStockDailyPriceDownloader(
        api_token=tushare_token,
        session_factory=session_factory,
    )

    stock_price_downloader.download(
        start_time=datetime(2025, 11, 10),
        end_time=datetime(2025, 11, 28),
        tradable_set=[
            Tradable(exchange=Exchange(country="CN", name="SZSE"), symbol="000001"),
            Tradable(exchange=Exchange(country="CN", name="SSE"), symbol="699999"),
        ],
    )
