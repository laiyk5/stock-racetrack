# Utility to check which (biz_key, symbol, timestamp) pairs are missing from raw_data
import logging
from datetime import datetime, timedelta
from functools import cache
from typing import Any, List, Optional, Tuple

import psycopg
from rich.logging import RichHandler

from . import config

logger = logging.getLogger(__name__)

DB_NAME = config.get("database", "dbname")
SQL_CONN_BASE_INFO = {
    "host": config.get("database", "host"),
    "port": config.get("database", "port"),
    "user": config.get("database", "user"),
    "password": config.get("database", "password"),
    "dbname": config.get("database", "dbname"),
}


def get_conn_str(db: Optional[str] = None) -> str:
    conn_info = SQL_CONN_BASE_INFO.copy()
    if db:
        conn_info["dbname"] = db
    else:
        conn_info.pop("dbname", None)
    return " ".join([f"{k}={v}" for k, v in conn_info.items()])


# reset table
def reset_tables():

    url = get_conn_str(DB_NAME)
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS dataset;")
            cur.execute("DROP TABLE IF EXISTS raw_data;")
            cur.execute("DROP TABLE IF EXISTS raw_data_coverage;")
            # Create a dataset table to store metadata about datasets
            cur.execute(
                """
                        CREATE TABLE IF NOT EXISTS dataset (
                            id SERIAL PRIMARY KEY,
                            provider VARCHAR(16) NOT NULL,
                            market VARCHAR(16) NOT NULL,
                            name VARCHAR(32) NOT NULL,
                            UNIQUE(provider, market, name)
                        )
                        """
            )
            # Create a raw_data table to store the actual data
            # The tstzrange column is used to store the time range of the data
            # The data column is used to store the actual data in JSONB format
            # Ensure that there is no overlap in tstzrange for the same (dataset_id, symbol)
            # Ensure that lower bound is inclusive and upper bound is exclusive
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_data (
                    id SERIAL PRIMARY KEY,
                    dataset_id INTEGER REFERENCES dataset(id) ON DELETE CASCADE,
                    symbol VARCHAR(16) NOT NULL,
                    tstzrange tstzrange NOT NULL,
                    data JSONB NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    CONSTRAINT raw_data_no_overlap EXCLUDE USING GIST (dataset_id WITH =, symbol WITH =, tstzrange WITH &&),
                    CONSTRAINT raw_data_enforce_bounds CHECK (lower_inc(tstzrange) AND NOT upper_inc(tstzrange)),
                    UNIQUE(dataset_id, symbol, tstzrange)
                );
                CREATE INDEX IF NOT EXISTS idx_raw_data_dataset_id ON raw_data(dataset_id);
                CREATE INDEX IF NOT EXISTS idx_raw_data_dataset_id_symbol ON raw_data(dataset_id, symbol);
                CREATE INDEX IF NOT EXISTS idx_raw_data_symbol_start_at ON raw_data(symbol, tstzrange);
                CREATE INDEX IF NOT EXISTS idx_raw_data_dataset_id_symbol_start_at ON raw_data(dataset_id, symbol, tstzrange);
                """
            )
            # Create a raw_data_coverage table to track the coverage of data for each (dataset_id, symbol)
            # Any new data inserted into raw_data_coverage should not overlap with existing coverage
            # If so, merge the coverage ranges
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_data_coverage (
                    id SERIAL PRIMARY KEY,
                    dataset_id INTEGER REFERENCES dataset(id) ON DELETE CASCADE,
                    symbol VARCHAR(16) NOT NULL,
                    tstzrange tstzrange NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    CONSTRAINT raw_data_coverage_no_overlap EXCLUDE USING GIST (dataset_id WITH =, symbol WITH =, tstzrange WITH &&),
                    CONSTRAINT raw_data_coverage_no_adjacent EXCLUDE USING gist (tstzrange WITH -|-),
                    CONSTRAINT raw_data_coverage_enforce_bounds CHECK (lower_inc(tstzrange) AND NOT upper_inc(tstzrange)),
                    UNIQUE(dataset_id, symbol, tstzrange)
                );
                CREATE INDEX IF NOT EXISTS idx_raw_data_coverage_dataset_id ON raw_data_coverage(dataset_id);
                CREATE INDEX IF NOT EXISTS idx_raw_data_coverage_dataset_id_symbol ON raw_data_coverage(dataset_id, symbol);
                """
            )
            conn.commit()


# reset database and tables
def reset_database():

    url = get_conn_str(f"postgres")
    with psycopg.connect(url, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS {DB_NAME};")
            cur.execute(f"CREATE DATABASE {DB_NAME};")

    with psycopg.connect(get_conn_str(DB_NAME), autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE EXTENSION IF NOT EXISTS btree_gist;")
            cur.execute(f"CREATE EXTENSION IF NOT EXISTS pg_trgm;")
    reset_tables()


class Dataset:
    def __init__(self, provider: str, market: str, name: str):
        self.provider = provider
        self.market = market
        self.name = name

    @cache
    def id(self, create=True) -> int:
        """Get the dataset ID from the database. If not exists, create a new entry if create=True."""
        url = get_conn_str(DB_NAME)
        with psycopg.connect(url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id FROM dataset WHERE provider = %s AND market = %s AND name = %s;
                    """,
                    (self.provider, self.market, self.name),
                )
                row = cur.fetchone()
                if row:
                    return row[0]
                else:
                    if create:
                        cur.execute(
                            """
                            INSERT INTO dataset (provider, market, name) VALUES (%s, %s, %s) RETURNING id;
                            """,
                            (self.provider, self.market, self.name),
                        )
                        dataset_id = cur.fetchone()[0]
                        conn.commit()
                    else:
                        raise ValueError(
                            f"Dataset with provider={self.provider}, market={self.market}, name={self.name} does not exist."
                        )
                    return dataset_id

    def __hash__(self):
        return hash((self.provider, self.market, self.name))


def delete_dataset(dataset: Dataset):

    url = get_conn_str(DB_NAME)
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                    DELETE FROM dataset WHERE provider = %s AND market = %s AND name = %s RETURNING id;
                """,
                (dataset.provider, dataset.market, dataset.name),
            )
            conn.commit()
    logger.info(
        f"Deleted all records with provider = {dataset.provider}, market = {dataset.market}, and name = {dataset.name} from dataset."
    )


class Query:
    def __init__(
        self,
        dataset: Dataset,
        symbols: List[str],
        start_at: datetime,
        stop_at: datetime,
    ):
        self.dataset = dataset
        self.symbols = symbols
        self.start_at = start_at
        self.stop_at = stop_at


class RawDataRecord:
    def __init__(
        self,
        symbol: str,
        start_at: datetime,
        stop_at: datetime,
        data: Any,
    ):
        self.symbol = symbol
        self.start_at = start_at
        self.stop_at = stop_at
        self.data = data


def get_missing_queries(
    raw_query: Query,
) -> List[Query]:
    """
    Given a raw query (dataset_id, symbols, start_at, stop_at), return a list of (dataset_id, [symbol], start_at, stop_at)
    tuples that are missing in raw_data.
    """

    # substract existing record coverage from raw_query coverage
    url = get_conn_str(DB_NAME)
    missing = []
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            dataset_id = raw_query.dataset.id()
            for symbol in raw_query.symbols:
                cur.execute(
                    """
                    SELECT lower(tstzrange), upper(tstzrange)
                    FROM raw_data_coverage
                    WHERE dataset_id = %s AND symbol = %s AND tstzrange && tstzrange(%s, %s, '[)');
                    """,
                    (
                        dataset_id,
                        symbol,
                        raw_query.start_at,
                        raw_query.stop_at,
                    ),
                )
                rows = cur.fetchall()
                if not rows:
                    missing.append(
                        Query(
                            raw_query.dataset,
                            [symbol],
                            raw_query.start_at,
                            raw_query.stop_at,
                        )
                    )
                else:
                    # calculate the missing ranges
                    current_start = raw_query.start_at
                    rows.sort()  # sort by lower bound
                    for row in rows:
                        existing_start, existing_end = row
                        if current_start < existing_start:
                            missing.append(
                                Query(
                                    raw_query.dataset,
                                    [symbol],
                                    current_start,
                                    existing_start,
                                )
                            )
                        if current_start < existing_end:
                            current_start = existing_end
                    if current_start < raw_query.stop_at:
                        missing.append(
                            Query(
                                raw_query.dataset,
                                [symbol],
                                current_start,
                                raw_query.stop_at,
                            )
                        )
    logger.debug(f"Sample missing queries: {missing[:5]}")
    return missing


if __name__ == "__main__":
    logger.addHandler(RichHandler(level=logging.DEBUG))
    logger.setLevel(logging.DEBUG)
    logger.debug("Logger initialized.")

    reset_database()
    logger.info("Database and tables have been reset.")

    logger.debug("The table's schema is as follows:")
    with psycopg.connect(get_conn_str()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'raw_data';
            """
            )
            rows = cur.fetchall()
            for row in rows:
                logger.debug(f"{row[0]}: {row[1]}")


def store_data(
    query: Query,
    transformed_data: list[RawDataRecord],
    delay: timedelta = timedelta(days=1),
) -> int:
    """Store the transformed data into the PostgreSQL database.

    transformed_data: List of RawDataRecord
    return: number of successfully inserted records
    """

    if len(transformed_data) == 0:
        logger.warning("No data to store.")
        return 0

    dataset_id = query.dataset.id(create=True)

    conn_str = get_conn_str(DB_NAME)
    inserted_count = 0

    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:

            for record in transformed_data:
                try:
                    # Insert into raw_data table
                    cur.execute(
                        """
                        INSERT INTO raw_data (dataset_id, symbol, tstzrange, data)
                        VALUES (%s, %s, tstzrange(%s, %s, '[)'), %s)
                        ON CONFLICT (dataset_id, symbol, tstzrange) DO NOTHING;
                        """,
                        (
                            dataset_id,
                            record.symbol,
                            record.start_at,
                            record.stop_at,
                            record.data,
                        ),
                    )
                    if cur.rowcount == 1:
                        inserted_count += 1
                except Exception as e:
                    logger.error(f"Error inserting record {record}: {e}")
                    raise e

            # Now update the raw_data_coverage table
            # find out all ranges that overlap or are adjacent to the new range
            cur.execute(
                """
                SELECT tstzrange
                FROM raw_data_coverage
                WHERE dataset_id = %s AND symbol = ANY(%s) AND (tstzrange && tstzrange(%s, %s, '[)') OR tstzrange -|- tstzrange(%s, %s, '[)'));
                """,
                (
                    dataset_id,
                    query.symbols,
                    query.start_at,
                    query.stop_at,
                    query.start_at,
                    query.stop_at,
                ),
            )
            rows = cur.fetchall()
            if rows:
                # merge all these ranges with the new range
                merged_start = query.start_at
                merged_end = min(query.stop_at, datetime.now() - delay)
                for row in rows:
                    existing_range = row[0]
                    existing_start = existing_range.lower
                    existing_end = existing_range.upper
                    if existing_start < merged_start:
                        merged_start = existing_start
                    if existing_end > merged_end:
                        merged_end = existing_end
                # delete the old ranges
                cur.execute(
                    """
                    DELETE FROM raw_data_coverage
                    WHERE dataset_id = %s AND symbol = ANY(%s) AND (tstzrange && tstzrange(%s, %s, '[)') OR tstzrange -|- tstzrange(%s, %s, '[)'));
                    """,
                    (
                        dataset_id,
                        query.symbols,
                        query.start_at,
                        query.stop_at,
                        query.start_at,
                        query.stop_at,
                    ),
                )
                # insert the merged range
                cur.execute(
                    """
                    INSERT INTO raw_data_coverage (dataset_id, symbol, tstzrange)
                    SELECT %s, symbol, tstzrange(%s, %s, '[)')
                    FROM unnest(%s::varchar[]) AS symbol
                    ON CONFLICT (dataset_id, symbol, tstzrange) DO NOTHING;
                    """,
                    (dataset_id, merged_start, merged_end, query.symbols),
                )
            else:
                # simply insert the new range
                cur.execute(
                    """
                    INSERT INTO raw_data_coverage (dataset_id, symbol, tstzrange)
                    SELECT %s, symbol, tstzrange(%s, %s, '[)')
                    FROM unnest(%s::varchar[]) AS symbol
                    ON CONFLICT (dataset_id, symbol, tstzrange) DO NOTHING;
                    """,
                    (dataset_id, query.start_at, query.stop_at, query.symbols),
                )

            conn.commit()
    logger.debug(
        f"Successfully inserted {inserted_count} / {len(transformed_data)} data."
    )
    return inserted_count
