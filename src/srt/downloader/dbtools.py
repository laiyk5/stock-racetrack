# Utility to check which (biz_key, symbol, timestamp) pairs are missing from raw_data
import logging
from datetime import datetime
from typing import Any, List, Optional, Tuple

import psycopg
from rich.logging import RichHandler

from . import config

logger = logging.getLogger(__name__)

DB_NAME = "stock_test"
SQL_CONN_BASE_INFO = {
    "host": config.get("database", "host"),
    "port": config.get("database", "port"),
    "user": config.get("database", "user"),
    "password": config.get("database", "password"),
    "dbname": DB_NAME,
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
            cur.execute("DROP TABLE IF EXISTS raw_data;")
            cur.execute("DROP TABLE IF EXISTS raw_data_coverage;")
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_data (
                    id SERIAL PRIMARY KEY,
                    biz_key VARCHAR(32) NOT NULL,
                    symbol VARCHAR(16) NOT NULL,
                    tstzrange tstzrange NOT NULL,
                    data JSONB NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    CONSTRAINT raw_data_no_overlap EXCLUDE USING GIST (biz_key WITH =, symbol WITH =, tstzrange WITH &&),
                    CONSTRAINT raw_data_enforce_bounds CHECK (lower_inc(tstzrange) AND NOT upper_inc(tstzrange)),
                    UNIQUE(biz_key, symbol, tstzrange)
                );
                CREATE INDEX IF NOT EXISTS idx_raw_data_biz_key ON raw_data(biz_key);
                CREATE INDEX IF NOT EXISTS idx_raw_data_biz_key_symbol ON raw_data(biz_key, symbol);
                CREATE INDEX IF NOT EXISTS idx_raw_data_biz_start_at ON raw_data(biz_key, tstzrange);
                CREATE INDEX IF NOT EXISTS idx_raw_data_biz_symbol_start_at ON raw_data(biz_key, symbol, tstzrange);
                """
            )
            # Create a raw_data_coverage table to track the coverage of data for each (biz_key, symbol)
            # Any new data inserted into raw_data_coverage should not overlap with existing coverage
            # If so, merge the coverage ranges
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_data_coverage (
                    id SERIAL PRIMARY KEY,
                    biz_key VARCHAR(32) NOT NULL,
                    symbol VARCHAR(16) NOT NULL,
                    tstzrange tstzrange NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    CONSTRAINT raw_data_coverage_no_overlap EXCLUDE USING GIST (biz_key WITH =, symbol WITH =, tstzrange WITH &&),
                    CONSTRAINT raw_data_coverage_no_adjacent EXCLUDE USING gist (tstzrange WITH -|-),
                    CONSTRAINT raw_data_coverage_enforce_bounds CHECK (lower_inc(tstzrange) AND NOT upper_inc(tstzrange)),
                    UNIQUE(biz_key, symbol, tstzrange)
                );
                CREATE INDEX IF NOT EXISTS idx_raw_data_coverage_biz_key ON raw_data_coverage(biz_key);
                CREATE INDEX IF NOT EXISTS idx_raw_data_coverage_biz_key_symbol ON raw_data_coverage(biz_key, symbol);
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


def delete_rawdata_by_bizkey(biz_key: str):

    url = get_conn_str(DB_NAME)
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                    DELETE FROM raw_data WHERE biz_key = %s;
                    DELETE FROM raw_data_coverage WHERE biz_key = %s;
                """,
                (biz_key, biz_key),
            )
            conn.commit()
    logger.info(f"Deleted all records with biz_key = {biz_key} from raw_data.")


def get_missing_queries(
    raw_query: Tuple[str, List[str], datetime, datetime],
) -> List[Tuple[str, str, datetime, datetime]]:
    """
    Given a raw query (biz_key, symbols, start_at, stop_at), return a list of (biz_key, symbol, start_at, stop_at)
    tuples that are missing in raw_data.
    """

    # substract existing record coverage from raw_query coverage
    url = get_conn_str(DB_NAME)
    missing = []
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            biz_key, symbols, start_at, stop_at = raw_query
            for symbol in symbols:
                cur.execute(
                    """
                    SELECT lower(tstzrange), upper(tstzrange)
                    FROM raw_data_coverage
                    WHERE biz_key = %s AND symbol = %s AND tstzrange && tstzrange(%s, %s, '[)');
                    """,
                    (biz_key, symbol, start_at, stop_at),
                )
                rows = cur.fetchall()
                if not rows:
                    missing.append((biz_key, symbol, start_at, stop_at))
                else:
                    # calculate the missing ranges
                    current_start = start_at
                    rows.sort()  # sort by lower bound
                    for row in rows:
                        existing_start, existing_end = row
                        if current_start < existing_start:
                            missing.append(
                                (biz_key, symbol, current_start, existing_start)
                            )
                        if current_start < existing_end:
                            current_start = existing_end
                    if current_start < stop_at:
                        missing.append((biz_key, symbol, current_start, stop_at))
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
    query: Tuple[str, List[str], datetime, datetime],
    transformed_data: list[Tuple[str, str, datetime, datetime, Any]],
) -> int:
    """Store the transformed data into the PostgreSQL database.

    transformed_data: List of tuples (biz_key, symbol, start_at, stop_at, data)
    return: number of successfully inserted records
    """

    conn_str = get_conn_str(DB_NAME)
    inserted_count = 0
    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:

            for record in transformed_data:
                biz_key, symbol, start_at, stop_at, data = record
                try:
                    # Insert into raw_data table
                    cur.execute(
                        """
                        INSERT INTO raw_data (biz_key, symbol, tstzrange, data)
                        VALUES (%s, %s, tstzrange(%s, %s, '[)'), %s)
                        ON CONFLICT (biz_key, symbol, tstzrange) DO NOTHING;
                        """,
                        (biz_key, symbol, start_at, stop_at, data),
                    )
                    if cur.rowcount == 1:
                        inserted_count += 1
                except Exception as e:
                    logger.error(f"Error inserting record {record}: {e}")
                    raise e

            # Now update the raw_data_coverage table
            biz_key, symbols, start_at, stop_at = query
            # find out all ranges that overlap or are adjacent to the new range
            cur.execute(
                """
                SELECT tstzrange
                FROM raw_data_coverage
                WHERE biz_key = %s AND symbol = ANY(%s) AND (tstzrange && tstzrange(%s, %s, '[)') OR tstzrange -|- tstzrange(%s, %s, '[)'));
                """,
                (biz_key, symbols, start_at, stop_at, start_at, stop_at),
            )
            rows = cur.fetchall()
            if rows:
                # merge all these ranges with the new range
                merged_start = start_at
                merged_end = stop_at
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
                    WHERE biz_key = %s AND symbol = ANY(%s) AND (tstzrange && tstzrange(%s, %s, '[)') OR tstzrange -|- tstzrange(%s, %s, '[)'));
                    """,
                    (biz_key, symbols, start_at, stop_at, start_at, stop_at),
                )
                # insert the merged range
                cur.execute(
                    """
                    INSERT INTO raw_data_coverage (biz_key, symbol, tstzrange)
                    SELECT %s, symbol, tstzrange(%s, %s, '[)')
                    FROM unnest(%s::varchar[]) AS symbol
                    ON CONFLICT (biz_key, symbol, tstzrange) DO NOTHING;
                    """,
                    (biz_key, merged_start, merged_end, symbols),
                )
            else:
                # simply insert the new range
                cur.execute(
                    """
                    INSERT INTO raw_data_coverage (biz_key, symbol, tstzrange)
                    SELECT %s, symbol, tstzrange(%s, %s, '[)')
                    FROM unnest(%s::varchar[]) AS symbol
                    ON CONFLICT (biz_key, symbol, tstzrange) DO NOTHING;
                    """,
                    (biz_key, start_at, stop_at, symbols),
                )

            conn.commit()
    logger.debug(
        f"Successfully inserted {inserted_count} / {len(transformed_data)} data."
    )
    return inserted_count
