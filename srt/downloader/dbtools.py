# Utility to check which (biz_key, symbol, timestamp) pairs are missing from raw_data
from typing import List, Tuple, Any

from typing import Optional
import psycopg
import logging
from rich.logging import RichHandler

from . import config

logger = logging.getLogger(__name__)

DB_NAME = "stock"
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
            cur.execute(
                "DROP TABLE IF EXISTS raw_data;"
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_data (
                    id SERIAL PRIMARY KEY,
                    biz_key VARCHAR(32) NOT NULL,
                    symbol VARCHAR(16) NOT NULL,
                    timestamp TIMESTAMPTZ NOT NULL,
                    data JSONB NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    UNIQUE(biz_key, symbol, timestamp)
                );
                CREATE INDEX IF NOT EXISTS idx_raw_data_biz_key ON raw_data(biz_key);
                CREATE INDEX IF NOT EXISTS idx_raw_data_biz_key_symbol ON raw_data(biz_key, symbol);
                CREATE INDEX IF NOT EXISTS idx_raw_data_biz_timestamp ON raw_data(biz_key, timestamp DESC);
                CREATE INDEX IF NOT EXISTS idx_raw_data_biz_symbol_time ON raw_data(biz_key, symbol, timestamp DESC);
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
    reset_tables()


def delete_rawdata_by_bizkey(biz_key: str):
    
    url = get_conn_str(DB_NAME)
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM raw_data WHERE biz_key = %s;
            """,
                (biz_key,),
            )
            conn.commit()
    logger.info(f"Deleted all records with biz_key = {biz_key} from raw_data.")


def get_missing_records(
    pairs: List[Tuple[str, str, Any]],
) -> List[Tuple[str, str, Any]]:
    """
    Given a list of (biz_key, symbol, timestamp) tuples, return those not present in raw_data.
    """

    url = get_conn_str(DB_NAME)
    missing = []
    with psycopg.connect(url) as conn:
        with conn.cursor() as cur:
            for biz_key, symbol, timestamp in pairs:
                cur.execute(
                    """
                    SELECT 1 FROM raw_data WHERE biz_key = %s AND symbol = %s AND timestamp = %s
                    """,
                    (biz_key, symbol, timestamp),
                )
                if cur.fetchone() is None:
                    missing.append((biz_key, symbol, timestamp))

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


def store_data(transformed_data: list):
    """
    Store the transformed data into the PostgreSQL database.
    """

    conn_str = get_conn_str(DB_NAME)
    inserted_count = 0
    with psycopg.connect(conn_str) as conn:
        with conn.cursor() as cur:
            for record in transformed_data:
                try:
                    cur.execute(
                        """
                        INSERT INTO raw_data (biz_key, symbol, timestamp, data)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (biz_key, symbol, timestamp) DO NOTHING;
                        """,
                        record,
                    )
                    if cur.rowcount == 1:
                        inserted_count += 1
                except Exception as e:
                    logger.error(f"Error inserting record {record}: {e}")
            conn.commit()
    logger.debug(
        f"Successfully inserted {inserted_count} / {len(transformed_data)} data."
    )
    return inserted_count
