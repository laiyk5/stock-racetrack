import logging
from typing import Optional

import sqlalchemy
import sqlalchemy.exc

from .tables import Base

logger = logging.getLogger(__name__)


class DB:
    _instance: Optional["DB"] = None

    def __init__(self, database_url: str):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        try:
            self._engine = create_engine(database_url)
        except sqlalchemy.exc.ArgumentError as e:
            logger.error(
                f"Failed to create engine with database URL '{database_url}': {e}"
            )
            raise

        self.Session = sessionmaker(bind=self._engine)
        Base.metadata.create_all(self._engine)

    def get_session_factory(self):
        return self.Session

    @classmethod
    def get_instance(cls, database_url: str) -> "DB":
        if DB._instance is None:
            DB._instance = DB(database_url)
        return DB._instance
