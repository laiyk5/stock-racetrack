import logging
from typing import Optional

from .tables import Base

logger = logging.getLogger(__name__)


class DB:
    _instance: Optional["DB"] = None

    def __init__(self, database_url: str):
        from sqlalchemy import create_engine
        from sqlalchemy.orm import sessionmaker

        self._engine = create_engine(database_url)
        self.Session = sessionmaker(bind=self._engine)
        Base.metadata.create_all(self._engine)

    def get_session_factory(self):
        return self.Session

    def get_instance(self, database_url: str) -> "DB":
        if DB._instance is None:
            DB._instance = DB(database_url)
        return DB._instance
