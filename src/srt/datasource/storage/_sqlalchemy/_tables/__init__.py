from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


def init_database(db_url: str):
    from sqlalchemy import create_engine

    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
