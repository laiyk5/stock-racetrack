from typing import Optional

from sqlalchemy import Column, UniqueConstraint, create_engine
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    MappedAsDataclass,
    mapped_column,
    sessionmaker,
)

from srt.datasource.storage._sqlalchemy._tables.tradable.tradable import TradableTable
from srt.datasource.types.tradable import Tradable

from . import Base


class StockTable(TradableTable, MappedAsDataclass):
    __tablename__ = "stocks"

    type: Mapped[str] = mapped_column(default="stock", nullable=False, init=False)

    __mapper_args__ = {
        "polymorphic_identity": "stock",
    }
