from datetime import datetime

from sqlalchemy import Float, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from . import Base


class TradablePrice(Base):
    __abstract__ = True

    start_time: Mapped[datetime] = mapped_column(nullable=False)
    end_time: Mapped[datetime] = mapped_column(nullable=False)

    open: Mapped = mapped_column(Float, nullable=False)
    high: Mapped = mapped_column(Float, nullable=False)
    low: Mapped = mapped_column(Float, nullable=False)
    close: Mapped = mapped_column(Float, nullable=False)
    volume: Mapped = mapped_column(Float, nullable=False)
