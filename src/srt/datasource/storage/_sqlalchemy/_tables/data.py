from datetime import datetime
from typing import Optional

from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import Mapped, MappedAsDataclass, mapped_column

from . import Base


class TimeSeriesDataCoverageTable(MappedAsDataclass, Base):
    __abstract__ = True

    id: Mapped[Optional[int]] = mapped_column(primary_key=True)
    market: Mapped[str] = mapped_column(nullable=False)
    symbol: Mapped[str] = mapped_column(nullable=False)
    data_type: Mapped[str] = mapped_column(
        nullable=False
    )  # e.g., "price", "volume", etc.
    start_time: Mapped[datetime] = mapped_column(nullable=False)
    end_time: Mapped[datetime] = mapped_column(nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "market", "symbol", "data_type", name="uix_market_symbol_data_type"
        ),
    )
