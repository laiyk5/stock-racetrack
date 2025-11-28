from datetime import datetime
from typing import Optional

from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import Mapped, MappedAsDataclass, mapped_column

from . import Base


class TradableTable(MappedAsDataclass, Base):
    __abstract__ = True

    market: Mapped[str] = mapped_column(nullable=False)
    symbol: Mapped[str] = mapped_column(nullable=False)
    type: Mapped[str] = mapped_column(nullable=False)  # e.g., "stock", "bond", etc.
    alias: Mapped[Optional[str]] = mapped_column(nullable=True)

    id: Mapped[Optional[int]] = mapped_column(
        primary_key=True, default=None, nullable=False, autoincrement=True
    )

    _last_updated: Mapped[Optional[datetime]] = mapped_column(
        nullable=False, default_factory=datetime.now, onupdate=datetime.now
    )

    __table_args__ = (
        # Ensure that the combination of market and symbol is unique
        UniqueConstraint("market", "symbol", "type", name="_uc_market_symbol_type"),
    )

    __mapper_args__ = {
        "polymorphic_on": type,
    }
