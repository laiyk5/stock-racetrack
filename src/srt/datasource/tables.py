from datetime import datetime
from typing import Optional

from sqlalchemy import (
    ForeignKey,
    UniqueConstraint,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    MappedAsDataclass,
    declared_attr,
    mapped_column,
    relationship,
)

from .data import Tradable, TradablePrice


class Base(DeclarativeBase):
    pass


class DataInfoTable(MappedAsDataclass, Base):
    __tablename__ = "data_info"

    name: Mapped[str] = mapped_column(nullable=False, unique=True)
    description: Mapped[Optional[str]] = mapped_column(nullable=True)
    last_updated: Mapped[Optional[datetime]] = mapped_column(nullable=True)

    id: Mapped[Optional[int]] = mapped_column(
        primary_key=True, default=None, nullable=False, autoincrement=True
    )


class TradableTable(MappedAsDataclass, Base):

    __tablename__ = "tradables"

    @declared_attr
    def prices(cls):
        return relationship(
            "TradablePriceTable",
            back_populates="tradable",
            cascade="all, delete-orphan",
        )

    exchange: Mapped[str] = mapped_column(nullable=False)
    symbol: Mapped[str] = mapped_column(nullable=False)
    type: Mapped[str] = mapped_column(nullable=False)  # e.g., "stock", "bond", etc.
    alias: Mapped[Optional[str]] = mapped_column(nullable=True)

    data_info_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("data_info.id"), nullable=False
    )

    id: Mapped[Optional[int]] = mapped_column(
        primary_key=True, default=None, nullable=False, autoincrement=True
    )

    _last_updated: Mapped[Optional[datetime]] = mapped_column(
        nullable=False, default_factory=datetime.now, onupdate=datetime.now
    )

    __table_args__ = (
        # Ensure that the combination of market and symbol is unique
        UniqueConstraint("exchange", "symbol", "type", name="_uc_exchange_symbol_type"),
    )

    def to_tradable(self) -> Tradable:
        from srt.datasource.data import Exchange, Tradable

        return Tradable(
            exchange=Exchange.parse_string(self.exchange),
            symbol=self.symbol,
        )


class TradablePriceTable(MappedAsDataclass, Base):
    __tablename__ = "stock_daily_price_data"

    @declared_attr
    def data_info(cls):
        return relationship("DataInfoTable", backref="price_data")

    start_time: Mapped[datetime] = mapped_column(nullable=False)
    end_time: Mapped[datetime] = mapped_column(nullable=False)

    open: Mapped[float] = mapped_column(nullable=False)
    high: Mapped[float] = mapped_column(nullable=False)
    low: Mapped[float] = mapped_column(nullable=False)
    close: Mapped[float] = mapped_column(nullable=False)
    volume: Mapped[int] = mapped_column(nullable=False)

    data_info_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("data_info.id"),
        nullable=False,
    )

    tradable_id: Mapped[int] = mapped_column(ForeignKey("tradables.id"), nullable=False)
    id: Mapped[Optional[int]] = mapped_column(
        primary_key=True, nullable=False, autoincrement=True, default=None
    )

    @declared_attr
    def tradable(cls):
        return relationship("TradableTable", back_populates="prices")

    def to_tradable_price(self) -> TradablePrice:
        from srt.datasource.data import TradablePrice

        return TradablePrice(
            start_time=self.start_time,
            end_time=self.end_time,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
        )

    __table_args__ = (
        UniqueConstraint(
            "data_info_id",
            "tradable_id",
            "start_time",
            name="_uc_datainfo_tradable_starttime",
        ),
    )
