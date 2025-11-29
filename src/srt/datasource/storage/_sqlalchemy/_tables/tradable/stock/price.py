from sqlalchemy import ForeignKey, PrimaryKeyConstraint, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from srt.datasource.storage._sqlalchemy._tables.tradable.price import TradablePrice
from srt.datasource.storage._sqlalchemy._tables.tradable.stock.stock import StockTable
from srt.datasource.types.tradable.stock.price import StockPrice


class StockPriceTable(TradablePrice):
    __abstract__ = True

    stock_id: Mapped[int] = mapped_column(ForeignKey("stocks.id"), nullable=False)

    __table_args__ = (
        UniqueConstraint("stock_id", "start_time", name="uix_stock_start_time"),
        PrimaryKeyConstraint("stock_id", "start_time", name="pk_stock_price"),
    )

    stock: Mapped[StockTable] = relationship("StockTable", back_populates="prices")

    def to_stock_price(self) -> StockPrice:
        return StockPrice(
            start_time=self.start_time,
            end_time=self.end_time,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            volume=self.volume,
            stock=self.stock.to_stock(),
        )


class StockDailyPriceTable(StockPriceTable):
    __tablename__ = "stock_daily_prices"
    pass


class StockWeeklyPriceTable(StockPriceTable):
    __tablename__ = "stock_weekly_prices"
    pass


class StockMonthlyPriceTable(StockPriceTable):
    __tablename__ = "stock_monthly_prices"
    pass


class StockYearlyPriceTable(StockPriceTable):
    __tablename__ = "stock_yearly_prices"
    pass


from datetime import datetime
from typing import Optional

from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, MappedAsDataclass, mapped_column

from . import Base


class StockPriceCoverageTable(MappedAsDataclass, Base):
    __abstract__ = True

    id: Mapped[Optional[int]] = mapped_column(primary_key=True)
    stocks_id: Mapped[int] = mapped_column(ForeignKey("stocks.id"), nullable=False)
    start_time: Mapped[datetime] = mapped_column(nullable=False)
    end_time: Mapped[datetime] = mapped_column(nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "stocks_id", "start_time", "end_time", name="uix_stock_price_coverage"
        ),
    )


class StockDailyPriceCoverageTable(StockPriceCoverageTable):
    __tablename__ = "stock_daily_price_coverage"
    pass


class StockWeeklyPriceCoverageTable(StockPriceCoverageTable):
    __tablename__ = "stock_weekly_price_coverage"
    pass


class StockMonthlyPriceCoverageTable(StockPriceCoverageTable):
    __tablename__ = "stock_monthly_price_coverage"
    pass


class StockYearlyPriceCoverageTable(StockPriceCoverageTable):
    __tablename__ = "stock_yearly_price_coverage"
    pass
