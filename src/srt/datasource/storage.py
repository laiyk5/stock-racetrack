import logging
from abc import ABC
from datetime import datetime
from typing import Literal

from sqlalchemy import Column, UniqueConstraint, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from . import Entity, EntityStorage, TimeSeriesData, TimeSeriesStorage

AllowedPropertyTypes = Literal["stock", "bond"]


class Property(Entity):
    market: str
    symbol: str
    type: AllowedPropertyTypes
    alias: str

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, Property):
            return False
        return (
            self.market == value.market
            and self.symbol == value.symbol
            and self.type == value.type
        )


class PropertyPrice(TimeSeriesData):
    property: Property
    end_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int

    def __eq__(self, value: object) -> bool:
        if not isinstance(value, PropertyPrice):
            return False
        return self.property == value.property and self.end_time == value.end_time


class Base(DeclarativeBase):
    pass


class PropertyTable(Base):
    __tablename__ = "properties"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    market: Mapped[str] = mapped_column()
    symbol: Mapped[str] = mapped_column()
    type: Mapped[AllowedPropertyTypes] = mapped_column()
    alias: Mapped[str] = mapped_column(nullable=True)

    __table_args__ = (UniqueConstraint("market", "symbol", name="_market_symbol_uc"),)


class SQLAlchemyStorage(ABC):
    def __init__(self, database_url: str):
        try:
            self.engine = create_engine(database_url)
            Base.metadata.create_all(self.engine)
            self.Session = sessionmaker(bind=self.engine)
        except Exception as e:
            raise RuntimeError(f"Failed to connect to database: {e}")


class SQLAlchemyPropertyStorage(SQLAlchemyStorage, EntityStorage[Property]):
    def __init__(self, database_url: str):
        super().__init__(database_url)

    def save(self, entity: Property) -> Property:
        session = self.Session()
        property_record = PropertyTable(
            market=entity.market,
            symbol=entity.symbol,
            alias=entity.alias,
            type=entity.type,
        )
        session.add(property_record)
        session.flush()
        property = Property(
            id=property_record.id,
            market=property_record.market,
            symbol=property_record.symbol,
            type=property_record.type,
            alias=property_record.alias,
        )
        session.commit()
        session.close()
        return property

    def load(self, entity_id: int) -> Property:
        session = self.Session()
        record = session.query(PropertyTable).filter_by(id=entity_id).first()
        session.close()
        if record is None:
            raise ValueError(f"Property with id {entity_id} not found")
        return Property(
            id=record.id,
            market=record.market,
            symbol=record.symbol,
            type=record.type,
            alias=record.alias,
        )

    def search(self, market: str, symbol: str) -> Property | None:
        session = self.Session()
        record = (
            session.query(PropertyTable).filter_by(market=market, symbol=symbol).first()
        )
        logging.debug(record)
        logging.debug(record)
        if record is None:
            return None
        property = Property(
            id=record.id,
            market=record.market,
            symbol=record.symbol,
            type=record.type,
            alias=record.alias,
        )
        session.close()
        return property


class PropertyPriceTable(Base):
    __tablename__ = "property_price_data"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    time: Mapped[datetime] = mapped_column()
    property_id: Mapped[int] = mapped_column()
    end_time: Mapped[datetime] = mapped_column()
    open: Mapped[float] = mapped_column()
    high: Mapped[float] = mapped_column()
    low: Mapped[float] = mapped_column()
    close: Mapped[float] = mapped_column()
    volume: Mapped[int] = mapped_column()

    __table_args__ = (
        UniqueConstraint("property_id", "time", name="_property_time_uc"),
    )


class SQLAlchemyPropertyPriceStorage(
    SQLAlchemyStorage, TimeSeriesStorage[Property, PropertyPrice]
):
    def __init__(self, database_url: str, property_storage: SQLAlchemyPropertyStorage):
        super().__init__(database_url)
        self._property_storage = property_storage

    def save(self, data: PropertyPrice) -> None:
        session = self.Session()
        data_record = PropertyPriceTable(
            property_id=data.property.id,
            time=data.time,
            end_time=data.end_time,
            open=data.open,
            high=data.high,
            low=data.low,
            close=data.close,
            volume=data.volume,
        )
        session.add(data_record)
        session.commit()
        session.close()

    def load(
        self, property_id: int, start_time: datetime, end_time: datetime
    ) -> list[PropertyPrice]:
        session = self.Session()
        records = (
            session.query(PropertyPriceTable)
            .filter_by(property_id=property_id)
            .filter(PropertyPriceTable.time >= start_time)
            .filter(PropertyPriceTable.end_time <= end_time)
            .all()
        )
        session.close()

        # load perperty
        property = self._property_storage.load(property_id)

        return [
            PropertyPrice(
                time=record.time,
                property=property,
                end_time=record.end_time,
                open=record.open,
                high=record.high,
                low=record.low,
                close=record.close,
                volume=record.volume,
            )
            for record in records
        ]
