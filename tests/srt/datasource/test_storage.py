from datetime import datetime, timedelta

import pytest

from srt.datasource.storage import (
    Property,
    PropertyPrice,
    SQLAlchemyPropertyPriceStorage,
    SQLAlchemyPropertyStorage,
)

database_str = "sqlite:///:memory:"


@pytest.fixture
def property_storage():
    storage = SQLAlchemyPropertyStorage(database_str)
    yield storage


@pytest.fixture
def property_example():
    yield Property(
        market="Test Market", symbol="TEST", alias="Test Alias", type="stock"
    )


class TestSQLAlchemyPropertyStorage:
    def test_save_and_load(self, property_storage, property_example):
        prop = property_storage.save(property_example)
        prop_loaded = property_storage.load(prop.id)
        assert prop_loaded == prop


class TestSQLAlchemyPropertyPriceStorage:
    @pytest.fixture
    def property_price_storage(self, property_storage):
        storage = SQLAlchemyPropertyPriceStorage(
            database_str, property_storage=property_storage
        )
        yield storage

    def test_save_and_load_price(
        self, property_price_storage, property_storage, property_example
    ):
        prop = property_storage.save(property_example)
        prop_loaded = property_storage.load(prop.id)
        assert prop_loaded == prop

        prop = property_storage.search(
            market=property_example.market, symbol=property_example.symbol
        )
        assert prop is not None

        now = datetime.now()
        now_yesterday = now - timedelta(days=1)

        price = PropertyPrice(
            time=now_yesterday,
            property=prop,
            end_time=now,
            open=100,
            high=110,
            low=90,
            close=105,
            volume=1000,
        )
        property_price_storage.save(price)
        loaded_price = property_price_storage.load(prop.id, price.time, price.end_time)

        assert loaded_price is not None
        assert len(loaded_price) > 0

        loaded_price = loaded_price[0]

        assert loaded_price == price
        assert loaded_price.property == prop
