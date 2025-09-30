import logging
import pprint

import numpy as np

from srt import config

logger = logging.getLogger(__name__)
logger.setLevel(config.get("app", "log_level", fallback="INFO"))

from backtesting import Backtest, Strategy
from talib import SMA

from srt.datasource.datasource import TushareDatasource


def pyramid_buy_sell_point(series, n):
    """Return buy and sell points for pyramid strategy."""
    buy_points = np.zeros(series.shape).tolist()
    sell_points = np.zeros(series.shape).tolist()
    pivot_price = series[0]
    for i, price in enumerate(series):
        buy_points[i] = sell_points[i] = None
        pct_change = (price - pivot_price) / pivot_price
        if abs(pct_change) >= n:
            pivot_price = price
            if pct_change < 0:
                buy_points[i] = price
            else:
                sell_points[i] = price
    return buy_points, sell_points


def pyramid_buy_point(series, n):
    return pyramid_buy_sell_point(series, n)[0]


def pyramid_sell_point(series, n):
    return pyramid_buy_sell_point(series, n)[1]


class Pyramid(Strategy):
    """Buy more when lossing every n% percent until no cash left,
    and sell more when winning every n% percent until no position left.
    Start buying when price goes down, and start selling when price goes up.

    Only long direction allowed.
    """

    gap = 0.1
    max_lots = 5  # max number of lots to buy or sell
    period1 = 5
    period2 = 60

    def init(self):

        self.sma1 = self.I(SMA, self.data.Close, self.period1)
        self.sma2 = self.I(SMA, self.data.Close, self.period2)

        self.pyramid_buy_points = self.I(
            pyramid_buy_point, self.data.Close, self.gap, scatter=True, overlay=True
        )

        self.pyramid_sell_points = self.I(
            pyramid_sell_point, self.data.Close, self.gap, scatter=True, overlay=True
        )

        self.pivot_price = self.data.Close[0]

        self.sell_idx_list = []
        self.buy_idx_list = list(range(self.max_lots))

        self.cycle = "buy"  # or "sell"

        # exponential size for each lot
        self.lot_sizes = [100 * (2**i) for i in range(self.max_lots)]
        self.position_size_list = []

    def next(self):
        # update position_size_list if position size changed outside
        if abs(self.position.size - sum(self.position_size_list)) < 1e-3:
            pass
        elif self.position.size > sum(self.position_size_list):  # bought
            self.position_size_list.append(
                self.position.size - sum(self.position_size_list)
            )
            self.position_size_list.sort()
        elif self.position.size < sum(self.position_size_list):  # sold
            # always sell the smallest lot
            self.position_size_list.pop(0)

        pct_change = (self.data.Close[-1] - self.pivot_price) / self.pivot_price

        if abs(pct_change) >= self.gap:
            logger.debug(
                f"Current position size: {self.position.size}, position_size_list: {self.position_size_list}"
            )
            logger.debug(
                f"buy_idx_list: {self.buy_idx_list}, sell_idx_list: {self.sell_idx_list}"
            )
            logger.debug(
                f"pct_change: {pct_change:.2%}, pivot_price: {self.pivot_price}, current price: {self.data.Close[-1]}"
            )
            logger.debug(
                f"Price change {pct_change:.2%} reached threshold {self.gap:.2%}, pivot_price: {self.pivot_price}, current price: {self.data.Close[-1]}"
            )
            self.pivot_price = self.data.Close[-1]

            logger.debug(f"sma1: {self.sma1[-1]}, sma2: {self.sma2[-1]}")
            if (
                self.sma1[-1] < self.sma2[-1] and pct_change < 0
            ):  # price going down, buy more
                if len(self.buy_idx_list) > 0:
                    buy_idx = self.buy_idx_list.pop(
                        0
                    )  # always buy the smallest available slot.
                    self.sell_idx_list.append(buy_idx)
                    self.sell_idx_list.sort()

                    # self.position.close()
                    size = self.lot_sizes[buy_idx] / sum(
                        [self.lot_sizes[i] for i in self.buy_idx_list + [buy_idx]]
                    )
                    if size == 1:
                        size -= 1e-9  # to avoid full cash used error
                    logger.debug(f"Buy size(in liquidity percentage): {size}")
                    order = self.buy(size=size)
                    logger.debug(f"Buy order: {order}")
            elif (
                self.sma1[-1] > self.sma2[-1] and pct_change > 0
            ):  # price going up, sell more
                if len(self.sell_idx_list) > 0 and len(self.position_size_list) > 0:
                    logger.debug(f"Sell position_size_list: {self.position_size_list}")
                    sell_idx = self.sell_idx_list.pop(
                        0
                    )  # always sell the smallest available slot
                    self.buy_idx_list.append(sell_idx)
                    self.buy_idx_list.sort()

                    self.position.close(
                        self.position_size_list[0] / sum(self.position_size_list)
                    )
                    self.position_size_list.pop(0)
                    # self.sell(size=self.position_size_list.pop(0))


import heapq


class NaivePyramid(Strategy):
    max_lots = 5  # max number of lots to buy or sell
    buy_gap = 0.05  # threshold percent for each buy
    sell_gap = 0.05  # threshold percent for each sell

    def init(self):
        self.buy_idxes = list(range(self.max_lots))
        self.sell_idxes = []
        self.position_size_list = []

        self.buy_min_price = None
        self.sell_max_price = None

        self.lot_weights = [i + 1 for i in range(self.max_lots)]

        heapq.heapify(self.buy_idxes)
        heapq.heapify(self.sell_idxes)
        heapq.heapify(self.position_size_list)

    def pyramid_buy(self):
        if (
            len(self.buy_idxes) == 0
            or len(self.sell_idxes) > len(self.position_size_list)
            or (
                self.buy_min_price is not None
                and self.data.Close[-1] > self.buy_min_price * (1 - self.buy_gap)
            )
        ):
            return
        buy_idx = heapq.heappop(self.buy_idxes)
        heapq.heappush(self.sell_idxes, buy_idx)
        used_pct = self.lot_weights[buy_idx] / sum(
            [self.lot_weights[i] for i in self.buy_idxes + [buy_idx]]
        )
        if used_pct == 1:
            used_pct -= 1e-9
        order = self.buy(size=used_pct)
        logger.debug(
            f"Buy order: {order}, position_size_list: {self.position_size_list}"
        )

    def pyramid_sell(self):
        if (
            len(self.sell_idxes) == 0
            or len(self.position_size_list) == 0
            or self.data.Close[-1] < self.buy_min_price * (1 + self.sell_gap)
            or (
                self.sell_max_price is not None
                and self.data.Close[-1] < self.sell_max_price * (1 + self.sell_gap)
            )
        ):
            return
        sell_idx = heapq.heappop(self.sell_idxes)
        heapq.heappush(self.buy_idxes, sell_idx)
        sell_size = heapq.heappop(self.position_size_list)
        heapq.heappush(self.position_size_list, sell_size)  # push back temporarily
        self.position.close(sell_size / sum(self.position_size_list))
        logger.debug(
            f"Sell size: {sell_size}, position_size_list: {self.position_size_list}"
        )

    def pyramid_update(self):
        if (
            self.position.size - sum(self.position_size_list) > 1e-3
        ):  # bought success, sell available.
            heapq.heappush(
                self.position_size_list,
                self.position.size - sum(self.position_size_list),
            )
            self.buy_min_price = (
                self.data.Open[-1]
                if self.buy_min_price is None
                else min(self.buy_min_price, self.data.Open[-1])
            )
            logger.debug(
                f"Buy success, new position_size_list: {self.position_size_list}, buy_min_price: {self.buy_min_price}, sell_max_price: {self.sell_max_price}"
            )
        elif sum(self.position_size_list) - self.position.size > 1e-3:
            heapq.heappop(self.position_size_list)
            self.sell_max_price = (
                self.data.Open[-1]
                if self.sell_max_price is None
                else max(self.sell_max_price, self.data.Open[-1])
            )
            if self.position.size < 1e-3:
                self.buy_min_price = None
                self.sell_max_price = None
            logger.debug(
                f"Sell success, new position_size_list: {self.position_size_list}, buy_min_price: {self.buy_min_price}, sell_max_price: {self.sell_max_price}"
            )

    def next(self):
        self.pyramid_update()
        self.pyramid_buy()
        self.pyramid_sell()


import talib


class SimplePyramid(NaivePyramid):
    mfi_period = 14

    def init(self):
        super().init()
        self.bbands_upper, self.bbands_middle, self.bbands_lower = self.I(
            talib.BBANDS,
            self.data.Close,
            timeperiod=20,
            nbdevup=2,
            nbdevdn=2,
            matype=0,
            overlay=True,
        )
        self.rsi = self.I(talib.RSI, self.data.Close, timeperiod=14, overlay=False)
        self.macd, self.macd_signal, self.macd_hist = self.I(
            talib.MACD,
            self.data.Close,
            fastperiod=12,
            slowperiod=26,
            signalperiod=9,
            overlay=False,
        )
        self.mfi = self.I(
            talib.MFI,
            self.data.High,
            self.data.Low,
            self.data.Close,
            self.data.Volume,
            timeperiod=self.mfi_period,
            overlay=False,
        )
        self.ma = self.I(talib.MA, self.data.Close, timeperiod=120, overlay=True)

    def next(self):
        # super().next()
        self.pyramid_update()
        if (
            True
            and self.macd_signal[-1] < self.macd[-1]
            # and self.data.Close[-1] < self.bbands_middle[-1]
            # and self.data.Low[-1] < self.bbands_lower[-1]
            # and self.mfi[-1] < 20
            # and self.data.Close[-1] < self.ma[-1]
        ):
            self.pyramid_buy()
        elif (  # high price zone, try selling
            True
            and self.macd_signal[-1] > self.macd[-1]
            # and self.data.High[-1] > self.bbands_middle[-1]
            # and self.data.Close[-1] > self.bbands_upper[-1]
            # and self.mfi[-1] > 80
            # and self.data.Close[-1] > self.ma[-1]
        ):
            self.pyramid_sell()


def backtest(symbol, start_at, end_at, optimize=False):
    import numpy as np

    ds = TushareDatasource()
    df = ds.get_stock_price_ohlcv_daily(symbol, start_at, end_at)

    bt = Backtest(
        df,
        NaivePyramid,
        cash=100_000,
        commission=(5, 0.0003),
        trade_on_close=False,
        finalize_trades=True,
    )

    if optimize:
        stats = bt.optimize(
            buy_gap=np.arange(0.01, 0.3, 0.01).tolist(),
            sell_gap=np.arange(0.01, 0.3, 0.01).tolist(),
            max_lots=range(3, 7, 1),
            maximize="SQN",
        )
    else:
        stats = bt.run()

    print(stats)
    bt.plot(plot_return=True)

    pprint.pprint(stats["_trades"].to_dict(orient="records"))


if __name__ == "__main__":
    backtest(
        symbol="000001.SZ",
        start_at="2020-01-01",
        end_at="2023-10-01",
        optimize=True,
    )
