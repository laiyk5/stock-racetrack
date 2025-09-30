---
date:
  created: 2025-09-23
  updated: 2025-09-24
categories:
  - Philosophy
---

# Trading Research

Every trader want a robot (autotrader) that earns money for them automatically, so they don't have to stair at the screen all the time.

But how to start a trading research?

<!-- more -->

## A simple idea

The life cycle of an autotrader would be:

1. We develop an autotrader
2. We backtest it on historical data
3. If good, test it in real life or directly put your money in
4. It underperform, so you come back to fix it.

The tools required are:

1. A datasource: a method to easily reach a variety of data with low latency and convinience.
2. A backtesting tool
3. An API to your account (If you want to risk your money)

Datasource is the most tricky part for independent researcher like me. They typically have very limited
funding, so they cannot afford high quality, high availability datasource. But luckily, many web-based
data providers are available.

As to backtesting tool, unluckily, as far as I know, there're no free full-fledge backtesting tool available.
Most of the backtesting tool are still at toy level. They can only run a strategy on a small set of symbols.

## A simple architecture of autotrader

The simplest form of autotrader is that we allocate one autotrader for every stock in the market, and each autotrader focuses on only one stock.

But how do we allocate our money? And if

## A series of questions

When we trade in the market, we always want to keep our portfolio most profitable in the long run.

1. How do we buy and sell?
    1. should we set a stop profit and a stop loss limit?
    2. what's the reason supporting our decision?
        1. technical factors: stock price? marginal capitals? moneyflow?
        2. basic factors: the value of the company? the profitability?
        3. market motion / industry tendency: If the market mood is good? If the industry is thriving? If the company is attched with some label that very popular among investers?
        4. big event: if the company is related to an uncoming event or emergencies that has not been realized?
2. How do we allocate our money?
    1. should we limit the highest among of money that the autotrader can use for each stock?
    2. should we isolate the available money for each stock?
    3. what to do with the profit or loss?
3. The frequency we trade?
    1. what's the minimum time interval between two trades?
        1. If we're just talking about a single stock, what's the frequecy? 1 minute?
        2. If we're talking about the whole market, what's the frequency? 1 hour? 1 day?
    2. how patient we are?
        1. Totally impatient, If I don't earn money in the next second, I just fire the autotrader.
        2. Lossing money for a month is acceptable, but not for a session.
        3. I'm really patient -- I can afford lossing the money for 3-5 years.
4. How to define success?


We don't want to waste our time on a set of slow growing stocks. One problem is the
market changes very fast, the decision we made is only valid if the market doesn't
change much before we execute the strategy. Another problem is
