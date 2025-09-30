---
date:
  created: 2025-09-30
  updated: 2025-09-30
categories:
  - Implementation
---

# Implementing a Pyramid Strategy

The core idea of the pyramid strategy is to divide your capital into several lots, with each subsequent lot being larger than the previous one. You start by buying a small lot at a higher price, then buy larger lots as the price decreases. Conversely, you sell small lots at lower prices and larger lots as the price rises. This approach aims to accumulate positions at lower average prices and realize profits by selling at higher average prices.

1. The buying price should be a low price.
2. There must be an opportunity that can help people sell at a higher price.

<!-- more -->

## Naive Pyramid

The algorithm should go like this:

1. Buy if no position held.
2. if so, start buying the stock, everytime we buy, we use the smallest slot available.
3. if the stock price rise and reach our take profit limit, start selling with the smallest occupied lot, and only sell more stocks as the stock rise even higher.
4. Buy more stocks only when the stock price dive into a price lower than previous buy.

But how to decide whether the stock is worth buying? It's not about pyramid; it's another aspect of
strategy. The function of pyramid strategy is just to help you avoid the risk.

## An application of Pyramid

To find a good starting buy point, we can have different methodologies. One of the useful strategies I'm
quite statisfied with is:

1. buy if the low price is lower than the bollinger's lower line
2. sell if the high price is higher than the bollinger's middle line.

## Limitation

Pyramid strategy is good method that help manage position, but it can't prevent you buy at a maxima of price. To find a good buying point and avoid sold too early, we should design a more delicate mechanism.
