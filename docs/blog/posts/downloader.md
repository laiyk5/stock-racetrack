---
date:
  created: 2025-09-23
  created: 2025-09-24
categories:
  - Data
---

# Difficulties of Implementing a Data Downloader

## 1. The value of a data downloader

HTTP based APIs (like Tushare and AKshare) are very convenient, but when it comes to stock research, backtesting or auto-trading, they have some fatal drawbacks:

1. Network delay: the network delay would accumulate to an unacceptable level when the computation involves many queries.
2. API limitation: most of the public data API endpoint has a low limitation of fetching data.

To overcome these difficulties, one solution is to "cached" them in your private database.

## 2. Nature of Web-based API

The nature of all APIs are:

1. Long RTT(round-trip-time)
2. Low request frequency
3. Limited records returned

- To overcome 1, the solution is parallelization and cache.
- To overcome 2, the solution would be batch and cache.
- To overcome 3, we use batch with limited size.

## 3. Nature of Stock Data API

Most Stock data APIs can be categorized as:

| API params \ Data indexed by | **symbol & timestamp**            |
|------------------------------|-----------------------------------|
| **symbol or timestamp**      | depending on the size of the data |
| **symbols(s)**               | by symbol                         |
| **timestamp / timerange**    | by timerange                      |

So Stock Data API can be defined as:

```python
class API:
{
  "biz_key": "tushare_daily"
  "fetch_methods": {
    "by_time": by_time # None | Callable[str, start, end]
    "by_symbol": by_symbol # None | Callable[list[str], start, end]
  },
  "limit": {
    "qps": 10 # query per second, int
    "rpq": 6000 # response per query, int
  },
  "frequency": timedelta(1) # frequencies of time.
}
```

## 4. Variety of data schema

For stock data, there're so many data endpoints with so many different schemas. It's not realistic to replicate such amount of APIs.
But we can directly store each data record as a json object to decouple the fetching and preprocessing stage.

Most of the stock data types are indexed by two dimension: time and symbol. The symbol can be stock, fund, index or something else.

So each record in the table would be:

(biz_key, symbol, timestamp, data) or
(biz_key, symbol, timerange, data)

timestamp can be represented by timerange as long as we state that the left endpoint of the
timerange is the timestamp to be represented, so record in the table could be:

```python
record = (biz_key, symbol, timerange, data)
```

, where `biz_key` is the identifier of the API.

With this schema, we can adapt to many different API of many different data providers easily.

## 5. API abuse: Meaning less request

The data request is usually like this:

```python
request = (biz_key, symbol_set, timerange)
```

But when most of the data in the request is already fetched,
fetching all data specified by the request would be wasteful.

What's more, the underlying API might be frequency limited or returning record limited,
which further complicates the problem.

### 5.1 Solution 1: brute force

We assume that the data provider can provide both fetch-by-time and fetch-by-symbol methods,
the size of the whole dataset is acceptable for both storage and time to fetch. Then we just need
to:

1. assume that all presented symbol is up-to-date for the last update time
2. and just fetch full history of those missing symbols till the last update time
3. and finally fetch all data from last update time to now by time.

### 5.2 Solution 2: fine grianed control

Remember our purpose: we want to avoid abusing the API. So the target is not
to avoid all meaningless request, but try to cut as more meaningless request as possible.

To avoid abusing the API, the solution is merging the underlying data request.
But before we can merge the data requests, we have to find them out first.

#### 5.2.1 Tracking the missing data

One solution is, for each `(biz_key, symbol)`, maintaining a table `raw_data_coverage` recording the
data fetched during `timerange`. Every time we bulk write the `raw_data` table, we also bulk request
and update `raw_data_ccoverage`. And every time we update the table, we spend `O(S)` time to calculate
the indexes of missing records, where `S` is the size of the symbol set given.

There're some corner stage. Some data is missing because of its nature: the market is close on that day, the
company is unlisted, not listed, suspended and so on. So the calculated set is a superset of the
missing data.

#### 5.2.2 The merging direction

Different API suits different merging direction. If the API is a crawler that craw detail page of a specific
symbol, then we should merge in the time direction; if the API can provide a fetch several symbols in batch,
then merging in the symbol direction is also a good choice. As long as the stride is under the limit specified
by the API, merge all requests.

But which are better? My solution is: take both and compare. The complexity is just `O(T) + O(S)`.
So the computing costs is much lower then the `O(TS)` fetching costs.

One corner case is that the missing data is distributed evenly. This would trigger a fetch of all existing data.
But since the request is both symbol locally and time locally, this case would be rare.
