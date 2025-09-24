---
date:
  created: 2025-09-23
  updated: 2025-09-24
categories:
  - Data
draft: true
---
# Data Processing Module Design

<!-- more -->

## What's important to a stock research system

1. timeliness. the timeliness of data is invaluable.
2. stability. the stability of data source is vital for auto-trading.
3. quality. higher data quality means less corner case programming and more accuracy.

## Collecting data

When we collects data from real world, there might be:

1. one data type from several data sources.
2. different data from different data sources.

We need the `1` to make sure our data source is consistent. The `2` require us to design a general data collecting mechanism to
cope with thses data.

## Preprocessing these data

I do not agree that we could design a good table to retrieve
data before we came up with a good idea that how we would use
these data. So I believe that as long as these data has high
quality, we should not touch them before we actually code the
strategies.
