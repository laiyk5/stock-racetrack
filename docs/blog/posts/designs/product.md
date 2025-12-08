---
date:
    created: 2025-12-08
    updated: 2025-12-08
links:
    - blog/posts/designs/general-data-maintainance-tool.md
pin: true
---

# Product Design

<!-- more -->

## Requirement Analysis

The strategies developing cycle is: collect data, write a strategies, save the running result, evaluate the performance, and compare it with other strategies or on different time ranges. And finally moving the strategy for actual use require more engineering. Maintaining the data is also a pain. So for strategies engineer, they need:

1. A consistent way of strategy backtesting/simulating/applying
2. A data maintainance tool that will help run the common maintainance routines and warn them if the data is lacked from the database.
3. A clear, easy to use interface that:
   1. notify them to interact with the strategy if it's design with a man in the loop.
   2. provide a monitor foor the running strategy so developer can understand what's going on wherever they are.
   3. provide a data exploration tool so the running results can be saved and the developer can check the results of different runs easily.

## Architecture

- Data management service: a framework that expose an API for daily data management and availability checking.
- Strategy service: a framework that execute the plugged strategy on the plugged broker and publish notification using the plugged notifiers. It also expose an API for monitoring and execution management.
- Web App: provide an Web UI for the two services above. Should be able to remember the user's setting and their configuration. The user should be able to check the performance or the running logs easily.
  - backend: remember the user and its data.
  - UI/frontend

## Roadmap

strategy service depends on the data service for the data avialbility checking.

### Data management Service

features:

- [ ] data coverage checking
- [ ] data maintainance routines scheduling configuration.
- [ ] data maintainance routines status checking

### Strategy service

features:

- [ ] plugable broker (account and way to apply suggestions)
- [ ] plugable strategy (read broker states and offer suggestions)
- [ ] price feed definition
- [ ] decision history checking
- [ ] account history checking
- [ ] notification
- [ ] human-decision making broker

### Data Exploration

!!! note
    Should be decide in the future
