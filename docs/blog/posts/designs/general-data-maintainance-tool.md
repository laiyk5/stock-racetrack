---
date:
    created: 2025-12-07
    updated: 2025-12-07
draft: true
---

# Data Maintainance and Management

<!-- more -->

## Non Functional Requirements

The data maintaince system should be highly extensible:

1. Extensible Relationship: new data table should be integrated into the existing relationship definition
2. Extensible Maintainance Routine: new maintainance routine should be easily integrated into the existing one.
3. Multi Source Updating: each data table should be able to be updated by various sources.

## User Requirements

User wants a maintainance tool to easily setup or execute the maintainance routines.

Users wants to be able to:

- **Setup** an auto update mode
- **Update** the selected data tables manually.
- **Inspect** the integrety and timeliness of the data.

## API

| HTTP Method | Endpoint                     | Parameter Schema                                                        | Return Schema                                  | Description                             |
| ----------- | ---------------------------- | ----------------------------------------------------------------------- | ---------------------------------------------- | --------------------------------------- |
| POST        | /api/v1/market-data/update   | {table_name: "", method: "", args: {}}                                  | {result: "", err_msg: ""}                      | update the data to the latest available |
| GET         | /api/v1/market-data/info     | {table_name: "", update_at: "", statistics: {}}                         | {result: "", err_msg: ""}                      | check the state of the data             |
| PUT         | /api/v1/market-data/settings | {db_url: "",  table_name: "", method: "", frequency: "", tolerance: ""} | {result: "", err: [ {field: "", err_msg: ""}]} | send settings                           |
| GET         | /api/v1/market-data/settings | {}                                                                      | {all fields presented in the PUT variant}      | get settings                            |

## Approaches

- Maintainance: Define abstract maintainer components for each data table.
- Other components of the system directly use the ORM to fetch data.

## Consultant & Evaluator & Simulator Module

1. `Tradable`: describing something tradable, including the `exchange`, the `code`
2. `Asset`: describing the position of your `Tradable`
3. `Account`: describing an abstract account that containing a list of `Asset`. Also preserve the history of the account.
4. `Suggestion`: Suggestion is given by `Consultant`, according to the current state of `Account`.
5. `Consultant`: describing an abstract consultant that give live suggestion according to the `Account` and the market intelligence.

Then it comes to monitor submodule

1. `AccountDelta`: describing a set of action that should be applied. Should be able to parsed from `Suggestion`.
2. `Executor`: accept certain type of account (simulated or real with credentials) and apply the supplied `AccountDelta` to it.
3. `Evaluator`: evaluate the performance of the account based on the history performance.
4. `Monitor`: this is the top level layer. It consists of an Account(attached with an executor), a Consultant and several Evaluator. By default it run at real time, as the Consultant

Then it comes to the simulator submodule

The simulator would help filter out `future` information.
