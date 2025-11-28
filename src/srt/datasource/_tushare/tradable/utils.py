def translate_market_and_symbol_to_ts_code(market: str, symbol: str) -> str:
    """
    Translate market and symbol to Tushare ts_code format.
    Tushare ts_code format is <symbol>.<exchange_code>
    where exchange_code is one of the following:
    - SSE for Shanghai Stock Exchange
    - SZSE for Shenzhen Stock Exchange
    - BJE for Beijing Stock Exchanges
    """
    exchange_map = {
        "CN.SSE": "SH",
        "CN.SZSE": "SZ",
        "CN.BJE": "BJ",
    }
    key = f"{market}"
    if key not in exchange_map:
        raise ValueError(f"Unsupported market: {market}")
    exchange_code = exchange_map[key]
    return f"{symbol}.{exchange_code}"


def translate_ts_code_to_market_and_symbol(ts_code: str) -> tuple[str, str]:
    """
    Translate Tushare ts_code format to market and symbol.
    Tushare ts_code format is <symbol>.<exchange_code>
    where exchange_code is one of the following:
    - SSE for Shanghai Stock Exchange
    - SZSE for Shenzhen Stock Exchange
    - BJE for Beijing Stock Exchange
    """
    exchange_map = {
        "SZ": "CN.SZSE",
        "SH": "CN.SSE",
        "BJ": "CN.BJE",
    }
    try:
        symbol, exchange_code = ts_code.split(".")
    except ValueError:
        raise ValueError(f"Invalid ts_code format: {ts_code}")
    if exchange_code not in exchange_map:
        raise ValueError(f"Unsupported exchange code: {exchange_code}")
    market = exchange_map[exchange_code]
    return market, symbol
