"""Yahoo Finance 非官方 chart 端點（僅作報價來源之一，無服務保證）。"""

CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=5d"


def fetch_chart_last_two(symbol: str, http_get, timeout: int = 8):
    """回傳 {"value", "prev", "time"}；失敗回傳 None。
    value 優先 meta.regularMarketPrice，prev 取倒數第二根日 K 收盤。"""
    try:
        r = http_get(CHART_URL.format(symbol=symbol), timeout=timeout,
                     headers={"User-Agent": "Mozilla/5.0"})
        res = r.json()["chart"]["result"][0]
        meta = res.get("meta", {})
        quote = (res.get("indicators", {}).get("quote") or [{}])[0]
        closes = [c for c in (quote.get("close") or []) if c is not None]
        value = meta.get("regularMarketPrice")
        if value is None and closes:
            value = closes[-1]
        if value is None or len(closes) < 2:
            return None
        return {"value": float(value), "prev": float(closes[-2]),
                "time": meta.get("regularMarketTime")}
    except Exception:
        return None
