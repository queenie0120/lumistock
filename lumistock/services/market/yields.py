"""美債殖利率服務。

v10.9.188：修正「2 年期」誤用 ^IRX（13 週國庫券）的問題。
來源順序：
  1. Yahoo 2YY=F：CBOT 2 年期殖利率期貨（盤中、與 ^TNX 時間一致；性質為期貨，需標示）
  2. FRED DGS2：美國財政部 2 年期固定期限殖利率（官方、日資料、落後約 1 個營業日）
兩者皆失敗 → 回傳 None，呼叫端不得改用 ^IRX 冒充。
"""
from lumistock.providers.fred import fetch_fred_latest
from lumistock.providers.yahoo import fetch_chart_last_two

US2Y_SOURCES = (
    {"id": "yahoo_2yy_f", "kind": "yahoo", "symbol": "2YY=F",
     "label": "CBOT 2年期殖利率期貨", "note": "期貨報價，非現貨公債殖利率"},
    {"id": "fred_dgs2", "kind": "fred", "series": "DGS2",
     "label": "FRED DGS2 2年期公債", "note": "官方日資料，約落後 1 個營業日"},
)


def _to_yield_dict(raw: dict, src: dict) -> dict:
    v, p = raw["value"], raw["prev"]
    chg = v - p
    return {
        "yield": v, "chg": chg, "pct": (chg / p * 100) if p else 0.0,
        "source": src["id"], "source_label": src["label"], "source_note": src["note"],
        "as_of": raw.get("date") or raw.get("time"),
    }


def get_us_2y_yield(http_get, sources=US2Y_SOURCES):
    """回傳與舊版 get_yld() 相容的 dict（yield/chg/pct），另加來源欄位；失敗回傳 None。"""
    for src in sources:
        if src["kind"] == "yahoo":
            raw = fetch_chart_last_two(src["symbol"], http_get)
        elif src["kind"] == "fred":
            raw = fetch_fred_latest(src["series"], http_get)
        else:
            raw = None
        if raw and raw.get("value") is not None and 0 < raw["value"] < 25:
            return _to_yield_dict(raw, src)
    return None
