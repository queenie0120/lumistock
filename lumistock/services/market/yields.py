"""美債殖利率服務。

v10.9.188：修正「2 年期」誤用 ^IRX（13 週國庫券）的問題，改用 2YY=F 期貨。

v10.9.189（資料正確性）：
  1. 主來源改為 FRED CMT（DGS2 / DGS10 / DGS30）。
     2YY=F 是 CBOT「殖利率期貨」，不是一般所稱的「2 年期公債殖利率」，
     與財政部 CMT 現貨存在基差，使用者拿外部網站對照會對不起來。
     CMT 才是 CNBC／Investing.com／財政部口徑的那個數字。
  2. 曲線利差改為「同源同日」才計算。
     舊版把 2Y（FRED，落後 1 個營業日）和 10Y（Yahoo，盤中即時）相減，
     兩腳不同日 → 利差與倒掛判斷本身就是錯的。現在只有三個期別取自
     同一來源、同一觀測日時才算利差；否則照常顯示數值但不給曲線判斷。
  3. 備援：FRED 不可用時整條曲線退到 Yahoo（2YY=F / ^TNX / ^TYX），
     仍維持同源，利差照算但標示為期貨口徑。
  絕不退回 ^IRX 冒充 2Y。
"""
from concurrent.futures import ThreadPoolExecutor

from lumistock.providers.fred import fetch_fred_latest, fetch_fred_observations
from lumistock.providers.yahoo import fetch_chart_last_two

# 合理殖利率區間，用來擋掉抓錯商品（例如誤抓到價格而非殖利率）
YIELD_MIN, YIELD_MAX = 0.0, 25.0

TENORS = ("y2", "y10", "y30")

FRED_SERIES = {"y2": "DGS2", "y10": "DGS10", "y30": "DGS30"}
YAHOO_SYMBOLS = {"y2": "2YY=F", "y10": "^TNX", "y30": "^TYX"}

FRED_LABEL = "FRED 美國財政部 CMT"
FRED_NOTE = "官方日資料，約落後 1 個營業日"
YAHOO_LABEL = "CBOT 殖利率期貨／CBOE 指數"
YAHOO_NOTE = "期貨與指數報價，非官方現貨 CMT"

# 舊版相容：單獨取 2Y 時的來源順序（v10.9.189 起 FRED 優先）
US2Y_SOURCES = (
    {"id": "fred_dgs2", "kind": "fred", "series": "DGS2",
     "label": "FRED DGS2 2年期公債", "note": FRED_NOTE},
    {"id": "yahoo_2yy_f", "kind": "yahoo", "symbol": "2YY=F",
     "label": "CBOT 2年期殖利率期貨", "note": "期貨報價，非現貨公債殖利率"},
)


def _valid(v) -> bool:
    return v is not None and YIELD_MIN < v < YIELD_MAX


def _mk(value, prev, source, label, note, as_of=None) -> dict:
    """統一的單一期別回傳格式（yield/chg/pct 與舊版相容，呼叫端與 Flex 卡不用改）。"""
    chg = value - prev
    return {
        "yield": value, "chg": chg, "pct": (chg / prev * 100) if prev else 0.0,
        "source": source, "source_label": label, "source_note": note,
        "as_of": as_of,
    }


def _to_yield_dict(raw: dict, src: dict) -> dict:
    return _mk(raw["value"], raw["prev"], src["id"], src["label"], src["note"],
               raw.get("date") or raw.get("time"))


def get_us_2y_yield(http_get, sources=US2Y_SOURCES):
    """只取 2Y（舊版相容介面）。回傳含 yield/chg/pct 的 dict；全失敗回 None。"""
    for src in sources:
        if src["kind"] == "yahoo":
            raw = fetch_chart_last_two(src["symbol"], http_get)
        elif src["kind"] == "fred":
            raw = fetch_fred_latest(src["series"], http_get)
        else:
            raw = None
        if raw and _valid(raw.get("value")):
            return _to_yield_dict(raw, src)
    return None


# webhook 同步呼叫，單次請求的 timeout 就是實際上限
# （ThreadPoolExecutor 離開 with 時會等待所有執行緒，future timeout 擋不住）。
# 三個期別併行 → 一輪往返即可，比舊版逐一串接快。
REQUEST_TIMEOUT = 6


def _parallel(fn, items, workers=3, timeout=8):
    """對 items 併行套用 fn，回傳 {key: result}。單一失敗不影響其他。"""
    out = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {k: pool.submit(fn, v) for k, v in items.items()}
        for k, f in futures.items():
            try:
                out[k] = f.result(timeout=timeout)
            except Exception:
                out[k] = None
    return out


def _fred_curve(http_get):
    """三個期別都取自 FRED，對齊到共同觀測日。

    對齊以「2Y 與 10Y」這一組為準 —— 利差只用到這兩腳，不該因為 30Y 缺一天
    就整張卡不判讀。30Y 只有在同一組日期也有值時才附上，否則留空。
    需要最新日與前一個共同日（chg 也要同日對齊）。
    成功回傳 (curve_dict, as_of)；不足以對齊則回傳 (None, None)。"""
    raw = _parallel(lambda s: fetch_fred_observations(s, http_get, timeout=REQUEST_TIMEOUT), dict(FRED_SERIES))
    maps = {}
    for tenor in TENORS:
        obs = raw.get(tenor) or []
        maps[tenor] = {d: v for d, v in obs if _valid(v)}

    common = set(maps["y2"]) & set(maps["y10"])
    if len(common) < 2:
        return None, None
    ordered = sorted(common)
    as_of, prev_date = ordered[-1], ordered[-2]

    def row(t):
        m = maps[t]
        if as_of not in m or prev_date not in m:
            return None
        return _mk(m[as_of], m[prev_date],
                   f"fred_{FRED_SERIES[t].lower()}", FRED_LABEL, FRED_NOTE, as_of)

    curve = {t: row(t) for t in TENORS}
    return curve, as_of


def _yahoo_curve(http_get):
    """備援：三個期別都取自 Yahoo（同為盤中報價，視為同源）。
    回傳 (curve_dict, missing_tenors)。"""
    raw = _parallel(lambda s: fetch_chart_last_two(s, http_get, timeout=REQUEST_TIMEOUT), dict(YAHOO_SYMBOLS))
    curve, missing = {}, []
    for tenor in TENORS:
        r = raw.get(tenor)
        if r and _valid(r.get("value")):
            curve[tenor] = _mk(r["value"], r["prev"], f"yahoo_{YAHOO_SYMBOLS[tenor]}",
                               YAHOO_LABEL, YAHOO_NOTE, r.get("time"))
        else:
            missing.append(tenor)
    return curve, missing


def get_us_yield_curve(http_get) -> dict:
    """取 2Y/10Y/30Y 殖利率曲線。

    回傳：
      {
        "y2" / "y10" / "y30": dict 或 None,
        "aligned": bool,        # 三個期別是否同源同日（決定能不能算利差）
        "as_of":   str 或 None, # 觀測日（FRED）或報價時間（Yahoo）
        "source_label": str,
        "degraded": bool,       # 是否為混源／不完整
      }
    """
    curve, as_of = _fred_curve(http_get)
    if curve:
        return {**curve, "aligned": True, "as_of": as_of,
                "source_label": FRED_LABEL, "degraded": False}

    curve, _missing = _yahoo_curve(http_get)
    filled = {t: curve.get(t) for t in TENORS}
    # Yahoo 三個期別都是同一批盤中報價，只要 2Y 與 10Y 都在就視為可比較。
    if filled["y2"] and filled["y10"]:
        return {**filled, "aligned": True, "as_of": filled["y2"].get("as_of"),
                "source_label": YAHOO_LABEL, "degraded": True}

    # 兩腳湊不齊：照常顯示拿得到的數值，但不得計算利差
    return {**filled, "aligned": False, "as_of": None,
            "source_label": YAHOO_LABEL if curve else "", "degraded": True}
