"""月營收計算（純函式）。

FinMind TaiwanStockMonthRevenue 欄位：date（公布月份的 1 日）、revenue、revenue_year、revenue_month。
該資料集沒有年增率欄位，舊版讀 revenue_year_growth 永遠拿不到 → 年增率全為 0。
本模組以「同月去年營收」自行計算年增率；無法計算時回傳 None，不用 0 代替。
"""


def _period_of(row: dict):
    """回傳 (year, month) 營收所屬期間；優先 revenue_year / revenue_month。"""
    try:
        y, m = row.get("revenue_year"), row.get("revenue_month")
        if y not in (None, "") and m not in (None, ""):
            return int(y), int(m)
    except (TypeError, ValueError):
        pass
    d = str(row.get("date") or "")
    if len(d) >= 7:
        try:
            y, m = int(d[:4]), int(d[5:7])
            # date 為公布月份 → 營收期間為前一個月
            return (y - 1, 12) if m == 1 else (y, m - 1)
        except ValueError:
            return None
    return None


def compute_monthly_yoy(rows: list, limit: int = 4) -> list:
    """rows：FinMind 原始列（任意順序，需含至少 13 個月才能算最新期年增）。
    回傳最新在前的 [{"date": "YYYY-MM", "revenue_million": int, "yoy_pct": float}]。
    只保留「從最新期開始連續可計算年增率」的期數，遇到無法計算即停止，
    以免把缺值當 0 或讓「連續 N 月」判斷跳月。"""
    by_period = {}
    for row in rows or []:
        p = _period_of(row)
        try:
            rev = float(row.get("revenue"))
        except (TypeError, ValueError):
            continue
        if p and rev >= 0:
            by_period[p] = rev
    out = []
    expected = None
    for (y, m) in sorted(by_period, reverse=True):
        if len(out) >= limit:
            break
        if expected is not None and (y, m) != expected:
            break  # 月份不連續
        expected = (y - 1, 12) if m == 1 else (y, m - 1)
        cur = by_period[(y, m)]
        base = by_period.get((y - 1, m))
        if base is None or base <= 0:
            break
        out.append({
            "date": f"{y:04d}-{m:02d}",
            "revenue_million": int(cur // 1_000_000),
            "yoy_pct": (cur / base - 1) * 100,
        })
    return out
