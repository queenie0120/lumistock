"""FRED（St. Louis Fed）資料供應商。

公開 CSV 端點不需 API key：https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS2
- 標題列第一欄可能是 DATE 或 observation_date（FRED 曾更改），一律取第 2 欄為值。
- 休市日值為 "." 或空字串，需略過。
- DGS2 / DGS10 / DGS30 = 美國公債固定期限殖利率 CMT（日資料，通常落後 1 個營業日）。

v10.9.189：
- 加 cosd（起始日）參數。原本每次都下載整段歷史（DGS2 自 1976 年起上萬列），
  只取最後兩筆卻付全部頻寬與解析成本。限縮視窗後回應時間明顯下降。
- 新增 fetch_fred_observations，供多序列「同日對齊」使用。
"""
import csv
import io
from datetime import date, timedelta

FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series}"

# 預設只取近 120 天：足以跨過連假與資料落後，又不必下載整段歷史。
DEFAULT_WINDOW_DAYS = 120


def build_fred_url(series: str, days: int = DEFAULT_WINDOW_DAYS, today=None) -> str:
    """組 FRED CSV 網址。days <= 0 代表不限縮（取整段歷史）。"""
    url = FRED_CSV_URL.format(series=series)
    if days and days > 0:
        start = (today or date.today()) - timedelta(days=days)
        url += f"&cosd={start.isoformat()}"
    return url


def parse_fred_csv(text: str) -> list:
    """回傳 [(date_str, float), ...]，依日期由舊到新，略過缺值。"""
    out = []
    if not text:
        return out
    reader = csv.reader(io.StringIO(text.strip()))
    header = next(reader, None)
    if not header or len(header) < 2:
        return out
    for row in reader:
        if len(row) < 2:
            continue
        v = row[1].strip()
        if v in ("", "."):
            continue
        try:
            out.append((row[0].strip(), float(v)))
        except ValueError:
            continue
    return out


def fetch_fred_observations(series: str, http_get, timeout: int = 8,
                            days: int = DEFAULT_WINDOW_DAYS) -> list:
    """取某序列近 days 天的觀測值 [(date_str, float), ...]（舊→新）。失敗回傳 []。"""
    try:
        r = http_get(build_fred_url(series, days), timeout=timeout,
                     headers={"User-Agent": "Mozilla/5.0 Lumistock"})
        if getattr(r, "status_code", 200) != 200:
            return []
        return parse_fred_csv(r.text)
    except Exception:
        return []


def fetch_fred_latest(series: str, http_get, timeout: int = 8,
                      days: int = DEFAULT_WINDOW_DAYS):
    """取最新兩筆有效觀測值。http_get 為 requests.get 相容函式（方便測試注入）。
    成功回傳 {"value", "prev", "date", "prev_date"}；失敗回傳 None。"""
    obs = fetch_fred_observations(series, http_get, timeout=timeout, days=days)
    if len(obs) < 2:
        return None
    (pd, pv), (d, v) = obs[-2], obs[-1]
    return {"value": v, "prev": pv, "date": d, "prev_date": pd}
