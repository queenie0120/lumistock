"""FRED（St. Louis Fed）資料供應商。

公開 CSV 端點不需 API key：https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS2
- 標題列第一欄可能是 DATE 或 observation_date（FRED 曾更改），一律取第 2 欄為值。
- 休市日值為 "." 或空字串，需略過。
- DGS2 = 美國 2 年期公債固定期限殖利率（日資料，通常落後 1 個營業日）。
"""
import csv
import io

FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series}"


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


def fetch_fred_latest(series: str, http_get, timeout: int = 8):
    """取最新兩筆有效觀測值。http_get 為 requests.get 相容函式（方便測試注入）。
    成功回傳 {"value", "prev", "date", "prev_date"}；失敗回傳 None。"""
    try:
        r = http_get(FRED_CSV_URL.format(series=series), timeout=timeout,
                     headers={"User-Agent": "Mozilla/5.0 Lumistock"})
        if getattr(r, "status_code", 200) != 200:
            return None
        obs = parse_fred_csv(r.text)
        if len(obs) < 2:
            return None
        (pd, pv), (d, v) = obs[-2], obs[-1]
        return {"value": v, "prev": pv, "date": d, "prev_date": pd}
    except Exception:
        return None
