"""v10.9.189 美債殖利率資料正確性測試。

涵蓋三個修正：
  1. 主來源改為 FRED CMT（官方口徑），2YY=F 期貨降為備援。
  2. 曲線三期別必須「同源同日」才計算利差（舊版拿不同日的兩腳相減）。
  3. 「查美債2Y」與曲線卡同源，不會兩張卡顯示不同數字。
另含 FRED cosd 視窗（效能）與既有卡片結構未被破壞的回歸檢查。
"""
import json
import unittest
from unittest import mock

from stubs import load_app
app = load_app()

from lumistock.providers.fred import (build_fred_url, fetch_fred_observations,
                                      fetch_fred_latest)
from lumistock.services.market.yields import get_us_2y_yield, get_us_yield_curve


class Resp:
    def __init__(self, text="", js=None, status=200):
        self.text, self._js, self.status_code = text, js, status
    def json(self):
        if self._js is None: raise ValueError("no json")
        return self._js


def csv_of(name, pairs):
    """pairs: [(date, value or None)]；None 代表該日缺值（FRED 以 '.' 表示）。"""
    body = "".join(f"{d},{'.' if v is None else v}\n" for d, v in pairs)
    return f"DATE,{name}\n{body}"


def yahoo_js(price, prev):
    return {"chart": {"result": [{"meta": {"regularMarketPrice": price,
                                           "regularMarketTime": 1789000000},
                                  "indicators": {"quote": [{"close": [prev - 0.01, prev, price]}]}}]}}


# 三條序列：DGS2 在 09-17 缺值，DGS10/DGS30 有。
# → 最新「共同日」必須是 09-16，不可用 09-17 的 10Y 去配 09-16 的 2Y。
S2 = [("2026-09-15", 3.52), ("2026-09-16", 3.58), ("2026-09-17", None)]
S10 = [("2026-09-15", 4.02), ("2026-09-16", 4.08), ("2026-09-17", 4.15)]
S30 = [("2026-09-15", 4.65), ("2026-09-16", 4.60), ("2026-09-17", 4.70)]


def fred_router(ok=True, series=(S2, S10, S30)):
    s2, s10, s30 = series
    calls = []
    def get(url, **k):
        calls.append(url)
        if "fred" not in url:
            raise AssertionError("unexpected non-fred url " + url)
        if not ok:
            raise OSError("blocked")
        if "DGS10" in url: return Resp(csv_of("DGS10", s10))
        if "DGS30" in url: return Resp(csv_of("DGS30", s30))
        return Resp(csv_of("DGS2", s2))
    return get, calls


def full_router(fred_ok=True, yahoo_ok=True, series=(S2, S10, S30)):
    s2, s10, s30 = series
    calls = []
    def get(url, **k):
        calls.append(url)
        if "fred" in url:
            if not fred_ok: raise OSError("blocked")
            if "DGS10" in url: return Resp(csv_of("DGS10", s10))
            if "DGS30" in url: return Resp(csv_of("DGS30", s30))
            return Resp(csv_of("DGS2", s2))
        if not yahoo_ok:
            raise OSError("blocked")
        if "2YY" in url: return Resp(js=yahoo_js(3.70, 3.65))
        if "%5ETNX" in url or "^TNX" in url: return Resp(js=yahoo_js(4.20, 4.15))
        if "%5ETYX" in url or "^TYX" in url: return Resp(js=yahoo_js(4.80, 4.82))
        raise AssertionError("unexpected url " + url)
    return get, calls


class T1_FredWindow(unittest.TestCase):
    """cosd 視窗：原本每次下載整段歷史（上萬列），只為取最後兩筆。"""

    def test_url_has_cosd(self):
        url = build_fred_url("DGS2", days=120)
        self.assertIn("id=DGS2", url)
        self.assertIn("cosd=", url)

    def test_days_zero_means_full_history(self):
        self.assertNotIn("cosd", build_fred_url("DGS2", days=0))

    def test_observations_parse(self):
        get, _ = fred_router()
        obs = fetch_fred_observations("DGS2", get)
        self.assertEqual(obs[-1], ("2026-09-16", 3.58))   # 09-17 缺值被略過

    def test_observations_failure_is_empty(self):
        get, _ = fred_router(ok=False)
        self.assertEqual(fetch_fred_observations("DGS2", get), [])

    def test_latest_still_works(self):
        get, _ = fred_router()
        r = fetch_fred_latest("DGS2", get)
        self.assertEqual((r["value"], r["prev"]), (3.58, 3.52))


class T2_Primary(unittest.TestCase):
    """主來源必須是官方 CMT，不是期貨。"""

    def test_2y_prefers_fred(self):
        get, _ = full_router()
        y = get_us_2y_yield(get)
        self.assertEqual(y["source"], "fred_dgs2")
        self.assertAlmostEqual(y["yield"], 3.58)

    def test_2y_falls_back_to_futures(self):
        get, _ = full_router(fred_ok=False)
        y = get_us_2y_yield(get)
        self.assertEqual(y["source"], "yahoo_2yy_f")
        self.assertAlmostEqual(y["yield"], 3.70)

    def test_never_irx_anywhere(self):
        get, calls = full_router()
        get_us_yield_curve(get)
        self.assertFalse(any("IRX" in u for u in calls))


class T3_Alignment(unittest.TestCase):
    """核心：三期別同源同日，利差才有意義。"""

    def test_picks_latest_common_date(self):
        get, _ = fred_router()
        c = get_us_yield_curve(get)
        self.assertTrue(c["aligned"])
        self.assertEqual(c["as_of"], "2026-09-16")
        # 09-17 的 10Y（4.15）不可被採用 —— 那天 2Y 沒有值
        self.assertAlmostEqual(c["y10"]["yield"], 4.08)
        self.assertAlmostEqual(c["y2"]["yield"], 3.58)
        self.assertAlmostEqual(c["y30"]["yield"], 4.60)

    def test_all_tenors_share_as_of(self):
        get, _ = fred_router()
        c = get_us_yield_curve(get)
        self.assertEqual({c[t]["as_of"] for t in ("y2", "y10", "y30")}, {"2026-09-16"})

    def test_chg_uses_previous_common_date(self):
        get, _ = fred_router()
        c = get_us_yield_curve(get)
        self.assertAlmostEqual(c["y2"]["chg"], 3.58 - 3.52)
        self.assertAlmostEqual(c["y10"]["chg"], 4.08 - 4.02)

    def test_needs_two_common_dates(self):
        one = [("2026-09-16", 3.58)]
        get, _ = fred_router(series=(one, one, one))
        c = get_us_yield_curve(get)
        self.assertFalse(c["aligned"])   # 只有一天 → 算不出 chg，不得宣稱對齊

    def test_missing_30y_does_not_block_spread(self):
        """利差只用 2Y/10Y：30Y 缺一天不該讓整張卡不判讀。"""
        s30_gap = [("2026-09-15", 4.65), ("2026-09-16", None)]
        get, _ = fred_router(series=(S2, S10, s30_gap))
        c = get_us_yield_curve(get)
        self.assertTrue(c["aligned"])
        self.assertEqual(c["as_of"], "2026-09-16")
        self.assertIsNone(c["y30"])
        self.assertAlmostEqual(c["y2"]["yield"], 3.58)

    def test_yahoo_whole_curve_fallback_is_aligned(self):
        get, _ = full_router(fred_ok=False)
        c = get_us_yield_curve(get)
        self.assertTrue(c["aligned"])
        self.assertTrue(c["degraded"])
        self.assertAlmostEqual(c["y2"]["yield"], 3.70)
        self.assertAlmostEqual(c["y10"]["yield"], 4.20)

    def test_partial_is_not_aligned(self):
        """FRED 全掛、Yahoo 只有 10Y → 不得宣稱對齊，也不得算利差。"""
        def get(url, **k):
            if "fred" in url: raise OSError("x")
            if "%5ETNX" in url or "^TNX" in url: return Resp(js=yahoo_js(4.20, 4.15))
            raise OSError("x")
        c = get_us_yield_curve(get)
        self.assertFalse(c["aligned"])
        self.assertIsNone(c["y2"])
        self.assertIsNotNone(c["y10"])

    def test_out_of_range_rejected(self):
        """抓到價格而非殖利率（例如 96.5）要擋掉。"""
        bad = [("2026-09-15", 96.4), ("2026-09-16", 96.5)]
        get, _ = fred_router(series=(bad, S10, S30))
        c = get_us_yield_curve(get)
        self.assertFalse(c["aligned"])


class T4_Analysis(unittest.TestCase):
    """app.get_yield_analysis 與 Flex 卡。"""

    def test_spread_from_aligned_curve(self):
        get, _ = full_router()
        with mock.patch.object(app.requests, "get", side_effect=get):
            d = app.get_yield_analysis()
        self.assertAlmostEqual(d["spread"], 4.08 - 3.58)
        self.assertFalse(d["inverted"])
        self.assertTrue(d["aligned"])
        self.assertEqual(d["as_of"], "2026-09-16")

    def test_spread_suppressed_when_not_aligned(self):
        """2Y 缺、10Y 有 → 絕不可相減產生假的曲線判斷。"""
        def get(url, **k):
            if "fred" in url: raise OSError("x")
            if "%5ETNX" in url or "^TNX" in url: return Resp(js=yahoo_js(4.20, 4.15))
            raise OSError("x")
        with mock.patch.object(app.requests, "get", side_effect=get):
            d = app.get_yield_analysis()
        self.assertIsNone(d["spread"])
        self.assertFalse(d["inverted"])

    def test_inversion_detected_when_aligned(self):
        inv2 = [("2026-09-15", 4.50), ("2026-09-16", 4.60)]
        inv10 = [("2026-09-15", 4.10), ("2026-09-16", 4.05)]
        get, _ = full_router(series=(inv2, inv10, S30))
        with mock.patch.object(app.requests, "get", side_effect=get):
            d = app.get_yield_analysis()
        self.assertTrue(d["inverted"])
        flex = json.dumps(app.make_yield_analysis_flex(d), ensure_ascii=False)
        self.assertIn("倒掛", flex)

    def test_flex_structure_intact(self):
        """卡片版面不得跑掉：仍是 bubble + header/body，三個期別與曲線狀態都在。"""
        get, _ = full_router()
        with mock.patch.object(app.requests, "get", side_effect=get):
            d = app.get_yield_analysis()
        flex = app.make_yield_analysis_flex(d)
        self.assertEqual(flex["type"], "bubble")
        self.assertIn("header", flex)
        self.assertIn("body", flex)
        s = json.dumps(flex, ensure_ascii=False)
        for must in ("2 年期", "10 年期", "30 年期", "曲線狀態", "AI 市場解讀"):
            self.assertIn(must, s)

    def test_flex_shows_source_and_as_of(self):
        get, _ = full_router()
        with mock.patch.object(app.requests, "get", side_effect=get):
            d = app.get_yield_analysis()
        s = json.dumps(app.make_yield_analysis_flex(d), ensure_ascii=False)
        self.assertIn("資料日 2026-09-16", s)
        self.assertIn("CMT", s)

    def test_flex_marks_missing_2y(self):
        """2Y 全掛時卡片要明講缺資料，不可顯示一個算出來的假利差。"""
        def get(url, **k):
            if "fred" in url: raise OSError("x")
            if "%5ETNX" in url or "^TNX" in url: return Resp(js=yahoo_js(4.20, 4.15))
            raise OSError("x")
        with mock.patch.object(app.requests, "get", side_effect=get):
            d = app.get_yield_analysis()
        s = json.dumps(app.make_yield_analysis_flex(d), ensure_ascii=False)
        self.assertIn("缺 2Y 資料", s)
        self.assertNotIn("✅ 正常", s)


class T5_Query2Y(unittest.TestCase):
    """「查美債2Y」必須與曲線卡同源，不能兩張卡各說各話。"""

    def test_routed_to_special_branch(self):
        sym, _ = app.MARKET_SYMBOLS["查美債2Y"]
        self.assertEqual(sym, "__US2Y__")
        self.assertNotIn("^IRX", json.dumps(app.MARKET_SYMBOLS["查美債2Y"]))

    def test_quote_uses_official_source(self):
        get, _ = full_router()
        with mock.patch.object(app.requests, "get", side_effect=get):
            q = app.get_us_2y_quote()
        self.assertAlmostEqual(q["price"], 3.58)
        self.assertFalse(q["meta"]["is_fallback"])
        self.assertIn("資料日 2026-09-16", q["source"])

    def test_quote_marks_futures_as_fallback(self):
        get, _ = full_router(fred_ok=False)
        with mock.patch.object(app.requests, "get", side_effect=get):
            q = app.get_us_2y_quote()
        self.assertAlmostEqual(q["price"], 3.70)
        self.assertTrue(q["meta"]["is_fallback"])

    def test_quote_matches_curve_card(self):
        get, _ = full_router()
        with mock.patch.object(app.requests, "get", side_effect=get):
            q = app.get_us_2y_quote()
            d = app.get_yield_analysis()
        self.assertAlmostEqual(q["price"], d["y2"]["yield"])

    def test_quote_failure_returns_empty(self):
        def boom(*a, **k): raise OSError("x")
        with mock.patch.object(app.requests, "get", side_effect=boom):
            self.assertEqual(app.get_us_2y_quote(), {})

    def test_quote_flex_renders(self):
        get, _ = full_router()
        with mock.patch.object(app.requests, "get", side_effect=get):
            q = app.get_us_2y_quote()
        flex = app.make_quote_flex("🇺🇸 美國2年期公債殖利率", q, "#5B8DB8")
        self.assertEqual(flex["type"], "bubble")
        json.dumps(flex, ensure_ascii=False)


if __name__ == "__main__":
    unittest.main()


class T6_AsOfFormat(unittest.TestCase):
    """v10.9.192：Yahoo 回的是 Unix epoch，不可當成觀測日顯示。

    症狀（實機抓到）：FRED 不可用時退到 Yahoo，卡片標題列出現
    「資料日 1790102734」。Yahoo 是盤中報價，沒有觀測日概念。
    """

    def test_yahoo_curve_has_no_as_of(self):
        get, _ = full_router(fred_ok=False)
        c = get_us_yield_curve(get)
        self.assertTrue(c["aligned"])
        self.assertIsNone(c["as_of"])
        for t in ("y2", "y10", "y30"):
            if c.get(t):
                self.assertIsNone(c[t]["as_of"], t)

    def test_fred_curve_keeps_real_date(self):
        get, _ = fred_router()
        c = get_us_yield_curve(get)
        self.assertEqual(c["as_of"], "2026-09-16")

    def test_epoch_never_reaches_flex(self):
        get, _ = full_router(fred_ok=False)
        with mock.patch.object(app.requests, "get", side_effect=get):
            d = app.get_yield_analysis()
        s = json.dumps(app.make_yield_analysis_flex(d), ensure_ascii=False)
        self.assertNotIn("資料日", s)
        self.assertNotIn("1789000000", s)

    def test_2y_quote_has_no_epoch(self):
        get, _ = full_router(fred_ok=False)
        with mock.patch.object(app.requests, "get", side_effect=get):
            q = app.get_us_2y_quote()
        self.assertNotIn("資料日", q["source"])
        self.assertTrue(q["meta"]["is_fallback"])
