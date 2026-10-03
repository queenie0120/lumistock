"""Lumistock v10.9.188 離線測試：2Y 殖利率、月營收年增率、除權息表、設定持久化、/api/v1。

執行：python3 -m unittest discover -s tests -v
外部 API 全部 mock；只證明邏輯，不代表 Render 上的即時資料正確。
"""
import json, os, sys, tempfile, threading, unittest
from unittest import mock
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from stubs import load_app

app = load_app()

from lumistock.providers.fred import parse_fred_csv, fetch_fred_latest
from lumistock.services.market.yields import get_us_2y_yield
from lumistock.calc.revenue import compute_monthly_yoy
from lumistock.db.settings_store import (JsonFileStore, MirroredStore, merge_shallow,
                                         merge_two_level, SheetsKVStore)


class Resp:
    def __init__(self, text="", js=None, status=200):
        self.text, self._js, self.status_code = text, js, status
    def json(self):
        if self._js is None: raise ValueError("no json")
        return self._js


def yahoo_js(price, prev):
    return {"chart": {"result": [{"meta": {"regularMarketPrice": price, "regularMarketTime": 1789000000},
                                  "indicators": {"quote": [{"close": [prev - 0.01, prev, price]}]}}]}}


FRED_OLD = "DATE,DGS2\n2026-09-14,3.52\n2026-09-15,.\n2026-09-15,3.55\n2026-09-16,3.58\n"
FRED_NEW = "observation_date,DGS2\n2026-09-15,3.55\n2026-09-16,\n2026-09-16,3.61\n"


def fred_series(name, prev, last):
    """v10.9.189：產生與 FRED_OLD 同日期軸的序列（09-15 prev、09-16 last）。"""
    return (f"DATE,{name}\n2026-09-14,{prev - 0.03:.2f}\n"
            f"2026-09-15,{prev:.2f}\n2026-09-16,{last:.2f}\n")


class T1_Fred(unittest.TestCase):
    def test_parse_both_headers_and_missing(self):
        self.assertEqual(parse_fred_csv(FRED_OLD)[-1], ("2026-09-16", 3.58))
        self.assertEqual(len(parse_fred_csv(FRED_OLD)), 3)
        self.assertEqual(parse_fred_csv(FRED_NEW)[-1], ("2026-09-16", 3.61))

    def test_latest(self):
        r = fetch_fred_latest("DGS2", lambda *a, **k: Resp(FRED_OLD))
        self.assertEqual((r["value"], r["prev"], r["date"]), (3.58, 3.55, "2026-09-16"))

    def test_failure_returns_none(self):
        def boom(*a, **k): raise OSError("blocked")
        self.assertIsNone(fetch_fred_latest("DGS2", boom))
        self.assertIsNone(fetch_fred_latest("DGS2", lambda *a, **k: Resp("x", status=500)))


class T2_Yield2Y(unittest.TestCase):
    def router(self, yahoo_ok=True, fred_ok=True):
        calls = []
        def get(url, **k):
            calls.append(url)
            if "2YY%3DF" in url or "2YY=F" in url:
                if not yahoo_ok: raise OSError("x")
                return Resp(js=yahoo_js(3.60, 3.55))
            if "fred" in url:
                if not fred_ok: raise OSError("x")
                # v10.9.189：曲線三期別都走 FRED，router 需能分辨序列並給不同值
                if "DGS10" in url: return Resp(fred_series("DGS10", 4.05, 4.08))
                if "DGS30" in url: return Resp(fred_series("DGS30", 4.62, 4.60))
                return Resp(FRED_OLD)   # DGS2：prev 3.55 → 3.58
            if "%5ETNX" in url or "^TNX" in url: return Resp(js=yahoo_js(4.10, 4.05))
            if "%5ETYX" in url or "^TYX" in url: return Resp(js=yahoo_js(4.60, 4.62))
            raise AssertionError("unexpected url " + url)
        return get, calls

    def test_never_uses_irx(self):
        get, calls = self.router(yahoo_ok=False, fred_ok=False)
        with mock.patch.object(app.requests, "get", side_effect=get):
            app.get_yield_analysis()
        self.assertFalse(any("IRX" in u for u in calls))
        self.assertNotIn("^IRX", json.dumps(app.MARKET_SYMBOLS["查美債2Y"]))

    # v10.9.189：主來源由 2YY=F 期貨改為 FRED CMT（官方口徑），期貨降為備援。
    # 原 test_primary_futures / test_fallback_fred 的預期行為刻意對調。
    def test_primary_is_official_cmt(self):
        get, _ = self.router()
        y = get_us_2y_yield(get)
        self.assertEqual(y["source"], "fred_dgs2")
        self.assertAlmostEqual(y["yield"], 3.58)
        self.assertAlmostEqual(y["chg"], 0.03)

    def test_fallback_futures_when_fred_down(self):
        get, _ = self.router(fred_ok=False)
        y = get_us_2y_yield(get)
        self.assertEqual(y["source"], "yahoo_2yy_f")
        self.assertAlmostEqual(y["yield"], 3.60)

    def test_analysis_spread(self):
        get, _ = self.router()
        with mock.patch.object(app.requests, "get", side_effect=get):
            d = app.get_yield_analysis()
        self.assertAlmostEqual(d["spread"], 0.50)
        self.assertFalse(d["inverted"])
        json.dumps(app.make_yield_analysis_flex(d))

    def test_analysis_without_2y(self):
        get, _ = self.router(yahoo_ok=False, fred_ok=False)
        with mock.patch.object(app.requests, "get", side_effect=get):
            d = app.get_yield_analysis()
        self.assertIsNone(d["spread"]); self.assertFalse(d["inverted"])
        self.assertTrue(any("取不到" in x for x in d["interpretations"]))
        flex = json.dumps(app.make_yield_analysis_flex(d), ensure_ascii=False)
        self.assertIn("缺 2Y 資料", flex)

    def test_out_of_range_rejected(self):
        get = lambda url, **k: Resp(js=yahoo_js(96.5, 96.4)) if "2YY" in url else Resp("x", status=500)
        self.assertIsNone(get_us_2y_yield(get))


def rev_rows(pairs):
    """pairs: [(year, month, revenue)] → FinMind 格式（date = 公布月份 1 日）"""
    out = []
    for y, m, rev in pairs:
        py, pm = (y + 1, 1) if m == 12 else (y, m + 1)
        out.append({"date": f"{py}-{pm:02d}-01", "stock_id": "2330", "revenue": rev,
                    "revenue_year": y, "revenue_month": m})
    return out


class T3_Revenue(unittest.TestCase):
    def full(self):
        pairs = [(2025, m, 100_000_000_000) for m in range(1, 13)]
        pairs += [(2026, m, 120_000_000_000 + m * 1_000_000_000) for m in range(1, 9)]
        return rev_rows(pairs)

    def test_yoy_computed(self):
        out = compute_monthly_yoy(self.full())
        self.assertEqual([r["date"] for r in out], ["2026-08", "2026-07", "2026-06", "2026-05"])
        self.assertAlmostEqual(out[0]["yoy_pct"], 28.0)
        self.assertEqual(out[0]["revenue_million"], 128_000)

    def test_no_base_returns_empty_not_zero(self):
        rows = rev_rows([(2026, m, 1_000_000_000) for m in range(3, 9)])
        self.assertEqual(compute_monthly_yoy(rows), [])

    def test_gap_stops(self):
        rows = [r for r in self.full() if not (r["revenue_year"] == 2026 and r["revenue_month"] == 6)]
        self.assertEqual([r["date"] for r in compute_monthly_yoy(rows)], ["2026-08", "2026-07"])

    def test_period_from_date_when_fields_missing(self):
        rows = [{"date": "2026-01-10", "revenue": 110}, {"date": "2025-01-10", "revenue": 100}]
        out = compute_monthly_yoy(rows)
        self.assertEqual(out[0]["date"], "2025-12")
        self.assertAlmostEqual(out[0]["yoy_pct"], 10.0)

    def test_zero_base_skipped(self):
        rows = rev_rows([(2025, 8, 0), (2026, 8, 50)])
        self.assertEqual(compute_monthly_yoy(rows), [])

    def test_app_loader_uses_calc(self):
        payload = {"status": 200, "data": self.full()}
        with mock.patch.object(app, "FINMIND_TOKEN", "t"), \
             mock.patch.object(app.requests, "get", return_value=Resp(js=payload)):
            out = app._load_finmind_monthly_revenue("2330")
        self.assertEqual(len(out), 4)
        self.assertGreater(out[0]["yoy_pct"], 0)

    def test_growth_scorer_empty_list_safe(self):
        tw = {"price": 100, "pct": 1.0}
        closes = [100 + i * 0.1 for i in range(80)]
        s = app.score_growth_stock(tw, closes, [], {}, "")
        self.assertIn("total", s)


class T4_ExDividendTable(unittest.TestCase):
    def test_no_expired_or_zero_cash_rows(self):
        for code, info in app.EX_DIVIDEND_FALLBACK.items():
            self.assertGreaterEqual(info["date"], "20260917", code)
            self.assertGreater(info["cash"] + info["stock"], 0, code)
            self.assertEqual(len(info["date"]), 8)

    def test_today_adjustment_and_upcoming(self):
        cal = {"00929": dict(app.EX_DIVIDEND_FALLBACK["00929"]), "2542": dict(app.EX_DIVIDEND_FALLBACK["2542"])}
        with mock.patch.dict(app.EX_DIVIDEND_CALENDAR, cal, clear=True), \
             mock.patch.object(app, "FINMIND_TOKEN", ""), \
             mock.patch.object(app, "now_taipei", return_value=app.datetime(2026, 9, 17, 10, 0, tzinfo=app.TZ_TAIPEI)):
            self.assertEqual(app.get_ex_dividend_info("00929")["cash"], 0.38)
            up = app.get_upcoming_ex_dividend_info("2542", within_days=14)
            self.assertEqual(up["days"], 6)


class FakeKVSheet:
    def __init__(self): self.rows = [["namespace", "json", "updated_at"]]
    def get_all_values(self): return [list(r) for r in self.rows]
    def update_cell(self, i, j, v): self.rows[i - 1][j - 1] = v
    def append_row(self, r): self.rows.append(list(r))


class T5_SettingsStore(unittest.TestCase):
    def setUp(self):
        self.d = tempfile.mkdtemp()
        self.sheet = FakeKVSheet()
        self.primary = JsonFileStore({"a": os.path.join(self.d, "a.json")})
        self.mirror = SheetsKVStore(lambda name, headers=None: self.sheet)
        self.store = MirroredStore(self.primary, self.mirror, async_mirror=False)

    def test_save_writes_both(self):
        self.store.save("a", {"x": 1})
        self.assertEqual(self.primary.load("a"), {"x": 1})
        self.assertEqual(self.mirror.load("a"), {"x": 1})
        self.store.save("a", {"x": 2})
        self.assertEqual(len(self.sheet.rows), 2)  # 同 namespace 更新同一列
        self.assertEqual(self.mirror.load("a"), {"x": 2})

    def test_restart_restores_into_global(self):
        self.store.save("a", {"U1": {"2330": {"stop_loss": 900.0}}})
        os.remove(os.path.join(self.d, "a.json"))           # 模擬 Render 重啟清空 /tmp
        g = {}
        self.store.restore("a", target=g, merge=merge_two_level)
        self.assertEqual(g["U1"]["2330"]["stop_loss"], 900.0)
        self.assertEqual(self.primary.load("a"), g)

    def test_restore_merges_changes_made_during_boot(self):
        self.store.save("a", {"U1": {"2330": {"stop_loss": 900.0}}})
        self.primary.save("a", {"U2": {"0050": {"target": 200.0}}})  # 開機 15 秒內新設定
        g = {}
        self.store.restore("a", target=g, merge=merge_two_level)
        self.assertIn("U1", g); self.assertIn("U2", g)
        self.assertIn("U2", self.mirror.load("a"))

    def test_shallow_merge_local_wins(self):
        self.assertEqual(merge_shallow({"t": "06:30", "on": True}, {"t": "07:00"}), {"t": "07:00", "on": True})

    def test_async_mirror_does_not_block(self):
        slow = threading.Event()
        class SlowMirror:
            def save(self, ns, data): slow.wait(2); return True
            def load(self, ns): return None
        st = MirroredStore(self.primary, SlowMirror(), async_mirror=True)
        import time; t0 = time.time()
        st.save("a", {"x": 1})
        self.assertLess(time.time() - t0, 0.5)
        slow.set()

    def test_sheets_cell_limit(self):
        self.assertFalse(self.mirror.save("a", {"big": "x" * 60000}))

    def test_app_wiring(self):
        saved = {}
        with mock.patch.object(app.SETTINGS_STORE, "save", side_effect=lambda ns, d: saved.setdefault(ns, d) or True):
            app.set_push_setting("portfolio_alerts_enabled", False)
            app.set_user_alert("U9", "2330", "stop_loss", 880)
        self.assertIn("push_settings", saved); self.assertIn("user_alerts", saved)
        app.set_push_setting("portfolio_alerts_enabled", True)
        app.set_user_alert("U9", "2330", "stop_loss", 0)


class T6_Api(unittest.TestCase):
    def setUp(self):
        app.STARTUP_DONE = True
        self.c = app.app.test_client()

    def test_health(self):
        r = self.c.get("/api/v1/health")
        self.assertEqual(r.status_code, 200)
        body = r.get_json()
        self.assertEqual(body["data"]["app_version"], app.VERSION)
        self.assertIsNone(body["error"])

    def test_api_404_envelope(self):
        r = self.c.get("/api/v1/nope")
        self.assertEqual(r.status_code, 404)

    def test_existing_routes_unchanged(self):
        self.assertEqual(self.c.get("/").status_code, 200)
        self.assertEqual(self.c.post("/callback", data="{}").status_code, 400)


if __name__ == "__main__":
    unittest.main(verbosity=2)
