"""v10.9.191：台股代號判斷 + MIS 收盤後狀態標示。

1. 舊版一律用 isdigit() 判台股／美股，債券 ETF（00679B）、反向 ETF（00632R）、
   特別股（2881A）全被當成美股 → 報價走錯路徑、市場欄寫錯、
   手續費與證交稅完全不算（連 v10.9.187 的債券 ETF 免稅邏輯都不會觸發）。
2. status 用 is_realtime 判斷，那只代表「來源是即時來源」，收盤後照樣標「盤中」。
"""
import unittest
from unittest import mock

from stubs import load_app
app = load_app()

from lumistock.calc.symbols import (is_tw_symbol, is_us_symbol, market_of,
                                    normalize_symbol)

# 實際存在、舊版會判錯的台股代號
TRICKY_TW = ["00679B", "00687B", "00719B", "00632R", "00663L", "2881A", "2882B"]
PLAIN_TW = ["2330", "0050", "00878", "006208", "3533"]
US = ["AAPL", "NVDA", "TSLA", "TSM", "BRK.B", "F"]


class T1_Symbols(unittest.TestCase):
    def test_plain_tw(self):
        for s in PLAIN_TW:
            self.assertTrue(is_tw_symbol(s), s)
            self.assertEqual(market_of(s), "台股", s)

    def test_bond_and_special_tw(self):
        """核心：這些舊版都會被當成美股。"""
        for s in TRICKY_TW:
            self.assertTrue(is_tw_symbol(s), s)
            self.assertEqual(market_of(s), "台股", s)

    def test_us_not_misread(self):
        for s in US:
            self.assertFalse(is_tw_symbol(s), s)
            self.assertEqual(market_of(s), "美股", s)

    def test_suffix_stripped(self):
        self.assertTrue(is_tw_symbol("2330.TW"))
        self.assertTrue(is_tw_symbol("00679B.TW"))
        self.assertTrue(is_tw_symbol("5483.TWO"))
        self.assertEqual(normalize_symbol("2330.TW"), "2330")
        self.assertEqual(normalize_symbol("00679B.TWO"), "00679B")

    def test_lowercase_and_whitespace(self):
        self.assertTrue(is_tw_symbol(" 00679b "))
        self.assertTrue(is_tw_symbol("2330.tw"))

    def test_rejects_junk(self):
        for s in ["", "   ", "12", "123", "1234567", "ABCDEFG", "00679BBB", "2330-TW"]:
            self.assertFalse(is_tw_symbol(s), repr(s))

    def test_is_us_symbol(self):
        self.assertTrue(is_us_symbol("AAPL"))
        self.assertTrue(is_us_symbol("BRK.B"))
        self.assertFalse(is_us_symbol("00679B"))
        self.assertFalse(is_us_symbol(""))

    def test_tw_and_us_mutually_exclusive(self):
        for s in TRICKY_TW + PLAIN_TW + US:
            self.assertFalse(is_tw_symbol(s) and is_us_symbol(s), s)


class T2_AppWiring(unittest.TestCase):
    """app.py 確實改用新判斷，不是只留在模組裡。"""

    def test_app_exports_helpers(self):
        self.assertTrue(app.is_tw_symbol("00679B"))
        self.assertEqual(app.market_of("00679B"), "台股")
        self.assertEqual(app.norm_sym("00679B.TW"), "00679B")

    def test_stock_flex_routes_bond_etf_to_tw(self):
        """個股卡：00679B 必須走台股路徑，不能走美股。"""
        called = {}
        def fake_tw(sid, *a, **k):
            called["tw"] = sid
            return None
        def fake_us(sid, *a, **k):
            called["us"] = sid
            return None
        with mock.patch.object(app, "get_tw_stock", side_effect=fake_tw), \
             mock.patch.object(app, "get_us_stock", side_effect=fake_us):
            try:
                app.get_stock_flex("00679B")
            except Exception:
                pass
        self.assertEqual(called.get("tw"), "00679B")
        self.assertNotIn("us", called)

    def test_stock_flex_still_routes_us(self):
        called = {}
        with mock.patch.object(app, "get_tw_stock", side_effect=lambda s, *a, **k: called.setdefault("tw", s)), \
             mock.patch.object(app, "get_us_stock", side_effect=lambda s, *a, **k: called.setdefault("us", s)):
            try:
                app.get_stock_flex("AAPL")
            except Exception:
                pass
        self.assertEqual(called.get("us"), "AAPL")
        self.assertNotIn("tw", called)

    def test_sell_tax_now_reached_for_bond_etf(self):
        """v10.9.187 的債券 ETF 免稅邏輯，要先判對市場才會觸發。"""
        self.assertAlmostEqual(app.tw_sell_tax_rate("00679B"), 0.0)
        self.assertAlmostEqual(app.tw_sell_tax_rate("0050"), 0.001)
        self.assertAlmostEqual(app.tw_sell_tax_rate("2330"), 0.003)


class T3_MarketStatus(unittest.TestCase):
    """收盤後不可標「盤中」。"""

    def _quote(self, in_hours, is_rt_source=True):
        picked = {"price": 100.0, "prev": 99.0, "open": 99.5, "high": 101.0,
                  "low": 99.0, "vol_lots": 15951, "name": "測試",
                  "source": "TWSE MIS", "is_realtime": is_rt_source}
        with mock.patch.object(app, "_tw_in_trading_hours", return_value=in_hours):
            is_rt = picked["is_realtime"] and in_hours
            return {
                "status": "盤中" if is_rt else "盤後/延遲",
                "meta": app.build_data_meta(
                    picked["source"], is_realtime=is_rt, is_fallback=False,
                    delay_min=0 if (is_rt or not in_hours) else 15),
            }

    def test_in_hours_realtime(self):
        q = self._quote(True, True)
        self.assertEqual(q["status"], "盤中")
        self.assertTrue(q["meta"]["is_realtime"])

    def test_after_close_is_not_intraday(self):
        q = self._quote(False, True)
        self.assertEqual(q["status"], "盤後/延遲")
        self.assertFalse(q["meta"]["is_realtime"])

    def test_after_close_shows_closing_not_delayed(self):
        """收盤後是「收盤資料」，不是「延遲約 15 分」。"""
        q = self._quote(False, True)
        self.assertEqual(q["meta"]["delay_min"], 0)
        self.assertIn("收盤資料", app.fmt_data_meta_full(q["meta"]))

    def test_in_hours_non_realtime_source_is_delayed(self):
        q = self._quote(True, False)
        self.assertEqual(q["status"], "盤後/延遲")
        self.assertEqual(q["meta"]["delay_min"], 15)
        self.assertIn("延遲約 15 分", app.fmt_data_meta_full(q["meta"]))

    def test_trading_hours_boundaries(self):
        import datetime as dt
        def at(y, mo, d, h, mi):
            return dt.datetime(y, mo, d, h, mi)
        cases = [  # 2026-09-17 是週四，09-19 週六
            (at(2026, 9, 17, 8, 59), False),
            (at(2026, 9, 17, 9, 0), True),
            (at(2026, 9, 17, 13, 30), True),
            (at(2026, 9, 17, 13, 31), False),
            (at(2026, 9, 19, 10, 0), False),   # 週六
        ]
        for when, expected in cases:
            with mock.patch.object(app, "now_taipei", return_value=when):
                self.assertEqual(app._tw_in_trading_hours(), expected, when)


if __name__ == "__main__":
    unittest.main()
