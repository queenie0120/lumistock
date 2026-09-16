"""Lumistock P0 離線回歸測試（v10.9.187）

執行：python3 -m unittest tests/test_p0.py -v
說明：沙盒無法連 TWSE / Yahoo / FinMind / LINE，以下全用 mock。
      這些測試證明「程式邏輯」正確，不代表 Render 上的即時資料正確。
"""
import os, sys, json, unittest
from unittest import mock
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from stubs import load_app

app = load_app()


class FakeResp:
    def __init__(self, payload): self._p = payload
    def json(self): return self._p


class FakeSheet:
    def __init__(self, header, rows):
        self.header = header; self.rows = [list(r) for r in rows]
        self.deleted = []
    def get_all_values(self): return [self.header] + self.rows
    def get_all_records(self): return [dict(zip(self.header, r)) for r in self.rows]
    def delete_rows(self, i):
        self.deleted.append(i); del self.rows[i - 2]
    def append_row(self, r): self.rows.append(list(r))
    def update_cell(self, *a): pass


PF_HEADER = ["用戶ID", "股票代號", "名稱", "市場", "股數", "買入價", "", "", "", "建立", "更新"]


class T1_MisVolume(unittest.TestCase):
    """P0：成交量不正確 —— MIS 應取累積量 v，而非當盤量 tv"""
    def test_uses_cumulative_v(self):
        payload = {"msgArray": [{"n": "台積電", "y": "1000", "z": "1010", "tv": "3", "v": "25431",
                                 "o": "1005", "h": "1015", "l": "1000"}]}
        with mock.patch.object(app.requests, "get", return_value=FakeResp(payload)), \
             mock.patch.object(app, "_tw_in_trading_hours", return_value=True):
            r = app._fetch_tw_mis("2330")
        self.assertEqual(r["vol_lots"], 25431)
        self.assertEqual(r["price"], 1010.0)
        self.assertEqual(r["prev"], 1000.0)


class T2_Rsi(unittest.TestCase):
    def test_all_up_is_100(self):
        closes = [100 + i for i in range(30)]
        self.assertEqual(app.get_kline_analysis(closes)["rsi"], 100)
        self.assertGreater(app._rsi(closes), 99)

    def test_flat_is_50(self):
        self.assertEqual(app.get_kline_analysis([100.0] * 30)["rsi"], 50)

    def test_all_down_is_0(self):
        closes = [200 - i for i in range(30)]
        self.assertLess(app.get_kline_analysis(closes)["rsi"], 1)

    def test_mixed_formula(self):
        closes = [100, 102, 101, 103, 102, 104, 103, 105, 104, 106, 105, 107, 106, 108, 107]
        g = sum(max(closes[i]-closes[i-1], 0) for i in range(1, 15)) / 14
        l = sum(max(closes[i-1]-closes[i], 0) for i in range(1, 15)) / 14
        self.assertAlmostEqual(app.get_kline_analysis(closes)["rsi"], 100 - 100/(1+g/l), places=6)


class T3_PortfolioSheets(unittest.TestCase):
    def setUp(self):
        self.tmp = "/tmp/lumistock_test_pf.json"
        self.p = mock.patch.object(app, "PORTFOLIO_FILE", self.tmp); self.p.start()
        if os.path.exists(self.tmp): os.remove(self.tmp)
    def tearDown(self): self.p.stop()

    def test_sold_out_does_not_resurrect(self):
        sh = FakeSheet(PF_HEADER, [["U1", "2330", "台積電", "台股", 1000, 900, "", "", "", "t", "t"],
                                   ["U1", "2330", "台積電", "台股", 0, 900, "", "", "", "t", "t"],
                                   ["U1", "0050", "元大台灣50", "台股", 2000, 150, "", "", "", "t", "t"]])
        with mock.patch.object(app, "get_sheet", return_value=sh):
            app.restore_portfolio_from_sheets()
        pf = app.load_portfolio()
        self.assertNotIn(app._pf_key("U1", "2330"), pf)
        self.assertIn(app._pf_key("U1", "0050"), pf)

    def test_rebuy_after_sellout(self):
        sh = FakeSheet(PF_HEADER, [["U1", "2330", "", "", 1000, 900, "", "", "", "", ""],
                                   ["U1", "2330", "", "", 0, 900, "", "", "", "", ""],
                                   ["U1", "2330", "", "", 500, 1000, "", "", "", "", ""]])
        with mock.patch.object(app, "get_sheet", return_value=sh):
            app.restore_portfolio_from_sheets()
        self.assertEqual(app.load_portfolio()[app._pf_key("U1", "2330")]["shares"], 500)

    def test_delete_removes_all_rows(self):
        sh = FakeSheet(PF_HEADER, [["U1", "2330", "", "", 1000, 900, "", "", "", "", ""],
                                   ["U2", "2330", "", "", 10, 900, "", "", "", "", ""],
                                   ["U1", "2330", "", "", 1500, 950, "", "", "", "", ""]])
        with mock.patch.object(app, "get_sheet", return_value=sh):
            app.delete_portfolio_from_sheets("U1", "2330")
        self.assertEqual([r[0] for r in sh.rows], ["U2"])


class T4_Permissions(unittest.TestCase):
    def _event(self, uid, data):
        ev = mock.MagicMock(); ev.source.user_id = uid; ev.postback.data = data; ev.reply_token = "rt"
        return ev

    def test_normal_user_cannot_view_user_detail(self):
        replies = []
        with mock.patch.object(app, "is_blocked_user", return_value=False), \
             mock.patch.object(app, "is_admin", return_value=False), \
             mock.patch.object(app, "is_registered", return_value=True), \
             mock.patch.object(app, "get_user_detail", return_value="SECRET") as gd, \
             mock.patch.object(app, "reply_text", side_effect=lambda t, m: replies.append(m)):
            app.handle_postback(self._event("Unormal", "action=user_detail&name=王小明"))
        gd.assert_not_called()
        self.assertTrue(any("無權限" in m for m in replies))

    def test_admin_can_view_user_detail(self):
        with mock.patch.object(app, "is_blocked_user", return_value=False), \
             mock.patch.object(app, "is_admin", return_value=True), \
             mock.patch.object(app, "get_user_detail", return_value="OK") as gd, \
             mock.patch.object(app, "reply_text"):
            app.handle_postback(self._event("Uadmin", "action=user_detail&name=王小明"))
        gd.assert_called_once()

    def test_cannot_block_owner(self):
        users = FakeSheet(["user_id", "註冊姓名", "a", "b", "c", "d", "狀態"],
                          [[app.OWNER_USER_ID, "Hui", "", "", "", "", "正常"]])
        bl = FakeSheet(["user_id"], [])
        with mock.patch.object(app, "get_sheet", side_effect=lambda n: users if n == "使用者名單" else bl), \
             mock.patch.object(app, "is_admin", side_effect=lambda u: u == app.OWNER_USER_ID):
            msg = app.block_user_by_name("Hui", "測試")
        self.assertIn("無法封鎖", msg)
        self.assertEqual(bl.rows, [])

    def test_callback_missing_signature_400(self):
        client = app.app.test_client()
        app.STARTUP_DONE = True
        self.assertEqual(client.post("/callback", data="{}").status_code, 400)


class T5_Tax(unittest.TestCase):
    def setUp(self):
        self.p = mock.patch.object(app, "get_user_fee_discount", return_value=1.0); self.p.start()
    def tearDown(self): self.p.stop()

    def test_stock_03(self):
        self.assertEqual(app.calc_sell_fee_tax(1000, 1000, "U", "2330")[1], 3000)

    def test_etf_01(self):
        self.assertEqual(app.calc_sell_fee_tax(20, 1000, "U", "00878")[1], 20)

    def test_bond_etf_exempt_until_end(self):
        with mock.patch.object(app, "now_taipei", return_value=app.datetime(2026, 9, 16, tzinfo=app.TZ_TAIPEI)):
            self.assertEqual(app.tw_sell_tax_rate("00679B"), 0.0)
        with mock.patch.object(app, "now_taipei", return_value=app.datetime(2027, 1, 2, tzinfo=app.TZ_TAIPEI)):
            self.assertEqual(app.tw_sell_tax_rate("00679B"), 0.001)

    def test_legacy_call_unchanged(self):
        self.assertEqual(app.calc_sell_fee_tax(1000, 1000, "U")[1], 3000)


class T6_NamesRegression(unittest.TestCase):
    """P0 回歸：已知名稱問題（只驗內建表，不代表 MIS 即時名稱）"""
    def test_builtin_names(self):
        import stock_names
        tbl = getattr(stock_names, "STOCK_NAMES", None) or next(
            v for v in vars(stock_names).values() if isinstance(v, dict) and "2330" in v)
        self.assertEqual(tbl.get("2330"), "台積電")
        self.assertTrue(tbl.get("3533"))
        self.assertIn("00904", tbl)  # 名稱正確性需與 TWSE 現行簡稱人工核對


if __name__ == "__main__":
    unittest.main(verbosity=2)
