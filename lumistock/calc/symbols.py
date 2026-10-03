"""證券代號判斷（純函式，無外部相依）。

問題：舊版一律用 `sid.isdigit()` 判斷台股／美股。台股代號不是只有純數字：

    00679B  元大美債20年（債券 ETF）
    00687B  國泰20年美債
    00632R  元大台灣50反1
    2881A   富邦金甲特（特別股）

這些 `isdigit()` 都是 False → 被當成美股，造成：
  - 報價走美股路徑，查不到或查錯
  - 存進 Sheets 的市場欄寫「美股」
  - 買賣手續費與證交稅完全不計算
    （連 v10.9.187 新增的「債券 ETF 停徵期證交稅 0」也永遠不會觸發）
  - 被排除在持股警報與觀察清單之外

台股代號格式：4~6 位數字，後面可接 0~2 個英文字母。
美股代號一律以英文字母開頭，不會誤判。
"""
import re

# 4~6 位數字 + 0~2 個英文字母
_TW_SYMBOL_RE = re.compile(r"^\d{4,6}[A-Za-z]{0,2}$")

# 台股常見的交易所後綴
_TW_SUFFIXES = (".TW", ".TWO")


def normalize_symbol(symbol: str) -> str:
    """去掉台股交易所後綴並去除前後空白。非台股後綴原樣保留。"""
    s = (symbol or "").strip()
    upper = s.upper()
    for suffix in _TW_SUFFIXES:
        if upper.endswith(suffix):
            return s[: -len(suffix)]
    return s


def is_tw_symbol(symbol: str) -> bool:
    """是否為台股代號（含債券 ETF、槓桿／反向 ETF、特別股、權證）。"""
    return bool(_TW_SYMBOL_RE.match(normalize_symbol(symbol)))


def is_us_symbol(symbol: str) -> bool:
    """是否為美股代號：純英文字母（可含一個點，如 BRK.B）。"""
    s = normalize_symbol(symbol).upper()
    if not s or is_tw_symbol(s):
        return False
    return bool(re.match(r"^[A-Z]{1,5}(\.[A-Z]{1,2})?$", s))


def market_of(symbol: str) -> str:
    """回傳「台股」或「美股」。判斷不出來時回「美股」，與舊版預設一致。"""
    return "台股" if is_tw_symbol(symbol) else "美股"
