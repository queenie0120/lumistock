"""慧股拾光 Lumistock 2.0 套件。

規則：
- app.py 保留既有 LINE 功能；新功能寫在此套件，app.py 只 import 與掛載。
- calc/ 只放純函式（無網路、無全域狀態），必須有單元測試。
- providers/ 只負責抓資料與解析，不做業務判斷；失敗回傳 None，不拋例外到 webhook。
- 缺值用 None，不以 0 代替。
"""
__version__ = "2.0.0-dev"
