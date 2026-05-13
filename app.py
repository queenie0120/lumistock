"""
慧拾股光 Lumistock – by Hui
LINE Bot 模組 v3.0（證交所＋櫃買中心＋美股）
"""

from flask import Flask, request, abort
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi,
    ReplyMessageRequest, TextMessage
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent
import yfinance as yf
import requests
import json, os
from datetime import datetime

app = Flask(__name__)

CHANNEL_SECRET       = "bf2705977fbf63744d3618febb57d9c3"
CHANNEL_ACCESS_TOKEN = "H+8d6OLcVkxCE20TuYI68ewZ+q7P3WDSoO6fcEqGiqMcgUj6TeU+zuwgsAwgfYNEmRJdipaZVxvpqZp3CvBC0wVheivgvknbZnaXeaxTdzwuIJ2EdjQSlJHDTnIOqzfwQZZkPVprYKct8csQA5UwVQdB04t89/1O/w1cDnyilFU="

configuration = Configuration(access_token=CHANNEL_ACCESS_TOKEN)
handler       = WebhookHandler(CHANNEL_SECRET)

PORTFOLIO_FILE = os.path.expanduser("~/lumistock_portfolio.json")

def load_portfolio():
    if os.path.exists(PORTFOLIO_FILE):
        with open(PORTFOLIO_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_portfolio(portfolio):
    with open(PORTFOLIO_FILE, "w", encoding="utf-8") as f:
        json.dump(portfolio, f, ensure_ascii=False, indent=2)


# ══════════════════════════════════════════
#  台股資料（證交所＋櫃買中心）
# ══════════════════════════════════════════
def get_tw_stock(stock_id: str) -> dict:
    """先查證交所（上市），找不到再查櫃買（上櫃）"""
    headers = {"User-Agent": "Mozilla/5.0"}

    # 證交所（上市）
    try:
        url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=tse_{stock_id}.tw&json=1&delay=0"
        r = requests.get(url, headers=headers, timeout=8)
        data = r.json()
        items = data.get("msgArray", [])
        if items and items[0].get("z", "-") != "-":
            d = items[0]
            price = float(d.get("z", 0) or d.get("y", 0))
            prev  = float(d.get("y", price))
            chg   = price - prev
            pct   = chg / prev * 100 if prev else 0
            high  = d.get("h", "N/A")
            low   = d.get("l", "N/A")
            vol   = d.get("v", "N/A")
            name  = d.get("n", stock_id)
            return {
                "name": name, "price": price, "prev": prev,
                "chg": chg, "pct": pct,
                "high": high, "low": low, "vol": vol,
                "market": "上市", "source": "twse"
            }
    except:
        pass

    # 櫃買中心（上櫃）
    try:
        url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=otc_{stock_id}.tw&json=1&delay=0"
        r = requests.get(url, headers=headers, timeout=8)
        data = r.json()
        items = data.get("msgArray", [])
        if items and items[0].get("z", "-") != "-":
            d = items[0]
            price = float(d.get("z", 0) or d.get("y", 0))
            prev  = float(d.get("y", price))
            chg   = price - prev
            pct   = chg / prev * 100 if prev else 0
            high  = d.get("h", "N/A")
            low   = d.get("l", "N/A")
            vol   = d.get("v", "N/A")
            name  = d.get("n", stock_id)
            return {
                "name": name, "price": price, "prev": prev,
                "chg": chg, "pct": pct,
                "high": high, "low": low, "vol": vol,
                "market": "上櫃", "source": "otc"
            }
    except:
        pass

    return None


def get_tw_fundamental(stock_id: str) -> dict:
    """從 yfinance 抓台股基本面（補充用）"""
    try:
        info = yf.Ticker(f"{stock_id}.TW").info
        return {
            "eps":  info.get("trailingEps"),
            "pe":   info.get("trailingPE"),
            "roe":  info.get("returnOnEquity"),
            "div":  info.get("dividendYield"),
            "pb":   info.get("priceToBook"),
        }
    except:
        return {}


# ══════════════════════════════════════════
#  股票分析主函數
# ══════════════════════════════════════════
def get_stock_summary(symbol: str) -> str:
    symbol = symbol.strip().upper()

    # 判斷台股（純數字）或美股（英文）
    is_tw = symbol.isdigit()

    if is_tw:
        # 台股：爬證交所
        tw = get_tw_stock(symbol)
        if not tw:
            return f"❌ 查無此股票：{symbol}\n請確認代碼是否正確"

        arrow = "▲" if tw["chg"] >= 0 else "▼"
        fund  = get_tw_fundamental(symbol)

        eps = f"{fund['eps']:.2f}" if fund.get("eps") else "N/A"
        pe  = f"{fund['pe']:.1f}"  if fund.get("pe")  else "N/A"
        roe = f"{fund['roe']:.1%}" if fund.get("roe") else "N/A"
        div = f"{fund['div']:.1%}" if fund.get("div") else "N/A"
        pb  = f"{fund['pb']:.2f}"  if fund.get("pb")  else "N/A"

        return f"""✨ 慧股拾光 Lumistock
━━━━━━━━━━━━━━
📊 {symbol}｜{tw['name']}（{tw['market']}）
現價：{tw['price']:.2f}　{arrow}{abs(tw['chg']):.2f}（{tw['pct']:+.2f}%）
最高：{tw['high']}　最低：{tw['low']}
成交量：{tw['vol']} 張

💰 基本面
EPS：{eps}　PE：{pe}
ROE：{roe}　PB：{pb}
殖利率：{div}

🕐 {datetime.now().strftime("%m/%d %H:%M")}"""

    else:
        # 美股：yfinance
        try:
            info  = yf.Ticker(symbol).info
            price = info.get("regularMarketPrice") or info.get("previousClose", 0)
            prev  = info.get("regularMarketPreviousClose") or info.get("previousClose", 1)
            chg   = price - prev
            pct   = chg / prev * 100 if prev else 0
            arrow = "▲" if chg >= 0 else "▼"
            name  = info.get("longName", symbol)[:20]
            eps   = info.get("trailingEps")
            pe    = info.get("trailingPE")
            roe   = info.get("returnOnEquity")
            div   = info.get("dividendYield")
            pb    = info.get("priceToBook")

            return f"""✨ 慧股拾光 Lumistock
━━━━━━━━━━━━━━
📊 {symbol}｜{name}（美股）
現價：{price:.2f}　{arrow}{abs(chg):.2f}（{pct:+.2f}%）

💰 基本面
EPS：{f"{eps:.2f}" if eps else "N/A"}
PE：{f"{pe:.1f}" if pe else "N/A"}
ROE：{f"{roe:.1%}" if roe else "N/A"}
PB：{f"{pb:.2f}" if pb else "N/A"}
殖利率：{f"{div:.1%}" if div else "N/A"}

🕐 {datetime.now().strftime("%m/%d %H:%M")}"""
        except:
            return f"❌ 查無此股票：{symbol}\n請確認代碼是否正確"


# ══════════════════════════════════════════
#  大盤摘要
# ══════════════════════════════════════════
def get_market_summary() -> str:
    indices = [("^TWII","台灣加權"),("^GSPC","S&P 500"),("^IXIC","那斯達克"),("^DJI","道瓊")]
    msg = f"🌍 全球大盤　{datetime.now().strftime('%m/%d %H:%M')}\n━━━━━━━━━━━━━━\n"
    for sym, name in indices:
        try:
            info  = yf.Ticker(sym).info
            price = info.get("regularMarketPrice") or info.get("previousClose", 0)
            prev  = info.get("regularMarketPreviousClose") or info.get("previousClose", 1)
            pct   = (price - prev) / prev * 100 if prev else 0
            icon  = "🟢" if pct >= 0 else "🔴"
            msg  += f"{icon} {name}　{price:,.2f}　{pct:+.2f}%\n"
        except:
            msg += f"⚪ {name}　--\n"
    return msg.strip()


# ══════════════════════════════════════════
#  持股查詢
# ══════════════════════════════════════════
def get_portfolio_summary() -> str:
    portfolio = load_portfolio()
    if not portfolio:
        return "📋 持股清單是空的\n\n新增方式：\n新增 2330 100 200\n（代碼 股數 買入價）"
    msg = "📋 我的持股\n━━━━━━━━━━━━━━\n"
    total = 0
    for symbol, data in portfolio.items():
        try:
            if symbol.replace(".TW","").isdigit():
                sid = symbol.replace(".TW","")
                tw = get_tw_stock(sid)
                price = tw["price"] if tw else 0
            else:
                price = yf.Ticker(symbol).info.get("regularMarketPrice", 0)
            shares    = data["shares"]
            buy_price = data["buy_price"]
            profit    = (price - buy_price) * shares
            pct       = (price - buy_price) / buy_price * 100
            icon      = "🟢" if profit >= 0 else "🔴"
            total    += profit
            msg += f"{icon} {symbol}\n   現價 {price:.2f}｜買入 {buy_price:.2f}\n   {shares}股　{profit:+,.0f}（{pct:+.1f}%）\n"
        except:
            msg += f"⚪ {symbol}　查詢失敗\n"
    msg += f"━━━━━━━━━━━━━━\n{'🟢' if total>=0 else '🔴'} 總損益：{total:+,.0f}"
    return msg


HELP_MSG = """✨ 慧股拾光 Lumistock
━━━━━━━━━━━━━━
📌 可用指令：

🔍 查股票（直接輸入代號）
2330　AAPL　6127

📋 持股管理
新增 2330 100 200
（代碼 股數 買入均價）
刪除 2330
持股

🌍 大盤

❓ 說明
━━━━━━━━━━━━━━
台股、美股都支援"""


def reply(reply_token, text):
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).reply_message(
            ReplyMessageRequest(
                reply_token=reply_token,
                messages=[TextMessage(text=text)]
            )
        )

@app.after_request
def add_header(response):
    response.headers["ngrok-skip-browser-warning"] = "true"
    return response

@app.route("/callback", methods=["POST"])
def callback():
    signature = request.headers["X-Line-Signature"]
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        abort(400)
    return "OK"

@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    text = event.message.text.strip()

    if text in ["大盤", "指數", "市場"]:
        reply(event.reply_token, get_market_summary())
    elif text in ["持股", "查持股", "我的持股"]:
        reply(event.reply_token, get_portfolio_summary())
    elif text in ["說明", "help", "Help", "?"]:
        reply(event.reply_token, HELP_MSG)
    elif text.startswith("新增"):
        parts = text.split()
        if len(parts) == 4:
            symbol = parts[1].upper()
            if symbol.isdigit(): symbol += ".TW"
            try:
                p = load_portfolio()
                p[symbol] = {"shares": int(parts[2]), "buy_price": float(parts[3])}
                save_portfolio(p)
                reply(event.reply_token, f"✅ 新增成功！\n{symbol}　{parts[2]}股　買入價 {parts[3]}")
            except:
                reply(event.reply_token, "格式錯誤\n範例：新增 2330 100 200")
        else:
            reply(event.reply_token, "格式：新增 代碼 股數 買入價\n範例：新增 2330 100 200")
    elif text.startswith("刪除"):
        parts = text.split()
        if len(parts) == 2:
            symbol = parts[1].upper()
            if symbol.isdigit(): symbol += ".TW"
            p = load_portfolio()
            if symbol in p:
                del p[symbol]
                save_portfolio(p)
                reply(event.reply_token, f"✅ 已刪除 {symbol}")
            else:
                reply(event.reply_token, f"找不到 {symbol}")
        else:
            reply(event.reply_token, "格式：刪除 代碼\n範例：刪除 2330")
    else:
        # 直接輸入代號查詢
        t = text.upper().replace("查","").strip()
        if t and (t.isdigit() or t.isalpha() or t.replace("-","").isalnum()):
            reply(event.reply_token, get_stock_summary(t))
        else:
            reply(event.reply_token, HELP_MSG)

if __name__ == "__main__":
    print("慧股拾光 Lumistock LINE Bot v3.0 啟動中...")
    app.run(port=5001, debug=False)
