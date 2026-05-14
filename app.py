"""
慧股拾光 Lumistock – by Hui
LINE Bot 模組 v8.0（Rich Menu 自動設定）
"""

from flask import Flask, request, abort
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi,
    ReplyMessageRequest, TextMessage, PushMessageRequest
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent
import requests
import json, os
from datetime import datetime
import xml.etree.ElementTree as ET
import gspread
from google.oauth2.service_account import Credentials

app = Flask(__name__)

CHANNEL_SECRET       = os.environ.get("LINE_CHANNEL_SECRET")
CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN")
OWNER_USER_ID        = "U972c7aec7b6628d70f52bc0bcbb4bf4a"
SHEETS_ID            = os.environ.get("GOOGLE_SHEETS_ID")

configuration = Configuration(access_token=CHANNEL_ACCESS_TOKEN)
handler       = WebhookHandler(CHANNEL_SECRET)

WAITING_SUGGESTION = set()
PORTFOLIO_FILE     = "/tmp/lumistock_portfolio.json"


# ══════════════════════════════════════════
#  Rich Menu 自動設定
# ══════════════════════════════════════════
def setup_rich_menu():
    try:
        headers_json = {
            "Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}",
            "Content-Type": "application/json"
        }

        # 先刪除舊的 Rich Menu
        r = requests.get("https://api.line.me/v2/bot/richmenu/list",
                         headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"})
        for menu in r.json().get("richmenus", []):
            requests.delete(f"https://api.line.me/v2/bot/richmenu/{menu['richMenuId']}",
                           headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"})

        # 建立新 Rich Menu
        rich_menu_body = {
            "size": {"width": 2500, "height": 1686},
            "selected": True,
            "name": "慧股拾光選單",
            "chatBarText": "✨ 慧股拾光 功能選單",
            "areas": [
                {"bounds": {"x": 0,    "y": 0,   "width": 833, "height": 843}, "action": {"type": "message", "text": "查股票"}},
                {"bounds": {"x": 833,  "y": 0,   "width": 834, "height": 843}, "action": {"type": "message", "text": "大盤"}},
                {"bounds": {"x": 1667, "y": 0,   "width": 833, "height": 843}, "action": {"type": "message", "text": "推薦股"}},
                {"bounds": {"x": 0,    "y": 843, "width": 833, "height": 843}, "action": {"type": "message", "text": "持股"}},
                {"bounds": {"x": 833,  "y": 843, "width": 834, "height": 843}, "action": {"type": "message", "text": "新聞"}},
                {"bounds": {"x": 1667, "y": 843, "width": 833, "height": 843}, "action": {"type": "message", "text": "建議"}}
            ]
        }

        r2 = requests.post("https://api.line.me/v2/bot/richmenu",
                           headers=headers_json, json=rich_menu_body)
        rich_menu_id = r2.json().get("richMenuId")
        if not rich_menu_id:
            return

        # 從 GitHub 下載圖片並上傳
        img_url = "https://raw.githubusercontent.com/queenie0120/lumistock/main/richmenu.png"
        img_r = requests.get(img_url, timeout=15)
        requests.post(
            f"https://api-data.line.me/v2/bot/richmenu/{rich_menu_id}/content",
            headers={
                "Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}",
                "Content-Type": "image/png"
            },
            data=img_r.content
        )

        # 設為預設選單
        requests.post(
            f"https://api.line.me/v2/bot/user/all/richmenu/{rich_menu_id}",
            headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"}
        )
        print("✅ Rich Menu 設定完成！")
    except Exception as e:
        print(f"Rich Menu 設定失敗：{e}")


# ══════════════════════════════════════════
#  Google Sheets 連線
# ══════════════════════════════════════════
def get_sheets_client():
    try:
        creds_json = os.environ.get("GOOGLE_SHEETS_CREDENTIALS")
        creds_dict = json.loads(creds_json)
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        return gspread.authorize(creds)
    except:
        return None

def get_sheet(sheet_name):
    try:
        client = get_sheets_client()
        if client:
            return client.open_by_key(SHEETS_ID).worksheet(sheet_name)
    except:
        pass
    return None

def log_to_sheets(user_id, action, content, result, suggestion="", error=""):
    try:
        sheet = get_sheet("系統記錄")
        if sheet:
            sheet.append_row([
                datetime.now().strftime("%Y-%m-%d %H:%M"),
                user_id, action, content, result, suggestion, error
            ])
    except:
        pass

def save_suggestion_to_sheets(user_id, text):
    try:
        sheet = get_sheet("系統記錄")
        if sheet:
            sheet.append_row([
                datetime.now().strftime("%Y-%m-%d %H:%M"),
                user_id, "建議", "", "", text, ""
            ])
    except:
        pass

def save_portfolio_to_sheets(user_id, symbol, name, market, shares, buy_price):
    try:
        sheet = get_sheet("自選股")
        if sheet:
            now = datetime.now().strftime("%Y-%m-%d %H:%M")
            sheet.append_row([
                user_id, symbol, name, market,
                shares, buy_price, "", "", "", now, now
            ])
    except:
        pass

def delete_portfolio_from_sheets(user_id, symbol):
    try:
        sheet = get_sheet("自選股")
        if sheet:
            records = sheet.get_all_records()
            for i, row in enumerate(records, start=2):
                if str(row.get("用戶ID")) == user_id and str(row.get("股票代號")) == symbol:
                    sheet.delete_rows(i)
                    break
    except:
        pass

def update_tw_data_to_sheets(stock_id, data):
    try:
        sheet = get_sheet("台股資料")
        if sheet and data:
            sheet.append_row([
                datetime.now().strftime("%Y-%m-%d"),
                stock_id, data.get("name",""),
                "", data.get("high",""), data.get("low",""),
                data.get("price",""), data.get("vol",""),
                f"{data.get('pct',0):+.2f}%",
                "", "", "", "", ""
            ])
    except:
        pass

def update_us_data_to_sheets(symbol, data):
    try:
        sheet = get_sheet("美股資料")
        if sheet and data:
            sheet.append_row([
                datetime.now().strftime("%Y-%m-%d"),
                symbol, data.get("name",""),
                data.get("price",""),
                f"{data.get('pct',0):+.2f}%",
                "", "", "", "", "", "", "", ""
            ])
    except:
        pass


# ══════════════════════════════════════════
#  持股
# ══════════════════════════════════════════
def load_portfolio():
    if os.path.exists(PORTFOLIO_FILE):
        with open(PORTFOLIO_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_portfolio(p):
    with open(PORTFOLIO_FILE, "w", encoding="utf-8") as f:
        json.dump(p, f, ensure_ascii=False, indent=2)


# ══════════════════════════════════════════
#  推播給管理者
# ══════════════════════════════════════════
def push_to_owner(text):
    try:
        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).push_message(
                PushMessageRequest(
                    to=OWNER_USER_ID,
                    messages=[TextMessage(text=text)]
                )
            )
    except:
        pass


# ══════════════════════════════════════════
#  台股資料
# ══════════════════════════════════════════
def get_tw_stock(stock_id: str) -> dict:
    headers = {"User-Agent": "Mozilla/5.0"}
    for market in ["tse", "otc"]:
        try:
            url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={market}_{stock_id}.tw&json=1&delay=0"
            r = requests.get(url, headers=headers, timeout=8)
            data = r.json()
            items = data.get("msgArray", [])
            if items and items[0].get("z", "-") != "-":
                d = items[0]
                price = float(d.get("z", 0) or d.get("y", 0))
                prev  = float(d.get("y", price))
                chg   = price - prev
                pct   = chg / prev * 100 if prev else 0
                return {
                    "name": d.get("n", stock_id), "price": price,
                    "chg": chg, "pct": pct,
                    "high": d.get("h", "N/A"), "low": d.get("l", "N/A"),
                    "vol": d.get("v", "N/A"),
                    "market": "上市" if market == "tse" else "上櫃"
                }
        except:
            pass
    try:
        url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&stockNo={stock_id}"
        r = requests.get(url, headers=headers, timeout=8)
        data = r.json()
        if data.get("stat") == "OK" and data.get("data"):
            rows = data["data"]
            last = rows[-1]
            price = float(last[6].replace(",", ""))
            prev  = float(rows[-2][6].replace(",", "")) if len(rows) > 1 else price
            chg   = price - prev
            pct   = chg / prev * 100 if prev else 0
            name  = data.get("title", "").split(" ")[-1] if data.get("title") else stock_id
            return {
                "name": name, "price": price,
                "chg": chg, "pct": pct,
                "high": last[4].replace(",",""), "low": last[5].replace(",",""),
                "vol": last[1].replace(",",""), "market": "上市（收盤）"
            }
    except:
        pass
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{stock_id}.TW?interval=1d&range=5d"
        r = requests.get(url, headers=headers, timeout=10)
        data = r.json()
        meta = data["chart"]["result"][0]["meta"]
        price = meta.get("regularMarketPrice") or meta.get("previousClose", 0)
        prev  = meta.get("chartPreviousClose", price)
        chg   = price - prev
        pct   = chg / prev * 100 if prev else 0
        name  = meta.get("longName") or meta.get("shortName") or stock_id
        return {
            "name": name[:10], "price": price,
            "chg": chg, "pct": pct,
            "high": "N/A", "low": "N/A", "vol": "N/A",
            "market": "收盤"
        }
    except:
        pass
    return None


# ══════════════════════════════════════════
#  美股資料
# ══════════════════════════════════════════
def get_us_stock(symbol: str) -> dict:
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=20d"
        r = requests.get(url, headers=headers, timeout=10)
        data = r.json()
        result = data["chart"]["result"][0]
        meta = result["meta"]
        closes = result.get("indicators", {}).get("quote", [{}])[0].get("close", [])
        closes = [c for c in closes if c is not None]
        price = meta.get("regularMarketPrice", 0)
        prev  = meta.get("chartPreviousClose", price)
        chg   = price - prev
        pct   = chg / prev * 100 if prev else 0
        name  = meta.get("longName") or meta.get("shortName") or symbol
        return {
            "name": name[:20], "price": price,
            "chg": chg, "pct": pct,
            "closes": closes
        }
    except:
        pass
    return None


# ══════════════════════════════════════════
#  K線分析
# ══════════════════════════════════════════
def get_kline_text(closes: list) -> str:
    if not closes or len(closes) < 2:
        return "📈 K線資料不足"
    mn, mx = min(closes), max(closes)
    bars = "▁▂▃▄▅▆▇█"
    spark = "─" * 10 if mx == mn else "".join(bars[int((c-mn)/(mx-mn)*7)] for c in closes[-10:])
    trend = "📈 上升" if closes[-1] > closes[0] else ("📉 下降" if closes[-1] < closes[0] else "➡️ 持平")
    ma5  = sum(closes[-5:])  / min(5,  len(closes))
    ma20 = sum(closes[-20:]) / min(20, len(closes))
    price = closes[-1]
    ma_signal = "強勢（站上均線）" if price > ma5 > ma20 else ("弱勢（跌破均線）" if price < ma5 < ma20 else "整理中")
    gains  = [max(closes[i]-closes[i-1], 0) for i in range(1, len(closes))]
    losses = [max(closes[i-1]-closes[i], 0) for i in range(1, len(closes))]
    avg_gain = sum(gains[-14:])  / min(14, len(gains))  if gains  else 0
    avg_loss = sum(losses[-14:]) / min(14, len(losses)) if losses else 0.001
    rsi = 100 - (100 / (1 + avg_gain / avg_loss)) if avg_loss else 50
    rsi_signal = "超買⚠️" if rsi > 70 else ("超賣💡" if rsi < 30 else "中性")
    return (f"📈 K線走勢（{len(closes)}日）\n"
            f"{spark}  {trend}\n"
            f"MA5：{ma5:.2f}　MA20：{ma20:.2f}\n"
            f"RSI：{rsi:.1f}　{rsi_signal}\n"
            f"均線訊號：{ma_signal}")

def get_tw_kline(stock_id: str) -> str:
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&stockNo={stock_id}"
        r = requests.get(url, headers=headers, timeout=8)
        data = r.json()
        if data.get("stat") == "OK" and data.get("data"):
            closes = []
            for row in data["data"]:
                try:
                    closes.append(float(row[6].replace(",", "")))
                except:
                    pass
            return get_kline_text(closes)
    except:
        pass
    return "📈 K線資料暫無法取得"


# ══════════════════════════════════════════
#  新聞
# ══════════════════════════════════════════
def get_news(query: str) -> str:
    try:
        url = f"https://news.google.com/rss/search?q={requests.utils.quote(query)}&hl=zh-TW&gl=TW&ceid=TW:zh-Hant"
        r = requests.get(url, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
        root = ET.fromstring(r.content)
        items = root.findall(".//item")[:3]
        if not items:
            return "📰 暫無相關新聞"
        news = "📰 相關新聞\n"
        for i, item in enumerate(items, 1):
            title = item.findtext("title", "").split(" - ")[0].strip()
            title = title[:28] + "…" if len(title) > 28 else title
            news += f"{i}. {title}\n"
        return news.strip()
    except:
        return "📰 新聞暫時無法取得"


# ══════════════════════════════════════════
#  股票查詢
# ══════════════════════════════════════════
def get_stock_summary(symbol: str, user_id: str = "") -> str:
    symbol = symbol.strip().upper()
    is_tw = symbol.isdigit()
    if is_tw:
        tw = get_tw_stock(symbol)
        if not tw:
            return f"❌ 查無此股票：{symbol}\n請確認代碼是否正確"
        arrow = "▲" if tw["chg"] >= 0 else "▼"
        kline = get_tw_kline(symbol)
        news  = get_news(f"{symbol} {tw['name']} 股票")
        update_tw_data_to_sheets(symbol, tw)
        log_to_sheets(user_id, "查詢台股", symbol, "成功")
        return (f"✨ 慧股拾光 Lumistock\n"
                f"━━━━━━━━━━━━━━\n"
                f"📊 {symbol}｜{tw['name']}（{tw['market']}）\n"
                f"現價：{tw['price']:.2f}　{arrow}{abs(tw['chg']):.2f}（{tw['pct']:+.2f}%）\n"
                f"最高：{tw['high']}　最低：{tw['low']}\n"
                f"成交量：{tw['vol']} 張\n\n"
                f"{kline}\n\n"
                f"{news}\n\n"
                f"🕐 {datetime.now().strftime('%m/%d %H:%M')}")
    else:
        us = get_us_stock(symbol)
        if not us:
            return f"❌ 查無此股票：{symbol}\n請確認代碼是否正確"
        arrow = "▲" if us["chg"] >= 0 else "▼"
        kline = get_kline_text(us.get("closes", []))
        news  = get_news(f"{symbol} {us['name']} 股票")
        update_us_data_to_sheets(symbol, us)
        log_to_sheets(user_id, "查詢美股", symbol, "成功")
        return (f"✨ 慧股拾光 Lumistock\n"
                f"━━━━━━━━━━━━━━\n"
                f"📊 {symbol}｜{us['name']}（美股）\n"
                f"現價：{us['price']:.2f}　{arrow}{abs(us['chg']):.2f}（{us['pct']:+.2f}%）\n\n"
                f"{kline}\n\n"
                f"{news}\n\n"
                f"🕐 {datetime.now().strftime('%m/%d %H:%M')}")


# ══════════════════════════════════════════
#  大盤
# ══════════════════════════════════════════
def get_market_summary() -> str:
    indices = [("^TWII","台灣加權"),("^GSPC","S&P 500"),("^IXIC","那斯達克"),("^DJI","道瓊")]
    headers = {"User-Agent": "Mozilla/5.0"}
    msg = f"🌐 全球大盤　{datetime.now().strftime('%m/%d %H:%M')}\n━━━━━━━━━━━━━━\n"
    for sym, name in indices:
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=5d"
            r = requests.get(url, headers=headers, timeout=10)
            data = r.json()
            meta = data["chart"]["result"][0]["meta"]
            price = meta.get("regularMarketPrice", 0)
            prev  = meta.get("chartPreviousClose", price)
            pct   = (price - prev) / prev * 100 if prev else 0
            icon  = "🟢" if pct >= 0 else "🔴"
            msg  += f"{icon} {name}　{price:,.2f}　{pct:+.2f}%\n"
        except:
            msg += f"⚪ {name}　--\n"
    return msg.strip()


# ══════════════════════════════════════════
#  推薦股
# ══════════════════════════════════════════
def get_recommendation() -> str:
    headers = {"User-Agent": "Mozilla/5.0"}
    msg = f"⭐ 今日推薦股　{datetime.now().strftime('%m/%d %H:%M')}\n━━━━━━━━━━━━━━\n"
    try:
        url = "https://www.twse.com.tw/rwd/zh/fund/T86?response=json&selectType=ALL"
        r = requests.get(url, headers=headers, timeout=8)
        data = r.json()
        if data.get("stat") == "OK":
            rows = data.get("data", [])
            candidates = []
            for row in rows:
                try:
                    foreign = int(row[4].replace(",","").replace("+",""))
                    invest  = int(row[6].replace(",","").replace("+",""))
                    total   = foreign + invest
                    if total > 0:
                        candidates.append((row[0], row[1], total))
                except:
                    pass
            candidates.sort(key=lambda x: x[2], reverse=True)
            for i, (sid, name, total) in enumerate(candidates[:3], 1):
                tw = get_tw_stock(sid)
                price_str = f"{tw['price']:.2f}" if tw else "N/A"
                msg += (f"{i}. {sid} {name}\n"
                        f"   現價：{price_str}\n"
                        f"   法人合計買超：{total:,} 張\n"
                        f"   📌 法人持續買進，籌碼集中\n\n")
        else:
            msg += "暫時無法取得法人資料\n"
    except:
        msg += "暫時無法取得推薦資料\n"
    msg += "⚠️ 以上僅供參考，投資請自行判斷"
    return msg


# ══════════════════════════════════════════
#  持股查詢
# ══════════════════════════════════════════
def get_portfolio_summary(user_id: str) -> str:
    portfolio = load_portfolio()
    user_portfolio = {k: v for k, v in portfolio.items() if v.get("user_id") == user_id}
    if not user_portfolio:
        return "📋 持股清單是空的\n\n新增方式：\n新增 2330 100 200\n（代碼 股數 買入價）"
    msg = "📋 我的持股\n━━━━━━━━━━━━━━\n"
    total = 0
    for symbol, data in user_portfolio.items():
        try:
            sid = symbol.replace(".TW", "")
            if sid.isdigit():
                tw = get_tw_stock(sid)
                price = tw["price"] if tw else 0
            else:
                us = get_us_stock(symbol)
                price = us["price"] if us else 0
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


# ══════════════════════════════════════════
#  市場新聞
# ══════════════════════════════════════════
def get_market_news() -> str:
    news1 = get_news("台股 股市 今日")
    news2 = get_news("美股 華爾街 今日")
    return (f"📰 市場新聞　{datetime.now().strftime('%m/%d %H:%M')}\n"
            f"━━━━━━━━━━━━━━\n"
            f"🇹🇼 台股新聞\n{news1.replace('📰 相關新聞', '').strip()}\n\n"
            f"🇺🇸 美股新聞\n{news2.replace('📰 相關新聞', '').strip()}")


HELP_MSG = """✨ 慧股拾光 Lumistock
━━━━━━━━━━━━━━
📌 功能說明：

🔍 查股票
直接輸入股票代號
台股：2330　美股：AAPL

🌐 大盤行情
輸入「大盤」

⭐ 今日推薦股
輸入「推薦股」

📋 我的持股
新增 2330 100 200
刪除 2330
輸入「持股」查詢

📰 市場新聞
輸入「新聞」

💬 建議
輸入「建議」給我們回饋
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


@app.route("/", methods=["GET"])
def index():
    return "Lumistock is running! 🌸", 200

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
    user_id = event.source.user_id

    # 查股票模式
    if text == "查股票":
        reply(event.reply_token, "🔍 請直接輸入股票代號\n台股範例：2330\n美股範例：AAPL")
        return

    if user_id in WAITING_SUGGESTION:
        WAITING_SUGGESTION.discard(user_id)
        save_suggestion_to_sheets(user_id, text)
        push_to_owner(f"💬 收到新建議！\n時間：{datetime.now().strftime('%Y-%m-%d %H:%M')}\n內容：{text}")
        reply(event.reply_token, "✅ 感謝你的建議！我們會持續改善 Lumistock 🌱")
        return

    if text in ["大盤", "指數", "市場", "大盤行情"]:
        log_to_sheets(user_id, "查詢大盤", "", "成功")
        reply(event.reply_token, get_market_summary())
    elif text in ["持股", "查持股", "我的持股"]:
        reply(event.reply_token, get_portfolio_summary(user_id))
    elif text in ["推薦股", "今日推薦股", "推薦"]:
        log_to_sheets(user_id, "查詢推薦股", "", "成功")
        reply(event.reply_token, get_recommendation())
    elif text in ["新聞", "市場新聞"]:
        log_to_sheets(user_id, "查詢新聞", "", "成功")
        reply(event.reply_token, get_market_news())
    elif text in ["建議"]:
        WAITING_SUGGESTION.add(user_id)
        reply(event.reply_token, "💬 請輸入您的建議，我們會持續改善！\n\n（直接輸入文字送出即可）")
    elif text == "查看建議" and user_id == OWNER_USER_ID:
        try:
            sheet = get_sheet("系統記錄")
            if sheet:
                records = sheet.get_all_records()
                suggestions = [r for r in records if r.get("操作類型") == "建議"]
                if not suggestions:
                    reply(event.reply_token, "目前還沒有建議")
                else:
                    msg = f"💬 收到 {len(suggestions)} 則建議\n━━━━━━━━━━━━━━\n"
                    for i, s in enumerate(suggestions[-10:], 1):
                        msg += f"{i}. {s.get('時間','')}\n{s.get('用戶建議','')}\n\n"
                    reply(event.reply_token, msg.strip())
        except:
            reply(event.reply_token, "查詢建議失敗")
    elif text in ["說明", "help", "Help", "?"]:
        reply(event.reply_token, HELP_MSG)
    elif text.startswith("新增"):
        parts = text.split()
        if len(parts) == 4:
            symbol = parts[1].upper()
            market = "台股" if symbol.isdigit() else "美股"
            if symbol.isdigit(): symbol += ".TW"
            try:
                p = load_portfolio()
                p[symbol] = {"shares": int(parts[2]), "buy_price": float(parts[3]), "user_id": user_id}
                save_portfolio(p)
                tw = get_tw_stock(parts[1]) if parts[1].isdigit() else None
                us = get_us_stock(symbol) if not parts[1].isdigit() else None
                name = (tw or us or {}).get("name", symbol) if (tw or us) else symbol
                save_portfolio_to_sheets(user_id, symbol, name, market, int(parts[2]), float(parts[3]))
                log_to_sheets(user_id, "新增持股", symbol, "成功")
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
                delete_portfolio_from_sheets(user_id, symbol)
                log_to_sheets(user_id, "刪除持股", symbol, "成功")
                reply(event.reply_token, f"✅ 已刪除 {symbol}")
            else:
                reply(event.reply_token, f"找不到 {symbol}")
        else:
            reply(event.reply_token, "格式：刪除 代碼\n範例：刪除 2330")
    else:
        t = text.upper().replace("查", "").strip()
        if t and (t.isdigit() or t.isalpha() or t.replace("-", "").isalnum()):
            reply(event.reply_token, get_stock_summary(t, user_id))
        else:
            reply(event.reply_token, HELP_MSG)


if __name__ == "__main__":
    print("慧股拾光 Lumistock LINE Bot v8.0 啟動中...")
    setup_rich_menu()
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=False)


# 用 gunicorn 啟動時也執行 Rich Menu 設定
setup_rich_menu()
