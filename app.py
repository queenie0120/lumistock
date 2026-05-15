"""
慧股拾光 Lumistock – by Hui
LINE Bot 模組 v10.1（名稱修正＋完整技術分析＋UI優化）
"""

from flask import Flask, request, abort
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi,
    ReplyMessageRequest, TextMessage, PushMessageRequest,
    FlexMessage, FlexContainer
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent
import requests
import json, os, re
from datetime import datetime, timezone, timedelta
import xml.etree.ElementTree as ET
import gspread
from google.oauth2.service_account import Credentials
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)

CHANNEL_SECRET       = os.environ.get("LINE_CHANNEL_SECRET")
CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN")
OWNER_USER_ID        = "U972c7aec7b6628d70f52bc0bcbb4bf4a"
SHEETS_ID            = os.environ.get("GOOGLE_SHEETS_ID")

configuration = Configuration(access_token=CHANNEL_ACCESS_TOKEN)
handler       = WebhookHandler(CHANNEL_SECRET)

WAITING_SUGGESTION = set()
PORTFOLIO_FILE     = "/tmp/lumistock_portfolio.json"
NAME_CACHE         = {}
TZ_TAIPEI          = timezone(timedelta(hours=8))

def now_taipei():
    return datetime.now(TZ_TAIPEI)


# ══════════════════════════════════════════
#  啟動時載入全部股票名稱
# ══════════════════════════════════════════
def init_name_cache():
    headers = {"User-Agent": "Mozilla/5.0"}
    for mode, label in [("2", "上市"), ("4", "上櫃")]:
        try:
            url = f"https://isin.twse.com.tw/isin/C_public.jsp?strMode={mode}"
            r = requests.get(url, headers=headers, timeout=15, verify=False)
            r.encoding = "big5"
            rows = re.findall(r'<td[^>]*>(\d{4,6})\s*</td>\s*<td[^>]*>([^<\s]+)', r.text)
            count = 0
            for code, name in rows:
                NAME_CACHE[code] = name.strip()
                count += 1
            print(f"✅ {label}股票名稱載入：{count} 筆")
        except Exception as e:
            print(f"{label}名稱載入失敗：{e}")
    print(f"✅ 股票名稱總計：{len(NAME_CACHE)} 筆")


# ══════════════════════════════════════════
#  Rich Menu
# ══════════════════════════════════════════
def setup_rich_menu():
    try:
        headers_json = {
            "Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}",
            "Content-Type": "application/json"
        }
        r = requests.get("https://api.line.me/v2/bot/richmenu/list",
                         headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"})
        for menu in r.json().get("richmenus", []):
            requests.delete(f"https://api.line.me/v2/bot/richmenu/{menu['richMenuId']}",
                           headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"})
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
        img_url = "https://raw.githubusercontent.com/queenie0120/lumistock/main/richmenu.png"
        img_r = requests.get(img_url, timeout=15)
        requests.post(
            f"https://api-data.line.me/v2/bot/richmenu/{rich_menu_id}/content",
            headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}", "Content-Type": "image/png"},
            data=img_r.content
        )
        requests.post(
            f"https://api.line.me/v2/bot/user/all/richmenu/{rich_menu_id}",
            headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"}
        )
        print("✅ Rich Menu 設定完成！")
    except Exception as e:
        print(f"Rich Menu 設定失敗：{e}")


# ══════════════════════════════════════════
#  Google Sheets
# ══════════════════════════════════════════
def get_sheets_client():
    try:
        creds_dict = json.loads(os.environ.get("GOOGLE_SHEETS_CREDENTIALS"))
        scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
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

def log_to_sheets(user_id, action, content, result):
    try:
        sheet = get_sheet("系統記錄")
        if sheet:
            sheet.append_row([now_taipei().strftime("%Y-%m-%d %H:%M"), user_id, action, content, result, "", ""])
    except:
        pass

def save_suggestion_to_sheets(user_id, text):
    try:
        sheet = get_sheet("系統記錄")
        if sheet:
            sheet.append_row([now_taipei().strftime("%Y-%m-%d %H:%M"), user_id, "建議", "", "", text, ""])
    except:
        pass

def save_portfolio_to_sheets(user_id, symbol, name, market, shares, buy_price):
    try:
        sheet = get_sheet("自選股")
        if sheet:
            now = now_taipei().strftime("%Y-%m-%d %H:%M")
            sheet.append_row([user_id, symbol, name, market, shares, buy_price, "", "", "", now, now])
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
                now_taipei().strftime("%Y-%m-%d"), stock_id, data.get("name",""),
                "", data.get("high",""), data.get("low",""),
                data.get("price",""), data.get("vol",""),
                f"{data.get('pct',0):+.2f}%", "", "", "", "", ""
            ])
    except:
        pass

def update_us_data_to_sheets(symbol, data):
    try:
        sheet = get_sheet("美股資料")
        if sheet and data:
            sheet.append_row([
                now_taipei().strftime("%Y-%m-%d"), symbol, data.get("name",""),
                data.get("price",""), f"{data.get('pct',0):+.2f}%",
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
#  推播
# ══════════════════════════════════════════
def push_to_owner(text):
    try:
        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).push_message(
                PushMessageRequest(to=OWNER_USER_ID, messages=[TextMessage(text=text)])
            )
    except:
        pass


# ══════════════════════════════════════════
#  台股名稱（統一來源）
# ══════════════════════════════════════════
def get_tw_stock_name(stock_id: str) -> str:
    """統一名稱查詢，優先從快取，再從API"""
    if stock_id in NAME_CACHE and NAME_CACHE[stock_id] != stock_id:
        return NAME_CACHE[stock_id]
    headers = {"User-Agent": "Mozilla/5.0"}
    # 上市
    try:
        url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&stockNo={stock_id}"
        r = requests.get(url, headers=headers, timeout=5)
        data = r.json()
        if data.get("stat") == "OK":
            title = data.get("title", "")
            parts = title.strip().split()
            if len(parts) >= 2:
                name = parts[-1].strip()
                if name and name != stock_id:
                    NAME_CACHE[stock_id] = name
                    return name
    except:
        pass
    # 上櫃
    try:
        today = now_taipei()
        civil_year = today.year - 1911
        date_str = f"{civil_year}/{today.month:02d}/{today.day:02d}"
        url = f"https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&o=json&d={date_str}&s=0,asc&q={stock_id}"
        r = requests.get(url, headers=headers, timeout=5)
        data = r.json()
        rows = data.get("aaData", [])
        if rows and len(rows[0]) > 1:
            name = rows[0][1].strip()
            if name and name != stock_id:
                NAME_CACHE[stock_id] = name
                return name
    except:
        pass
    return NAME_CACHE.get(stock_id, stock_id)


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
                raw_name = d.get("n", "").strip()
                name = raw_name if raw_name and raw_name != stock_id else get_tw_stock_name(stock_id)
                NAME_CACHE[stock_id] = name
                return {
                    "name": name, "price": price, "chg": chg, "pct": pct,
                    "open": d.get("o", "N/A"), "high": d.get("h", "N/A"),
                    "low": d.get("l", "N/A"), "vol": d.get("v", "N/A"),
                    "market": "上市" if market == "tse" else "上櫃",
                    "time": d.get("t", "")
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
            name  = get_tw_stock_name(stock_id)
            return {
                "name": name, "price": price, "chg": chg, "pct": pct,
                "open": last[3].replace(",",""), "high": last[4].replace(",",""),
                "low": last[5].replace(",",""), "vol": last[1].replace(",",""),
                "market": "上市（收盤）", "time": last[0]
            }
    except:
        pass

    try:
        today = now_taipei()
        civil_year = today.year - 1911
        date_str = f"{civil_year}/{today.month:02d}/{today.day:02d}"
        url = f"https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&o=json&d={date_str}&s=0,asc&q={stock_id}"
        r = requests.get(url, headers=headers, timeout=8)
        data = r.json()
        rows = data.get("aaData", [])
        if rows:
            last = rows[-1]
            price = float(last[2].replace(",", ""))
            prev  = float(rows[-2][2].replace(",", "")) if len(rows) > 1 else price
            chg   = price - prev
            pct   = chg / prev * 100 if prev else 0
            name  = get_tw_stock_name(stock_id)
            return {
                "name": name, "price": price, "chg": chg, "pct": pct,
                "open": last[3].replace(",","") if len(last) > 3 else "N/A",
                "high": last[4].replace(",","") if len(last) > 4 else "N/A",
                "low":  last[5].replace(",","") if len(last) > 5 else "N/A",
                "vol":  last[0].replace(",","") if len(last) > 0 else "N/A",
                "market": "上櫃（收盤）", "time": date_str
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
        name  = get_tw_stock_name(stock_id)
        return {
            "name": name, "price": price, "chg": chg, "pct": pct,
            "open": "N/A", "high": "N/A", "low": "N/A", "vol": "N/A",
            "market": "收盤", "time": ""
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
        quotes = result.get("indicators", {}).get("quote", [{}])[0]
        closes = [c for c in quotes.get("close", []) if c is not None]
        opens  = [o for o in quotes.get("open",  []) if o is not None]
        highs  = [h for h in quotes.get("high",  []) if h is not None]
        lows   = [l for l in quotes.get("low",   []) if l is not None]
        price = meta.get("regularMarketPrice", 0)
        prev  = meta.get("chartPreviousClose", price)
        chg   = price - prev
        pct   = chg / prev * 100 if prev else 0
        name  = meta.get("longName") or meta.get("shortName") or symbol
        return {
            "name": name[:20], "price": price, "chg": chg, "pct": pct,
            "open":  f"{opens[-1]:.2f}"  if opens  else "N/A",
            "high":  f"{highs[-1]:.2f}"  if highs  else "N/A",
            "low":   f"{lows[-1]:.2f}"   if lows   else "N/A",
            "closes": closes
        }
    except:
        pass
    return None


# ══════════════════════════════════════════
#  K線 Sparkline + 完整均線分析
# ══════════════════════════════════════════
def get_sparkline(closes: list) -> str:
    if not closes or len(closes) < 2:
        return "▁▁▁▁▁▁▁▁▁▁"
    data = closes[-10:]
    mn, mx = min(data), max(data)
    if mx == mn:
        return "▄▄▄▄▄▄▄▄▄▄"
    bars = "▁▂▃▄▅▆▇█"
    return "".join(bars[int((c - mn) / (mx - mn) * 7)] for c in data)

def get_kline_analysis(closes: list) -> dict:
    if not closes or len(closes) < 2:
        return {"spark": "▄▄▄▄▄▄▄▄▄▄", "trend": "--", "rsi": 0, "rsi_label": "--"}

    def ma(n):
        if len(closes) >= n:
            return sum(closes[-n:]) / n
        return None

    ma5   = ma(5)
    ma20  = ma(20)
    ma60  = ma(60)
    ma120 = ma(120)
    ma240 = ma(240)
    price = closes[-1]

    # 趨勢判斷
    if ma5 and ma20 and ma60:
        if ma5 > ma20 > ma60:
            trend = "多頭排列 📈"
        elif ma5 < ma20 < ma60:
            trend = "空頭排列 📉"
        elif price > ma60:
            trend = "季線之上"
        else:
            trend = "季線之下"
    elif closes[-1] > closes[-min(5, len(closes))]:
        trend = "短線向上"
    else:
        trend = "短線向下"

    # 均線訊號
    if ma20:
        ma_sig = "站穩月線" if price > ma20 else "跌破月線"
    else:
        ma_sig = "--"

    # RSI
    gains  = [max(closes[i]-closes[i-1], 0) for i in range(1, len(closes))]
    losses = [max(closes[i-1]-closes[i], 0) for i in range(1, len(closes))]
    avg_gain = sum(gains[-14:]) / min(14, len(gains)) if gains else 0
    avg_loss = sum(losses[-14:]) / min(14, len(losses)) if losses else 0.001
    rsi = 100 - (100 / (1 + avg_gain / avg_loss)) if avg_loss else 50

    if rsi > 80:   rsi_label = "短線過熱"
    elif rsi > 70: rsi_label = "短線偏熱"
    elif rsi < 20: rsi_label = "極度超賣"
    elif rsi < 30: rsi_label = "短線偏冷"
    else:          rsi_label = "中性區間"

    return {
        "spark": get_sparkline(closes),
        "trend": trend, "ma_sig": ma_sig,
        "ma5": ma5, "ma20": ma20, "ma60": ma60,
        "ma120": ma120, "ma240": ma240,
        "rsi": rsi, "rsi_label": rsi_label
    }

def get_tw_closes(stock_id: str) -> list:
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
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
            if closes:
                return closes
    except:
        pass
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{stock_id}.TW?interval=1d&range=60d"
        r = requests.get(url, headers=headers, timeout=10)
        data = r.json()
        closes = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        return [c for c in closes if c is not None]
    except:
        pass
    return []


# ══════════════════════════════════════════
#  新聞（3-5則）
# ══════════════════════════════════════════
def get_news(query: str, count: int = 4) -> list:
    headers = {"User-Agent": "Mozilla/5.0"}
    results = []
    try:
        url = f"https://news.google.com/rss/search?q={requests.utils.quote(query)}&hl=zh-TW&gl=TW&ceid=TW:zh-Hant"
        r = requests.get(url, timeout=8, headers=headers)
        root = ET.fromstring(r.content)
        for item in root.findall(".//item")[:count]:
            title = item.findtext("title", "").split(" - ")[0].strip()
            link  = item.findtext("link", "").strip()
            if title:
                results.append((title[:28] + "…" if len(title) > 28 else title, link))
        if results:
            return results
    except:
        pass
    try:
        url = "https://news.cnyes.com/rss/news/tw_stock"
        r = requests.get(url, timeout=8, headers=headers)
        root = ET.fromstring(r.content)
        for item in root.findall(".//item")[:count]:
            title = item.findtext("title", "").strip()
            link  = item.findtext("link", "").strip()
            if title:
                results.append((title[:28] + "…" if len(title) > 28 else title, link))
        if results:
            return results
    except:
        pass
    return []


# ══════════════════════════════════════════
#  Flex Message 股票卡片
# ══════════════════════════════════════════
def make_ma_row(label, value, color="#B06050"):
    val_str = f"{value:.0f}" if value else "N/A"
    return {
        "type": "box",
        "layout": "horizontal",
        "contents": [
            {"type": "text", "text": label, "size": "xs", "color": "#9B6B5A", "flex": 3},
            {"type": "text", "text": val_str, "size": "xs", "color": color, "flex": 2, "weight": "bold"},
        ]
    }

def make_stock_flex(symbol, name, market, price, chg, pct, open_p, high, low, vol, kline, news_list, query_time):
    is_up  = chg >= 0
    color  = "#C47055" if is_up else "#5B8DB8"
    arrow  = "▲" if is_up else "▼"
    sign   = "+" if is_up else ""
    spark  = kline.get("spark", "▄▄▄▄▄▄▄▄▄▄")
    trend  = kline.get("trend", "--")
    ma5    = kline.get("ma5")
    ma20   = kline.get("ma20")
    ma60   = kline.get("ma60")
    ma120  = kline.get("ma120")
    ma240  = kline.get("ma240")
    rsi    = kline.get("rsi", 0)
    rsi_label = kline.get("rsi_label", "--")

    # 新聞按鈕（每則獨立按鈕）
    news_contents = []
    for title, url in news_list[:4]:
        if url:
            news_contents.append({
                "type": "button",
                "style": "link",
                "height": "sm",
                "action": {"type": "uri", "label": f"📰 {title}", "uri": url},
                "color": "#7A3828"
            })
        else:
            news_contents.append({
                "type": "text",
                "text": f"📰 {title}",
                "size": "xs",
                "color": "#B06050",
                "wrap": True
            })

    if not news_contents:
        news_contents = [{"type": "text", "text": "暫無相關新聞", "size": "xs", "color": "#C4907A"}]

    # RSI 顏色
    rsi_color = "#C47055" if rsi > 70 else ("#5B8DB8" if rsi < 30 else "#8B6B5A")

    flex = {
        "type": "bubble",
        "size": "mega",
        "header": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#C47055",
            "paddingAll": "16px",
            "contents": [
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": "✨ 慧股拾光 Lumistock", "size": "xxs", "color": "#F0D0C0", "flex": 1},
                        {"type": "text", "text": market, "size": "xxs", "color": "#F0D0C0", "align": "end"}
                    ]
                },
                {
                    "type": "text",
                    "text": f"{symbol}",
                    "size": "md",
                    "color": "#FFE8DC",
                    "weight": "bold"
                },
                {
                    "type": "text",
                    "text": name,
                    "size": "xl",
                    "color": "#FFFFFF",
                    "weight": "bold",
                    "wrap": True
                }
            ]
        },
        "body": {
            "type": "box",
            "layout": "vertical",
            "backgroundColor": "#FDF6F0",
            "paddingAll": "14px",
            "spacing": "sm",
            "contents": [
                # 價格區
                {
                    "type": "box",
                    "layout": "vertical",
                    "contents": [
                        {
                            "type": "text",
                            "text": f"{price:.2f}",
                            "size": "3xl",
                            "weight": "bold",
                            "color": color
                        },
                        {
                            "type": "text",
                            "text": f"{arrow} {abs(chg):.2f}　{sign}{pct:.2f}%",
                            "size": "sm",
                            "color": color
                        }
                    ]
                },
                {"type": "separator", "color": "#E8C4B4"},
                # 開高低收
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {
                            "type": "box", "layout": "vertical", "flex": 1,
                            "contents": [
                                {"type": "text", "text": "開盤", "size": "xxs", "color": "#9B6B5A"},
                                {"type": "text", "text": str(open_p), "size": "sm", "color": "#5B4040", "weight": "bold"}
                            ]
                        },
                        {
                            "type": "box", "layout": "vertical", "flex": 1,
                            "contents": [
                                {"type": "text", "text": "最高", "size": "xxs", "color": "#9B6B5A"},
                                {"type": "text", "text": str(high), "size": "sm", "color": "#C47055", "weight": "bold"}
                            ]
                        },
                        {
                            "type": "box", "layout": "vertical", "flex": 1,
                            "contents": [
                                {"type": "text", "text": "最低", "size": "xxs", "color": "#9B6B5A"},
                                {"type": "text", "text": str(low), "size": "sm", "color": "#5B8DB8", "weight": "bold"}
                            ]
                        },
                        {
                            "type": "box", "layout": "vertical", "flex": 1,
                            "contents": [
                                {"type": "text", "text": "成交量", "size": "xxs", "color": "#9B6B5A"},
                                {"type": "text", "text": str(vol), "size": "sm", "color": "#5B4040", "weight": "bold"}
                            ]
                        }
                    ]
                },
                {"type": "separator", "color": "#E8C4B4"},
                # 技術分析標題
                {"type": "text", "text": "📊 技術分析", "size": "sm", "weight": "bold", "color": "#7A3828"},
                # Sparkline
                {"type": "text", "text": spark, "size": "xl", "color": color},
                # 趨勢
                {"type": "text", "text": f"趨勢　{trend}", "size": "sm", "color": "#7A3828"},
                # 均線 - 分行顯示
                {
                    "type": "box",
                    "layout": "vertical",
                    "spacing": "xs",
                    "contents": [
                        make_ma_row("MA5　　短線", ma5),
                        make_ma_row("MA20　　月線", ma20),
                        make_ma_row("MA60　　季線", ma60),
                        make_ma_row("MA120　半年線", ma120),
                        make_ma_row("MA240　年線", ma240),
                    ]
                },
                # RSI
                {
                    "type": "box",
                    "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": "RSI", "size": "xs", "color": "#9B6B5A", "flex": 2},
                        {"type": "text", "text": f"{rsi:.0f}", "size": "xs", "color": rsi_color, "weight": "bold", "flex": 1},
                        {"type": "text", "text": rsi_label, "size": "xs", "color": rsi_color, "flex": 4}
                    ]
                },
                {"type": "separator", "color": "#E8C4B4"},
                # 新聞
                {"type": "text", "text": "📰 相關新聞", "size": "sm", "weight": "bold", "color": "#7A3828"},
            ] + news_contents + [
                {"type": "separator", "color": "#E8C4B4"},
                {"type": "text", "text": f"🕐 查詢時間　{query_time}", "size": "xxs", "color": "#C4907A"}
            ]
        }
    }
    return flex


# ══════════════════════════════════════════
#  股票查詢
# ══════════════════════════════════════════
def get_stock_flex(symbol: str, user_id: str = ""):
    symbol = symbol.strip().upper()
    is_tw = symbol.isdigit()
    query_time = now_taipei().strftime("%m/%d %H:%M")

    if is_tw:
        tw = get_tw_stock(symbol)
        if not tw:
            return None, f"查無此股票：{symbol}\n請確認代碼是否正確"
        # 確保名稱正確
        if not tw.get("name") or tw["name"] == symbol:
            tw["name"] = get_tw_stock_name(symbol)
        closes = get_tw_closes(symbol)
        kline  = get_kline_analysis(closes)
        news   = get_news(f"{symbol} {tw['name']} 股票")
        update_tw_data_to_sheets(symbol, tw)
        log_to_sheets(user_id, "查詢台股", symbol, "成功")
        flex = make_stock_flex(
            symbol, tw["name"], tw["market"],
            tw["price"], tw["chg"], tw["pct"],
            tw.get("open", "N/A"), tw["high"], tw["low"], tw["vol"],
            kline, news, query_time
        )
        return flex, None
    else:
        us = get_us_stock(symbol)
        if not us:
            return None, f"查無此股票：{symbol}\n請確認代碼是否正確"
        kline  = get_kline_analysis(us.get("closes", []))
        news   = get_news(f"{symbol} {us['name']} 股票")
        update_us_data_to_sheets(symbol, us)
        log_to_sheets(user_id, "查詢美股", symbol, "成功")
        flex = make_stock_flex(
            symbol, us["name"], "美股",
            us["price"], us["chg"], us["pct"],
            us.get("open", "N/A"), us.get("high", "N/A"),
            us.get("low", "N/A"), "N/A",
            kline, news, query_time
        )
        return flex, None


# ══════════════════════════════════════════
#  大盤
# ══════════════════════════════════════════
def get_market_summary() -> str:
    headers = {"User-Agent": "Mozilla/5.0"}
    msg = (f"🌐 全球大盤\n"
           f"━━━━━━━━━━━━━━\n"
           f"　{now_taipei().strftime('%m/%d %H:%M')} 更新\n"
           f"━━━━━━━━━━━━━━\n")
    try:
        url = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=tse_t00.tw&json=1&delay=0"
        r = requests.get(url, headers=headers, timeout=8)
        data = r.json()
        items = data.get("msgArray", [])
        if items:
            d = items[0]
            price = float(d.get("z", 0) or d.get("y", 0))
            prev  = float(d.get("y", price))
            pct   = (price - prev) / prev * 100 if prev else 0
            icon  = "🟢" if pct >= 0 else "🔴"
            msg  += f"{icon} 台灣加權　{price:,.2f}　{pct:+.2f}%\n"
        else:
            msg += "⚪ 台灣加權　--\n"
    except:
        msg += "⚪ 台灣加權　--\n"

    for sym, name in [("^GSPC","S&P 500"),("^IXIC","那斯達克"),("^DJI","道瓊")]:
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

    msg += "━━━━━━━━━━━━━━\n⚠️ 僅供參考，非投資建議"
    return msg


# ══════════════════════════════════════════
#  推薦股
# ══════════════════════════════════════════
def get_recommendation() -> str:
    headers = {"User-Agent": "Mozilla/5.0"}
    msg = (f"⭐ 今日法人買超\n"
           f"━━━━━━━━━━━━━━\n"
           f"　{now_taipei().strftime('%m/%d')} 資料\n"
           f"━━━━━━━━━━━━━━\n")
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
                        candidates.append((row[0], row[1], total, foreign, invest))
                except:
                    pass
            candidates.sort(key=lambda x: x[2], reverse=True)
            for i, (sid, name, total, foreign, invest) in enumerate(candidates[:5], 1):
                tw = get_tw_stock(sid)
                price_str = f"{tw['price']:.2f}" if tw else "N/A"
                pct_str = f"{tw['pct']:+.2f}%" if tw else ""
                msg += (f"　{i}. {sid} {name}\n"
                        f"　   現價 {price_str}　{pct_str}\n"
                        f"　   外資 {foreign:+,} 投信 {invest:+,} 張\n\n")
        else:
            msg += "　暫時無法取得法人資料\n"
    except:
        msg += "　暫時無法取得推薦資料\n"
    msg += "━━━━━━━━━━━━━━\n⚠️ 以上僅供參考，非投資建議"
    return msg


# ══════════════════════════════════════════
#  持股查詢
# ══════════════════════════════════════════
def get_portfolio_summary(user_id: str) -> str:
    portfolio = load_portfolio()
    user_portfolio = {k: v for k, v in portfolio.items() if v.get("user_id") == user_id}
    if not user_portfolio:
        return ("📋 持股清單是空的\n"
                "━━━━━━━━━━━━━━\n"
                "新增方式：\n"
                "　新增 2330 100 200\n"
                "　（代碼 股數 買入均價）")
    msg = "📋 我的持股\n━━━━━━━━━━━━━━\n"
    total = 0
    for symbol, data in user_portfolio.items():
        try:
            sid = symbol.replace(".TW", "")
            if sid.isdigit():
                tw = get_tw_stock(sid)
                price = tw["price"] if tw else 0
                name  = tw["name"] if tw else get_tw_stock_name(sid)
            else:
                us = get_us_stock(symbol)
                price = us["price"] if us else 0
                name  = us["name"] if us else symbol
            shares    = data["shares"]
            buy_price = data["buy_price"]
            profit    = (price - buy_price) * shares
            pct       = (price - buy_price) / buy_price * 100
            icon      = "🟢" if profit >= 0 else "🔴"
            total    += profit
            msg += (f"{icon} {symbol}｜{name}\n"
                    f"　現價 {price:.2f}　買入 {buy_price:.2f}\n"
                    f"　{shares}股　損益 {profit:+,.0f}（{pct:+.1f}%）\n\n")
        except:
            msg += f"　{symbol}　查詢失敗\n\n"
    msg += f"━━━━━━━━━━━━━━\n{'🟢' if total>=0 else '🔴'} 總損益　{total:+,.0f}"
    return msg


# ══════════════════════════════════════════
#  市場新聞
# ══════════════════════════════════════════
def get_market_news() -> str:
    news1 = get_news("台股 股市 今日", 3)
    news2 = get_news("美股 華爾街 今日", 3)

    def fmt(news_list):
        if not news_list:
            return "　暫無新聞"
        lines = []
        for title, url in news_list:
            lines.append(f"　• {title}\n　  🔗 {url}" if url else f"　• {title}")
        return "\n".join(lines)

    return (f"📰 市場新聞\n"
            f"━━━━━━━━━━━━━━\n"
            f"　{now_taipei().strftime('%m/%d %H:%M')} 更新\n"
            f"━━━━━━━━━━━━━━\n"
            f"🇹🇼 台股\n{fmt(news1)}\n"
            f"━━━━━━━━━━━━━━\n"
            f"🇺🇸 美股\n{fmt(news2)}")


HELP_MSG = """✨ 慧股拾光 Lumistock
━━━━━━━━━━━━━━
📌 功能說明

🔍 查股票
　輸入股票代號即可
　台股：2330　美股：AAPL
　ETF：0050　00878

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
台股／美股／ETF 皆支援"""


def reply_text(reply_token, text):
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).reply_message(
            ReplyMessageRequest(reply_token=reply_token, messages=[TextMessage(text=text)])
        )

def reply_flex(reply_token, flex_content, alt_text="股票資訊"):
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).reply_message(
            ReplyMessageRequest(
                reply_token=reply_token,
                messages=[FlexMessage(
                    alt_text=alt_text,
                    contents=FlexContainer.from_dict(flex_content)
                )]
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

    if text == "查股票":
        reply_text(event.reply_token,
              "🔍 請直接輸入股票代號\n"
              "━━━━━━━━━━━━━━\n"
              "　台股：2330\n"
              "　美股：AAPL\n"
              "　ETF：0050　00878")
        return

    if user_id in WAITING_SUGGESTION:
        WAITING_SUGGESTION.discard(user_id)
        save_suggestion_to_sheets(user_id, text)
        push_to_owner(f"💬 收到新建議！\n時間：{now_taipei().strftime('%Y-%m-%d %H:%M')}\n內容：{text}")
        reply_text(event.reply_token, "✅ 感謝您的建議！\n我們會持續改善 Lumistock 🌱")
        return

    if text in ["大盤", "指數", "市場", "大盤行情"]:
        log_to_sheets(user_id, "查詢大盤", "", "成功")
        reply_text(event.reply_token, get_market_summary())
    elif text in ["持股", "查持股", "我的持股"]:
        reply_text(event.reply_token, get_portfolio_summary(user_id))
    elif text in ["推薦股", "今日推薦股", "推薦"]:
        log_to_sheets(user_id, "查詢推薦股", "", "成功")
        reply_text(event.reply_token, get_recommendation())
    elif text in ["新聞", "市場新聞"]:
        log_to_sheets(user_id, "查詢新聞", "", "成功")
        reply_text(event.reply_token, get_market_news())
    elif text in ["建議"]:
        WAITING_SUGGESTION.add(user_id)
        reply_text(event.reply_token,
              "💬 請輸入您的建議\n"
              "━━━━━━━━━━━━━━\n"
              "直接輸入文字送出即可\n"
              "我們會認真參考每一則建議 🙏")
    elif text == "查看建議" and user_id == OWNER_USER_ID:
        try:
            sheet = get_sheet("系統記錄")
            if sheet:
                records = sheet.get_all_records()
                suggestions = [r for r in records if r.get("操作類型") == "建議"]
                if not suggestions:
                    reply_text(event.reply_token, "目前還沒有建議")
                else:
                    msg = f"💬 共 {len(suggestions)} 則建議\n━━━━━━━━━━━━━━\n"
                    for i, s in enumerate(suggestions[-10:], 1):
                        msg += f"{i}. {s.get('時間','')}\n　{s.get('用戶建議','')}\n\n"
                    reply_text(event.reply_token, msg.strip())
        except:
            reply_text(event.reply_token, "查詢建議失敗")
    elif text in ["說明", "help", "Help", "?"]:
        reply_text(event.reply_token, HELP_MSG)
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
                if not name or name == symbol:
                    name = get_tw_stock_name(parts[1]) if parts[1].isdigit() else symbol
                save_portfolio_to_sheets(user_id, symbol, name, market, int(parts[2]), float(parts[3]))
                log_to_sheets(user_id, "新增持股", symbol, "成功")
                reply_text(event.reply_token,
                      f"✅ 新增成功\n"
                      f"━━━━━━━━━━━━━━\n"
                      f"　{symbol}｜{name}\n"
                      f"　{parts[2]} 股　均價 {parts[3]}")
            except:
                reply_text(event.reply_token, "格式錯誤\n範例：新增 2330 100 200")
        else:
            reply_text(event.reply_token, "格式：新增 代碼 股數 買入價\n範例：新增 2330 100 200")
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
                reply_text(event.reply_token, f"✅ 已刪除 {symbol}")
            else:
                reply_text(event.reply_token, f"找不到 {symbol}")
        else:
            reply_text(event.reply_token, "格式：刪除 代碼\n範例：刪除 2330")
    else:
        t = text.upper().replace("查", "").strip()
        if t and (t.isdigit() or t.isalpha() or t.replace("-", "").isalnum()):
            flex, err = get_stock_flex(t, user_id)
            if flex:
                reply_flex(event.reply_token, flex, f"{t} 股票資訊")
            else:
                reply_text(event.reply_token, err or "查詢失敗")
        else:
            reply_text(event.reply_token, HELP_MSG)


if __name__ == "__main__":
    print("慧股拾光 Lumistock LINE Bot v10.1 啟動中...")
    init_name_cache()
    setup_rich_menu()
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=False)


init_name_cache()
setup_rich_menu()
