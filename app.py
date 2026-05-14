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
            "chg": chg, "pct":
