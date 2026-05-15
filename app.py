"""
慧股拾光 Lumistock – by Hui
LINE Bot 模組 v10.9.8（法人欄位修正＋名稱強制中文）
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
STARTUP_DONE       = False
TZ_TAIPEI          = timezone(timedelta(hours=8))

def now_taipei():
    return datetime.now(TZ_TAIPEI)

def clean_value(v):
    if v in ["-", "", None, "0", 0]:
        return "N/A"
    return str(v)

def has_chinese(text: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", str(text)))

def format_tw_volume(v) -> str:
    if v in ["-", "", None, "N/A"]:
        return "N/A"
    try:
        n = int(float(str(v).replace(",", "")))
        return f"{n:,} 張"
    except:
        return str(v)

def format_us_volume(v) -> str:
    if v in ["-", "", None, "N/A", 0]:
        return "N/A"
    try:
        n = int(float(str(v).replace(",", "")))
        if n >= 100_000_000:
            return f"{n/100_000_000:.2f} 億"
        elif n >= 10_000:
            return f"{n/10_000:.2f} 萬"
        else:
            return f"{n:,}"
    except:
        return str(v)


# ══════════════════════════════════════════
#  啟動初始化
# ══════════════════════════════════════════
@app.before_request
def startup():
    global STARTUP_DONE
    if not STARTUP_DONE:
        STARTUP_DONE = True
        init_name_cache()
        setup_rich_menu()


# ══════════════════════════════════════════
#  啟動時載入全部股票名稱
# ══════════════════════════════════════════
def init_name_cache():
    headers = {"User-Agent": "Mozilla/5.0"}
    tw_loaded = False

    try:
        url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 200 and r.text.strip().startswith("["):
            data = r.json()
            count = 0
            for item in data:
                code = item.get("Code", "").strip()
                name = item.get("Name", "").strip()
                if code and name and has_chinese(name):
                    NAME_CACHE[code] = name
                    count += 1
            if count > 0:
                print(f"✅ 上市股票名稱載入：{count} 筆")
                tw_loaded = True
    except Exception as e:
        print(f"上市方法1失敗：{e}")

    if not tw_loaded:
        try:
            url = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL?response=json"
            r = requests.get(url, headers=headers, timeout=15)
            data = r.json()
            count = 0
            for item in data.get("data", []):
                if len(item) >= 2:
                    code = item[0].strip()
                    name = item[1].strip()
                    if code and name and has_chinese(name):
                        NAME_CACHE[code] = name
                        count += 1
            if count > 0:
                print(f"✅ 上市備援2載入：{count} 筆")
                tw_loaded = True
        except Exception as e:
            print(f"上市方法2失敗：{e}")

    if not tw_loaded:
        try:
            url = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code == 200 and r.text.strip().startswith("["):
                data = r.json()
                count = 0
                for item in data:
                    code = item.get("公司代號", "").strip()
                    name = (item.get("公司簡稱", "") or item.get("公司名稱", "")).strip()
                    if code and name and has_chinese(name):
                        NAME_CACHE[code] = name
                        count += 1
                if count > 0:
                    print(f"✅ 上市備援3載入：{count} 筆")
                    tw_loaded = True
        except Exception as e:
            print(f"上市方法3失敗：{e}")

    try:
        url = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes"
        r = requests.get(url, headers=headers, timeout=15)
        data = r.json()
        count = 0
        for item in data:
            code = item.get("SecuritiesCompanyCode", "").strip()
            name = item.get("CompanyName", "").strip()
            if code and name and has_chinese(name):
                NAME_CACHE[code] = name
                count += 1
        print(f"✅ 上櫃股票名稱載入：{count} 筆")
    except Exception as e:
        print(f"上櫃失敗：{e}")

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
#  會員系統
# ══════════════════════════════════════════
def get_line_profile(user_id: str) -> dict:
    try:
        r = requests.get(
            f"https://api.line.me/v2/bot/profile/{user_id}",
            headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"},
            timeout=5
        )
        data = r.json()
        return {
            "displayName": data.get("displayName", ""),
            "pictureUrl": data.get("pictureUrl", "")
        }
    except:
        return {"displayName": "", "pictureUrl": ""}

def get_user_record(user_id: str) -> dict:
    try:
        sheet = get_sheet("使用者名單")
        if sheet:
            records = sheet.get_all_records()
            for row in records:
                if str(row.get("user_id")) == user_id:
                    return row
    except:
        pass
    return {}

def is_registered(user_id: str) -> bool:
    record = get_user_record(user_id)
    return bool(record.get("註冊姓名"))

def is_blocked_user(user_id: str) -> bool:
    try:
        sheet = get_sheet("黑名單")
        if sheet:
            records = sheet.get_all_records()
            for row in records:
                if str(row.get("user_id")) == user_id and str(row.get("狀態")) == "封鎖":
                    return True
    except:
        pass
    return False

def register_user(user_id: str, reg_name: str) -> str:
    try:
        sheet = get_sheet("使用者名單")
        if not sheet:
            return "❌ 系統錯誤，請稍後再試"
        records = sheet.get_all_records()
        now = now_taipei().strftime("%Y-%m-%d %H:%M")
        profile = get_line_profile(user_id)
        display_name = profile.get("displayName", "")
        picture_url  = profile.get("pictureUrl", "")
        for i, row in enumerate(records, start=2):
            if str(row.get("user_id")) == user_id:
                if row.get("註冊姓名"):
                    return f"✅ 您已經註冊過了！\n姓名：{row.get('註冊姓名')}"
                sheet.update_cell(i, 4, reg_name)
                sheet.update_cell(i, 5, now)
                sheet.update_cell(i, 7, "正常")
                return f"✅ 註冊成功！歡迎 {reg_name} 使用慧股拾光 🌸"
            if str(row.get("註冊姓名")) == reg_name:
                return (f"❌ 姓名「{reg_name}」已被使用\n"
                        f"請換一個名字重新註冊\n\n"
                        f"例如：\n"
                        f"　註冊 {reg_name}2\n"
                        f"　註冊 {reg_name}（暱稱）")
        sheet.append_row([user_id, display_name, picture_url, reg_name, now, "", "正常"])
        return f"✅ 註冊成功！歡迎 {reg_name} 使用慧股拾光 🌸\n\n現在可以直接輸入股票代號查詢！"
    except Exception as e:
        print(f"註冊失敗：{e}")
        return "❌ 註冊失敗，請稍後再試"

def update_user_activity(user_id: str, message: str):
    try:
        sheet = get_sheet("使用者名單")
        if not sheet:
            return
        records = sheet.get_all_records()
        now = now_taipei().strftime("%Y-%m-%d %H:%M")
        for i, row in enumerate(records, start=2):
            if str(row.get("user_id")) == user_id:
                sheet.update_cell(i, 5, now)
                sheet.update_cell(i, 6, message[:50])
                return
        profile = get_line_profile(user_id)
        sheet.append_row([
            user_id, profile.get("displayName", ""),
            profile.get("pictureUrl", ""),
            "", now, message[:50], "未註冊"
        ])
    except Exception as e:
        print(f"更新互動失敗：{e}")

def block_user_by_name(reg_name: str, reason: str) -> str:
    try:
        sheet = get_sheet("使用者名單")
        bl_sheet = get_sheet("黑名單")
        if not sheet or not bl_sheet:
            return "❌ 系統錯誤"
        records = sheet.get_all_records()
        for i, row in enumerate(records, start=2):
            if str(row.get("註冊姓名")) == reg_name:
                uid = str(row.get("user_id"))
                now = now_taipei().strftime("%Y-%m-%d %H:%M")
                sheet.update_cell(i, 7, "封鎖")
                bl_sheet.append_row([uid, reg_name, reason, now, "封鎖"])
                return f"✅ 已封鎖 {reg_name}\n原因：{reason}"
        return f"❌ 找不到用戶：{reg_name}"
    except Exception as e:
        return f"❌ 封鎖失敗：{e}"

def unblock_user_by_name(reg_name: str) -> str:
    try:
        sheet = get_sheet("使用者名單")
        bl_sheet = get_sheet("黑名單")
        if not sheet or not bl_sheet:
            return "❌ 系統錯誤"
        records = sheet.get_all_records()
        found = False
        for i, row in enumerate(records, start=2):
            if str(row.get("註冊姓名")) == reg_name:
                sheet.update_cell(i, 7, "正常")
                found = True
                break
        bl_records = bl_sheet.get_all_records()
        for i, row in enumerate(bl_records, start=2):
            if str(row.get("註冊姓名")) == reg_name and str(row.get("狀態")) == "封鎖":
                bl_sheet.update_cell(i, 5, "解除")
                break
        return f"✅ 已解除封鎖 {reg_name}" if found else f"❌ 找不到用戶：{reg_name}"
    except Exception as e:
        return f"❌ 解除失敗：{e}"

def get_user_list() -> str:
    try:
        sheet = get_sheet("使用者名單")
        if not sheet:
            return "❌ 無法讀取使用者名單"
        records = sheet.get_all_records()
        if not records:
            return "📋 目前沒有使用者記錄"
        msg = f"📋 使用者名單（共 {len(records)} 人）\n━━━━━━━━━━━━━━\n"
        for row in records:
            name   = row.get("註冊姓名", "未註冊")
            nick   = row.get("LINE暱稱", "")
            status = row.get("狀態", "")
            last   = row.get("最後互動時間", "")
            icon   = "🔴" if status == "封鎖" else ("⚪" if status == "未註冊" else "🟢")
            msg += f"{icon} {name}（{nick}）\n　{status}　{last}\n"
        return msg.strip()
    except Exception as e:
        return f"❌ 查詢失敗：{e}"

def get_user_detail(reg_name: str) -> str:
    try:
        sheet = get_sheet("使用者名單")
        if not sheet:
            return "❌ 系統錯誤"
        records = sheet.get_all_records()
        for row in records:
            if str(row.get("註冊姓名")) == reg_name:
                return (f"👤 {reg_name}\n"
                        f"━━━━━━━━━━━━━━\n"
                        f"LINE暱稱：{row.get('LINE暱稱','')}\n"
                        f"user_id：{row.get('user_id','')}\n"
                        f"狀態：{row.get('狀態','')}\n"
                        f"最後互動：{row.get('最後互動時間','')}\n"
                        f"最後訊息：{row.get('最後訊息','')}")
        return f"❌ 找不到用戶：{reg_name}"
    except Exception as e:
        return f"❌ 查詢失敗：{e}"


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
#  台股名稱（強制中文，Yahoo 英文不寫快取）
# ══════════════════════════════════════════
def get_tw_stock_name(stock_id: str) -> str:
    # 1. NAME_CACHE 有中文直接回傳
    cached = NAME_CACHE.get(stock_id, "")
    if cached and has_chinese(cached):
        return cached

    headers = {"User-Agent": "Mozilla/5.0"}

    # 2. TWSE STOCK_DAY title
    try:
        url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&stockNo={stock_id}"
        r = requests.get(url, headers=headers, timeout=5)
        data = r.json()
        if data.get("stat") == "OK":
            title = data.get("title", "")
            parts = title.strip().split()
            if len(parts) >= 2:
                name = parts[-1].strip()
                if name and has_chinese(name):
                    NAME_CACHE[stock_id] = name
                    return name
    except:
        pass

    # 3. TWSE 盤中 API
    for ex in ["tse", "otc"]:
        try:
            url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={ex}_{stock_id}.tw&json=1&delay=0"
            r = requests.get(url, headers=headers, timeout=5)
            data = r.json()
            items = data.get("msgArray", [])
            if items:
                name = items[0].get("n", "").strip()
                if name and has_chinese(name):
                    NAME_CACHE[stock_id] = name
                    return name
        except:
            pass

    # 4. TPEx 盤後
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
            if name and has_chinese(name):
                NAME_CACHE[stock_id] = name
                return name
    except:
        pass

    # 5. Yahoo Finance 最後備援（英文不寫快取）
    for suffix in [".TW", ".TWO"]:
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{stock_id}{suffix}?interval=1d&range=5d"
            r = requests.get(url, headers=headers, timeout=5)
            data = r.json()
            meta = data["chart"]["result"][0]["meta"]
            name = (meta.get("shortName") or meta.get("longName") or "").strip()
            if name:
                if has_chinese(name):
                    NAME_CACHE[stock_id] = name
                # 英文名稱只回傳，不寫快取
                return name
        except:
            pass

    return stock_id


# ══════════════════════════════════════════
#  台股資料（名稱強制走 get_tw_stock_name）
# ══════════════════════════════════════════
def get_tw_stock(stock_id: str) -> dict:
    headers = {"User-Agent": "Mozilla/5.0"}

    for market_type in ["tse", "otc"]:
        try:
            url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={market_type}_{stock_id}.tw&json=1&delay=0"
            r = requests.get(url, headers=headers, timeout=8)
            data = r.json()
            items = data.get("msgArray", [])
            if not items:
                continue
            d = items[0]
            raw_name = d.get("n", "").strip()
            if not raw_name:
                continue
            z = d.get("z", "-")
            y = d.get("y", "-")
            prev = float(y) if y not in ["-", "", "0"] else None
            if prev is None:
                continue
            if z not in ["-", "", "0"]:
                price = float(z)
                is_realtime = True
            else:
                price = prev
                is_realtime = False
            chg  = price - prev
            pct  = chg / prev * 100 if prev else 0

            # 名稱強制中文
            if has_chinese(raw_name):
                NAME_CACHE[stock_id] = raw_name
            name = get_tw_stock_name(stock_id)
            if not has_chinese(name):
                name = raw_name  # fallback 到 API 回傳值

            status = "盤中" if is_realtime else "試撮"
            open_v = d.get("o", "-")
            high_v = d.get("h", "-")
            low_v  = d.get("l", "-")
            vol_v  = d.get("v", "-")
            open_v = "N/A" if open_v in ["-", "", "0"] else open_v
            high_v = "N/A" if high_v in ["-", "", "0"] else high_v
            low_v  = "N/A" if low_v  in ["-", "", "0"] else low_v
            if vol_v not in ["-", "", "0"]:
                try:
                    vol_v = f"{int(float(vol_v.replace(',',''))):,} 張"
                except:
                    vol_v = "N/A"
            else:
                vol_v = "N/A"
            return {
                "name": name, "price": price, "chg": chg, "pct": pct,
                "open": open_v, "high": high_v, "low": low_v, "vol": vol_v,
                "market_type": "台股", "status": status, "source": "TWSE 即時"
            }
        except:
            pass

    try:
        url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&stockNo={stock_id}"
        r = requests.get(url, headers=headers, timeout=8)
        data = r.json()
        if data.get("stat") == "OK" and data.get("data"):
            rows = data["data"]
            last  = rows[-1]
            price = float(last[6].replace(",", ""))
            prev  = float(rows[-2][6].replace(",", "")) if len(rows) > 1 else price
            chg   = price - prev
            pct   = chg / prev * 100 if prev else 0
            try:
                vol_str = f"{int(float(last[1].replace(',',''))//1000):,} 張"
            except:
                vol_str = "N/A"
            name = get_tw_stock_name(stock_id)
            return {
                "name": name, "price": price, "chg": chg, "pct": pct,
                "open": last[3].replace(",",""), "high": last[4].replace(",",""),
                "low":  last[5].replace(",",""), "vol": vol_str,
                "market_type": "台股", "status": "收盤", "source": "TWSE"
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
            last  = rows[-1]
            price = float(last[2].replace(",", ""))
            prev  = float(rows[-2][2].replace(",", "")) if len(rows) > 1 else price
            chg   = price - prev
            pct   = chg / prev * 100 if prev else 0
            try:
                vol_str = f"{int(float(last[0].replace(',',''))):,} 張"
            except:
                vol_str = "N/A"
            open_v = last[5].replace(",","") if len(last) > 5 else "N/A"
            high_v = last[6].replace(",","") if len(last) > 6 else "N/A"
            low_v  = last[7].replace(",","") if len(last) > 7 else "N/A"
            name   = get_tw_stock_name(stock_id)
            return {
                "name": name, "price": price, "chg": chg, "pct": pct,
                "open": open_v, "high": high_v, "low": low_v, "vol": vol_str,
                "market_type": "台股", "status": "收盤", "source": "TPEx"
            }
    except:
        pass

    for suffix in [".TW", ".TWO"]:
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{stock_id}{suffix}?interval=1d&range=5d"
            r = requests.get(url, headers=headers, timeout=10)
            data = r.json()
            result = data["chart"]["result"][0]
            meta   = result["meta"]
            quotes = result.get("indicators", {}).get("quote", [{}])[0]
            opens  = [o for o in quotes.get("open",  []) if o is not None]
            highs  = [h for h in quotes.get("high",  []) if h is not None]
            lows   = [l for l in quotes.get("low",   []) if l is not None]
            vols   = [v for v in quotes.get("volume",[]) if v is not None]
            closes = [c for c in quotes.get("close", []) if c is not None]
            price  = meta.get("regularMarketPrice") or (closes[-1] if closes else 0)
            prev   = meta.get("regularMarketPreviousClose") or (closes[-2] if len(closes) >= 2 else price)
            chg    = price - prev
            pct    = chg / prev * 100 if prev else 0
            # 強制走名稱函數
            name   = get_tw_stock_name(stock_id)
            vol_str = f"{int(vols[-1]/1000):,} 張" if vols else "N/A"
            return {
                "name": name, "price": price, "chg": chg, "pct": pct,
                "open": f"{opens[-1]:.2f}" if opens else "N/A",
                "high": f"{highs[-1]:.2f}" if highs else "N/A",
                "low":  f"{lows[-1]:.2f}"  if lows  else "N/A",
                "vol":  vol_str,
                "market_type": "台股", "status": "收盤", "source": "Yahoo Finance"
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
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=5d"
        r = requests.get(url, headers=headers, timeout=10)
        data = r.json()
        result = data["chart"]["result"][0]
        meta   = result["meta"]
        quotes = result.get("indicators", {}).get("quote", [{}])[0]
        opens  = [o for o in quotes.get("open",   []) if o is not None]
        highs  = [h for h in quotes.get("high",   []) if h is not None]
        lows   = [l for l in quotes.get("low",    []) if l is not None]
        vols   = [v for v in quotes.get("volume", []) if v is not None]
        closes = [c for c in quotes.get("close",  []) if c is not None]
        price  = meta.get("regularMarketPrice") or (closes[-1] if closes else 0)
        prev   = meta.get("regularMarketPreviousClose") or (closes[-2] if len(closes) >= 2 else price)
        chg    = price - prev
        pct    = chg / prev * 100 if prev else 0
        name   = meta.get("shortName") or meta.get("longName") or symbol
        return {
            "name": name[:18], "price": price, "chg": chg, "pct": pct,
            "open":  f"{opens[-1]:.2f}"  if opens  else "N/A",
            "high":  f"{highs[-1]:.2f}"  if highs  else "N/A",
            "low":   f"{lows[-1]:.2f}"   if lows   else "N/A",
            "vol":   format_us_volume(vols[-1]) if vols else "N/A",
            "closes": []
        }
    except:
        pass
    return None

def get_us_closes(symbol: str) -> list:
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=1y"
        r = requests.get(url, headers=headers, timeout=10)
        data = r.json()
        closes = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        return [c for c in closes if c is not None]
    except:
        pass
    return []


# ══════════════════════════════════════════
#  K線
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
        return {"spark": "▄▄▄▄▄▄▄▄▄▄", "trend": "--", "rsi": 0, "rsi_label": "--",
                "ma5": None, "ma20": None, "ma60": None, "ma120": None, "ma240": None}

    def ma(n):
        return sum(closes[-n:]) / n if len(closes) >= n else None

    ma5   = ma(5)
    ma20  = ma(20)
    ma60  = ma(60)
    ma120 = ma(120)
    ma240 = ma(240)

    if ma5 and ma20 and ma60:
        if ma5 > ma20 > ma60:
            trend = "多頭排列 📈"
        elif ma5 < ma20 < ma60:
            trend = "空頭排列 📉"
        elif closes[-1] > ma60:
            trend = "季線之上"
        else:
            trend = "季線之下"
    elif len(closes) >= 5 and closes[-1] > closes[-5]:
        trend = "短線向上"
    else:
        trend = "短線向下"

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
        "spark": get_sparkline(closes), "trend": trend,
        "ma5": ma5, "ma20": ma20, "ma60": ma60,
        "ma120": ma120, "ma240": ma240,
        "rsi": rsi, "rsi_label": rsi_label
    }

def get_tw_closes(stock_id: str) -> list:
    headers = {"User-Agent": "Mozilla/5.0"}
    for suffix in [".TW", ".TWO"]:
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{stock_id}{suffix}?interval=1d&range=1y"
            r = requests.get(url, headers=headers, timeout=10)
            data = r.json()
            closes = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]
            closes = [c for c in closes if c is not None]
            if len(closes) >= 20:
                return closes
        except:
            pass
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
    return []


# ══════════════════════════════════════════
#  新聞（白名單＋去重）
# ══════════════════════════════════════════
TRUSTED_SOURCES = [
    "cnyes.com", "anue.com",
    "money.udn.com", "udn.com",
    "ctee.com.tw",
    "moneydj.com",
    "cna.com.tw",
    "tw.stock.yahoo.com", "yahoo.com",
    "reuters.com",
    "bloomberg.com",
    "marketwatch.com",
    "finance.yahoo.com",
    "technews.tw",
    "bnext.com.tw",
]

def is_trusted_source(url: str) -> bool:
    if not url:
        return False
    for source in TRUSTED_SOURCES:
        if source in url:
            return True
    return False

def deduplicate_news(news_list: list) -> list:
    seen = []
    result = []
    for title, url in news_list:
        key = re.sub(r'[^\u4e00-\u9fffa-zA-Z0-9]', '', title)[:10]
        if key not in seen:
            seen.append(key)
            result.append((title, url))
    return result

def get_news(query: str, count: int = 4, trusted_only: bool = False) -> list:
    headers = {"User-Agent": "Mozilla/5.0"}
    all_results = []
    try:
        url = f"https://news.google.com/rss/search?q={requests.utils.quote(query)}&hl=zh-TW&gl=TW&ceid=TW:zh-Hant"
        r = requests.get(url, timeout=8, headers=headers)
        root = ET.fromstring(r.content)
        for item in root.findall(".//item")[:count * 3]:
            title = item.findtext("title", "").split(" - ")[0].strip()
            link  = item.findtext("link", "").strip()
            if title and link:
                all_results.append((title[:28] + "…" if len(title) > 28 else title, link))
    except:
        pass
    try:
        url = "https://news.cnyes.com/rss/news/tw_stock"
        r = requests.get(url, timeout=8, headers=headers)
        root = ET.fromstring(r.content)
        for item in root.findall(".//item")[:count]:
            title = item.findtext("title", "").strip()
            link  = item.findtext("link", "").strip()
            if title and link:
                all_results.append((title[:28] + "…" if len(title) > 28 else title, link))
    except:
        pass
    trusted   = deduplicate_news([(t,u) for t,u in all_results if is_trusted_source(u)])
    untrusted = deduplicate_news([(t,u) for t,u in all_results if not is_trusted_source(u)])
    if trusted_only:
        return trusted[:count]
    combined = trusted[:count]
    if len(combined) < count:
        combined += untrusted[:count - len(combined)]
    return combined[:count]


# ══════════════════════════════════════════
#  新聞情緒分析
# ══════════════════════════════════════════
BULLISH_KEYWORDS = [
    "上漲", "漲停", "創高", "突破", "買超", "法人買", "外資買",
    "營收創新高", "獲利", "配息", "訂單", "擴產", "樂觀",
    "上調", "買進", "正面", "利多", "強勢", "漲幅", "攻"
]
BEARISH_KEYWORDS = [
    "下跌", "跌停", "破底", "賣超", "法人賣", "外資賣",
    "虧損", "衰退", "砍單", "減產", "悲觀", "下調",
    "賣出", "負面", "利空", "弱勢", "跌幅", "崩"
]

def analyze_news_sentiment(news_list: list) -> dict:
    bull = 0
    bear = 0
    for title, _ in news_list:
        for kw in BULLISH_KEYWORDS:
            if kw in title:
                bull += 1
                break
        for kw in BEARISH_KEYWORDS:
            if kw in title:
                bear += 1
                break
    if bull > bear:
        label = "偏多 📈"
        score = min(bull * 10, 30)
    elif bear > bull:
        label = "偏空 📉"
        score = -min(bear * 10, 30)
    else:
        label = "中性"
        score = 0
    return {"label": label, "score": score}


# ══════════════════════════════════════════
#  推薦股評分
# ══════════════════════════════════════════
def score_technical(closes: list, pct: float) -> dict:
    score = 0
    if not closes or len(closes) < 5:
        return {"score": 0, "rsi": 50}

    def ma(n):
        return sum(closes[-n:]) / n if len(closes) >= n else None

    ma5  = ma(5)
    ma20 = ma(20)
    ma60 = ma(60)
    price = closes[-1]

    if ma5 and ma20 and ma60 and ma5 > ma20 > ma60:
        score += 15
    elif ma5 and ma20 and ma5 > ma20:
        score += 8
    if ma60 and price > ma60:
        score += 8

    gains  = [max(closes[i]-closes[i-1], 0) for i in range(1, len(closes))]
    losses = [max(closes[i-1]-closes[i], 0) for i in range(1, len(closes))]
    avg_gain = sum(gains[-14:]) / min(14, len(gains)) if gains else 0
    avg_loss = sum(losses[-14:]) / min(14, len(losses)) if losses else 0.001
    rsi = 100 - (100 / (1 + avg_gain / avg_loss)) if avg_loss else 50

    if 45 <= rsi <= 70:
        score += 10
    elif rsi < 30:
        score += 5
    elif rsi > 80:
        score -= 5

    if 1 <= pct <= 6:
        score += 7
    elif pct > 8:
        score -= 3
    elif pct < -5:
        score -= 8

    return {"score": max(0, min(score, 40)), "rsi": rsi}

def score_chip(foreign_lot: int, invest_lot: int) -> dict:
    score = 0
    if foreign_lot > 5000:
        score += 15
    elif foreign_lot > 1000:
        score += 8
    elif foreign_lot < -3000:
        score -= 10
    if invest_lot > 2000:
        score += 10
    elif invest_lot > 500:
        score += 5
    if foreign_lot > 0 and invest_lot > 0:
        score += 5
    return {"score": max(0, min(score, 30))}

def score_news_sentiment(sentiment: dict) -> int:
    return max(0, min(30 + sentiment["score"], 30))


# ══════════════════════════════════════════
#  法人資料（修正欄位＋自動往前找5交易日）
# ══════════════════════════════════════════
def fetch_institution_data() -> tuple:
    """
    回傳 (candidates, data_date, is_today)
    T86 欄位：
      row[0]  = 證券代號
      row[1]  = 證券名稱
      row[4]  = 外資買賣超（股）
      row[10] = 投信買賣超（股）
    單位轉換：股 ÷ 1000 = 張
    """
    headers = {"User-Agent": "Mozilla/5.0"}

    for offset in range(7):
        try:
            check_date = now_taipei() - timedelta(days=offset)
            # 跳過週末
            if check_date.weekday() >= 5:
                continue

            if offset == 0:
                url = "https://www.twse.com.tw/rwd/zh/fund/T86?response=json&selectType=ALL"
            else:
                date_str = check_date.strftime("%Y%m%d")
                url = f"https://www.twse.com.tw/rwd/zh/fund/T86?response=json&selectType=ALL&date={date_str}"

            r = requests.get(url, headers=headers, timeout=10)
            data = r.json()

            if data.get("stat") == "OK" and data.get("data"):
                candidates = []
                for row in data.get("data", []):
                    try:
                        if len(row) < 11:
                            continue
                        # 外資：row[4]，投信：row[10]，單位為股
                        foreign = int(row[4].replace(",","").replace("+","").replace("-","0") or 0)
                        invest  = int(row[10].replace(",","").replace("+","").replace("-","0") or 0)
                        # 有負號需保留
                        if row[4].strip().startswith("-"):
                            foreign = -abs(foreign)
                        if row[10].strip().startswith("-"):
                            invest = -abs(invest)
                        # 轉換為張
                        foreign_lot = foreign // 1000
                        invest_lot  = invest  // 1000
                        total_lot   = foreign_lot + invest_lot
                        if total_lot > 500:
                            candidates.append((row[0], row[1], total_lot, foreign_lot, invest_lot))
                    except:
                        pass

                if candidates:
                    data_date = data.get("date", check_date.strftime("%Y/%m/%d"))
                    is_today  = (offset == 0)
                    print(f"✅ 法人資料載入：{data_date}，共 {len(candidates)} 筆")
                    return candidates, data_date, is_today
        except Exception as e:
            print(f"法人資料 offset={offset} 失敗：{e}")

    return [], "", False


# ══════════════════════════════════════════
#  推薦股（多維度評分）
# ══════════════════════════════════════════
def get_recommendation() -> str:
    headers = {"User-Agent": "Mozilla/5.0"}

    # 大盤
    mkt_str   = ""
    market_ok = True
    try:
        url = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=tse_t00.tw&json=1&delay=0"
        r = requests.get(url, headers=headers, timeout=8)
        d = r.json().get("msgArray", [{}])[0]
        mkt_price = float(d.get("z", 0) or d.get("y", 0))
        mkt_prev  = float(d.get("y", mkt_price))
        mkt_pct   = (mkt_price - mkt_prev) / mkt_prev * 100 if mkt_prev else 0
        mkt_icon  = "🟢" if mkt_pct >= 0 else "🔴"
        mkt_str   = f"{mkt_icon} 加權 {mkt_price:,.0f}（{mkt_pct:+.2f}%）"
        if mkt_pct < -2:
            market_ok = False
    except:
        mkt_str = "⚪ 大盤資料取得中"

    # 法人資料
    candidates, data_date, is_today = fetch_institution_data()

    if not candidates:
        return ("⭐ 今日推薦股\n"
                "━━━━━━━━━━━━━━\n"
                "　法人資料尚未更新\n"
                "　請於交易日查詢")

    candidates.sort(key=lambda x: x[2], reverse=True)
    top10 = candidates[:10]

    # 評分
    scored = []
    for sid, name, total_lot, foreign_lot, invest_lot in top10:
        tw = get_tw_stock(sid)
        if not tw:
            continue
        closes    = get_tw_closes(sid)
        tech      = score_technical(closes, tw["pct"])
        chip      = score_chip(foreign_lot, invest_lot)
        news_list = get_news(f"{sid} {tw['name']} 股票", count=4, trusted_only=True)
        sentiment = analyze_news_sentiment(news_list)
        news_score = score_news_sentiment(sentiment)

        total_score = tech["score"] + chip["score"] + news_score
        if not market_ok:
            total_score = int(total_score * 0.8)

        scored.append({
            "sid": sid, "name": tw["name"],
            "price": tw["price"], "pct": tw["pct"],
            "foreign": foreign_lot, "invest": invest_lot,
            "sentiment": sentiment["label"],
            "score": total_score
        })

    scored.sort(key=lambda x: x["score"], reverse=True)
    top5 = scored[:5]

    if not top5:
        return ("⭐ 今日推薦股\n"
                "━━━━━━━━━━━━━━\n"
                "　目前無符合條件個股")

    # 日期標註
    date_note = f"　資料日期：{data_date}"
    if not is_today:
        date_note += "（前交易日）"

    msg = (f"⭐ 慧股推薦榜\n"
           f"━━━━━━━━━━━━━━\n"
           f"　{now_taipei().strftime('%m/%d %H:%M')} 更新\n"
           f"{date_note}\n"
           f"　{mkt_str}\n"
           f"━━━━━━━━━━━━━━\n")

    for i, s in enumerate(top5, 1):
        filled  = s["score"] // 10
        empty   = 10 - filled
        bar     = "█" * filled + "░" * empty
        pct_str = f"{s['pct']:+.2f}%"
        msg += (f"　{i}. {s['sid']} {s['name']}\n"
                f"　   現價 {s['price']:.2f}　{pct_str}\n"
                f"　   外資 {s['foreign']:+,}　投信 {s['invest']:+,} 張\n"
                f"　   新聞 {s['sentiment']}\n"
                f"　   評分 {bar} {s['score']}/100\n\n")

    msg += ("━━━━━━━━━━━━━━\n"
            "📊 技術面＋籌碼面＋新聞情緒\n"
            "⚠️ 以上僅供參考，非投資建議")
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
    news1 = get_news("台股 股市 今日", 3, trusted_only=True)
    news2 = get_news("美股 華爾街 今日", 3, trusted_only=True)

    def fmt(news_list):
        if not news_list:
            return "　暫無可信新聞"
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
        d = r.json().get("msgArray", [{}])[0]
        price = float(d.get("z", 0) or d.get("y", 0))
        prev  = float(d.get("y", price))
        pct   = (price - prev) / prev * 100 if prev else 0
        icon  = "🟢" if pct >= 0 else "🔴"
        msg  += f"{icon} 台灣加權　{price:,.2f}　{pct:+.2f}%\n"
    except:
        msg += "⚪ 台灣加權　--\n"
    for sym, name in [("^GSPC","S&P 500"),("^IXIC","那斯達克"),("^DJI","道瓊")]:
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=5d"
            r = requests.get(url, headers=headers, timeout=10)
            meta  = r.json()["chart"]["result"][0]["meta"]
            price = meta.get("regularMarketPrice", 0)
            prev  = meta.get("regularMarketPreviousClose") or meta.get("chartPreviousClose", price)
            pct   = (price - prev) / prev * 100 if prev else 0
            icon  = "🟢" if pct >= 0 else "🔴"
            msg  += f"{icon} {name}　{price:,.2f}　{pct:+.2f}%\n"
        except:
            msg += f"⚪ {name}　--\n"
    msg += "━━━━━━━━━━━━━━\n⚠️ 僅供參考，非投資建議"
    return msg


# ══════════════════════════════════════════
#  Flex Message 股票卡片
# ══════════════════════════════════════════
def make_ma_row(label, value):
    val_str = f"{value:.0f}" if value else "N/A"
    color = "#7A3828" if value else "#C4907A"
    return {
        "type": "box", "layout": "horizontal",
        "contents": [
            {"type": "text", "text": label, "size": "xs", "color": "#9B6B5A", "flex": 4},
            {"type": "text", "text": val_str, "size": "xs", "color": color, "flex": 2, "weight": "bold", "align": "end"},
        ]
    }

def make_stock_flex(symbol, name, market_type, status, source, price, chg, pct, open_p, high, low, vol, kline, news_list, query_time):
    is_up     = chg >= 0
    color     = "#C47055" if is_up else "#5B8DB8"
    arrow     = "▲" if is_up else "▼"
    sign      = "+" if is_up else ""
    spark     = kline.get("spark", "▄▄▄▄▄▄▄▄▄▄")
    trend     = kline.get("trend", "--")
    ma5       = kline.get("ma5")
    ma20      = kline.get("ma20")
    ma60      = kline.get("ma60")
    ma120     = kline.get("ma120")
    ma240     = kline.get("ma240")
    rsi       = kline.get("rsi", 0)
    rsi_label = kline.get("rsi_label", "--")
    rsi_color = "#C47055" if rsi > 70 else ("#5B8DB8" if rsi < 30 else "#8B6B5A")
    display_name = f"{symbol} {name}" if name and name != symbol else symbol

    news_contents = []
    for title, url in news_list[:4]:
        if url:
            news_contents.append({
                "type": "button", "style": "link", "height": "sm",
                "action": {"type": "uri", "label": f"📰 {title}", "uri": url},
            })
        else:
            news_contents.append({
                "type": "text", "text": f"📰 {title}",
                "size": "xs", "color": "#B06050", "wrap": True
            })
    if not news_contents:
        news_contents = [{"type": "text", "text": "暫無相關新聞", "size": "xs", "color": "#C4907A"}]

    return {
        "type": "bubble", "size": "mega",
        "header": {
            "type": "box", "layout": "vertical",
            "backgroundColor": "#C47055", "paddingAll": "16px",
            "contents": [
                {
                    "type": "box", "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": "✨ 慧股拾光 Lumistock", "size": "xxs", "color": "#F0D0C0", "flex": 1},
                        {"type": "text", "text": market_type, "size": "xxs", "color": "#F0D0C0", "align": "end"}
                    ]
                },
                {"type": "text", "text": display_name, "size": "xl", "color": "#FFFFFF", "weight": "bold", "wrap": True}
            ]
        },
        "body": {
            "type": "box", "layout": "vertical",
            "backgroundColor": "#FDF6F0", "paddingAll": "14px", "spacing": "sm",
            "contents": [
                {
                    "type": "box", "layout": "vertical",
                    "contents": [
                        {"type": "text", "text": f"{price:.2f}", "size": "3xl", "weight": "bold", "color": color},
                        {"type": "text", "text": f"{arrow} {abs(chg):.2f}　{sign}{pct:.2f}%", "size": "sm", "color": color}
                    ]
                },
                {"type": "separator", "color": "#E8C4B4"},
                {
                    "type": "box", "layout": "horizontal",
                    "contents": [
                        {"type": "box", "layout": "vertical", "flex": 1, "contents": [
                            {"type": "text", "text": "開盤", "size": "xxs", "color": "#9B6B5A"},
                            {"type": "text", "text": str(open_p), "size": "sm", "color": "#5B4040", "weight": "bold"}
                        ]},
                        {"type": "box", "layout": "vertical", "flex": 1, "contents": [
                            {"type": "text", "text": "最高", "size": "xxs", "color": "#9B6B5A"},
                            {"type": "text", "text": str(high), "size": "sm", "color": "#C47055", "weight": "bold"}
                        ]},
                        {"type": "box", "layout": "vertical", "flex": 1, "contents": [
                            {"type": "text", "text": "最低", "size": "xxs", "color": "#9B6B5A"},
                            {"type": "text", "text": str(low), "size": "sm", "color": "#5B8DB8", "weight": "bold"}
                        ]},
                        {"type": "box", "layout": "vertical", "flex": 1, "contents": [
                            {"type": "text", "text": "成交量", "size": "xxs", "color": "#9B6B5A"},
                            {"type": "text", "text": str(vol), "size": "sm", "color": "#5B4040", "weight": "bold"}
                        ]}
                    ]
                },
                {"type": "separator", "color": "#E8C4B4"},
                {"type": "text", "text": "📊 技術分析", "size": "sm", "weight": "bold", "color": "#7A3828"},
                {"type": "text", "text": spark, "size": "xl", "color": color},
                {"type": "text", "text": f"趨勢　{trend}", "size": "sm", "color": "#7A3828"},
                {
                    "type": "box", "layout": "vertical", "spacing": "xs",
                    "contents": [
                        make_ma_row("MA5　　短線", ma5),
                        make_ma_row("MA20　　月線", ma20),
                        make_ma_row("MA60　　季線", ma60),
                        make_ma_row("MA120　半年線", ma120),
                        make_ma_row("MA240　年　線", ma240),
                    ]
                },
                {
                    "type": "box", "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": "RSI", "size": "xs", "color": "#9B6B5A", "flex": 1},
                        {"type": "text", "text": f"{rsi:.0f}", "size": "xs", "color": rsi_color, "weight": "bold", "flex": 1},
                        {"type": "text", "text": rsi_label, "size": "xs", "color": rsi_color, "flex": 3}
                    ]
                },
                {"type": "separator", "color": "#E8C4B4"},
                {"type": "text", "text": "📰 相關新聞", "size": "sm", "weight": "bold", "color": "#7A3828"},
            ] + news_contents + [
                {"type": "separator", "color": "#E8C4B4"},
                {
                    "type": "box", "layout": "horizontal",
                    "contents": [
                        {"type": "text", "text": f"🕐 {query_time}　{status}", "size": "xxs", "color": "#C4907A", "flex": 1},
                        {"type": "text", "text": source, "size": "xxs", "color": "#D4B0A0", "align": "end", "flex": 1}
                    ]
                }
            ]
        }
    }


# ══════════════════════════════════════════
#  股票查詢
# ══════════════════════════════════════════
def get_stock_flex(symbol: str, user_id: str = ""):
    symbol = symbol.strip().upper()
    is_tw  = symbol.isdigit()
    query_time = now_taipei().strftime("%m/%d %H:%M")

    if is_tw:
        tw = get_tw_stock(symbol)
        if not tw:
            return None, f"查無此股票：{symbol}\n請確認代碼是否正確"
        # 最終名稱保護
        if not has_chinese(tw.get("name", "")):
            tw["name"] = get_tw_stock_name(symbol)
        closes = get_tw_closes(symbol)
        kline  = get_kline_analysis(closes)
        news   = get_news(f"{symbol} {tw['name']} 股票", count=4)
        update_tw_data_to_sheets(symbol, tw)
        log_to_sheets(user_id, "查詢台股", symbol, "成功")
        return make_stock_flex(
            symbol, tw["name"],
            tw.get("market_type","台股"), tw.get("status",""), tw.get("source",""),
            tw["price"], tw["chg"], tw["pct"],
            tw.get("open","N/A"), tw["high"], tw["low"], tw["vol"],
            kline, news, query_time
        ), None
    else:
        us = get_us_stock(symbol)
        if not us:
            return None, f"查無此股票：{symbol}\n請確認代碼是否正確"
        closes = get_us_closes(symbol)
        kline  = get_kline_analysis(closes)
        news   = get_news(f"{symbol} {us['name']} stock", count=4)
        update_us_data_to_sheets(symbol, us)
        log_to_sheets(user_id, "查詢美股", symbol, "成功")
        return make_stock_flex(
            symbol, us["name"], "美股", "", "Yahoo Finance",
            us["price"], us["chg"], us["pct"],
            us.get("open","N/A"), us.get("high","N/A"),
            us.get("low","N/A"), us.get("vol","N/A"),
            kline, news, query_time
        ), None


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
    response.headers["ngrok-skip-browser-warning"] as "true"
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
    text    = event.message.text.strip()
    user_id = event.source.user_id

    if is_blocked_user(user_id):
        reply_text(event.reply_token, "⛔ 此帳號已停止使用權限\n如有疑問請聯繫管理員")
        return

    update_user_activity(user_id, text)

    if user_id == OWNER_USER_ID:
        if text.startswith("封鎖 "):
            parts  = text.split(" ", 2)
            name   = parts[1] if len(parts) > 1 else ""
            reason = parts[2] if len(parts) > 2 else "未說明"
            if name:
                reply_text(event.reply_token, block_user_by_name(name, reason))
                return
        elif text.startswith("解除封鎖 "):
            name = text.replace("解除封鎖 ", "").strip()
            if name:
                reply_text(event.reply_token, unblock_user_by_name(name))
                return
        elif text == "使用者列表":
            reply_text(event.reply_token, get_user_list())
            return
        elif text.startswith("查使用者 "):
            name = text.replace("查使用者 ", "").strip()
            if name:
                reply_text(event.reply_token, get_user_detail(name))
                return

    if text.startswith("註冊 "):
        reg_name = text.replace("註冊 ", "").strip()
        if reg_name:
            result = register_user(user_id, reg_name)
            reply_text(event.reply_token, result)
            if "成功" in result:
                push_to_owner(f"🆕 新用戶註冊！\n姓名：{reg_name}\n時間：{now_taipei().strftime('%Y-%m-%d %H:%M')}")
        else:
            reply_text(event.reply_token, "格式：註冊 姓名\n例如：註冊 王小明")
        return

    if not is_registered(user_id):
        reply_text(event.reply_token,
              "👋 歡迎使用慧股拾光 Lumistock！\n"
              "━━━━━━━━━━━━━━\n"
              "請先完成註冊才能使用全部功能\n\n"
              "📝 註冊方式：\n"
              "　輸入「註冊 您的姓名」\n\n"
              "　例如：\n"
              "　註冊 王小明")
        return

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
    print("慧股拾光 Lumistock LINE Bot v10.9.8 啟動中...")
    init_name_cache()
    setup_rich_menu()
    port = int(os.environ.get("PORT", 5001))
    app.run(host="0.0.0.0", port=port, debug=False)
