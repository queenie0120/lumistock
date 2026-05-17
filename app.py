"""
慧股拾光 Lumistock – by Hui
LINE Bot 模組 v11.0（5 張圖 Rich Menu Alias 多頁切換）
"""

from flask import Flask, request, abort
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi,
    ReplyMessageRequest, TextMessage, PushMessageRequest,
    FlexMessage, FlexContainer, QuickReply, QuickReplyItem,
    MessageAction
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent
import requests
import json, os, re, threading, time
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
NAME_CACHE_LOADING = False
NAME_CACHE_LOADED  = False
TZ_TAIPEI          = timezone(timedelta(hours=8))

def now_taipei():
    return datetime.now(TZ_TAIPEI)

def has_chinese(text: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", str(text)))

def is_after_close() -> bool:
    now = now_taipei()
    if now.weekday() >= 5:
        return True
    return now.hour * 60 + now.minute >= 930

def format_us_volume(v) -> str:
    if v in ["-", "", None, "N/A", 0]: return "N/A"
    try:
        n = int(float(str(v).replace(",", "")))
        if n >= 100_000_000: return f"{n/100_000_000:.2f} 億"
        elif n >= 10_000: return f"{n/10_000:.2f} 萬"
        else: return f"{n:,}"
    except: return str(v)


# ══════════════════════════════════════════
#  保底名稱
# ══════════════════════════════════════════
FALLBACK_NAMES = {
    "2330":"台積電",   "2317":"鴻海",     "2454":"聯發科",
    "2308":"台達電",   "2382":"廣達",     "3711":"日月光投控",
    "2412":"中華電",   "6505":"台塑化",   "2303":"聯電",
    "2881":"富邦金",   "2882":"國泰金",   "2891":"中信金",
    "2886":"兆豐金",   "2884":"玉山金",   "2885":"元大金",
    "2892":"第一金",   "2890":"永豐金",   "5880":"合庫金",
    "2823":"中壽",     "2801":"彰銀",     "5876":"上海商銀",
    "3533":"嘉澤端子", "6147":"頎邦",     "2379":"瑞昱",
    "2395":"研華",     "3008":"大立光",   "2357":"華碩",
    "2376":"技嘉",     "2353":"宏碁",     "2367":"耀華",
    "2337":"旺宏",     "2408":"南亞科",   "3034":"聯詠",
    "3231":"緯創",     "2356":"英業達",   "2324":"仁寶",
    "2327":"國巨",     "2347":"聯強",     "2383":"台光電",
    "6415":"矽力-KY",  "4938":"和碩",     "2344":"華邦電",
    "2360":"致茂",     "2448":"晶電",     "3481":"群創",
    "2474":"可成",     "2049":"上銀",     "1301":"台塑",
    "1303":"南亞",     "1326":"台化",     "6669":"緯穎",
    "2409":"友達",     "2492":"華新科",   "3045":"台灣大",
    "4904":"遠傳",     "2498":"宏達電",   "2207":"和泰車",
    "2105":"正新",     "1216":"統一",     "2912":"統一超",
    "2313":"華通",     "2301":"光寶科",   "2352":"佳世達",
    "2371":"大同",     "2385":"群光",     "3388":"崇越電",
    "2002":"中鋼",     "1102":"亞泥",     "1101":"台泥",
    "2883":"開發金",   "2887":"台新金",   "2809":"京城銀",
    "6285":"啟碁",     "6271":"同欣電",   "6239":"力成",
    "6176":"瑞儀",     "6230":"超眾",     "6414":"樺漢",
    "6446":"藥華藥",   "6331":"玉晶光",   "6438":"迅得",
    "0050":"元大台灣50",        "0056":"元大高股息",
    "00878":"國泰永續高股息",   "006208":"富邦台50",
    "00919":"群益台灣精選高息", "00713":"元大台灣高息低波",
    "00929":"復華台灣科技優息", "00934":"中信成長高股息",
    "00940":"元大台灣價值高息",
}


# ══════════════════════════════════════════
#  啟動初始化
# ══════════════════════════════════════════
@app.before_request
def startup():
    global STARTUP_DONE
    if not STARTUP_DONE:
        STARTUP_DONE = True
        for code, name in FALLBACK_NAMES.items():
            NAME_CACHE[code] = name
        print(f"✅ 保底名稱立即載入：{len(FALLBACK_NAMES)} 筆")
        t = threading.Thread(target=_bg_init)
        t.daemon = True
        t.start()
        t2 = threading.Thread(target=setup_rich_menus)
        t2.daemon = True
        t2.start()

def _bg_init():
    time.sleep(15)
    init_name_cache()


# ══════════════════════════════════════════
#  名稱快取
# ══════════════════════════════════════════
def _load_opendata(url: str, label: str) -> int:
    headers = {"User-Agent": "Mozilla/5.0"}
    for attempt in range(3):
        try:
            r = requests.get(url, headers=headers, timeout=30)
            if r.status_code == 200 and r.text.strip().startswith("["):
                count = 0
                for item in r.json():
                    code = str(item.get("公司代號","")).strip()
                    name = (str(item.get("公司簡稱","")) or str(item.get("公司名稱",""))).strip()
                    if code and name and has_chinese(name):
                        NAME_CACHE[code] = name
                        count += 1
                if count > 0:
                    print(f"✅ {label}:{count} 筆")
                    return count
        except Exception as e:
            print(f"{label} 第{attempt+1}次失敗：{e}")
            time.sleep(2)
    return 0

def _load_twse_stock_day_all() -> int:
    headers = {"User-Agent": "Mozilla/5.0"}
    for attempt in range(3):
        try:
            r = requests.get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL",
                           headers=headers, timeout=30)
            if r.status_code == 200 and r.text.strip().startswith("["):
                count = 0
                for item in r.json():
                    code = str(item.get("Code","")).strip()
                    name = str(item.get("Name","")).strip()
                    if code and name and has_chinese(name):
                        NAME_CACHE[code] = name
                        count += 1
                if count > 100:
                    print(f"✅ TWSE STOCK_DAY_ALL：{count} 筆")
                    return count
        except Exception as e:
            print(f"STOCK_DAY_ALL 第{attempt+1}次失敗：{e}")
            time.sleep(2)
    return 0

def _load_tpex_quotes() -> int:
    headers = {"User-Agent": "Mozilla/5.0"}
    for attempt in range(3):
        try:
            r = requests.get("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes",
                           headers=headers, timeout=30)
            count = 0
            for item in r.json():
                code = str(item.get("SecuritiesCompanyCode","")).strip()
                name = str(item.get("CompanyName","")).strip()
                if code and name and has_chinese(name):
                    NAME_CACHE[code] = name
                    count += 1
            if count > 0:
                print(f"✅ TPEx mainboard_quotes：{count} 筆")
                return count
        except Exception as e:
            print(f"TPEx quotes 第{attempt+1}次失敗：{e}")
            time.sleep(2)
    return 0

def init_name_cache():
    global NAME_CACHE_LOADING, NAME_CACHE_LOADED
    if NAME_CACHE_LOADING: return
    NAME_CACHE_LOADING = True

    tw_count  = _load_twse_stock_day_all()
    _load_opendata("https://openapi.twse.com.tw/v1/opendata/t187ap03_L","上市")
    otc_count = _load_opendata("https://openapi.twse.com.tw/v1/opendata/t187ap03_O","上櫃")
    _load_opendata("https://openapi.twse.com.tw/v1/opendata/t187ap03_R","興櫃")

    if otc_count == 0: _load_tpex_quotes()
    if tw_count == 0:
        headers = {"User-Agent": "Mozilla/5.0"}
        for attempt in range(3):
            try:
                r = requests.get("https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL?response=json",
                               headers=headers, timeout=30)
                count = 0
                for item in r.json().get("data",[]):
                    if len(item) >= 2:
                        code = str(item[0]).strip()
                        name = str(item[1]).strip()
                        if code and name and has_chinese(name):
                            NAME_CACHE[code] = name
                            count += 1
                if count > 100:
                    print(f"✅ TWSE rwd備援：{count} 筆")
                    break
            except Exception as e:
                print(f"TWSE rwd第{attempt+1}次失敗：{e}")
                time.sleep(2)

    for code, name in FALLBACK_NAMES.items():
        if not has_chinese(NAME_CACHE.get(code,"")): NAME_CACHE[code] = name

    NAME_CACHE_LOADING = False
    NAME_CACHE_LOADED  = True
    print(f"✅ 名稱快取完整載入：{len(NAME_CACHE)} 筆")
    try:
        push_to_owner(f"✅ Lumistock 啟動完成\n名稱快取：{len(NAME_CACHE)} 筆\n{now_taipei().strftime('%m/%d %H:%M')}")
    except: pass


# ══════════════════════════════════════════
#  Rich Menu（5 張圖 + Alias 多頁切換）v11.0
# ══════════════════════════════════════════
ALIAS_USER        = "lumistock-user"
ALIAS_OWNER_MAIN  = "lumistock-owner-main"
ALIAS_OWNER_ADMIN = "lumistock-owner-admin"
ALIAS_ADMIN_MAIN  = "lumistock-admin-main"
ALIAS_ADMIN_MGMT  = "lumistock-admin-mgmt"


def _delete_all_rich_menus():
    try:
        r = requests.get("https://api.line.me/v2/bot/richmenu/list",
                        headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"})
        for menu in r.json().get("richmenus", []):
            requests.delete(f"https://api.line.me/v2/bot/richmenu/{menu['richMenuId']}",
                          headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"})
    except: pass


def _delete_all_aliases():
    try:
        r = requests.get("https://api.line.me/v2/bot/richmenu/alias/list",
                        headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"})
        for alias in r.json().get("aliases", []):
            requests.delete(f"https://api.line.me/v2/bot/richmenu/alias/{alias['richMenuAliasId']}",
                          headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"})
    except: pass


def _create_rich_menu(body: dict, img_url: str) -> str:
    headers_json = {"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}",
                    "Content-Type": "application/json"}
    r = requests.post("https://api.line.me/v2/bot/richmenu",
                     headers=headers_json, json=body)
    rid = r.json().get("richMenuId", "")
    if not rid:
        print(f"❌ 建立 Rich Menu 失敗：{r.text}")
        return ""
    try:
        img_r = requests.get(img_url, timeout=15)
        img_resp = requests.post(f"https://api-data.line.me/v2/bot/richmenu/{rid}/content",
                     headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}",
                              "Content-Type": "image/png"},
                     data=img_r.content)
        if img_resp.status_code != 200:
            print(f"❌ 上傳圖片失敗 {rid}：{img_resp.text}")
    except Exception as e:
        print(f"❌ 上傳圖片例外 {rid}：{e}")
    return rid


def _create_alias(alias_id: str, rich_menu_id: str):
    headers_json = {"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}",
                    "Content-Type": "application/json"}
    requests.delete(f"https://api.line.me/v2/bot/richmenu/alias/{alias_id}",
                   headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"})
    r = requests.post("https://api.line.me/v2/bot/richmenu/alias",
                     headers=headers_json,
                     json={"richMenuAliasId": alias_id, "richMenuId": rich_menu_id})
    if r.status_code == 200:
        print(f"✅ Alias 建立成功：{alias_id}")
    else:
        print(f"❌ Alias 建立失敗 {alias_id}：{r.text}")


# 一般用戶（6 格 2x3）
AREAS_USER = [
    {"bounds":{"x":0,   "y":0,   "width":833,"height":843},"action":{"type":"message","text":"查股票"}},
    {"bounds":{"x":833, "y":0,   "width":834,"height":843},"action":{"type":"message","text":"全球大盤"}},
    {"bounds":{"x":1667,"y":0,   "width":833,"height":843},"action":{"type":"message","text":"外匯資金"}},
    {"bounds":{"x":0,   "y":843, "width":833,"height":843},"action":{"type":"message","text":"AI分析"}},
    {"bounds":{"x":833, "y":843, "width":834,"height":843},"action":{"type":"message","text":"財經新聞"}},
    {"bounds":{"x":1667,"y":843, "width":833,"height":843},"action":{"type":"message","text":"持股管理"}},
]

# Owner 主頁（6 格，右下切到管理頁）
AREAS_OWNER_MAIN = [
    {"bounds":{"x":0,   "y":0,   "width":833,"height":843},"action":{"type":"message","text":"查股票"}},
    {"bounds":{"x":833, "y":0,   "width":834,"height":843},"action":{"type":"message","text":"全球大盤"}},
    {"bounds":{"x":1667,"y":0,   "width":833,"height":843},"action":{"type":"message","text":"外匯資金"}},
    {"bounds":{"x":0,   "y":843, "width":833,"height":843},"action":{"type":"message","text":"AI分析"}},
    {"bounds":{"x":833, "y":843, "width":834,"height":843},"action":{"type":"message","text":"財經新聞"}},
    {"bounds":{"x":1667,"y":843, "width":833,"height":843},
     "action":{"type":"richmenuswitch","richMenuAliasId":ALIAS_OWNER_ADMIN,"data":"to_owner_admin"}},
]

# Owner 管理頁（6 格，右下返回主頁）
AREAS_OWNER_ADMIN = [
    {"bounds":{"x":0,   "y":0,   "width":833,"height":843},"action":{"type":"message","text":"使用者管理"}},
    {"bounds":{"x":833, "y":0,   "width":834,"height":843},"action":{"type":"message","text":"系統管理"}},
    {"bounds":{"x":1667,"y":0,   "width":833,"height":843},"action":{"type":"message","text":"推播管理"}},
    {"bounds":{"x":0,   "y":843, "width":833,"height":843},"action":{"type":"message","text":"AI管理"}},
    {"bounds":{"x":833, "y":843, "width":834,"height":843},"action":{"type":"message","text":"持股管理"}},
    {"bounds":{"x":1667,"y":843, "width":833,"height":843},
     "action":{"type":"richmenuswitch","richMenuAliasId":ALIAS_OWNER_MAIN,"data":"to_owner_main"}},
]

# 管理者主頁（6 格，右下切到管理頁）
AREAS_ADMIN_MAIN = [
    {"bounds":{"x":0,   "y":0,   "width":833,"height":843},"action":{"type":"message","text":"查股票"}},
    {"bounds":{"x":833, "y":0,   "width":834,"height":843},"action":{"type":"message","text":"全球大盤"}},
    {"bounds":{"x":1667,"y":0,   "width":833,"height":843},"action":{"type":"message","text":"外匯資金"}},
    {"bounds":{"x":0,   "y":843, "width":833,"height":843},"action":{"type":"message","text":"AI分析"}},
    {"bounds":{"x":833, "y":843, "width":834,"height":843},"action":{"type":"message","text":"財經新聞"}},
    {"bounds":{"x":1667,"y":843, "width":833,"height":843},
     "action":{"type":"richmenuswitch","richMenuAliasId":ALIAS_ADMIN_MGMT,"data":"to_admin_mgmt"}},
]

# 管理者管理頁（3 格 1x3，右側返回主頁）
AREAS_ADMIN_MGMT = [
    {"bounds":{"x":0,    "y":0, "width":833, "height":1686},"action":{"type":"message","text":"使用者管理"}},
    {"bounds":{"x":833,  "y":0, "width":834, "height":1686},"action":{"type":"message","text":"持股管理"}},
    {"bounds":{"x":1667, "y":0, "width":833, "height":1686},
     "action":{"type":"richmenuswitch","richMenuAliasId":ALIAS_ADMIN_MAIN,"data":"to_admin_main"}},
]


RICH_MENU_IDS = {}


def setup_rich_menus():
    global RICH_MENU_IDS
    print("🌸 開始建立 Rich Menu...")
    _delete_all_aliases()
    _delete_all_rich_menus()
    base_url = "https://raw.githubusercontent.com/queenie0120/lumistock/main/static/richmenu"

    # 1. 一般用戶（玫瑰金）
    user_body = {
        "size":{"width":2500,"height":1686},"selected":True,
        "name":"一般用戶選單","chatBarText":"✨ 慧股拾光 功能選單",
        "areas": AREAS_USER
    }
    uid = _create_rich_menu(user_body, f"{base_url}/richmenu_user.png")
    if uid:
        RICH_MENU_IDS["user"] = uid
        _create_alias(ALIAS_USER, uid)
        requests.post(f"https://api.line.me/v2/bot/user/all/richmenu/{uid}",
                     headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"})

    # 2. Owner 主頁（粉白少女）
    owner_main_body = {
        "size":{"width":2500,"height":1686},"selected":True,
        "name":"Owner主頁","chatBarText":"👑 慧股拾光 Owner",
        "areas": AREAS_OWNER_MAIN
    }
    omid = _create_rich_menu(owner_main_body, f"{base_url}/richmenu_owner_main.png")
    if omid:
        RICH_MENU_IDS["owner_main"] = omid
        _create_alias(ALIAS_OWNER_MAIN, omid)

    # 3. Owner 管理頁
    owner_admin_body = {
        "size":{"width":2500,"height":1686},"selected":True,
        "name":"Owner管理頁","chatBarText":"👑 Owner 管理後台",
        "areas": AREAS_OWNER_ADMIN
    }
    oaid = _create_rich_menu(owner_admin_body, f"{base_url}/richmenu_owner_admin.png")
    if oaid:
        RICH_MENU_IDS["owner_admin"] = oaid
        _create_alias(ALIAS_OWNER_ADMIN, oaid)

    # 4. 管理者主頁（粉紫）
    admin_main_body = {
        "size":{"width":2500,"height":1686},"selected":True,
        "name":"管理者主頁","chatBarText":"🛡️ 慧股拾光 管理者",
        "areas": AREAS_ADMIN_MAIN
    }
    amid = _create_rich_menu(admin_main_body, f"{base_url}/richmenu_admin_main.png")
    if amid:
        RICH_MENU_IDS["admin_main"] = amid
        _create_alias(ALIAS_ADMIN_MAIN, amid)

    # 5. 管理者管理頁
    admin_mgmt_body = {
        "size":{"width":2500,"height":1686},"selected":True,
        "name":"管理者管理頁","chatBarText":"🛡️ 管理者後台",
        "areas": AREAS_ADMIN_MGMT
    }
    amgid = _create_rich_menu(admin_mgmt_body, f"{base_url}/richmenu_admin_mgmt.png")
    if amgid:
        RICH_MENU_IDS["admin_mgmt"] = amgid
        _create_alias(ALIAS_ADMIN_MGMT, amgid)

    # 綁定 Owner 個人選單
    if omid:
        requests.post(f"https://api.line.me/v2/bot/user/{OWNER_USER_ID}/richmenu/{omid}",
                     headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"})

    print(f"✅ Rich Menu 設定完成")
    print(f"   user        = {uid}")
    print(f"   owner_main  = {omid}")
    print(f"   owner_admin = {oaid}")
    print(f"   admin_main  = {amid}")
    print(f"   admin_mgmt  = {amgid}")


def assign_rich_menu(user_id: str):
    """依角色綁定對應的主頁 Rich Menu"""
    if user_id == OWNER_USER_ID:
        rid = RICH_MENU_IDS.get("owner_main", "")
    elif is_admin(user_id):
        rid = RICH_MENU_IDS.get("admin_main", "")
    else:
        rid = RICH_MENU_IDS.get("user", "")
    if rid:
        r = requests.post(f"https://api.line.me/v2/bot/user/{user_id}/richmenu/{rid}",
                     headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"})
        if r.status_code == 200:
            print(f"✅ 已綁定 Rich Menu：{user_id[:10]}...")
        else:
            print(f"❌ 綁定失敗：{r.text}")


# ══════════════════════════════════════════
#  Quick Reply 工具
# ══════════════════════════════════════════
def make_quick_reply(items: list) -> QuickReply:
    return QuickReply(items=[
        QuickReplyItem(action=MessageAction(label=label, text=text))
        for label, text in items
    ])


# ══════════════════════════════════════════
#  Google Sheets
# ══════════════════════════════════════════
def get_sheets_client():
    try:
        creds_dict = json.loads(os.environ.get("GOOGLE_SHEETS_CREDENTIALS"))
        scopes = ["https://www.googleapis.com/auth/spreadsheets",
                  "https://www.googleapis.com/auth/drive"]
        creds  = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        return gspread.authorize(creds)
    except: return None

def get_sheet(sheet_name):
    try:
        client = get_sheets_client()
        if client: return client.open_by_key(SHEETS_ID).worksheet(sheet_name)
    except: pass
    return None

def log_to_sheets(user_id, action, content, result):
    try:
        sheet = get_sheet("系統記錄")
        if sheet:
            sheet.append_row([now_taipei().strftime("%Y-%m-%d %H:%M"),
                             user_id, action, content, result, "", ""])
    except: pass

def save_suggestion_to_sheets(user_id, text):
    try:
        sheet = get_sheet("系統記錄")
        if sheet:
            sheet.append_row([now_taipei().strftime("%Y-%m-%d %H:%M"),
                             user_id, "建議", "", "", text, ""])
    except: pass

def save_portfolio_to_sheets(user_id, symbol, name, market, shares, buy_price):
    try:
        sheet = get_sheet("自選股")
        if sheet:
            now = now_taipei().strftime("%Y-%m-%d %H:%M")
            sheet.append_row([user_id, symbol, name, market, shares, buy_price, "", "", "", now, now])
    except: pass

def delete_portfolio_from_sheets(user_id, symbol):
    try:
        sheet = get_sheet("自選股")
        if sheet:
            records = sheet.get_all_records()
            for i, row in enumerate(records, start=2):
                if str(row.get("用戶ID"))==user_id and str(row.get("股票代號"))==symbol:
                    sheet.delete_rows(i); break
    except: pass

def update_tw_data_to_sheets(stock_id, data):
    try:
        sheet = get_sheet("台股資料")
        if sheet and data:
            sheet.append_row([now_taipei().strftime("%Y-%m-%d"), stock_id,
                             data.get("name",""), "", data.get("high",""), data.get("low",""),
                             data.get("price",""), data.get("vol",""),
                             f"{data.get('pct',0):+.2f}%", "", "", "", "", ""])
    except: pass

def update_us_data_to_sheets(symbol, data):
    try:
        sheet = get_sheet("美股資料")
        if sheet and data:
            sheet.append_row([now_taipei().strftime("%Y-%m-%d"), symbol,
                             data.get("name",""), data.get("price",""),
                             f"{data.get('pct',0):+.2f}%", "", "", "", "", "", "", "", ""])
    except: pass


# ══════════════════════════════════════════
#  權限系統
# ══════════════════════════════════════════
def is_owner(user_id: str) -> bool:
    return user_id == OWNER_USER_ID

def is_admin(user_id: str) -> bool:
    if is_owner(user_id): return True
    try:
        sheet = get_sheet("管理者名單")
        if sheet:
            for row in sheet.get_all_records():
                if str(row.get("user_id"))==user_id and str(row.get("狀態"))=="正常":
                    return True
    except: pass
    return False

def add_admin(user_id: str, name: str) -> str:
    try:
        sheet = get_sheet("管理者名單")
        if not sheet: return "❌ 無法讀取管理者名單"
        for row in sheet.get_all_records():
            if str(row.get("user_id"))==user_id: return f"⚠️ {name} 已經是管理者了"
        sheet.append_row([user_id, name, now_taipei().strftime("%Y-%m-%d %H:%M"), "Owner", "正常"])
        assign_rich_menu(user_id)
        return f"✅ 已新增管理者：{name}"
    except Exception as e: return f"❌ 新增失敗：{e}"

def remove_admin(name: str) -> str:
    try:
        sheet = get_sheet("管理者名單")
        if not sheet: return "❌ 無法讀取管理者名單"
        for i, row in enumerate(sheet.get_all_records(), start=2):
            if str(row.get("姓名"))==name:
                uid = str(row.get("user_id",""))
                sheet.update_cell(i, 5, "停用")
                if uid: assign_rich_menu(uid)
                return f"✅ 已移除管理者：{name}"
        return f"❌ 找不到管理者：{name}"
    except Exception as e: return f"❌ 移除失敗：{e}"

def get_admin_list() -> str:
    try:
        sheet = get_sheet("管理者名單")
        if not sheet: return "❌ 無法讀取管理者名單"
        records = sheet.get_all_records()
        if not records: return "📋 目前沒有管理者"
        msg = f"🛡️ 管理者名單（共 {len(records)} 人）\n━━━━━━━━━━━━━━\n"
        for row in records:
            icon = "🟢" if row.get("狀態")=="正常" else "🔴"
            msg += f"{icon} {row.get('姓名','')}（{row.get('user_id','')}）\n　{row.get('新增時間','')}\n"
        return msg.strip()
    except Exception as e: return f"❌ 查詢失敗：{e}"


# ══════════════════════════════════════════
#  會員系統
# ══════════════════════════════════════════
def get_line_profile(user_id: str) -> dict:
    try:
        r = requests.get(f"https://api.line.me/v2/bot/profile/{user_id}",
                        headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"}, timeout=5)
        data = r.json()
        return {"displayName": data.get("displayName",""), "pictureUrl": data.get("pictureUrl","")}
    except: return {"displayName":"", "pictureUrl":""}

def get_user_record(user_id: str) -> dict:
    try:
        sheet = get_sheet("使用者名單")
        if sheet:
            for row in sheet.get_all_records():
                if str(row.get("user_id"))==user_id: return row
    except: pass
    return {}

def is_registered(user_id: str) -> bool:
    return bool(get_user_record(user_id).get("註冊姓名"))

def is_blocked_user(user_id: str) -> bool:
    try:
        sheet = get_sheet("黑名單")
        if sheet:
            for row in sheet.get_all_records():
                if str(row.get("user_id"))==user_id and str(row.get("狀態"))=="封鎖":
                    return True
    except: pass
    return False

def register_user(user_id: str, reg_name: str) -> str:
    try:
        sheet = get_sheet("使用者名單")
        if not sheet: return "❌ 系統錯誤，請稍後再試"
        records = sheet.get_all_records()
        now     = now_taipei().strftime("%Y-%m-%d %H:%M")
        profile = get_line_profile(user_id)
        dn = profile.get("displayName",""); pu = profile.get("pictureUrl","")
        for i, row in enumerate(records, start=2):
            if str(row.get("user_id"))==user_id:
                if row.get("註冊姓名"): return f"✅ 您已經註冊過了！\n姓名：{row.get('註冊姓名')}"
                sheet.update_cell(i,4,reg_name); sheet.update_cell(i,5,now); sheet.update_cell(i,7,"正常")
                assign_rich_menu(user_id)
                return f"✅ 註冊成功！歡迎 {reg_name} 使用慧股拾光 🌸"
            if str(row.get("註冊姓名"))==reg_name:
                return f"❌ 姓名「{reg_name}」已被使用\n請換一個名字"
        sheet.append_row([user_id, dn, pu, reg_name, now, "", "正常"])
        assign_rich_menu(user_id)
        return f"✅ 註冊成功！歡迎 {reg_name} 使用慧股拾光 🌸\n\n現在可以直接輸入股票代號查詢！"
    except Exception as e:
        print(f"註冊失敗：{e}"); return "❌ 註冊失敗，請稍後再試"

def update_user_activity(user_id: str, message: str):
    try:
        sheet = get_sheet("使用者名單")
        if not sheet: return
        records = sheet.get_all_records()
        now     = now_taipei().strftime("%Y-%m-%d %H:%M")
        for i, row in enumerate(records, start=2):
            if str(row.get("user_id"))==user_id:
                sheet.update_cell(i,5,now); sheet.update_cell(i,6,message[:50]); return
        profile = get_line_profile(user_id)
        sheet.append_row([user_id, profile.get("displayName",""), profile.get("pictureUrl",""),
                         "", now, message[:50], "未註冊"])
    except: pass

def block_user_by_name(reg_name: str, reason: str) -> str:
    try:
        sheet = get_sheet("使用者名單"); bl = get_sheet("黑名單")
        if not sheet or not bl: return "❌ 系統錯誤"
        for i, row in enumerate(sheet.get_all_records(), start=2):
            if str(row.get("註冊姓名"))==reg_name:
                now = now_taipei().strftime("%Y-%m-%d %H:%M")
                sheet.update_cell(i,7,"封鎖")
                bl.append_row([str(row.get("user_id")), reg_name, reason, now, "封鎖"])
                return f"✅ 已封鎖 {reg_name}\n原因：{reason}"
        return f"❌ 找不到用戶：{reg_name}"
    except Exception as e: return f"❌ 封鎖失敗：{e}"

def unblock_user_by_name(reg_name: str) -> str:
    try:
        sheet = get_sheet("使用者名單"); bl = get_sheet("黑名單")
        if not sheet or not bl: return "❌ 系統錯誤"
        found = False
        for i, row in enumerate(sheet.get_all_records(), start=2):
            if str(row.get("註冊姓名"))==reg_name:
                sheet.update_cell(i,7,"正常"); found = True; break
        for i, row in enumerate(bl.get_all_records(), start=2):
            if str(row.get("註冊姓名"))==reg_name and str(row.get("狀態"))=="封鎖":
                bl.update_cell(i,5,"解除"); break
        return f"✅ 已解除封鎖 {reg_name}" if found else f"❌ 找不到用戶：{reg_name}"
    except Exception as e: return f"❌ 解除失敗：{e}"

def get_user_list() -> str:
    try:
        sheet = get_sheet("使用者名單")
        if not sheet: return "❌ 無法讀取使用者名單"
        records = sheet.get_all_records()
        if not records: return "📋 目前沒有使用者記錄"
        msg = f"📋 使用者名單（共 {len(records)} 人）\n━━━━━━━━━━━━━━\n"
        for row in records:
            name=row.get("註冊姓名","未註冊"); nick=row.get("LINE暱稱","")
            status=row.get("狀態",""); last=row.get("最後互動時間","")
            icon="🔴" if status=="封鎖" else ("⚪" if status=="未註冊" else "🟢")
            msg+=f"{icon} {name}（{nick}）\n　{status}　{last}\n"
        return msg.strip()
    except Exception as e: return f"❌ 查詢失敗：{e}"

def get_user_detail(reg_name: str) -> str:
    try:
        sheet = get_sheet("使用者名單")
        if not sheet: return "❌ 系統錯誤"
        for row in sheet.get_all_records():
            if str(row.get("註冊姓名"))==reg_name:
                return (f"👤 {reg_name}\n━━━━━━━━━━━━━━\n"
                        f"LINE暱稱：{row.get('LINE暱稱','')}\n"
                        f"user_id：{row.get('user_id','')}\n"
                        f"狀態：{row.get('狀態','')}\n"
                        f"最後互動：{row.get('最後互動時間','')}\n"
                        f"最後訊息：{row.get('最後訊息','')}")
        return f"❌ 找不到用戶：{reg_name}"
    except Exception as e: return f"❌ 查詢失敗：{e}"


# ══════════════════════════════════════════
#  持股
# ══════════════════════════════════════════
def load_portfolio():
    if os.path.exists(PORTFOLIO_FILE):
        with open(PORTFOLIO_FILE,"r",encoding="utf-8") as f: return json.load(f)
    return {}

def save_portfolio(p):
    with open(PORTFOLIO_FILE,"w",encoding="utf-8") as f:
        json.dump(p, f, ensure_ascii=False, indent=2)


# ══════════════════════════════════════════
#  推播
# ══════════════════════════════════════════
def push_to_owner(text):
    try:
        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).push_message(
                PushMessageRequest(to=OWNER_USER_ID, messages=[TextMessage(text=text)]))
    except: pass

def push_message(user_id: str, text: str):
    try:
        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).push_message(
                PushMessageRequest(to=user_id, messages=[TextMessage(text=text)]))
    except: pass

def push_flex(user_id: str, flex_content: dict, alt_text: str = "推薦股"):
    try:
        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).push_message(
                PushMessageRequest(to=user_id,
                    messages=[FlexMessage(alt_text=alt_text,
                        contents=FlexContainer.from_dict(flex_content))]))
    except Exception as e: print(f"push_flex失敗：{e}")


# ══════════════════════════════════════════
#  Flex 選單卡片
# ══════════════════════════════════════════
def make_menu_flex(title: str, subtitle: str, color: str, buttons: list) -> dict:
    btn_contents = []
    for label, text in buttons:
        btn_contents.append({
            "type":"button","style":"primary","height":"sm","color": color,
            "action":{"type":"message","label":label,"text":text}
        })
    return {
        "type":"bubble","size":"mega",
        "header":{
            "type":"box","layout":"vertical","backgroundColor":color,"paddingAll":"14px",
            "contents":[
                {"type":"text","text":title,"size":"xl","color":"#FFFFFF","weight":"bold"},
                {"type":"text","text":subtitle,"size":"xs","color":"#FFFFFF"}
            ]
        },
        "body":{
            "type":"box","layout":"vertical","spacing":"sm","paddingAll":"12px",
            "contents": btn_contents
        }
    }

def make_stock_menu_flex() -> dict:
    return make_menu_flex(
        "🔍 查股票", "請選擇查詢類別", "#C47055",
        [("🇹🇼 台股","查台股"), ("🇺🇸 美股","查美股"),
         ("📊 ETF","查ETF"), ("🏪 興/上櫃","查興上櫃"),
         ("⭐ 自選股","查自選股")]
    )

def make_market_menu_flex() -> dict:
    return make_menu_flex(
        "🌐 全球大盤", "請選擇指數或商品", "#5B8DB8",
        [("🇹🇼 台股加權","查台股加權"), ("🏪 櫃買指數","查櫃買指數"),
         ("🇺🇸 道瓊","查道瓊"), ("📊 Nasdaq","查Nasdaq"),
         ("📈 S&P500","查SP500"), ("🔵 SOX半導體","查SOX"),
         ("😱 VIX恐慌","查VIX"), ("🥇 黃金","查黃金"),
         ("🛢️ 原油","查原油"), ("📉 美債殖利率","查美債"),
         ("⚡ 天然氣","查天然氣"), ("📦 期貨","查期貨")]
    )

def make_forex_menu_flex() -> dict:
    return {
        "type":"bubble","size":"mega",
        "header":{
            "type":"box","layout":"vertical","backgroundColor":"#8B6B9B","paddingAll":"14px",
            "contents":[
                {"type":"text","text":"💹 全球外匯與資金市場","size":"lg","color":"#FFFFFF","weight":"bold"},
                {"type":"text","text":"匯率・市場分析・資金流向","size":"xs","color":"#FFFFFF"}
            ]
        },
        "body":{
            "type":"box","layout":"vertical","spacing":"sm","paddingAll":"12px",
            "contents":[
                {"type":"text","text":"主要匯率","size":"sm","weight":"bold","color":"#8B6B9B"},
                {"type":"box","layout":"horizontal","spacing":"sm","contents":[
                    {"type":"button","style":"primary","height":"sm","color":"#8B6B9B",
                     "action":{"type":"message","label":"USD/TWD","text":"查USDTWD"}},
                    {"type":"button","style":"primary","height":"sm","color":"#8B6B9B",
                     "action":{"type":"message","label":"DXY","text":"查DXY"}},
                ]},
                {"type":"box","layout":"horizontal","spacing":"sm","contents":[
                    {"type":"button","style":"primary","height":"sm","color":"#8B6B9B",
                     "action":{"type":"message","label":"USD/JPY","text":"查USDJPY"}},
                    {"type":"button","style":"primary","height":"sm","color":"#8B6B9B",
                     "action":{"type":"message","label":"EUR/USD","text":"查EURUSD"}},
                ]},
                {"type":"box","layout":"horizontal","spacing":"sm","contents":[
                    {"type":"button","style":"primary","height":"sm","color":"#8B6B9B",
                     "action":{"type":"message","label":"GBP/USD","text":"查GBPUSD"}},
                    {"type":"button","style":"primary","height":"sm","color":"#8B6B9B",
                     "action":{"type":"message","label":"USD/CNY","text":"查USDCNY"}},
                ]},
                {"type":"box","layout":"horizontal","spacing":"sm","contents":[
                    {"type":"button","style":"primary","height":"sm","color":"#8B6B9B",
                     "action":{"type":"message","label":"AUD/USD","text":"查AUDUSD"}},
                    {"type":"button","style":"primary","height":"sm","color":"#8B6B9B",
                     "action":{"type":"message","label":"USD/CHF","text":"查USDCHF"}},
                ]},
                {"type":"separator","color":"#E8D4F0"},
                {"type":"text","text":"市場分析","size":"sm","weight":"bold","color":"#8B6B9B"},
                {"type":"box","layout":"horizontal","spacing":"sm","contents":[
                    {"type":"button","style":"primary","height":"sm","color":"#9B7BAB",
                     "action":{"type":"message","label":"外匯市場分析","text":"外匯市場分析"}},
                    {"type":"button","style":"primary","height":"sm","color":"#9B7BAB",
                     "action":{"type":"message","label":"市場連動分析","text":"市場連動分析"}},
                ]},
                {"type":"button","style":"primary","height":"sm","color":"#9B7BAB",
                 "action":{"type":"message","label":"全球資金流向","text":"全球資金流向"}},
            ]
        }
    }

def make_ai_menu_flex() -> dict:
    return make_menu_flex(
        "🤖 AI 分析", "智慧選股・多維度評分", "#C47055",
        [("⭐ 推薦股","推薦股"), ("📈 趨勢股","趨勢股"),
         ("🌱 成長股","成長股"), ("💰 存股","存股"),
         ("🌊 波段股","波段股"), ("🤖 AI概念股","AI概念股")]
    )

def make_news_menu_flex() -> dict:
    return make_menu_flex(
        "📰 財經新聞", "個股・台股・美股・國際", "#7A6B5A",
        [("📊 個股新聞","個股新聞"), ("🇹🇼 台股新聞","台股新聞"),
         ("🇺🇸 美股新聞","美股新聞"), ("🌐 國際新聞","國際新聞"),
         ("🌏 地緣政治","地緣政治新聞")]
    )

def make_portfolio_menu_flex() -> dict:
    return make_menu_flex(
        "📋 持股管理", "新增・查詢・損益分析", "#5B8B6B",
        [("➕ 新增持股","新增持股說明"), ("📋 查持股","持股"),
         ("📊 損益分析","損益分析"), ("🔴 停損提醒","停損提醒說明"),
         ("🎯 目標價提醒","目標價提醒說明")]
    )

def make_user_mgmt_flex(owner: bool) -> dict:
    buttons = [("🔍 查詢用戶","查使用者說明"), ("👥 使用者列表","使用者列表"),
               ("🔴 封鎖","封鎖說明"), ("🟢 解除封鎖","解除封鎖說明")]
    if owner:
        buttons += [("➕ 新增管理者","新增管理者說明"), ("➖ 移除管理者","移除管理者說明")]
    return make_menu_flex("👥 使用者管理","","#7A3828", buttons)

def make_system_mgmt_flex() -> dict:
    return make_menu_flex(
        "⚙️ 系統管理","","#7A3828",
        [("📊 快取狀態","快取狀態"), ("🔄 重載名稱","重載名稱"),
         ("🔍 查快取","查快取說明")]
    )


# ══════════════════════════════════════════
#  外匯/商品資料
# ══════════════════════════════════════════
FOREX_SYMBOLS = {
    "查USDTWD": ("TWD=X",  "USD/TWD 美元台幣"),
    "查DXY":    ("DX-Y.NYB","DXY 美元指數"),
    "查USDJPY": ("JPY=X",  "USD/JPY 美元日圓"),
    "查EURUSD": ("EURUSD=X","EUR/USD 歐元美元"),
    "查GBPUSD": ("GBPUSD=X","GBP/USD 英鎊美元"),
    "查USDCNY": ("CNY=X",  "USD/CNY 美元人民幣"),
    "查AUDUSD": ("AUDUSD=X","AUD/USD 澳幣美元"),
    "查USDCHF": ("CHFUSD=X","USD/CHF 美元瑞郎"),
}

MARKET_SYMBOLS = {
    "查台股加權": ("^TWII",  "台股加權指數"),
    "查櫃買指數": ("^TWOII", "台灣櫃買指數"),
    "查道瓊":    ("^DJI",   "道瓊工業指數"),
    "查Nasdaq":  ("^IXIC",  "那斯達克指數"),
    "查SP500":   ("^GSPC",  "S&P 500"),
    "查SOX":     ("^SOX",   "費城半導體 SOX"),
    "查VIX":     ("^VIX",   "VIX 恐慌指數"),
    "查黃金":    ("GC=F",   "黃金期貨"),
    "查原油":    ("CL=F",   "WTI 原油期貨"),
    "查美債":    ("^TNX",   "美國10年期公債殖利率"),
    "查天然氣":  ("NG=F",   "天然氣期貨"),
    "查期貨":    ("ES=F",   "S&P500 期貨"),
}

def get_yahoo_quote(symbol: str) -> dict:
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=5d"
        r   = requests.get(url, headers=headers, timeout=10)
        result = r.json()["chart"]["result"][0]
        meta   = result["meta"]
        quotes = result.get("indicators",{}).get("quote",[{}])[0]
        closes = [c for c in quotes.get("close",[]) if c is not None]
        ms     = meta.get("marketState","")
        price  = meta.get("regularMarketPrice") or (closes[-1] if closes else 0)
        prev   = closes[-2] if len(closes)>=2 else price
        chg    = price - prev
        pct    = chg / prev * 100 if prev else 0
        return {"price":price,"chg":chg,"pct":pct,"ms":ms}
    except: return {}

def make_quote_flex(name: str, data: dict, color: str = "#5B8DB8") -> dict:
    if not data: return None
    price = data.get("price",0)
    chg   = data.get("chg",0)
    pct   = data.get("pct",0)
    is_up = chg >= 0
    c     = "#C47055" if is_up else "#5B8DB8"
    arrow = "▲" if is_up else "▼"
    sign  = "+" if is_up else ""
    return {
        "type":"bubble","size":"kilo",
        "header":{
            "type":"box","layout":"vertical","backgroundColor":color,"paddingAll":"10px",
            "contents":[{"type":"text","text":name,"size":"sm","color":"#FFFFFF","weight":"bold"}]
        },
        "body":{
            "type":"box","layout":"vertical","paddingAll":"12px","spacing":"xs",
            "contents":[
                {"type":"text","text":f"{price:,.4f}" if price < 100 else f"{price:,.2f}",
                 "size":"xxl","weight":"bold","color":c},
                {"type":"text","text":f"{arrow} {abs(chg):.4f}　{sign}{pct:.2f}%",
                 "size":"sm","color":c},
                {"type":"text","text":now_taipei().strftime("%m/%d %H:%M"),
                 "size":"xxs","color":"#AAAAAA"}
            ]
        }
    }


# ══════════════════════════════════════════
#  台股名稱備援
# ══════════════════════════════════════════
def get_tw_stock_name_fallback(stock_id: str) -> str:
    cached = NAME_CACHE.get(stock_id,"")
    if cached and has_chinese(cached): return cached
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&stockNo={stock_id}"
        r   = requests.get(url, headers=headers, timeout=5)
        data = r.json()
        if data.get("stat")=="OK":
            parts = data.get("title","").strip().split()
            if len(parts)>=2:
                name = parts[-1].strip()
                if name and has_chinese(name):
                    NAME_CACHE[stock_id]=name; return name
    except: pass
    for ex in ["tse","otc"]:
        try:
            url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={ex}_{stock_id}.tw&json=1&delay=0"
            r   = requests.get(url, headers=headers, timeout=5)
            items = r.json().get("msgArray",[])
            if items:
                name = items[0].get("n","").strip()
                if name and has_chinese(name):
                    NAME_CACHE[stock_id]=name; return name
        except: pass
    for suffix in [".TW",".TWO"]:
        try:
            url  = f"https://query1.finance.yahoo.com/v8/finance/chart/{stock_id}{suffix}?interval=1d&range=5d"
            r    = requests.get(url, headers=headers, timeout=5)
            meta = r.json()["chart"]["result"][0]["meta"]
            name = (meta.get("shortName") or meta.get("longName") or "").strip()
            if name and has_chinese(name):
                NAME_CACHE[stock_id]=name; return name
        except: pass
    return stock_id


# ══════════════════════════════════════════
#  台股資料
# ══════════════════════════════════════════
def get_tw_stock(stock_id: str) -> dict:
    headers = {"User-Agent": "Mozilla/5.0"}
    for ex in ["tse","otc"]:
        try:
            url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={ex}_{stock_id}.tw&json=1&delay=0"
            r   = requests.get(url, headers=headers, timeout=8)
            d   = r.json().get("msgArray",[])
            if not d: continue
            d = d[0]
            raw_name = d.get("n","").strip()
            if not raw_name: continue
            y = d.get("y","-")
            if y in ["-","","0",None]: continue
            prev = float(y)
            z = d.get("z","-")
            if z not in ["-","","0",None]: price=float(z); is_rt=True
            else: price=prev; is_rt=False
            chg=price-prev; pct=chg/prev*100 if prev else 0
            if has_chinese(raw_name): NAME_CACHE[stock_id]=raw_name; name=raw_name
            else:
                name=NAME_CACHE.get(stock_id,"")
                if not has_chinese(name): name=get_tw_stock_name_fallback(stock_id)
                if not has_chinese(name): name=stock_id
            tv=d.get("tv","-"); v=d.get("v","-")
            if tv not in ["-","","0",None]:
                try: vol_str=f"{int(float(str(tv).replace(',',''))):,} 張"
                except: vol_str="N/A"
            elif v not in ["-","","0",None]:
                try: vol_str=f"{int(float(str(v).replace(',',''))):,} 張"
                except: vol_str="N/A"
            else: vol_str="N/A"
            open_v=d.get("o","-"); high_v=d.get("h","-"); low_v=d.get("l","-")
            open_v="N/A" if open_v in ["-","","0"] else open_v
            high_v="N/A" if high_v in ["-","","0"] else high_v
            low_v ="N/A" if low_v  in ["-","","0"] else low_v
            return {"name":name,"price":price,"chg":chg,"pct":pct,
                    "open":open_v,"high":high_v,"low":low_v,"vol":vol_str,
                    "market_type":"台股","status":"盤中" if is_rt else "試撮","source":"TWSE 即時"}
        except: pass

    try:
        url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&stockNo={stock_id}"
        r   = requests.get(url, headers=headers, timeout=8)
        data = r.json()
        if data.get("stat")=="OK" and data.get("data"):
            rows=data["data"]; last=rows[-1]
            price=float(last[6].replace(",","")); prev=float(rows[-2][6].replace(",","")) if len(rows)>1 else price
            chg=price-prev; pct=chg/prev*100 if prev else 0
            try: vol_str=f"{int(float(last[1].replace(',',''))//1000):,} 張"
            except: vol_str="N/A"
            name=NAME_CACHE.get(stock_id,"")
            if not has_chinese(name): name=get_tw_stock_name_fallback(stock_id)
            if not has_chinese(name): name=stock_id
            return {"name":name,"price":price,"chg":chg,"pct":pct,
                    "open":last[3].replace(",",""),"high":last[4].replace(",",""),
                    "low":last[5].replace(",",""),"vol":vol_str,
                    "market_type":"台股","status":"收盤","source":"TWSE"}
    except: pass

    try:
        today=now_taipei(); cy=today.year-1911
        ds=f"{cy}/{today.month:02d}/{today.day:02d}"
        url=f"https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&o=json&d={ds}&s=0,asc&q={stock_id}"
        r=requests.get(url,headers=headers,timeout=8)
        rows=r.json().get("aaData",[])
        if rows:
            last=rows[-1]; price=float(last[2].replace(",",""))
            prev=float(rows[-2][2].replace(",","")) if len(rows)>1 else price
            chg=price-prev; pct=chg/prev*100 if prev else 0
            try: vol_str=f"{int(float(last[0].replace(',',''))):,} 張"
            except: vol_str="N/A"
            name=NAME_CACHE.get(stock_id,"")
            if not has_chinese(name): name=get_tw_stock_name_fallback(stock_id)
            if not has_chinese(name): name=stock_id
            return {"name":name,"price":price,"chg":chg,"pct":pct,
                    "open":last[5].replace(",","") if len(last)>5 else "N/A",
                    "high":last[6].replace(",","") if len(last)>6 else "N/A",
                    "low":last[7].replace(",","") if len(last)>7 else "N/A","vol":vol_str,
                    "market_type":"台股","status":"收盤","source":"TPEx"}
    except: pass

    for suffix in [".TW",".TWO"]:
        try:
            url=f"https://query1.finance.yahoo.com/v8/finance/chart/{stock_id}{suffix}?interval=1d&range=5d"
            r=requests.get(url,headers=headers,timeout=10)
            result=r.json()["chart"]["result"][0]; meta=result["meta"]
            quotes=result.get("indicators",{}).get("quote",[{}])[0]
            opens=[o for o in quotes.get("open",[]) if o is not None]
            highs=[h for h in quotes.get("high",[]) if h is not None]
            lows=[l for l in quotes.get("low",[]) if l is not None]
            vols=[v for v in quotes.get("volume",[]) if v is not None]
            closes=[c for c in quotes.get("close",[]) if c is not None]
            price=meta.get("regularMarketPrice") or (closes[-1] if closes else 0)
            prev=closes[-2] if len(closes)>=2 else (meta.get("chartPreviousClose") or price)
            chg=price-prev; pct=chg/prev*100 if prev else 0
            name=NAME_CACHE.get(stock_id,"")
            if not has_chinese(name): name=get_tw_stock_name_fallback(stock_id)
            if not has_chinese(name): name=stock_id
            vol_str=f"{int(vols[-1]/1000):,} 張" if vols else "N/A"
            return {"name":name,"price":price,"chg":chg,"pct":pct,
                    "open":f"{opens[-1]:.2f}" if opens else "N/A",
                    "high":f"{highs[-1]:.2f}" if highs else "N/A",
                    "low":f"{lows[-1]:.2f}" if lows else "N/A","vol":vol_str,
                    "market_type":"台股","status":"收盤","source":"Yahoo Finance"}
        except: pass
    return None


# ══════════════════════════════════════════
#  美股資料
# ══════════════════════════════════════════
def get_us_stock(symbol: str) -> dict:
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        url=f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=5d"
        r=requests.get(url,headers=headers,timeout=10)
        result=r.json()["chart"]["result"][0]; meta=result["meta"]
        quotes=result.get("indicators",{}).get("quote",[{}])[0]
        opens=[o for o in quotes.get("open",[]) if o is not None]
        highs=[h for h in quotes.get("high",[]) if h is not None]
        lows=[l for l in quotes.get("low",[]) if l is not None]
        vols=[v for v in quotes.get("volume",[]) if v is not None]
        closes=[c for c in quotes.get("close",[]) if c is not None]
        ms=meta.get("marketState","")
        if ms=="POST":
            price=(meta.get("postMarketPrice") or meta.get("regularMarketPrice") or (closes[-1] if closes else 0))
            prev=closes[-1] if closes else price
        elif ms=="PRE":
            price=(meta.get("preMarketPrice") or meta.get("regularMarketPrice") or (closes[-1] if closes else 0))
            prev=closes[-2] if len(closes)>=2 else price
        else:
            price=meta.get("regularMarketPrice") or (closes[-1] if closes else 0)
            prev=closes[-2] if len(closes)>=2 else price
        chg=price-prev; pct=chg/prev*100 if prev else 0
        name=meta.get("shortName") or meta.get("longName") or symbol
        sl={"POST":"盤後","PRE":"盤前","REGULAR":"盤中","CLOSED":"收盤"}.get(ms,"")
        return {"name":name[:20],"price":price,"chg":chg,"pct":pct,
                "open":f"{opens[-1]:.2f}" if opens else "N/A",
                "high":f"{highs[-1]:.2f}" if highs else "N/A",
                "low":f"{lows[-1]:.2f}" if lows else "N/A",
                "vol":format_us_volume(vols[-1]) if vols else "N/A",
                "status":sl,"closes":[]}
    except: pass
    return None

def get_us_closes(symbol: str) -> list:
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        url=f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=1y"
        r=requests.get(url,headers=headers,timeout=10)
        closes=r.json()["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        return [c for c in closes if c is not None]
    except: return []


# ══════════════════════════════════════════
#  K線分析
# ══════════════════════════════════════════
def get_sparkline(closes: list) -> str:
    if not closes or len(closes)<2: return "▁▁▁▁▁▁▁▁▁▁"
    data=closes[-10:]; mn,mx=min(data),max(data)
    if mx==mn: return "▄▄▄▄▄▄▄▄▄▄"
    bars="▁▂▃▄▅▆▇█"
    return "".join(bars[int((c-mn)/(mx-mn)*7)] for c in data)

def get_kline_analysis(closes: list) -> dict:
    if not closes or len(closes)<2:
        return {"spark":"▄▄▄▄▄▄▄▄▄▄","trend":"--","rsi":0,"rsi_label":"--",
                "ma5":None,"ma20":None,"ma60":None,"ma120":None,"ma240":None}
    def ma(n): return sum(closes[-n:])/n if len(closes)>=n else None
    ma5=ma(5); ma20=ma(20); ma60=ma(60); ma120=ma(120); ma240=ma(240)
    if ma5 and ma20 and ma60:
        if   ma5>ma20>ma60: trend="多頭排列 📈"
        elif ma5<ma20<ma60: trend="空頭排列 📉"
        elif closes[-1]>ma60: trend="季線之上"
        else: trend="季線之下"
    elif len(closes)>=5 and closes[-1]>closes[-5]: trend="短線向上"
    else: trend="短線向下"
    gains=[max(closes[i]-closes[i-1],0) for i in range(1,len(closes))]
    losses=[max(closes[i-1]-closes[i],0) for i in range(1,len(closes))]
    ag=sum(gains[-14:])/min(14,len(gains)) if gains else 0
    al=sum(losses[-14:])/min(14,len(losses)) if losses else 0.001
    rsi=100-(100/(1+ag/al)) if al else 50
    if   rsi>80: rl="短線過熱"
    elif rsi>70: rl="短線偏熱"
    elif rsi<20: rl="極度超賣"
    elif rsi<30: rl="短線偏冷"
    else:        rl="中性區間"
    return {"spark":get_sparkline(closes),"trend":trend,
            "ma5":ma5,"ma20":ma20,"ma60":ma60,"ma120":ma120,"ma240":ma240,
            "rsi":rsi,"rsi_label":rl}

def get_tw_closes(stock_id: str) -> list:
    headers={"User-Agent":"Mozilla/5.0"}
    for suffix in [".TW",".TWO"]:
        try:
            url=f"https://query1.finance.yahoo.com/v8/finance/chart/{stock_id}{suffix}?interval=1d&range=1y"
            r=requests.get(url,headers=headers,timeout=10)
            closes=r.json()["chart"]["result"][0]["indicators"]["quote"][0]["close"]
            closes=[c for c in closes if c is not None]
            if len(closes)>=20: return closes
        except: pass
    try:
        url=f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&stockNo={stock_id}"
        r=requests.get(url,headers=headers,timeout=8)
        data=r.json()
        if data.get("stat")=="OK" and data.get("data"):
            closes=[]
            for row in data["data"]:
                try: closes.append(float(row[6].replace(",","")))
                except: pass
            if closes: return closes
    except: pass
    return []


# ══════════════════════════════════════════
#  新聞
# ══════════════════════════════════════════
STRICT_TRUSTED=[
    "cnyes.com","anue.com","moneydj.com","ctee.com.tw",
    "money.udn.com","udn.com","cna.com.tw","wealth.com.tw",
    "tw.stock.yahoo.com","technews.tw","bnext.com.tw","stockfeel.com.tw",
    "reuters.com","bloomberg.com","marketwatch.com","finance.yahoo.com",
    "cnbc.com","wsj.com","barrons.com","investing.com",
]
NON_NEWS_KEYWORDS=[
    "股票價格","股價圖","圖表","K線圖","個股概覽","個股頁",
    "持倉","ETF持股","成分股","歷史資料","歷史股價","技術圖",
    "stock price","stock chart","chart","quote","overview",
    "portfolio","historical data","price history","TSM股票","TSMC股票",
]

def is_trusted_source(url:str)->bool: return any(s in url for s in STRICT_TRUSTED) if url else False
def is_real_news(title:str)->bool: return not any(kw in title for kw in NON_NEWS_KEYWORDS)

def deduplicate_news(nl:list)->list:
    seen,result=[],[]
    for t,u in nl:
        key=re.sub(r'[^\u4e00-\u9fffa-zA-Z0-9]','',t)[:12]
        if key not in seen: seen.append(key); result.append((t,u))
    return result

def clean_title(t:str)->str:
    t=t.split(" - ")[0].strip(); t=re.sub(r'\s+',' ',t)
    return t[:32]+"…" if len(t)>32 else t

def get_news(query:str, count:int=4, trusted_only:bool=True)->list:
    headers={"User-Agent":"Mozilla/5.0"}; all_results=[]
    try:
        url=f"https://news.google.com/rss/search?q={requests.utils.quote(query)}&hl=zh-TW&gl=TW&ceid=TW:zh-Hant"
        r=requests.get(url,timeout=8,headers=headers); root=ET.fromstring(r.content)
        for item in root.findall(".//item")[:count*5]:
            title=clean_title(item.findtext("title",""))
            link=item.findtext("link","").strip()
            if title and link and is_real_news(title): all_results.append((title,link))
    except: pass
    try:
        r=requests.get("https://news.cnyes.com/rss/news/tw_stock",timeout=8,headers=headers)
        root=ET.fromstring(r.content)
        for item in root.findall(".//item")[:count]:
            title=clean_title(item.findtext("title",""))
            link=item.findtext("link","").strip()
            if title and link and is_real_news(title): all_results.append((title,link))
    except: pass
    trusted=deduplicate_news([(t,u) for t,u in all_results if is_trusted_source(u)])
    untrusted=deduplicate_news([(t,u) for t,u in all_results if not is_trusted_source(u)])
    if trusted_only: return trusted[:count]
    combined=trusted[:count]
    if len(combined)<count: combined+=untrusted[:count-len(combined)]
    return combined[:count]

def get_tw_stock_news(stock_id:str, cn_name:str, count:int=4)->list:
    results=[]
    if has_chinese(cn_name) and cn_name!=stock_id:
        results=get_news(f"{cn_name} 台股 財經",count=count,trusted_only=True)
    if not results:
        results=get_news(f"{stock_id} {cn_name} 股票",count=count,trusted_only=True)
    if not results:
        results=get_news(f"{stock_id} 台股",count=count,trusted_only=False)
    return results

def format_news_text(news_list: list, title: str) -> str:
    if not news_list: return f"📰 {title}\n━━━━━━━━━━━━━━\n　暫無可信新聞"
    msg = f"📰 {title}\n━━━━━━━━━━━━━━\n"
    for t,u in news_list:
        msg += f"　• {t}\n"
        if u: msg += f"　  🔗 {u}\n"
    return msg.strip()


# ══════════════════════════════════════════
#  新聞情緒
# ══════════════════════════════════════════
BULLISH_KEYWORDS=["上漲","漲停","創高","突破","買超","法人買","外資買",
    "營收創新高","獲利","配息","訂單","擴產","樂觀","上調","買進",
    "利多","強勢","漲幅","攻","大漲","飆升"]
BEARISH_KEYWORDS=["下跌","跌停","破底","賣超","法人賣","外資賣",
    "虧損","衰退","砍單","減產","悲觀","下調","賣出",
    "利空","弱勢","跌幅","崩","重挫","大跌"]

def analyze_news_sentiment(nl:list)->dict:
    bull,bear=0,0
    for t,_ in nl:
        for kw in BULLISH_KEYWORDS:
            if kw in t: bull+=1; break
        for kw in BEARISH_KEYWORDS:
            if kw in t: bear+=1; break
    if   bull>bear: return {"label":"偏多 📈","score":min(20+bull*5,30)}
    elif bear>bull: return {"label":"偏空 📉","score":max(10-bear*5,0)}
    else:           return {"label":"中性 ➡️","score":15}


# ══════════════════════════════════════════
#  推薦股評分
# ══════════════════════════════════════════
def score_technical(closes:list, pct:float)->dict:
    score,signals=0,[]
    if not closes or len(closes)<5:
        return {"score":0,"rsi":50,"signals":[],"trend":"--"}
    def ma(n): return sum(closes[-n:])/n if len(closes)>=n else None
    ma5=ma(5); ma20=ma(20); ma60=ma(60); price=closes[-1]; trend="--"
    if ma5 and ma20 and ma60 and ma5>ma20>ma60:
        score+=15; signals.append("均線多頭"); trend="多頭排列 📈"
    elif ma5 and ma20 and ma5>ma20:
        score+=8; signals.append("短均線向上"); trend="短線向上"
    elif ma5 and ma20 and ma5<ma20: trend="短線向下"
    if ma60 and price>ma60: score+=8; signals.append("站上季線")
    gains=[max(closes[i]-closes[i-1],0) for i in range(1,len(closes))]
    losses=[max(closes[i-1]-closes[i],0) for i in range(1,len(closes))]
    ag=sum(gains[-14:])/min(14,len(gains)) if gains else 0
    al=sum(losses[-14:])/min(14,len(losses)) if losses else 0.001
    rsi=100-(100/(1+ag/al)) if al else 50
    if   45<=rsi<=70: score+=10; signals.append(f"RSI健康({rsi:.0f})")
    elif rsi<30:      score+=5;  signals.append(f"RSI超賣({rsi:.0f})")
    elif rsi>80:      score-=5
    if   1<=pct<=6:  score+=7; signals.append(f"今漲{pct:.1f}%")
    elif pct>8:      score-=3
    elif pct<-5:     score-=8
    return {"score":max(0,min(score,40)),"rsi":rsi,"signals":signals,"trend":trend}

def score_chip(fl:int, il:int)->dict:
    score,signals=0,[]
    if   fl>5000:  score+=15; signals.append(f"外資大買+{fl:,}")
    elif fl>1000:  score+=8;  signals.append(f"外資買+{fl:,}")
    elif fl<-3000: score-=10
    if   il>2000:  score+=10; signals.append(f"投信大買+{il:,}")
    elif il>500:   score+=5;  signals.append(f"投信買+{il:,}")
    if fl>0 and il>0: score+=5; signals.append("法人同買")
    return {"score":max(0,min(score,30)),"signals":signals}

def classify_stock(tech:dict, chip:dict, pct:float)->str:
    rsi=tech.get("rsi",50); trend=tech.get("trend","")
    cs=chip.get("score",0); ts=" ".join(tech.get("signals",[]))
    if   "多頭排列" in trend and cs>=20: return "趨勢強股 🚀"
    elif rsi<35:                          return "低接機會 🎯"
    elif cs>=25:                          return "籌碼集中 💰"
    elif "站上季線" in ts:                return "技術突破 📊"
    elif 0<pct<=3:                        return "穩健上漲 ✅"
    else:                                 return "綜合評估 📋"

def get_dynamic_watchlist()->list:
    headers={"User-Agent":"Mozilla/5.0"}; wl=[]
    try:
        url="https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX20?response=json"
        r=requests.get(url,headers=headers,timeout=8); data=r.json()
        if data.get("stat")=="OK":
            for row in data.get("data",[])[:15]:
                sid=row[1].strip() if len(row)>1 else ""
                nm=row[2].strip() if len(row)>2 else ""
                if sid and sid.isdigit(): wl.append((sid,nm,0,0,0))
    except: pass
    for sid,nm in [("0050","元大台灣50"),("00878","國泰永續高股息"),
                   ("006208","富邦台50"),("0056","元大高股息"),
                   ("2330","台積電"),("2454","聯發科"),
                   ("2308","台達電"),("3711","日月光投控"),
                   ("2382","廣達"),("3533","嘉澤端子")]:
        if not any(w[0]==sid for w in wl): wl.append((sid,nm,0,0,0))
    return wl[:20]

def fetch_institution_data()->tuple:
    headers={"User-Agent":"Mozilla/5.0"}
    now=now_taipei(); weekday=now.weekday(); afc=is_after_close()
    dq=[]
    if weekday<5 and afc: dq.append((now,True))
    for i in range(1,10):
        d=now-timedelta(days=i)
        if d.weekday()<5: dq.append((d,False))
        if len(dq)>=7: break
    for cd,is_today in dq:
        try:
            if is_today: url="https://www.twse.com.tw/rwd/zh/fund/T86?response=json&selectType=ALL"
            else:
                ds=cd.strftime("%Y%m%d")
                url=f"https://www.twse.com.tw/rwd/zh/fund/T86?response=json&selectType=ALL&date={ds}"
            r=requests.get(url,headers=headers,timeout=10); data=r.json()
            if data.get("stat")=="OK" and data.get("data"):
                candidates=[]
                for row in data.get("data",[]):
                    try:
                        if len(row)<11: continue
                        f_str=row[4].strip().replace(",","").replace("+","")
                        i_str=row[10].strip().replace(",","").replace("+","")
                        foreign=int(f_str) if f_str and f_str not in ["-",""] else 0
                        invest=int(i_str) if i_str and i_str not in ["-",""] else 0
                        fl=foreign//1000; il=invest//1000; tl=fl+il
                        if tl>500: candidates.append((row[0],row[1],tl,fl,il))
                    except: pass
                if candidates:
                    dd=data.get("date",cd.strftime("%Y/%m/%d")); ts=now.strftime("%Y/%m/%d")
                    if   dd==ts:                sn=f"✅ 已使用當日法人資料({dd})"
                    elif weekday<5 and not afc: sn=f"📅 今日法人資料尚未公布，暫用 {dd} 資料"
                    else:                       sn=f"📅 使用 {dd} 前交易日資料"
                    return candidates,dd,sn
        except Exception as e: print(f"法人資料失敗：{e}")
    return [],"","⚠️ 法人資料來源連線失敗"

def fetch_tpex_institution_data()->list:
    headers={"User-Agent":"Mozilla/5.0"}; candidates=[]
    try:
        url="https://www.tpex.org.tw/openapi/v1/tpex_mainboard_institution_trading"
        r=requests.get(url,headers=headers,timeout=10); data=r.json()
        if data and isinstance(data,list):
            for item in data:
                try:
                    sid=(item.get("SecuritiesCompanyCode") or item.get("Code") or "").strip()
                    nm=(item.get("CompanyName") or item.get("Name") or "").strip()
                    fb=int(str(item.get("ForeignInvestorBuyShares",0)).replace(",","") or 0)
                    fs=int(str(item.get("ForeignInvestorSellShares",0)).replace(",","") or 0)
                    ib=int(str(item.get("InvestmentTrustBuyShares",0)).replace(",","") or 0)
                    is_=int(str(item.get("InvestmentTrustSellShares",0)).replace(",","") or 0)
                    fl=(fb-fs)//1000; il=(ib-is_)//1000; tl=fl+il
                    if sid and tl>200: candidates.append((sid,nm,tl,fl,il))
                except: pass
    except Exception as e: print(f"TPEx法人失敗：{e}")
    return candidates


# ══════════════════════════════════════════
#  大盤
# ══════════════════════════════════════════
def get_market_status()->dict:
    headers={"User-Agent":"Mozilla/5.0"}
    result={"price":0,"pct":0,"ok":True,"str":"⚪ 大盤資料取得中"}
    try:
        url="https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=tse_t00.tw&json=1&delay=0"
        r=requests.get(url,headers=headers,timeout=8)
        d=r.json().get("msgArray",[{}])[0]
        price=float(d.get("z",0) or d.get("y",0)); prev=float(d.get("y",price))
        pct=(price-prev)/prev*100 if prev else 0
        icon="🟢" if pct>=0 else "🔴"
        result={"price":price,"pct":pct,"ok":pct>=-2,"str":f"{icon} 加權 {price:,.0f}({pct:+.2f}%)"}
    except: pass
    return result

def get_market_summary()->str:
    headers={"User-Agent":"Mozilla/5.0"}
    msg=(f"🌐 全球大盤\n━━━━━━━━━━━━━━\n"
         f"　{now_taipei().strftime('%m/%d %H:%M')} 更新\n━━━━━━━━━━━━━━\n")
    try:
        url="https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=tse_t00.tw&json=1&delay=0"
        r=requests.get(url,headers=headers,timeout=8)
        d=r.json().get("msgArray",[{}])[0]
        price=float(d.get("z",0) or d.get("y",0)); prev=float(d.get("y",price))
        pct=(price-prev)/prev*100 if prev else 0
        msg+=f"{'🟢' if pct>=0 else '🔴'} 台灣加權　{price:,.2f}　{pct:+.2f}%\n"
    except: msg+="⚪ 台灣加權　--\n"
    for sym,name in [("^GSPC","S&P 500"),("^IXIC","那斯達克"),("^DJI","道瓊"),("^SOX","SOX半導體"),("^VIX","VIX恐慌")]:
        try:
            url=f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=5d"
            r=requests.get(url,headers=headers,timeout=10)
            result=r.json()["chart"]["result"][0]; meta=result["meta"]
            quotes=result.get("indicators",{}).get("quote",[{}])[0]
            closes=[c for c in quotes.get("close",[]) if c is not None]
            price=meta.get("regularMarketPrice",0)
            prev=closes[-2] if len(closes)>=2 else price
            pct=(price-prev)/prev*100 if prev else 0
            ms=meta.get("marketState","")
            state="(盤後)" if ms=="POST" else ""
            msg+=f"{'🟢' if pct>=0 else '🔴'} {name}　{price:,.2f}　{pct:+.2f}%{state}\n"
        except: msg+=f"⚪ {name}　--\n"
    msg+="━━━━━━━━━━━━━━\n⚠️ 僅供參考，非投資建議"
    return msg


# ══════════════════════════════════════════
#  推薦股 Flex
# ══════════════════════════════════════════
def make_rec_card(rank:int, s:dict)->dict:
    is_up=s["pct"]>=0; color="#C47055" if is_up else "#5B8DB8"
    arrow="▲" if is_up else "▼"; pct_str=f"{arrow} {abs(s['pct']):.2f}%"
    filled=s["score"]//10; bar="█"*filled+"░"*(10-filled)
    tech_sig="　".join(s.get("tech_signals",[])[:2]) or "--"
    chip_sig="　".join(s.get("chip_signals",[])[:2]) or "--"
    return {
        "type":"bubble","size":"mega",
        "header":{"type":"box","layout":"horizontal","backgroundColor":"#C47055","paddingAll":"12px",
            "contents":[
                {"type":"box","layout":"vertical","flex":0,
                 "contents":[{"type":"text","text":f"#{rank}","size":"xl","color":"#FFFFFF","weight":"bold"}]},
                {"type":"box","layout":"vertical","flex":1,"paddingStart":"10px",
                 "contents":[
                     {"type":"text","text":f"{s['sid']} {s['name']}","size":"md","color":"#FFFFFF","weight":"bold","wrap":True},
                     {"type":"text","text":s.get("category","綜合評估"),"size":"xs","color":"#F0D0C0"}
                 ]}
            ]},
        "body":{"type":"box","layout":"vertical","backgroundColor":"#FDF6F0","paddingAll":"12px","spacing":"sm",
            "contents":[
                {"type":"box","layout":"horizontal","contents":[
                    {"type":"text","text":f"{s['price']:.2f}","size":"xxl","weight":"bold","color":color,"flex":1},
                    {"type":"text","text":pct_str,"size":"sm","color":color,"align":"end","flex":1,"gravity":"bottom"}
                ]},
                {"type":"separator","color":"#E8C4B4"},
                {"type":"box","layout":"horizontal","spacing":"xs","contents":[
                    {"type":"text","text":"📊 技術","size":"xxs","color":"#9B6B5A","flex":2},
                    {"type":"text","text":tech_sig,"size":"xxs","color":"#5B4040","flex":5,"wrap":True}
                ]},
                {"type":"box","layout":"horizontal","spacing":"xs","contents":[
                    {"type":"text","text":"💰 籌碼","size":"xxs","color":"#9B6B5A","flex":2},
                    {"type":"text","text":chip_sig,"size":"xxs","color":"#5B4040","flex":5,"wrap":True}
                ]},
                {"type":"box","layout":"horizontal","spacing":"xs","contents":[
                    {"type":"text","text":"📰 新聞","size":"xxs","color":"#9B6B5A","flex":2},
                    {"type":"text","text":s.get("sentiment","中性"),"size":"xxs","color":"#5B4040","flex":5}
                ]},
                {"type":"separator","color":"#E8C4B4"},
                {"type":"box","layout":"horizontal","contents":[
                    {"type":"text","text":"評分","size":"xxs","color":"#9B6B5A","flex":1},
                    {"type":"text","text":f"{bar} {s['score']}/100","size":"xxs","color":"#7A3828","weight":"bold","flex":5}
                ]}
            ]}
    }

def make_rec_flex(scored:list, mkt:dict, source_note:str)->dict:
    now_str=now_taipei().strftime("%m/%d %H:%M")
    overview={
        "type":"bubble","size":"mega",
        "header":{"type":"box","layout":"vertical","backgroundColor":"#C47055","paddingAll":"14px",
            "contents":[
                {"type":"text","text":"⭐ 慧股推薦榜","size":"xl","color":"#FFFFFF","weight":"bold"},
                {"type":"text","text":f"🇹🇼 台股　{now_str}","size":"xs","color":"#F0D0C0"}
            ]},
        "body":{"type":"box","layout":"vertical","backgroundColor":"#FDF6F0","paddingAll":"14px","spacing":"md",
            "contents":[
                {"type":"text","text":mkt["str"],"size":"sm","color":"#5B4040","wrap":True},
                {"type":"separator","color":"#E8C4B4"},
                {"type":"text","text":source_note,"size":"xs","color":"#9B6B5A","wrap":True},
                {"type":"separator","color":"#E8C4B4"},
                {"type":"text","text":"📊 評分維度","size":"sm","color":"#7A3828","weight":"bold"},
                {"type":"box","layout":"vertical","spacing":"xs","contents":[
                    {"type":"box","layout":"horizontal","contents":[
                        {"type":"text","text":"技術面","size":"xs","color":"#9B6B5A","flex":2},
                        {"type":"text","text":"均線 RSI 漲幅","size":"xs","color":"#5B4040","flex":3},
                        {"type":"text","text":"40分","size":"xs","color":"#C47055","flex":1,"align":"end"}
                    ]},
                    {"type":"box","layout":"horizontal","contents":[
                        {"type":"text","text":"籌碼面","size":"xs","color":"#9B6B5A","flex":2},
                        {"type":"text","text":"外資 投信 同買","size":"xs","color":"#5B4040","flex":3},
                        {"type":"text","text":"30分","size":"xs","color":"#C47055","flex":1,"align":"end"}
                    ]},
                    {"type":"box","layout":"horizontal","contents":[
                        {"type":"text","text":"新聞情緒","size":"xs","color":"#9B6B5A","flex":2},
                        {"type":"text","text":"白名單財經媒體","size":"xs","color":"#5B4040","flex":3},
                        {"type":"text","text":"30分","size":"xs","color":"#C47055","flex":1,"align":"end"}
                    ]},
                ]},
                {"type":"separator","color":"#E8C4B4"},
                {"type":"text","text":"⚠️ 僅供參考，非投資建議","size":"xxs","color":"#C4907A","wrap":True}
            ]}
    }
    bubbles=[overview]+[make_rec_card(i+1,s) for i,s in enumerate(scored[:5])]
    return {"type":"carousel","contents":bubbles}

def build_and_push_recommendation(user_id:str):
    try:
        mkt=get_market_status()
        candidates,data_date,source_note=fetch_institution_data()
        tpex=fetch_tpex_institution_data()
        candidates=candidates+tpex
        if len(candidates)<5:
            source_note="⚠️ 法人資料不足，暫以技術面與新聞面評估"
            candidates=get_dynamic_watchlist()
        if not candidates:
            push_message(user_id,"⭐ 推薦股\n━━━━━━━━━━━━━━\n　目前無法取得資料\n　請稍後再試"); return
        candidates.sort(key=lambda x:x[2],reverse=True)
        scored=[]
        for sid,name,tl,fl,il in candidates[:15]:
            tw=get_tw_stock(sid)
            if not tw: continue
            closes=get_tw_closes(sid)
            tech=score_technical(closes,tw["pct"]); chip=score_chip(fl,il)
            nl=get_tw_stock_news(sid,tw["name"],count=3); sentiment=analyze_news_sentiment(nl)
            ts=tech["score"]+chip["score"]+sentiment["score"]
            if not mkt["ok"]: ts=int(ts*0.8)
            scored.append({"sid":sid,"name":tw["name"],"price":tw["price"],"pct":tw["pct"],
                           "sentiment":sentiment["label"],"tech_signals":tech.get("signals",[]),
                           "chip_signals":chip.get("signals",[]),"category":classify_stock(tech,chip,tw["pct"]),
                           "score":ts,"support":"--","resistance":"--","stop_loss":"--"})
        scored.sort(key=lambda x:x["score"],reverse=True)
        top5=scored[:5]
        if not top5:
            push_message(user_id,"⭐ 推薦股\n━━━━━━━━━━━━━━\n　目前無符合條件個股"); return
        push_flex(user_id,make_rec_flex(top5,mkt,source_note),"慧股推薦榜")
    except Exception as e:
        print(f"推薦股運算失敗：{e}")
        push_message(user_id,"⭐ 推薦股\n━━━━━━━━━━━━━━\n　系統處理中發生錯誤\n　請稍後再試")


# ══════════════════════════════════════════
#  持股
# ══════════════════════════════════════════
def get_portfolio_summary(user_id:str)->str:
    portfolio=load_portfolio()
    up={k:v for k,v in portfolio.items() if v.get("user_id")==user_id}
    if not up:
        return "📋 持股清單是空的\n━━━━━━━━━━━━━━\n新增方式：\n　新增 2330 100 200\n　(代碼 股數 買入均價)"
    msg="📋 我的持股\n━━━━━━━━━━━━━━\n"; total=0
    for symbol,data in up.items():
        try:
            sid=symbol.replace(".TW","")
            if sid.isdigit():
                tw=get_tw_stock(sid); price=tw["price"] if tw else 0; name=tw["name"] if tw else sid
            else:
                us=get_us_stock(symbol); price=us["price"] if us else 0; name=us["name"] if us else symbol
            shares=data["shares"]; bp=data["buy_price"]
            profit=(price-bp)*shares; pct=(price-bp)/bp*100
            icon="🟢" if profit>=0 else "🔴"; total+=profit
            msg+=f"{icon} {symbol}｜{name}\n　現價 {price:.2f}　買入 {bp:.2f}\n　{shares}股　損益 {profit:+,.0f}({pct:+.1f}%)\n\n"
        except: msg+=f"　{symbol}　查詢失敗\n\n"
    msg+=f"━━━━━━━━━━━━━━\n{'🟢' if total>=0 else '🔴'} 總損益　{total:+,.0f}"
    return msg


# ══════════════════════════════════════════
#  股票 Flex 卡片
# ══════════════════════════════════════════
def make_ma_row(label,value):
    vs=f"{value:.0f}" if value else "N/A"
    color="#7A3828" if value else "#C4907A"
    return {"type":"box","layout":"horizontal","contents":[
        {"type":"text","text":label,"size":"xs","color":"#9B6B5A","flex":4},
        {"type":"text","text":vs,"size":"xs","color":color,"flex":2,"weight":"bold","align":"end"},
    ]}

def make_stock_flex(symbol,name,market_type,status,source,
                    price,chg,pct,open_p,high,low,vol,
                    kline,news_list,query_time):
    is_up=chg>=0; color="#C47055" if is_up else "#5B8DB8"
    arrow="▲" if is_up else "▼"; sign="+" if is_up else ""
    spark=kline.get("spark","▄▄▄▄▄▄▄▄▄▄"); trend=kline.get("trend","--")
    ma5=kline.get("ma5"); ma20=kline.get("ma20"); ma60=kline.get("ma60")
    ma120=kline.get("ma120"); ma240=kline.get("ma240")
    rsi=kline.get("rsi",0); rl=kline.get("rsi_label","--")
    rc="#C47055" if rsi>70 else ("#5B8DB8" if rsi<30 else "#8B6B5A")
    dn=f"{symbol} {name}" if name and name!=symbol else symbol
    nc=[]
    for t,u in news_list[:4]:
        if u: nc.append({"type":"button","style":"link","height":"sm",
            "action":{"type":"uri","label":f"📰 {t}","uri":u}})
        else: nc.append({"type":"text","text":f"📰 {t}","size":"xs","color":"#B06050","wrap":True})
    if not nc: nc=[{"type":"text","text":"暫無相關新聞","size":"xs","color":"#C4907A"}]
    return {
        "type":"bubble","size":"mega",
        "header":{"type":"box","layout":"vertical","backgroundColor":"#C47055","paddingAll":"16px","contents":[
            {"type":"box","layout":"horizontal","contents":[
                {"type":"text","text":"✨ 慧股拾光 Lumistock","size":"xxs","color":"#F0D0C0","flex":1},
                {"type":"text","text":market_type,"size":"xxs","color":"#F0D0C0","align":"end"}
            ]},
            {"type":"text","text":dn,"size":"xl","color":"#FFFFFF","weight":"bold","wrap":True}
        ]},
        "body":{"type":"box","layout":"vertical","backgroundColor":"#FDF6F0","paddingAll":"14px","spacing":"sm",
            "contents":[
                {"type":"box","layout":"vertical","contents":[
                    {"type":"text","text":f"{price:.2f}","size":"3xl","weight":"bold","color":color},
                    {"type":"text","text":f"{arrow} {abs(chg):.2f}　{sign}{pct:.2f}%","size":"sm","color":color}
                ]},
                {"type":"separator","color":"#E8C4B4"},
                {"type":"box","layout":"horizontal","contents":[
                    {"type":"box","layout":"vertical","flex":1,"contents":[
                        {"type":"text","text":"開盤","size":"xxs","color":"#9B6B5A"},
                        {"type":"text","text":str(open_p),"size":"sm","color":"#5B4040","weight":"bold"}
                    ]},
                    {"type":"box","layout":"vertical","flex":1,"contents":[
                        {"type":"text","text":"最高","size":"xxs","color":"#9B6B5A"},
                        {"type":"text","text":str(high),"size":"sm","color":"#C47055","weight":"bold"}
                    ]},
                    {"type":"box","layout":"vertical","flex":1,"contents":[
                        {"type":"text","text":"最低","size":"xxs","color":"#9B6B5A"},
                        {"type":"text","text":str(low),"size":"sm","color":"#5B8DB8","weight":"bold"}
                    ]},
                    {"type":"box","layout":"vertical","flex":1,"contents":[
                        {"type":"text","text":"成交量","size":"xxs","color":"#9B6B5A"},
                        {"type":"text","text":str(vol),"size":"sm","color":"#5B4040","weight":"bold"}
                    ]}
                ]},
                {"type":"separator","color":"#E8C4B4"},
                {"type":"text","text":"📊 技術分析","size":"sm","weight":"bold","color":"#7A3828"},
                {"type":"text","text":spark,"size":"xl","color":color},
                {"type":"text","text":f"趨勢　{trend}","size":"sm","color":"#7A3828"},
                {"type":"box","layout":"vertical","spacing":"xs","contents":[
                    make_ma_row("MA5　　短線",ma5), make_ma_row("MA20　　月線",ma20),
                    make_ma_row("MA60　　季線",ma60), make_ma_row("MA120　半年線",ma120),
                    make_ma_row("MA240　年　線",ma240),
                ]},
                {"type":"box","layout":"horizontal","contents":[
                    {"type":"text","text":"RSI","size":"xs","color":"#9B6B5A","flex":1},
                    {"type":"text","text":f"{rsi:.0f}","size":"xs","color":rc,"weight":"bold","flex":1},
                    {"type":"text","text":rl,"size":"xs","color":rc,"flex":3}
                ]},
                {"type":"separator","color":"#E8C4B4"},
                {"type":"text","text":"📰 相關新聞","size":"sm","weight":"bold","color":"#7A3828"},
            ]+nc+[
                {"type":"separator","color":"#E8C4B4"},
                {"type":"box","layout":"horizontal","contents":[
                    {"type":"text","text":f"🕐 {query_time}　{status}","size":"xxs","color":"#C4907A","flex":1},
                    {"type":"text","text":source,"size":"xxs","color":"#D4B0A0","align":"end","flex":1}
                ]}
            ]}
    }

def get_stock_flex(symbol:str, user_id:str="")->tuple:
    symbol=symbol.strip().upper(); is_tw=symbol.isdigit()
    query_time=now_taipei().strftime("%m/%d %H:%M")
    if is_tw:
        tw=get_tw_stock(symbol)
        if not tw: return None,f"查無此股票：{symbol}\n請確認代碼是否正確"
        if not has_chinese(tw.get("name","")): tw["name"]=NAME_CACHE.get(symbol,"")
        if not has_chinese(tw.get("name","")): tw["name"]=get_tw_stock_name_fallback(symbol)
        if not has_chinese(tw.get("name","")): tw["name"]=symbol
        closes=get_tw_closes(symbol); kline=get_kline_analysis(closes)
        news=get_tw_stock_news(symbol,tw["name"],count=4)
        update_tw_data_to_sheets(symbol,tw)
        log_to_sheets(user_id,"查詢台股",symbol,"成功")
        return make_stock_flex(symbol,tw["name"],tw.get("market_type","台股"),
                               tw.get("status",""),tw.get("source",""),
                               tw["price"],tw["chg"],tw["pct"],
                               tw.get("open","N/A"),tw["high"],tw["low"],tw["vol"],
                               kline,news,query_time),None
    else:
        us=get_us_stock(symbol)
        if not us: return None,f"查無此股票：{symbol}\n請確認代碼是否正確"
        closes=get_us_closes(symbol); kline=get_kline_analysis(closes)
        news=get_news(f"{symbol} {us['name']} stock news",4,trusted_only=True)
        update_us_data_to_sheets(symbol,us)
        log_to_sheets(user_id,"查詢美股",symbol,"成功")
        return make_stock_flex(symbol,us["name"],"美股",us.get("status",""),"Yahoo Finance",
                               us["price"],us["chg"],us["pct"],
                               us.get("open","N/A"),us.get("high","N/A"),
                               us.get("low","N/A"),us.get("vol","N/A"),
                               kline,news,query_time),None


HELP_MSG="""✨ 慧股拾光 Lumistock
━━━━━━━━━━━━━━
📌 功能說明

🔍 查股票　輸入代號即可
　台股：2330　美股：AAPL
　ETF：0050　00878

🌐 全球大盤　點選選單

💹 外匯資金　匯率與市場分析

🤖 AI分析　智慧選股推薦

📰 財經新聞　台股美股國際

📋 持股管理　損益追蹤
━━━━━━━━━━━━━━
台股／美股／ETF 皆支援"""


def reply_text(reply_token,text):
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).reply_message(
            ReplyMessageRequest(reply_token=reply_token,messages=[TextMessage(text=text)]))

def reply_flex(reply_token,flex_content,alt_text="資訊"):
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).reply_message(
            ReplyMessageRequest(reply_token=reply_token,
                messages=[FlexMessage(alt_text=alt_text,
                    contents=FlexContainer.from_dict(flex_content))]))

def reply_text_with_qr(reply_token, text, qr_items):
    qr = make_quick_reply(qr_items)
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).reply_message(
            ReplyMessageRequest(reply_token=reply_token,
                messages=[TextMessage(text=text, quick_reply=qr)]))

def reply_flex_with_qr(reply_token, flex_content, alt_text, qr_items):
    qr = make_quick_reply(qr_items)
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).reply_message(
            ReplyMessageRequest(reply_token=reply_token,
                messages=[FlexMessage(alt_text=alt_text,
                    contents=FlexContainer.from_dict(flex_content),
                    quick_reply=qr)]))


@app.route("/",methods=["GET"])
def index():
    return "Lumistock is running! 🌸",200

@app.after_request
def add_header(response):
    response.headers["ngrok-skip-browser-warning"]="true"
    return response

@app.route("/callback",methods=["POST"])
def callback():
    signature=request.headers["X-Line-Signature"]
    body=request.get_data(as_text=True)
    try: handler.handle(body,signature)
    except InvalidSignatureError: abort(400)
    return "OK"

@handler.add(MessageEvent,message=TextMessageContent)
def handle_message(event):
    text=event.message.text.strip(); user_id=event.source.user_id

    if is_blocked_user(user_id):
        reply_text(event.reply_token,"⛔ 此帳號已停止使用權限\n如有疑問請聯繫管理員")
        return

    update_user_activity(user_id,text)

    # ══ 主選單觸發 ══
    if text=="查股票":
        reply_flex(event.reply_token, make_stock_menu_flex(), "查股票")
        return

    if text=="全球大盤":
        reply_flex_with_qr(event.reply_token, make_market_menu_flex(), "全球大盤",
            [("台股加權","查台股加權"),("Nasdaq","查Nasdaq"),("S&P500","查SP500"),
             ("SOX","查SOX"),("VIX","查VIX"),("黃金","查黃金"),("原油","查原油")])
        return

    if text=="外匯資金":
        reply_flex(event.reply_token, make_forex_menu_flex(), "全球外匯與資金市場")
        return

    if text=="AI分析":
        reply_flex_with_qr(event.reply_token, make_ai_menu_flex(), "AI分析",
            [("推薦股","推薦股"),("趨勢股","趨勢股"),("成長股","成長股"),
             ("存股","存股"),("波段股","波段股"),("AI概念股","AI概念股")])
        return

    if text=="財經新聞":
        reply_flex_with_qr(event.reply_token, make_news_menu_flex(), "財經新聞",
            [("台股新聞","台股新聞"),("美股新聞","美股新聞"),
             ("個股新聞","個股新聞"),("國際新聞","國際新聞"),("地緣政治","地緣政治新聞")])
        return

    if text=="持股管理":
        reply_flex_with_qr(event.reply_token, make_portfolio_menu_flex(), "持股管理",
            [("查持股","持股"),("新增持股","新增持股說明"),
             ("損益分析","損益分析"),("停損提醒","停損提醒說明")])
        return

    # ══ 管理頁按鈕（從 Rich Menu 切換後觸發）══
    if text=="使用者管理" and is_admin(user_id):
        reply_flex(event.reply_token, make_user_mgmt_flex(is_owner(user_id)), "使用者管理")
        return

    if text=="系統管理" and is_admin(user_id):
        reply_flex(event.reply_token, make_system_mgmt_flex(), "系統管理")
        return

    if text=="推播管理" and is_owner(user_id):
        reply_text(event.reply_token,
            "📢 推播管理\n━━━━━━━━━━━━━━\n功能開發中 🚧\n\n後續版本將開放：\n　• 晨報推播\n　• 夜報推播\n　• 全體公告")
        return

    if text=="AI管理" and is_owner(user_id):
        reply_text(event.reply_token,
            "🤖 AI 管理\n━━━━━━━━━━━━━━\n功能開發中 🚧\n\n後續版本將開放：\n　• AI 模型參數調整\n　• 推薦演算法設定\n　• 評分權重配置")
        return

    # ══ 子選單 ══
    if text=="使用者管理選單" and is_admin(user_id):
        reply_flex(event.reply_token, make_user_mgmt_flex(is_owner(user_id)), "使用者管理")
        return

    if text=="系統管理選單" and is_admin(user_id):
        reply_flex(event.reply_token, make_system_mgmt_flex(), "系統管理")
        return

    if text=="持股管理選單" and is_admin(user_id):
        reply_flex_with_qr(event.reply_token, make_portfolio_menu_flex(), "持股管理",
            [("查持股","持股"),("新增持股","新增持股說明"),("損益分析","損益分析")])
        return

    # ══ 市場指數查詢 ══
    if text in MARKET_SYMBOLS:
        sym, name = MARKET_SYMBOLS[text]
        data = get_yahoo_quote(sym)
        if data:
            flex = make_quote_flex(name, data, "#5B8DB8")
            if flex: reply_flex(event.reply_token, flex, name)
            else: reply_text(event.reply_token, f"⚠️ {name} 資料取得失敗")
        else:
            reply_text(event.reply_token, f"⚠️ {name} 資料取得失敗")
        return

    # ══ 外匯查詢 ══
    if text in FOREX_SYMBOLS:
        sym, name = FOREX_SYMBOLS[text]
        data = get_yahoo_quote(sym)
        if data:
            flex = make_quote_flex(name, data, "#8B6B9B")
            if flex: reply_flex(event.reply_token, flex, name)
            else: reply_text(event.reply_token, f"⚠️ {name} 資料取得失敗")
        else:
            reply_text(event.reply_token, f"⚠️ {name} 資料取得失敗")
        return

    # ══ 外匯市場分析 ══
    if text=="外匯市場分析":
        reply_text_with_qr(event.reply_token,
            "💹 外匯市場分析\n━━━━━━━━━━━━━━\n請選擇分析主題：",
            [("Fed利率影響","查Fed利率"),("BOJ日本央行","查BOJ"),
             ("ECB歐洲央行","查ECB"),("避險資金流向","查避險資金"),
             ("全球資金流向","全球資金流向")])
        return

    if text=="市場連動分析":
        reply_text_with_qr(event.reply_token,
            "🔗 市場連動分析\n━━━━━━━━━━━━━━\n請選擇分析主題：",
            [("匯率對台股","查匯率台股"),("匯率對美股","查匯率美股"),
             ("匯率對黃金","查匯率黃金"),("匯率對半導體","查匯率半導體")])
        return

    if text=="全球資金流向":
        news = get_news("全球資金流向 外資 匯率", count=4, trusted_only=True)
        reply_text(event.reply_token, format_news_text(news,"全球資金流向"))
        return

    # ══ 新聞查詢 ══
    if text=="台股新聞":
        news=get_news("台股 股市 財經 今日",4,trusted_only=True)
        reply_text(event.reply_token, format_news_text(news,"🇹🇼 台股新聞"))
        return
    if text=="美股新聞":
        news=get_news("美股 華爾街 財經",4,trusted_only=True)
        reply_text(event.reply_token, format_news_text(news,"🇺🇸 美股新聞"))
        return
    if text=="國際新聞":
        news=get_news("國際財經 全球市場 Fed",4,trusted_only=True)
        reply_text(event.reply_token, format_news_text(news,"🌐 國際財經新聞"))
        return
    if text=="地緣政治新聞":
        news=get_news("地緣政治 貿易戰 美中 台海",4,trusted_only=True)
        reply_text(event.reply_token, format_news_text(news,"🌏 地緣政治"))
        return
    if text=="個股新聞":
        reply_text_with_qr(event.reply_token,
            "📊 個股新聞\n━━━━━━━━━━━━━━\n請直接輸入股票代號查詢\n例如：2330",
            [("台積電","2330"),("聯發科","2454"),("鴻海","2317"),("廣達","2382")])
        return

    # ══ AI 選股 ══
    if text in ["推薦股","今日推薦股"]:
        log_to_sheets(user_id,"查詢推薦股","","成功")
        reply_text(event.reply_token,
              "⭐ 推薦股分析中...\n━━━━━━━━━━━━━━\n"
              "正在整合法人籌碼、技術面、新聞情緒\n約 15～30 秒後將推送結果 📊")
        t=threading.Thread(target=build_and_push_recommendation,args=(user_id,))
        t.daemon=True; t.start()
        return

    if text in ["趨勢股","成長股","存股","波段股","AI概念股"]:
        reply_text(event.reply_token,
            f"🤖 {text} 分析功能開發中...\n━━━━━━━━━━━━━━\n"
            f"目前請使用「推薦股」功能\n後續版本將加入更多 AI 分析類別 🚀")
        return

    # ══ Owner 專屬指令 ══
    if is_owner(user_id):
        if text.startswith("新增管理者 "):
            parts=text.split(" ",2)
            if len(parts)==3:
                _,uid,name=parts
                reply_text(event.reply_token, add_admin(uid.strip(),name.strip()))
            else:
                reply_text(event.reply_token,"格式：新增管理者 user_id 姓名")
            return
        elif text.startswith("移除管理者 "):
            name=text.replace("移除管理者 ","").strip()
            if name: reply_text(event.reply_token, remove_admin(name))
            return
        elif text=="管理者名單":
            reply_text(event.reply_token, get_admin_list()); return
        elif text=="快取狀態":
            total=len(NAME_CACHE)
            samples=list(NAME_CACHE.items())[:5]
            ss="\n".join(f"　{k}：{v}" for k,v in samples)
            reply_text(event.reply_token,
                f"📊 NAME_CACHE 狀態\n━━━━━━━━━━━━━━\n"
                f"總筆數：{total}\n載入完成：{'✅' if NAME_CACHE_LOADED else '⏳載入中'}\n前5筆：\n{ss}"); return
        elif text.startswith("查快取 "):
            sid=text.replace("查快取 ","").strip()
            cached=NAME_CACHE.get(sid,"(無)")
            reply_text(event.reply_token,
                f"🔍 快取查詢\n━━━━━━━━━━━━━━\n"
                f"代號：{sid}\n快取名稱：{cached}\n"
                f"總筆數：{len(NAME_CACHE)}\n中文：{'✅' if has_chinese(cached) else '❌'}"); return
        elif text=="重載名稱":
            if not NAME_CACHE_LOADING:
                t=threading.Thread(target=init_name_cache); t.daemon=True; t.start()
                reply_text(event.reply_token,"🔄 開始重新載入名稱快取，約30秒後完成")
            else:
                reply_text(event.reply_token,"⏳ 名稱快取載入中，請稍後")
            return

    # ══ 管理者共用指令 ══
    if is_admin(user_id):
        if text.startswith("封鎖 "):
            parts=text.split(" ",2); name=parts[1] if len(parts)>1 else ""; reason=parts[2] if len(parts)>2 else "未說明"
            if name: reply_text(event.reply_token, block_user_by_name(name,reason)); return
        elif text.startswith("解除封鎖 "):
            name=text.replace("解除封鎖 ","").strip()
            if name: reply_text(event.reply_token, unblock_user_by_name(name)); return
        elif text=="使用者列表":
            reply_text(event.reply_token, get_user_list()); return
        elif text.startswith("查使用者 "):
            name=text.replace("查使用者 ","").strip()
            if name: reply_text(event.reply_token, get_user_detail(name)); return

    # ══ 說明按鈕回應 ══
    if text=="封鎖說明": reply_text(event.reply_token,"格式：封鎖 姓名 原因\n例如：封鎖 王小明 違規"); return
    if text=="解除封鎖說明": reply_text(event.reply_token,"格式：解除封鎖 姓名\n例如：解除封鎖 王小明"); return
    if text=="查使用者說明": reply_text(event.reply_token,"格式：查使用者 姓名\n例如：查使用者 王小明"); return
    if text=="新增管理者說明": reply_text(event.reply_token,"格式：新增管理者 user_id 姓名"); return
    if text=="移除管理者說明": reply_text(event.reply_token,"格式：移除管理者 姓名"); return
    if text=="新增持股說明": reply_text(event.reply_token,"格式：新增 代碼 股數 買入價\n例如：新增 2330 100 200"); return
    if text=="停損提醒說明": reply_text(event.reply_token,"停損提醒功能開發中 🚧\n後續版本將開放設定"); return
    if text=="目標價提醒說明": reply_text(event.reply_token,"目標價提醒功能開發中 🚧\n後續版本將開放設定"); return
    if text=="損益分析": reply_text(event.reply_token, get_portfolio_summary(user_id)); return
    if text=="查快取說明": reply_text(event.reply_token,"格式：查快取 代號\n例如：查快取 2330"); return

    # ══ 查股票子選單 ══
    if text=="查台股":
        reply_text_with_qr(event.reply_token,
            "🇹🇼 台股查詢\n━━━━━━━━━━━━━━\n請直接輸入股票代號\n例如：2330",
            [("台積電","2330"),("聯發科","2454"),("鴻海","2317"),
             ("廣達","2382"),("台達電","2308")])
        return
    if text=="查美股":
        reply_text_with_qr(event.reply_token,
            "🇺🇸 美股查詢\n━━━━━━━━━━━━━━\n請直接輸入股票代號\n例如：AAPL",
            [("NVDA","NVDA"),("AAPL","AAPL"),("MSFT","MSFT"),
             ("META","META"),("TSLA","TSLA")])
        return
    if text=="查ETF":
        reply_text_with_qr(event.reply_token,
            "📊 ETF查詢\n━━━━━━━━━━━━━━\n請直接輸入ETF代號",
            [("0050","0050"),("00878","00878"),("006208","006208"),
             ("0056","0056"),("00919","00919")])
        return
    if text=="查興上櫃":
        reply_text_with_qr(event.reply_token,
            "🏪 興/上櫃查詢\n━━━━━━━━━━━━━━\n請直接輸入股票代號",
            [("嘉澤","3533"),("頎邦","6147"),("旺矽","6223")])
        return
    if text=="查自選股":
        reply_text(event.reply_token, get_portfolio_summary(user_id)); return

    # ══ 查詢分類子選單 ══
    if text in ["查Fed利率","查BOJ","查ECB","查避險資金","查匯率台股",
                "查匯率美股","查匯率黃金","查匯率半導體"]:
        query_map = {
            "查Fed利率": "Fed 利率 美元 影響",
            "查BOJ":    "日本央行 BOJ 日圓 利率",
            "查ECB":    "歐洲央行 ECB 歐元 利率",
            "查避險資金": "避險資金 黃金 美債 日圓",
            "查匯率台股": "美元台幣 匯率 台股 影響",
            "查匯率美股": "美元指數 DXY 美股 影響",
            "查匯率黃金": "美元 黃金 匯率 關係",
            "查匯率半導體": "美元 半導體 匯率 出口",
        }
        label_map = {
            "查Fed利率": "Fed利率影響分析",
            "查BOJ":    "日本央行BOJ分析",
            "查ECB":    "歐洲央行ECB分析",
            "查避險資金": "避險資金流向",
            "查匯率台股": "匯率對台股影響",
            "查匯率美股": "匯率對美股影響",
            "查匯率黃金": "匯率對黃金影響",
            "查匯率半導體": "匯率對半導體影響",
        }
        news=get_news(query_map[text],4,trusted_only=True)
        reply_text(event.reply_token, format_news_text(news, label_map[text]))
        return

    # ══ 註冊 ══
    if text.startswith("註冊 "):
        reg_name=text.replace("註冊 ","").strip()
        if reg_name:
            result=register_user(user_id,reg_name)
            reply_text(event.reply_token,result)
            if "成功" in result:
                push_to_owner(f"🆕 新用戶註冊！\n姓名：{reg_name}\n時間：{now_taipei().strftime('%Y-%m-%d %H:%M')}")
        else:
            reply_text(event.reply_token,"格式：註冊 姓名\n例如：註冊 王小明")
        return

    if not is_registered(user_id):
        reply_text(event.reply_token,
              "👋 歡迎使用慧股拾光 Lumistock！\n━━━━━━━━━━━━━━\n"
              "請先完成註冊才能使用全部功能\n\n"
              "📝 註冊方式：\n　輸入「註冊 您的姓名」\n\n　例如：\n　註冊 王小明")
        return

    # ══ WAITING_SUGGESTION ══
    if user_id in WAITING_SUGGESTION:
        WAITING_SUGGESTION.discard(user_id)
        save_suggestion_to_sheets(user_id,text)
        push_to_owner(f"💬 收到新建議！\n時間：{now_taipei().strftime('%Y-%m-%d %H:%M')}\n內容：{text}")
        reply_text(event.reply_token,"✅ 感謝您的建議！\n我們會持續改善 Lumistock 🌱")
        return

    # ══ 持股管理指令 ══
    if text=="持股": reply_text(event.reply_token, get_portfolio_summary(user_id)); return

    if text.startswith("新增 "):
        parts=text.split()
        if len(parts)==4:
            symbol=parts[1].upper(); market="台股" if symbol.isdigit() else "美股"
            if symbol.isdigit(): symbol+=".TW"
            try:
                p=load_portfolio()
                p[symbol]={"shares":int(parts[2]),"buy_price":float(parts[3]),"user_id":user_id}
                save_portfolio(p)
                tw=get_tw_stock(parts[1]) if parts[1].isdigit() else None
                us=get_us_stock(symbol) if not parts[1].isdigit() else None
                name=(tw or us or {}).get("name",symbol) if (tw or us) else symbol
                if not name or name==symbol:
                    name=NAME_CACHE.get(parts[1],symbol) if parts[1].isdigit() else symbol
                save_portfolio_to_sheets(user_id,symbol,name,market,int(parts[2]),float(parts[3]))
                log_to_sheets(user_id,"新增持股",symbol,"成功")
                reply_text(event.reply_token,
                      f"✅ 新增成功\n━━━━━━━━━━━━━━\n　{symbol}｜{name}\n　{parts[2]} 股　均價 {parts[3]}")
            except: reply_text(event.reply_token,"格式錯誤\n範例：新增 2330 100 200")
        else: reply_text(event.reply_token,"格式：新增 代碼 股數 買入價\n範例：新增 2330 100 200")
        return

    if text.startswith("刪除 "):
        parts=text.split()
        if len(parts)==2:
            symbol=parts[1].upper()
            if symbol.isdigit(): symbol+=".TW"
            p=load_portfolio()
            if symbol in p:
                del p[symbol]; save_portfolio(p)
                delete_portfolio_from_sheets(user_id,symbol)
                reply_text(event.reply_token,f"✅ 已刪除 {symbol}")
            else: reply_text(event.reply_token,f"找不到 {symbol}")
        else: reply_text(event.reply_token,"格式：刪除 代碼\n範例：刪除 2330")
        return

    # ══ 大盤快捷 ══
    if text in ["大盤","全球大盤行情"]:
        reply_text(event.reply_token, get_market_summary()); return

    # ══ 說明 ══
    if text in ["說明","help","Help","?"]:
        reply_text(event.reply_token, HELP_MSG); return

    # ══ 股票代號查詢 ══
    t=text.upper().replace("查","").strip()
    if t and (t.isdigit() or (t.isalpha() and len(t)>=1) or t.replace("-","").isalnum()):
        flex,err=get_stock_flex(t,user_id)
        if flex: reply_flex(event.reply_token,flex,f"{t} 股票資訊")
        else: reply_text(event.reply_token,err or "查詢失敗")
        return

    reply_text(event.reply_token, HELP_MSG)


if __name__=="__main__":
    print("慧股拾光 Lumistock LINE Bot v11.0 啟動中...")
    for code,name in FALLBACK_NAMES.items():
        NAME_CACHE[code]=name
    t=threading.Thread(target=_bg_init); t.daemon=True; t.start()
    setup_rich_menus()
    port=int(os.environ.get("PORT",5001))
    app.run(host="0.0.0.0",port=port,debug=False)
