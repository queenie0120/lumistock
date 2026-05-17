"""
慧股拾光 Lumistock – by Hui
LINE Bot 模組 v10.9.32（台灣金價修復 + Dubai 原油 + 美債順序 + 殖利率 AI 解讀）

【本次更新】
1. Rich Menu 從 3 張圖升級為 5 張圖 Alias 切換
   - richmenu_user.png      一般用戶（玫瑰金）
   - richmenu_owner_main.png  Owner 主頁（粉白少女）
   - richmenu_owner_admin.png Owner 管理頁
   - richmenu_admin_main.png  管理者主頁（粉紫）
   - richmenu_admin_mgmt.png  管理者管理頁
2. 圖片路徑改為 static/richmenu/
3. 新增 debug log：每筆訊息、每個 handler 都會記錄
4. 完整保留 v10.9.20 所有功能（查股票/推薦股/新聞/權限/Sheets/Flex/Quick Reply）
"""

from flask import Flask, request, abort
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi,
    ReplyMessageRequest, TextMessage, PushMessageRequest,
    FlexMessage, FlexContainer, QuickReply, QuickReplyItem,
    MessageAction, PostbackAction
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent, PostbackEvent
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


# ══════════════════════════════════════════
#  Debug Log（新增 v10.9.21）
# ══════════════════════════════════════════
def dlog(category: str, msg: str):
    """統一 debug log 格式：[時間][類別] 訊息"""
    ts = datetime.now(TZ_TAIPEI).strftime("%H:%M:%S")
    print(f"[{ts}][{category}] {msg}", flush=True)


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
        dlog("STARTUP", f"保底名稱立即載入：{len(FALLBACK_NAMES)} 筆")
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
            r = requests.get(url, headers=headers, timeout=30, verify=False)
            if r.status_code == 200 and r.text.strip().startswith("["):
                count = 0
                for item in r.json():
                    code = str(item.get("公司代號","")).strip()
                    name = (str(item.get("公司簡稱","")) or str(item.get("公司名稱",""))).strip()
                    if code and name and has_chinese(name):
                        NAME_CACHE[code] = name
                        count += 1
                if count > 0:
                    dlog("CACHE", f"{label}：{count} 筆")
                    return count
        except Exception as e:
            dlog("CACHE", f"{label} 第{attempt+1}次失敗：{e}")
            time.sleep(2)
    return 0

def _load_twse_stock_day_all() -> int:
    headers = {"User-Agent": "Mozilla/5.0"}
    for attempt in range(3):
        try:
            r = requests.get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL",
                           headers=headers, timeout=30, verify=False)
            if r.status_code == 200 and r.text.strip().startswith("["):
                count = 0
                for item in r.json():
                    code = str(item.get("Code","")).strip()
                    name = str(item.get("Name","")).strip()
                    if code and name and has_chinese(name):
                        NAME_CACHE[code] = name
                        count += 1
                if count > 100:
                    dlog("CACHE", f"TWSE STOCK_DAY_ALL：{count} 筆")
                    return count
        except Exception as e:
            dlog("CACHE", f"STOCK_DAY_ALL 第{attempt+1}次失敗：{e}")
            time.sleep(2)
    return 0

def _load_tpex_quotes() -> int:
    headers = {"User-Agent": "Mozilla/5.0"}
    for attempt in range(3):
        try:
            r = requests.get("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes",
                           headers=headers, timeout=30, verify=False)
            count = 0
            for item in r.json():
                code = str(item.get("SecuritiesCompanyCode","")).strip()
                name = str(item.get("CompanyName","")).strip()
                if code and name and has_chinese(name):
                    NAME_CACHE[code] = name
                    count += 1
            if count > 0:
                dlog("CACHE", f"TPEx mainboard_quotes：{count} 筆")
                return count
        except Exception as e:
            dlog("CACHE", f"TPEx quotes 第{attempt+1}次失敗：{e}")
            time.sleep(2)
    return 0


def _load_tpex_etf() -> int:
    """上櫃 ETF 名單（v10.9.27 新增）"""
    headers = {"User-Agent": "Mozilla/5.0"}
    for attempt in range(2):
        try:
            r = requests.get("https://www.tpex.org.tw/openapi/v1/tpex_etf_summary_quotes",
                           headers=headers, timeout=20, verify=False)
            count = 0
            for item in r.json():
                code = (str(item.get("SecuritiesCompanyCode","")) or str(item.get("Code",""))).strip()
                name = (str(item.get("CompanyName","")) or str(item.get("Name",""))).strip()
                if code and name and has_chinese(name):
                    NAME_CACHE[code] = name
                    count += 1
            if count > 0:
                dlog("CACHE", f"TPEx ETF：{count} 筆")
                return count
        except Exception as e:
            dlog("CACHE", f"TPEx ETF 第{attempt+1}次失敗：{e}")
            time.sleep(2)
    return 0


def _load_tpex_emerging() -> int:
    """興櫃股票（v10.9.27 新增）"""
    headers = {"User-Agent": "Mozilla/5.0"}
    for attempt in range(2):
        try:
            r = requests.get("https://www.tpex.org.tw/openapi/v1/tpex_esb_latest_statistics",
                           headers=headers, timeout=20, verify=False)
            count = 0
            for item in r.json():
                code = (str(item.get("SecuritiesCompanyCode","")) or str(item.get("Code",""))).strip()
                name = (str(item.get("CompanyName","")) or str(item.get("Name",""))).strip()
                if code and name and has_chinese(name):
                    NAME_CACHE[code] = name
                    count += 1
            if count > 0:
                dlog("CACHE", f"TPEx 興櫃：{count} 筆")
                return count
        except Exception as e:
            dlog("CACHE", f"TPEx 興櫃第{attempt+1}次失敗：{e}")
            time.sleep(2)
    return 0


def _load_twse_etf() -> int:
    """上市 ETF 名單（v10.9.27 新增）"""
    headers = {"User-Agent": "Mozilla/5.0"}
    for attempt in range(2):
        try:
            # ETF e添富 JSON
            r = requests.get("https://www.twse.com.tw/rwd/zh/ETFortune/ETFRanking?response=json",
                           headers=headers, timeout=20, verify=False)
            data = r.json()
            count = 0
            for row in data.get("data", []):
                if len(row) >= 2:
                    code = str(row[0]).strip()
                    name = str(row[1]).strip()
                    if code and name and has_chinese(name):
                        NAME_CACHE[code] = name
                        count += 1
            if count > 0:
                dlog("CACHE", f"TWSE ETF：{count} 筆")
                return count
        except Exception as e:
            dlog("CACHE", f"TWSE ETF 第{attempt+1}次失敗：{e}")
            time.sleep(2)
    return 0


def _load_twse_securities_list() -> int:
    """證券基本資料（包含全部上市股票，最完整）（v10.9.27 新增）"""
    headers = {"User-Agent": "Mozilla/5.0"}
    for attempt in range(2):
        try:
            # 證券編碼公告檔
            r = requests.get("https://isin.twse.com.tw/isin/C_public.jsp?strMode=2",
                           headers=headers, timeout=20, verify=False)
            r.encoding = "big5"
            count = 0
            # 解析 HTML 表格（簡單版）
            import re as _re
            rows = _re.findall(r"<tr[^>]*>(.*?)</tr>", r.text, _re.DOTALL)
            for row in rows:
                cells = _re.findall(r"<td[^>]*>(.*?)</td>", row, _re.DOTALL)
                if len(cells) >= 2:
                    first = _re.sub(r"<[^>]+>","",cells[0]).strip()
                    # 格式像「2330　台積電」或「0050　元大台灣50」
                    parts = first.replace("\u3000", " ").split()
                    if len(parts) >= 2:
                        code = parts[0].strip()
                        name = parts[1].strip()
                        if code and name and has_chinese(name) and (code.isdigit() or code[:1].isdigit()):
                            NAME_CACHE[code] = name
                            count += 1
            if count > 100:
                dlog("CACHE", f"TWSE ISIN 證券公告：{count} 筆")
                return count
        except Exception as e:
            dlog("CACHE", f"TWSE ISIN 第{attempt+1}次失敗：{e}")
            time.sleep(2)
    return 0


def init_name_cache():
    global NAME_CACHE_LOADING, NAME_CACHE_LOADED
    if NAME_CACHE_LOADING: return
    NAME_CACHE_LOADING = True

    # ── 第一輪：核心 API（上市/上櫃公司 + 報價）
    _load_twse_stock_day_all()
    _load_opendata("https://openapi.twse.com.tw/v1/opendata/t187ap03_L","上市公司")
    _load_opendata("https://openapi.twse.com.tw/v1/opendata/t187ap03_O","上櫃公司")
    _load_opendata("https://openapi.twse.com.tw/v1/opendata/t187ap03_R","興櫃公司")
    _load_tpex_quotes()

    # ── 第二輪：ETF（v10.9.27 新增）
    _load_twse_etf()
    _load_tpex_etf()

    # ── 第三輪：興櫃股票（v10.9.27 新增）
    _load_tpex_emerging()

    # ── 第四輪：證券公告檔（最完整）（v10.9.27 新增）
    _load_twse_securities_list()

    # ── 第五輪：rwd 備援
    headers = {"User-Agent": "Mozilla/5.0"}
    for attempt in range(2):
        try:
            r = requests.get("https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL?response=json",
                           headers=headers, timeout=20, verify=False)
            count = 0
            for item in r.json().get("data",[]):
                if len(item) >= 2:
                    code = str(item[0]).strip()
                    name = str(item[1]).strip()
                    if code and name and has_chinese(name):
                        NAME_CACHE[code] = name
                        count += 1
            if count > 100:
                dlog("CACHE", f"TWSE rwd備援：{count} 筆")
                break
        except Exception as e:
            dlog("CACHE", f"TWSE rwd第{attempt+1}次失敗：{e}")
            time.sleep(2)

    # ── 補保底
    for code, name in FALLBACK_NAMES.items():
        if not has_chinese(NAME_CACHE.get(code,"")): NAME_CACHE[code] = name

    NAME_CACHE_LOADING = False
    NAME_CACHE_LOADED  = True
    dlog("CACHE", f"✅ 名稱快取完整載入：{len(NAME_CACHE)} 筆")
    try:
        push_to_owner(f"✅ Lumistock 啟動完成\n名稱快取：{len(NAME_CACHE)} 筆\n{now_taipei().strftime('%m/%d %H:%M')}")
    except: pass


# ══════════════════════════════════════════════════════════
#  Rich Menu v10.9.21（5 張圖 Alias 多頁切換）⭐ 本次重點改動
# ══════════════════════════════════════════════════════════
ALIAS_USER        = "lumistock-user"
ALIAS_OWNER_MAIN  = "lumistock-owner-main"
ALIAS_OWNER_ADMIN = "lumistock-owner-admin"
ALIAS_ADMIN_MAIN  = "lumistock-admin-main"
ALIAS_ADMIN_MGMT  = "lumistock-admin-mgmt"


def _delete_all_rich_menus():
    try:
        r = requests.get("https://api.line.me/v2/bot/richmenu/list",
                        headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"})
        menus = r.json().get("richmenus", [])
        dlog("RICHMENU", f"刪除舊 Rich Menu：{len(menus)} 個")
        for menu in menus:
            requests.delete(f"https://api.line.me/v2/bot/richmenu/{menu['richMenuId']}",
                          headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"})
    except Exception as e:
        dlog("RICHMENU", f"刪除舊 Rich Menu 失敗：{e}")


def _delete_all_aliases():
    try:
        r = requests.get("https://api.line.me/v2/bot/richmenu/alias/list",
                        headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"})
        aliases = r.json().get("aliases", [])
        dlog("RICHMENU", f"刪除舊 Alias：{len(aliases)} 個")
        for alias in aliases:
            requests.delete(f"https://api.line.me/v2/bot/richmenu/alias/{alias['richMenuAliasId']}",
                          headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"})
    except Exception as e:
        dlog("RICHMENU", f"刪除舊 Alias 失敗：{e}")


def _create_rich_menu(body: dict, img_url: str, label: str) -> str:
    """建立單一 Rich Menu，回傳 rich_menu_id；失敗回傳 ''"""
    headers_json = {"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}",
                    "Content-Type": "application/json"}
    # Step 1：註冊 Rich Menu 結構
    r = requests.post("https://api.line.me/v2/bot/richmenu",
                     headers=headers_json, json=body)
    if r.status_code != 200:
        dlog("RICHMENU", f"❌ [{label}] 建立結構失敗 HTTP {r.status_code}：{r.text[:200]}")
        return ""
    rid = r.json().get("richMenuId", "")
    if not rid:
        dlog("RICHMENU", f"❌ [{label}] 沒拿到 richMenuId")
        return ""
    dlog("RICHMENU", f"✅ [{label}] 結構建立 {rid[:20]}...")

    # Step 2：下載圖片
    try:
        img_r = requests.get(img_url, timeout=20)
        if img_r.status_code != 200:
            dlog("RICHMENU", f"❌ [{label}] 圖片下載失敗 HTTP {img_r.status_code}：{img_url}")
            return ""
        img_size_kb = len(img_r.content) / 1024
        dlog("RICHMENU", f"   [{label}] 圖片下載成功：{img_size_kb:.1f} KB")
    except Exception as e:
        dlog("RICHMENU", f"❌ [{label}] 圖片下載例外：{e}")
        return ""

    # Step 3：上傳圖片到 LINE
    try:
        upload = requests.post(f"https://api-data.line.me/v2/bot/richmenu/{rid}/content",
                     headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}",
                              "Content-Type": "image/png"},
                     data=img_r.content)
        if upload.status_code != 200:
            dlog("RICHMENU", f"❌ [{label}] 圖片上傳失敗 HTTP {upload.status_code}：{upload.text[:200]}")
            return ""
        dlog("RICHMENU", f"✅ [{label}] 圖片上傳成功")
    except Exception as e:
        dlog("RICHMENU", f"❌ [{label}] 圖片上傳例外：{e}")
        return ""

    return rid


def _create_alias(alias_id: str, rich_menu_id: str):
    """建立 Alias，已存在時先刪除再建"""
    headers_json = {"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}",
                    "Content-Type": "application/json"}
    requests.delete(f"https://api.line.me/v2/bot/richmenu/alias/{alias_id}",
                   headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"})
    r = requests.post("https://api.line.me/v2/bot/richmenu/alias",
                     headers=headers_json,
                     json={"richMenuAliasId": alias_id, "richMenuId": rich_menu_id})
    if r.status_code == 200:
        dlog("RICHMENU", f"✅ Alias 建立：{alias_id}")
    else:
        dlog("RICHMENU", f"❌ Alias 建立失敗 {alias_id}：{r.text[:200]}")


# 一般用戶（6 格 2x3）
AREAS_USER = [
    {"bounds":{"x":0,   "y":0,   "width":833,"height":843},"action":{"type":"message","text":"查股票"}},
    {"bounds":{"x":833, "y":0,   "width":834,"height":843},"action":{"type":"message","text":"全球大盤"}},
    {"bounds":{"x":1667,"y":0,   "width":833,"height":843},"action":{"type":"message","text":"外匯資金"}},
    {"bounds":{"x":0,   "y":843, "width":833,"height":843},"action":{"type":"message","text":"AI分析"}},
    {"bounds":{"x":833, "y":843, "width":834,"height":843},"action":{"type":"message","text":"財經新聞"}},
    {"bounds":{"x":1667,"y":843, "width":833,"height":843},"action":{"type":"message","text":"持股管理"}},
]

# Owner 主頁（右下「管理後台」→ 切換到 Owner 管理頁）
AREAS_OWNER_MAIN = [
    {"bounds":{"x":0,   "y":0,   "width":833,"height":843},"action":{"type":"message","text":"查股票"}},
    {"bounds":{"x":833, "y":0,   "width":834,"height":843},"action":{"type":"message","text":"全球大盤"}},
    {"bounds":{"x":1667,"y":0,   "width":833,"height":843},"action":{"type":"message","text":"外匯資金"}},
    {"bounds":{"x":0,   "y":843, "width":833,"height":843},"action":{"type":"message","text":"AI分析"}},
    {"bounds":{"x":833, "y":843, "width":834,"height":843},"action":{"type":"message","text":"財經新聞"}},
    {"bounds":{"x":1667,"y":843, "width":833,"height":843},
     "action":{"type":"richmenuswitch","richMenuAliasId":ALIAS_OWNER_ADMIN,"data":"to_owner_admin"}},
]

# Owner 管理頁（右下「返回主頁」→ 切回 Owner 主頁）
AREAS_OWNER_ADMIN = [
    {"bounds":{"x":0,   "y":0,   "width":833,"height":843},"action":{"type":"message","text":"使用者管理"}},
    {"bounds":{"x":833, "y":0,   "width":834,"height":843},"action":{"type":"message","text":"系統管理"}},
    {"bounds":{"x":1667,"y":0,   "width":833,"height":843},"action":{"type":"message","text":"推播管理"}},
    {"bounds":{"x":0,   "y":843, "width":833,"height":843},"action":{"type":"message","text":"AI管理"}},
    {"bounds":{"x":833, "y":843, "width":834,"height":843},"action":{"type":"message","text":"持股管理"}},
    {"bounds":{"x":1667,"y":843, "width":833,"height":843},
     "action":{"type":"richmenuswitch","richMenuAliasId":ALIAS_OWNER_MAIN,"data":"to_owner_main"}},
]

# 管理者主頁（右下「管理後台」→ 切換到管理者管理頁）
AREAS_ADMIN_MAIN = [
    {"bounds":{"x":0,   "y":0,   "width":833,"height":843},"action":{"type":"message","text":"查股票"}},
    {"bounds":{"x":833, "y":0,   "width":834,"height":843},"action":{"type":"message","text":"全球大盤"}},
    {"bounds":{"x":1667,"y":0,   "width":833,"height":843},"action":{"type":"message","text":"外匯資金"}},
    {"bounds":{"x":0,   "y":843, "width":833,"height":843},"action":{"type":"message","text":"AI分析"}},
    {"bounds":{"x":833, "y":843, "width":834,"height":843},"action":{"type":"message","text":"財經新聞"}},
    {"bounds":{"x":1667,"y":843, "width":833,"height":843},
     "action":{"type":"richmenuswitch","richMenuAliasId":ALIAS_ADMIN_MGMT,"data":"to_admin_mgmt"}},
]

# 管理者管理頁（3 格 1x3）
AREAS_ADMIN_MGMT = [
    {"bounds":{"x":0,    "y":0, "width":833, "height":1686},"action":{"type":"message","text":"使用者管理"}},
    {"bounds":{"x":833,  "y":0, "width":834, "height":1686},"action":{"type":"message","text":"持股管理"}},
    {"bounds":{"x":1667, "y":0, "width":833, "height":1686},
     "action":{"type":"richmenuswitch","richMenuAliasId":ALIAS_ADMIN_MAIN,"data":"to_admin_main"}},
]


RICH_MENU_IDS = {}


def setup_rich_menus():
    global RICH_MENU_IDS
    dlog("RICHMENU", "🌸 開始建立 Rich Menu (v10.9.21 - 5張圖 Alias)")
    _delete_all_aliases()
    _delete_all_rich_menus()
    base_url = "https://raw.githubusercontent.com/queenie0120/lumistock/main/static/richmenu"

    # 1. 一般用戶（玫瑰金）
    uid = _create_rich_menu(
        {"size":{"width":2500,"height":1686},"selected":True,
         "name":"一般用戶選單","chatBarText":"✨ 慧股拾光 功能選單",
         "areas": AREAS_USER},
        f"{base_url}/richmenu_user.png", "user")
    if uid:
        RICH_MENU_IDS["user"] = uid
        _create_alias(ALIAS_USER, uid)
        # 設為全體新加好友的預設選單
        r = requests.post(f"https://api.line.me/v2/bot/user/all/richmenu/{uid}",
                     headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"})
        dlog("RICHMENU", f"✅ user 設為預設選單 HTTP {r.status_code}")

    # 2. Owner 主頁（粉白少女）
    omid = _create_rich_menu(
        {"size":{"width":2500,"height":1686},"selected":True,
         "name":"Owner主頁","chatBarText":"👑 慧股拾光 Owner",
         "areas": AREAS_OWNER_MAIN},
        f"{base_url}/richmenu_owner_main.png", "owner_main")
    if omid:
        RICH_MENU_IDS["owner_main"] = omid
        _create_alias(ALIAS_OWNER_MAIN, omid)

    # 3. Owner 管理頁
    oaid = _create_rich_menu(
        {"size":{"width":2500,"height":1686},"selected":True,
         "name":"Owner管理頁","chatBarText":"👑 Owner 管理後台",
         "areas": AREAS_OWNER_ADMIN},
        f"{base_url}/richmenu_owner_admin.png", "owner_admin")
    if oaid:
        RICH_MENU_IDS["owner_admin"] = oaid
        _create_alias(ALIAS_OWNER_ADMIN, oaid)

    # 4. 管理者主頁（粉紫）
    amid = _create_rich_menu(
        {"size":{"width":2500,"height":1686},"selected":True,
         "name":"管理者主頁","chatBarText":"🛡️ 慧股拾光 管理者",
         "areas": AREAS_ADMIN_MAIN},
        f"{base_url}/richmenu_admin_main.png", "admin_main")
    if amid:
        RICH_MENU_IDS["admin_main"] = amid
        _create_alias(ALIAS_ADMIN_MAIN, amid)

    # 5. 管理者管理頁
    amgid = _create_rich_menu(
        {"size":{"width":2500,"height":1686},"selected":True,
         "name":"管理者管理頁","chatBarText":"🛡️ 管理者後台",
         "areas": AREAS_ADMIN_MGMT},
        f"{base_url}/richmenu_admin_mgmt.png", "admin_mgmt")
    if amgid:
        RICH_MENU_IDS["admin_mgmt"] = amgid
        _create_alias(ALIAS_ADMIN_MGMT, amgid)

    # 綁定 Owner 個人選單
    if omid:
        r = requests.post(f"https://api.line.me/v2/bot/user/{OWNER_USER_ID}/richmenu/{omid}",
                     headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"})
        dlog("RICHMENU", f"✅ Owner 個人選單綁定 HTTP {r.status_code}")

    dlog("RICHMENU", "=" * 50)
    dlog("RICHMENU", f"Rich Menu 設定完成總結：")
    dlog("RICHMENU", f"  user        = {uid or '❌ FAIL'}")
    dlog("RICHMENU", f"  owner_main  = {omid or '❌ FAIL'}")
    dlog("RICHMENU", f"  owner_admin = {oaid or '❌ FAIL'}")
    dlog("RICHMENU", f"  admin_main  = {amid or '❌ FAIL'}")
    dlog("RICHMENU", f"  admin_mgmt  = {amgid or '❌ FAIL'}")
    dlog("RICHMENU", "=" * 50)


def assign_rich_menu(user_id: str):
    """新用戶或角色變更時指派正確主頁選單"""
    if user_id == OWNER_USER_ID:
        rid = RICH_MENU_IDS.get("owner_main","")
        role = "owner"
    elif is_admin(user_id):
        rid = RICH_MENU_IDS.get("admin_main","")
        role = "admin"
    else:
        rid = RICH_MENU_IDS.get("user","")
        role = "user"
    if rid:
        r = requests.post(f"https://api.line.me/v2/bot/user/{user_id}/richmenu/{rid}",
                     headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"})
        if r.status_code == 200:
            dlog("RICHMENU", f"✅ 綁定 {role}：{user_id[:10]}...")
        else:
            dlog("RICHMENU", f"❌ 綁定失敗 {role}：{r.text[:200]}")
    else:
        dlog("RICHMENU", f"⚠️ 角色 {role} 沒有對應的 rich_menu_id")
# ══════════════════════════════════════════════════════════
#  ⬆️ Rich Menu 改動結束
# ══════════════════════════════════════════════════════════


# ══════════════════════════════════════════
#  Quick Reply 工具
# ══════════════════════════════════════════
def make_quick_reply(items: list) -> QuickReply:
    return QuickReply(items=[
        QuickReplyItem(action=MessageAction(label=label, text=text))
        for label, text in items
    ])


# ══════════════════════════════════════════════════════════
#  互動清單模組 v10.9.25（Phase 1 + Phase 2 - 點按鈕代替打字）
#  ⭐ 新增功能：清單按鈕、Postback action、等待狀態管理
# ══════════════════════════════════════════════════════════

# 等待輸入封鎖原因的使用者狀態（key=user_id, value=要被封鎖的對象姓名）
WAITING_BLOCK_REASON = {}


def make_postback_quick_reply(items: list) -> QuickReply:
    """items = [(label, data), ...]  data 格式：action=xxx&param=yyy"""
    return QuickReply(items=[
        QuickReplyItem(action=PostbackAction(label=label, data=data, display_text=label))
        for label, data in items
    ])


def make_action_card(title: str, subtitle: str, color: str, action_buttons: list) -> dict:
    """
    產生一張帶 Postback 按鈕的卡片
    action_buttons = [(label, postback_data), ...]
    """
    if not subtitle or not str(subtitle).strip():
        subtitle = " "
    btn_contents = []
    for label, data in action_buttons:
        btn_contents.append({
            "type":"button","style":"primary","height":"sm","color": color,
            "action":{"type":"postback","label":label,"data":data,"displayText":label}
        })
    return {
        "type":"bubble","size":"kilo",
        "header":{
            "type":"box","layout":"vertical","backgroundColor":color,"paddingAll":"10px",
            "contents":[
                {"type":"text","text":title,"size":"md","color":"#FFFFFF","weight":"bold","wrap":True},
                {"type":"text","text":subtitle,"size":"xxs","color":"#FFFFFF","wrap":True}
            ]
        },
        "body":{
            "type":"box","layout":"vertical","spacing":"xs","paddingAll":"10px",
            "contents": btn_contents
        }
    }


def make_user_list_carousel(page: int = 0) -> dict:
    """使用者列表（每個用戶一張卡片，有「查詳情」「封鎖」按鈕）
    v10.9.27 新增分頁：page=0 為第一頁（前 9 筆），第 10 張是「載入更多」"""
    try:
        sheet = get_sheet("使用者名單")
        if not sheet:
            return None
        records = sheet.get_all_records()
        if not records:
            return None

        # 依「最後互動時間」排序（新到舊）
        records.sort(key=lambda r: str(r.get("最後互動時間","")), reverse=True)

        PER_PAGE = 9  # 一頁 9 張，第 10 張留給「載入更多」
        start = page * PER_PAGE
        end = start + PER_PAGE
        page_records = records[start:end]
        has_more = len(records) > end

        if not page_records:
            return None

        bubbles = []
        for row in page_records:
            name = row.get("註冊姓名","未註冊")
            nick = row.get("LINE暱稱","")
            status = row.get("狀態","")
            icon = "🔴" if status=="封鎖" else ("⚪" if status=="未註冊" else "🟢")
            subtitle = f"{icon} {status}　{nick[:10]}"

            buttons = [("🔍 查詳情", f"action=user_detail&name={name}")]
            if status != "封鎖":
                buttons.append(("🔴 封鎖", f"action=block_start&name={name}"))
                buttons.append(("👑 設為管理者", f"action=add_admin&name={name}"))
            else:
                buttons.append(("🟢 解除封鎖", f"action=unblock&name={name}"))

            bubbles.append(make_action_card(f"👤 {name}", subtitle, "#E8B8A8", buttons))

        # 加「載入更多」卡片
        if has_more:
            remaining = len(records) - end
            more_card = {
                "type":"bubble","size":"kilo",
                "header":{
                    "type":"box","layout":"vertical","backgroundColor":"#C9B0DB","paddingAll":"10px",
                    "contents":[
                        {"type":"text","text":"📋 還有更多用戶","size":"md","color":"#FFFFFF","weight":"bold"},
                        {"type":"text","text":f"剩餘 {remaining} 位","size":"xxs","color":"#FFFFFF"}
                    ]
                },
                "body":{
                    "type":"box","layout":"vertical","spacing":"sm","paddingAll":"10px",
                    "contents":[
                        {"type":"button","style":"primary","height":"sm","color":"#C9B0DB",
                         "action":{"type":"postback","label":f"➡️ 載入下 {min(PER_PAGE, remaining)} 位",
                                   "data":f"action=user_list_page&page={page+1}",
                                   "displayText":"載入下一頁"}}
                    ]
                }
            }
            bubbles.append(more_card)

        return {"type":"carousel","contents":bubbles}
    except Exception as e:
        dlog("UI", f"make_user_list_carousel 失敗：{e}")
        return None


def make_user_search_quickreply(filter_status: str = "all", batch: int = 0) -> tuple:
    """
    產生 Quick Reply：列出使用者名字。
    filter_status: "all" / "正常" / "封鎖" / "未註冊"
    batch: 0 = 前 12 個，1 = 第 13-24 個，依此類推
    回傳：(items 列表, total 該篩選總人數, has_more 是否還有更多)
    """
    try:
        sheet = get_sheet("使用者名單")
        if not sheet: return [], 0, False
        records = sheet.get_all_records()
        # 篩選
        if filter_status != "all":
            records = [r for r in records if str(r.get("狀態","")) == filter_status]
        # 依互動時間排序，新到舊
        records.sort(key=lambda r: str(r.get("最後互動時間","")), reverse=True)

        total = len(records)
        PER_BATCH = 12  # 留 1 格給「下一批」
        start = batch * PER_BATCH
        end = start + PER_BATCH
        batch_records = records[start:end]
        has_more = total > end

        items = []
        for row in batch_records:
            name = row.get("註冊姓名","")
            nick = row.get("LINE暱稱","")
            display = name if name else f"未註冊({nick[:6]})"
            target = name if name else nick
            if target:
                items.append((display[:10], f"action=user_card&name={target}"))

        # 加「下一批」按鈕（如果還有更多）
        if has_more:
            items.append(("➡️ 下一批", f"action=user_search&filter={filter_status}&batch={batch+1}"))

        return items, total, has_more
    except Exception as e:
        dlog("UI", f"make_user_search_quickreply 失敗：{e}")
        return [], 0, False


def make_user_text_list(filter_status: str = "all") -> tuple:
    """產生文字版使用者列表
    filter_status: "all" / "正常" / "封鎖" / "未註冊"
    回傳：(訊息文字, 該篩選總人數)
    """
    try:
        sheet = get_sheet("使用者名單")
        if not sheet: return "❌ 無法讀取使用者名單", 0
        records = sheet.get_all_records()
        # 篩選
        if filter_status != "all":
            records = [r for r in records if str(r.get("狀態","")) == filter_status]
        records.sort(key=lambda r: str(r.get("最後互動時間","")), reverse=True)
        total = len(records)
        if total == 0:
            label = {"all":"使用者", "正常":"正常用戶", "封鎖":"封鎖用戶", "未註冊":"未註冊用戶"}.get(filter_status, "用戶")
            return f"📋 目前沒有{label}", 0

        # 構造文字
        title_map = {
            "all":"👥 使用者名單",
            "正常":"🟢 正常用戶名單",
            "封鎖":"🔴 封鎖用戶名單",
            "未註冊":"⚪ 未註冊用戶名單",
        }
        title = title_map.get(filter_status, "👥 使用者名單")
        msg = f"{title}（共 {total} 人）\n━━━━━━━━━━━━━━\n"

        for row in records[:30]:  # 文字最多顯示 30 個
            name = row.get("註冊姓名","")
            nick = row.get("LINE暱稱","")
            status = row.get("狀態","")
            last = str(row.get("最後互動時間",""))[:10]  # 只取日期
            icon = "🔴" if status=="封鎖" else ("⚪" if status=="未註冊" else "🟢")
            display = name if name else f"未註冊({nick[:6]})"
            msg += f"{icon} {display}　{last}\n"

        if total > 30:
            msg += f"\n（顯示前 30 位，共 {total} 人）"

        msg += "\n\n🔍 點下方按鈕進入個人操作"
        return msg, total
    except Exception as e:
        dlog("UI", f"make_user_text_list 失敗：{e}")
        return f"❌ 列表載入失敗：{e}", 0


def make_single_user_action_flex(reg_name: str) -> dict:
    """單人操作 Flex 卡片（含查詳情/封鎖/設管理者按鈕）"""
    try:
        sheet = get_sheet("使用者名單")
        if not sheet: return None
        target_row = None
        for row in sheet.get_all_records():
            if str(row.get("註冊姓名","")) == reg_name or str(row.get("LINE暱稱","")) == reg_name:
                target_row = row
                break
        if not target_row:
            return None

        name = target_row.get("註冊姓名","") or f"未註冊({target_row.get('LINE暱稱','')[:6]})"
        nick = target_row.get("LINE暱稱","")
        status = target_row.get("狀態","")
        last = str(target_row.get("最後互動時間",""))[:16]
        icon = "🔴" if status=="封鎖" else ("⚪" if status=="未註冊" else "🟢")

        subtitle = f"{icon} {status}　LINE：{nick[:10]}"

        buttons = [
            ("🔍 查詳細資料", f"action=user_detail&name={name}"),
        ]
        if status == "封鎖":
            buttons.append(("🟢 解除封鎖", f"action=unblock&name={name}"))
        elif status == "正常":
            buttons.append(("🔴 封鎖此用戶", f"action=block_start&name={name}"))
            buttons.append(("👑 設為管理者", f"action=add_admin&name={name}"))

        return make_action_card(f"👤 {name}", subtitle + f"\n最後互動：{last}", "#E8B8A8", buttons)
    except Exception as e:
        dlog("UI", f"make_single_user_action_flex 失敗：{e}")
        return None


def make_admin_list_carousel() -> dict:
    """管理者名單（每位旁邊「移除」按鈕）"""
    try:
        sheet = get_sheet("管理者名單")
        if not sheet:
            return None
        records = sheet.get_all_records()
        active = [r for r in records if str(r.get("狀態")) == "正常"]
        if not active:
            return None
        bubbles = []
        for row in active[:10]:
            name = row.get("姓名","")
            uid = str(row.get("user_id",""))
            added = row.get("新增時間","")
            subtitle = f"🟢 正常　{added[:10]}"
            buttons = [("➖ 移除管理者", f"action=remove_admin&name={name}")]
            bubbles.append(make_action_card(f"🛡️ {name}", subtitle, "#E8B8A8", buttons))
        return {"type":"carousel","contents":bubbles}
    except Exception as e:
        dlog("UI", f"make_admin_list_carousel 失敗：{e}")
        return None


def make_blocked_list_carousel() -> dict:
    """黑名單清單（每位旁邊「解除封鎖」按鈕）"""
    try:
        sheet = get_sheet("黑名單")
        if not sheet:
            return None
        records = sheet.get_all_records()
        active = [r for r in records if str(r.get("狀態")) == "封鎖"]
        if not active:
            return None
        bubbles = []
        for row in active[:10]:
            name = row.get("註冊姓名","")
            reason = row.get("封鎖原因","")
            when = row.get("封鎖時間","")
            subtitle = f"🔴 {reason[:15]}　{when[:10]}"
            buttons = [("🟢 解除封鎖", f"action=unblock&name={name}")]
            bubbles.append(make_action_card(f"⛔ {name}", subtitle, "#E8B8A8", buttons))
        return {"type":"carousel","contents":bubbles}
    except Exception as e:
        dlog("UI", f"make_blocked_list_carousel 失敗：{e}")
        return None


def make_portfolio_action_carousel(user_id: str) -> dict:
    """持股清單帶「刪除」按鈕"""
    try:
        portfolio = load_portfolio()
        up = {k:v for k,v in portfolio.items() if v.get("user_id")==user_id}
        if not up:
            return None
        bubbles = []
        for symbol, data in list(up.items())[:10]:
            sid = symbol.replace(".TW","")
            try:
                if sid.isdigit():
                    tw = get_tw_stock(sid)
                    price = tw["price"] if tw else 0
                    name = tw["name"] if tw else sid
                else:
                    us = get_us_stock(symbol)
                    price = us["price"] if us else 0
                    name = us["name"] if us else symbol
                shares = data["shares"]
                bp = data["buy_price"]
                profit = (price - bp) * shares
                pct = (price - bp) / bp * 100 if bp else 0
                icon = "🟢" if profit >= 0 else "🔴"
                subtitle = f"{icon} {shares}股　損益 {profit:+,.0f}（{pct:+.1f}%）"
            except:
                name = sid
                subtitle = "查詢失敗"
            buttons = [("🗑️ 刪除持股", f"action=del_portfolio&symbol={symbol}")]
            bubbles.append(make_action_card(f"📊 {symbol}｜{name}", subtitle, "#E8B8A8", buttons))
        return {"type":"carousel","contents":bubbles}
    except Exception as e:
        dlog("UI", f"make_portfolio_action_carousel 失敗：{e}")
        return None


def get_addable_users_for_admin() -> list:
    """列出可以設為管理者的人（已註冊、未封鎖、非現任管理者）"""
    try:
        user_sheet = get_sheet("使用者名單")
        admin_sheet = get_sheet("管理者名單")
        if not user_sheet or not admin_sheet:
            return []
        # 找出現任管理者的 user_id
        current_admins = set()
        for row in admin_sheet.get_all_records():
            if str(row.get("狀態")) == "正常":
                current_admins.add(str(row.get("user_id","")))
        # 找出可選名單
        candidates = []
        for row in user_sheet.get_all_records():
            uid = str(row.get("user_id",""))
            name = str(row.get("註冊姓名",""))
            status = str(row.get("狀態",""))
            if name and status != "封鎖" and uid not in current_admins:
                candidates.append((name, uid))
        return candidates
    except Exception as e:
        dlog("UI", f"get_addable_users 失敗：{e}")
        return []


# ══════════════════════════════════════════════════════════
#  ⬆️ 互動清單模組結束
# ══════════════════════════════════════════════════════════


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

def add_admin_by_name(reg_name: str) -> str:
    """用註冊姓名新增管理者（自動從使用者名單找 user_id）"""
    try:
        user_sheet = get_sheet("使用者名單")
        admin_sheet = get_sheet("管理者名單")
        if not user_sheet or not admin_sheet:
            return "❌ 無法讀取名單"

        # Step 1：從使用者名單找到該姓名對應的 user_id
        target_uid = None
        for row in user_sheet.get_all_records():
            if str(row.get("註冊姓名")) == reg_name:
                target_uid = str(row.get("user_id", ""))
                break

        if not target_uid:
            return f"❌ 找不到註冊姓名為「{reg_name}」的用戶\n請確認對方已完成註冊"

        # Step 2：檢查是否已是管理者
        for row in admin_sheet.get_all_records():
            if str(row.get("user_id")) == target_uid:
                if str(row.get("狀態")) == "正常":
                    return f"⚠️ {reg_name} 已經是管理者了"
                else:
                    # 之前被停用過，重新啟用
                    for i, r in enumerate(admin_sheet.get_all_records(), start=2):
                        if str(r.get("user_id")) == target_uid:
                            admin_sheet.update_cell(i, 5, "正常")
                            assign_rich_menu(target_uid)
                            return f"✅ 已重新啟用管理者：{reg_name}"

        # Step 3：新增管理者
        admin_sheet.append_row([target_uid, reg_name,
                                now_taipei().strftime("%Y-%m-%d %H:%M"),
                                "Owner", "正常"])
        assign_rich_menu(target_uid)
        return f"✅ 已新增管理者：{reg_name}"
    except Exception as e:
        return f"❌ 新增失敗：{e}"


def add_admin(user_id: str, name: str) -> str:
    """舊版：直接用 user_id 新增（向下相容，保留不刪）"""
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
        dlog("USER", f"註冊失敗：{e}")
        return "❌ 註冊失敗，請稍後再試"

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
    except Exception as e: dlog("PUSH", f"push_flex失敗：{e}")


# ══════════════════════════════════════════
#  Flex 選單卡片
# ══════════════════════════════════════════
def make_menu_flex(title: str, subtitle: str, color: str, buttons: list) -> dict:
    # 防呆：LINE Flex 的 text 不接受空字串，空白時填一個非空字
    if not subtitle or not str(subtitle).strip():
        subtitle = " "
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
        "🔍 查股票", "請選擇查詢類別", "#E89B82",
        [("🇹🇼 台股","查台股"), ("🇺🇸 美股","查美股"),
         ("📊 ETF","查ETF"), ("🏪 興/上櫃","查興上櫃"),
         ("⭐ 自選股","查自選股")]
    )

def make_market_menu_flex() -> dict:
    """全球大盤選單（v10.9.29 重做：分區式儀表板 + 移除模糊的「期貨」按鈕）"""
    # 共用 header 樣式
    def section_bubble(title, subtitle, color, buttons):
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
                    {"type":"text","text":title,"size":"lg","color":"#FFFFFF","weight":"bold"},
                    {"type":"text","text":subtitle,"size":"xs","color":"#FFFFFF"}
                ]
            },
            "body":{
                "type":"box","layout":"vertical","spacing":"sm","paddingAll":"12px",
                "contents": btn_contents
            }
        }

    bubbles = [
        # 🌐 概覽
        section_bubble("🌐 全球市場儀表板","點任一指數查詢即時行情","#5B8DB8",[
            ("🇹🇼 台股加權","查台股加權"), ("🏪 櫃買指數","查櫃買指數"),
            ("🇺🇸 道瓊","查道瓊"), ("📊 Nasdaq","查Nasdaq"),
            ("📈 S&P 500","查SP500"),
        ]),
        # 🇪🇺 歐洲 + 亞洲（v10.9.29 新增）
        section_bubble("🌏 全球指數","歐洲、亞洲主要市場","#B89BC4",[
            ("🇩🇪 德國 DAX","查DAX"),
            ("🇫🇷 法國 CAC40","查CAC40"),
            ("🇬🇧 英國 FTSE","查FTSE"),
            ("🇯🇵 日經 225","查日經"),
            ("🇭🇰 恆生指數","查恆生"),
            ("🇨🇳 上證指數","查上證"),
            ("🇰🇷 KOSPI","查KOSPI"),
            ("🇺🇸 Russell 2000","查Russell"),
        ]),
        # 🧠 科技與情緒
        section_bubble("🧠 科技 / 風險指標","半導體、恐慌、市場情緒","#D9C5B3",[
            ("🔵 SOX 半導體","查SOX"),
            ("😱 VIX 恐慌指數","查VIX"),
        ]),
        # 🥇 貴金屬（v10.9.29 細分）
        section_bubble("🥇 貴金屬","黃金現貨 / 期貨 / 台灣金價","#E8C99B",[
            ("🥇 現貨黃金 XAU/USD","查現貨黃金"),
            ("🥇 黃金期貨 COMEX","查黃金期貨"),
            ("🥇 台灣金價（每兩）","查台灣金價"),
            ("🥈 白銀","查白銀"),
        ]),
        # 🛢️ 能源（v10.9.29 細分 / v10.9.32 加 Dubai）
        section_bubble("🛢️ 能源市場","WTI、Brent、Dubai、天然氣","#D9B8A8",[
            ("🛢️ WTI 原油（美國）","查WTI"),
            ("🛢️ Brent 原油（北海）","查Brent"),
            ("🛢️ Dubai/Oman（中東）","查Dubai"),
            ("⚡ 天然氣","查天然氣"),
        ]),
        # 📉 債券（v10.9.32 調整順序：2Y → 10Y → 30Y）
        section_bubble("📉 美債殖利率","利率預期 / 經濟風險 / 通膨","#C9B0DB",[
            ("📊 2 年期（短期/Fed 預期）","查美債2Y"),
            ("📉 10 年期（長期經濟）","查美債"),
            ("📈 30 年期（通膨/財政）","查美債30Y"),
            ("🧠 殖利率 AI 解讀","殖利率分析"),  # v10.9.32 新增
        ]),
    ]
    return {"type":"carousel","contents":bubbles}

def make_forex_menu_flex() -> dict:
    """全球外匯（v10.9.29 擴充：USD/TWD 第一、DXY 第二、新增 KRW/HKD/CNH）"""
    return {
        "type":"bubble","size":"mega",
        "header":{
            "type":"box","layout":"vertical","backgroundColor":"#B89BC4","paddingAll":"14px",
            "contents":[
                {"type":"text","text":"💹 全球外匯與資金市場","size":"lg","color":"#FFFFFF","weight":"bold"},
                {"type":"text","text":"匯率・市場分析・資金流向","size":"xs","color":"#FFFFFF"}
            ]
        },
        "body":{
            "type":"box","layout":"vertical","spacing":"sm","paddingAll":"12px",
            "contents":[
                # 🇹🇼 台股最重要 → 第一順位
                {"type":"text","text":"🇹🇼 台股關鍵匯率","size":"sm","weight":"bold","color":"#B89BC4"},
                {"type":"button","style":"primary","height":"sm","color":"#E89B82",
                 "action":{"type":"message","label":"🇹🇼 USD/TWD 美元台幣","text":"查USDTWD"}},
                # 💵 美元指數 → 第二順位（全球市場核心）
                {"type":"text","text":"💵 美元指數（全球核心）","size":"sm","weight":"bold","color":"#B89BC4"},
                {"type":"button","style":"primary","height":"sm","color":"#D9C5B3",
                 "action":{"type":"message","label":"💵 DXY 美元指數","text":"查DXY"}},
                {"type":"separator","color":"#E8D4F0"},
                # 🌏 五大幣
                {"type":"text","text":"🌏 五大主要貨幣","size":"sm","weight":"bold","color":"#B89BC4"},
                {"type":"box","layout":"horizontal","spacing":"sm","contents":[
                    {"type":"button","style":"primary","height":"sm","color":"#B89BC4",
                     "action":{"type":"message","label":"💴 USD/JPY","text":"查USDJPY"}},
                    {"type":"button","style":"primary","height":"sm","color":"#B89BC4",
                     "action":{"type":"message","label":"💶 EUR/USD","text":"查EURUSD"}},
                ]},
                {"type":"box","layout":"horizontal","spacing":"sm","contents":[
                    {"type":"button","style":"primary","height":"sm","color":"#B89BC4",
                     "action":{"type":"message","label":"💷 GBP/USD","text":"查GBPUSD"}},
                    {"type":"button","style":"primary","height":"sm","color":"#B89BC4",
                     "action":{"type":"message","label":"🇨🇳 USD/CNY","text":"查USDCNY"}},
                ]},
                {"type":"box","layout":"horizontal","spacing":"sm","contents":[
                    {"type":"button","style":"primary","height":"sm","color":"#B89BC4",
                     "action":{"type":"message","label":"🇨🇳 USD/CNH 離岸","text":"查USDCNH"}},
                    {"type":"button","style":"primary","height":"sm","color":"#B89BC4",
                     "action":{"type":"message","label":"🇰🇷 USD/KRW","text":"查USDKRW"}},
                ]},
                # 其他
                {"type":"box","layout":"horizontal","spacing":"sm","contents":[
                    {"type":"button","style":"primary","height":"sm","color":"#B89BC4",
                     "action":{"type":"message","label":"🇦🇺 AUD/USD","text":"查AUDUSD"}},
                    {"type":"button","style":"primary","height":"sm","color":"#B89BC4",
                     "action":{"type":"message","label":"🇭🇰 USD/HKD","text":"查USDHKD"}},
                ]},
                {"type":"separator","color":"#E8D4F0"},
                # 市場分析
                {"type":"text","text":"📊 市場分析","size":"sm","weight":"bold","color":"#B89BC4"},
                {"type":"box","layout":"horizontal","spacing":"sm","contents":[
                    {"type":"button","style":"primary","height":"sm","color":"#C9B0DB",
                     "action":{"type":"message","label":"外匯市場分析","text":"外匯市場分析"}},
                    {"type":"button","style":"primary","height":"sm","color":"#C9B0DB",
                     "action":{"type":"message","label":"市場連動分析","text":"市場連動分析"}},
                ]},
                {"type":"button","style":"primary","height":"sm","color":"#C9B0DB",
                 "action":{"type":"message","label":"全球資金流向","text":"全球資金流向"}},
            ]
        }
    }

def make_ai_menu_flex() -> dict:
    return make_menu_flex(
        "🤖 AI 分析", "智慧選股・多維度評分", "#E89B82",
        [("⭐ 推薦股","推薦股"), ("📈 趨勢股","趨勢股"),
         ("🌱 成長股","成長股"), ("💰 存股","存股"),
         ("🌊 波段股","波段股"), ("🤖 AI概念股","AI概念股")]
    )

def make_news_menu_flex() -> dict:
    return make_menu_flex(
        "📰 財經新聞", "個股・台股・美股・國際", "#D9C5B3",
        [("📊 個股新聞","個股新聞"), ("🇹🇼 台股新聞","台股新聞"),
         ("🇺🇸 美股新聞","美股新聞"), ("🌐 國際新聞","國際新聞"),
         ("🌏 地緣政治","地緣政治新聞")]
    )

def make_portfolio_menu_flex() -> dict:
    return make_menu_flex(
        "📋 持股管理", "新增・查詢・損益分析", "#5B8B6B",
        [("➕ 新增持股","新增持股說明"), ("📋 查持股","持股"),
         ("🗑️ 我的持股（可刪除）","我的持股"),
         ("📊 損益分析","損益分析"), ("🔴 停損提醒","停損提醒說明"),
         ("🎯 目標價提醒","目標價提醒說明")]
    )

def make_admin_menu_flex(user_id: str) -> dict:
    """打字「管理後台」時用的 Flex（粉白少女系）"""
    owner = is_owner(user_id)
    color = "#E8B8A8"
    buttons = [
        ("👥 使用者管理","使用者管理選單"),
        ("⚙️ 系統管理","系統管理選單"),
    ]
    if owner:
        buttons += [
            ("📢 推播管理","推播管理選單"),
            ("🤖 AI管理","AI管理選單"),
            ("🛡️ 管理者名單","管理者名單"),
            ("📋 持股管理","持股管理選單"),
        ]
    else:
        buttons.append(("📋 持股管理","持股管理選單"))

    return make_menu_flex(
        "👑 管理後台" if owner else "🛡️ 管理後台",
        now_taipei().strftime("%m/%d %H:%M"),
        color, buttons
    )

def make_user_mgmt_flex(owner: bool) -> dict:
    # v10.9.27：加搜尋功能
    buttons = [
        ("👥 使用者列表","使用者列表"),    # 點進去可選人封鎖/查詳情/設管理者
        ("🔍 搜尋使用者","搜尋使用者"),    # 用 Quick Reply 快速找人
        ("⛔ 黑名單","黑名單"),            # 點進去可選人解除封鎖
    ]
    if owner:
        buttons += [
            ("🛡️ 管理者名單","管理者名單"),  # 點進去可移除管理者
        ]
    return make_menu_flex("👥 使用者管理","點清單即可操作 ✨","#E8B8A8", buttons)

def make_system_mgmt_flex() -> dict:
    return make_menu_flex(
        "⚙️ 系統管理","點按鈕即可操作 ✨","#E8B8A8",
        [("🔄 重新載入名稱快取","重載名稱"),    # 最常用，放第一個
         ("📊 查看快取狀態","快取狀態"),
         ("🔍 查詢個別代號","查快取說明"),
         ("🌸 重設 Rich Menu","重設選單")]     # 緊急救援
    )


# ══════════════════════════════════════════
#  外匯/商品資料
# ══════════════════════════════════════════
FOREX_SYMBOLS = {
    # 🇹🇼 台股最重要匯率 - 放最前面
    "查USDTWD": ("TWD=X",  "USD/TWD 美元台幣"),
    # 💵 美元指數 - 全球市場核心
    "查DXY":    ("DX-Y.NYB","DXY 美元指數"),
    # 🌏 五大幣
    "查USDJPY": ("JPY=X",  "USD/JPY 美元日圓"),
    "查EURUSD": ("EURUSD=X","EUR/USD 歐元美元"),
    "查GBPUSD": ("GBPUSD=X","GBP/USD 英鎊美元"),
    "查USDCNY": ("CNY=X",  "USD/CNY 美元人民幣"),
    "查USDCNH": ("CNH=X",  "USD/CNH 美元離岸人民幣"),
    # 其他常用
    "查AUDUSD": ("AUDUSD=X","AUD/USD 澳幣美元"),
    "查USDCHF": ("CHFUSD=X","USD/CHF 美元瑞郎"),
    "查USDKRW": ("KRW=X",  "USD/KRW 美元韓元"),
    "查USDHKD": ("HKD=X",  "USD/HKD 美元港幣"),
}

MARKET_SYMBOLS = {
    # 🇹🇼 台股
    "查台股加權": ("^TWII",  "台股加權指數"),
    "查櫃買指數": ("^TWOII", "台灣櫃買指數"),
    # 🇺🇸 美股
    "查道瓊":    ("^DJI",   "道瓊工業指數"),
    "查Nasdaq":  ("^IXIC",  "那斯達克指數"),
    "查SP500":   ("^GSPC",  "S&P 500"),
    "查Russell": ("^RUT",   "Russell 2000"),
    "查SOX":     ("^SOX",   "費城半導體 SOX"),
    "查VIX":     ("^VIX",   "VIX 恐慌指數"),
    # 🇪🇺 歐洲指數（v10.9.29 新增）
    "查DAX":     ("^GDAXI", "🇩🇪 德國 DAX"),
    "查CAC40":   ("^FCHI",  "🇫🇷 法國 CAC 40"),
    "查FTSE":    ("^FTSE",  "🇬🇧 英國 FTSE 100"),
    "查STOXX":   ("^STOXX50E", "🇪🇺 歐洲 STOXX 50"),
    # 🌏 亞洲指數（v10.9.29 新增）
    "查日經":    ("^N225",  "🇯🇵 日經 225"),
    "查恆生":    ("^HSI",   "🇭🇰 恆生指數"),
    "查上證":    ("000001.SS","🇨🇳 上證指數"),
    "查KOSPI":   ("^KS11",  "🇰🇷 韓國 KOSPI"),
    # 🥇 貴金屬（v10.9.29 細分）
    "查現貨黃金": ("XAUUSD=X", "🥇 現貨黃金 XAU/USD"),
    "查黃金期貨": ("GC=F",   "🥇 黃金期貨 COMEX"),
    "查黃金":     ("GC=F",   "🥇 黃金期貨 COMEX"),   # 別名相容
    "查白銀":     ("SI=F",   "🥈 白銀期貨"),
    "查台灣金價": ("__TWGOLD__", "🥇 台灣金價（每兩）"),  # 特殊處理
    # 🛢️ 能源（v10.9.29 細分 / v10.9.32 加 Dubai）
    "查WTI":     ("CL=F",   "🛢️ WTI 原油（美國）"),
    "查Brent":   ("BZ=F",   "🛢️ Brent 原油（北海）"),
    "查Dubai":   ("OQ=F",   "🛢️ Dubai/Oman 原油（中東）"),  # v10.9.32 新增
    "查Oman":    ("OQ=F",   "🛢️ Dubai/Oman 原油（中東）"),  # 別名
    "查原油":    ("CL=F",   "🛢️ WTI 原油（美國）"),   # 別名相容
    "查天然氣":  ("NG=F",   "⚡ 天然氣期貨"),
    # 📉 債券
    "查美債":    ("^TNX",   "美國10年期公債殖利率"),
    "查美債2Y":  ("^IRX",   "美國2年期公債殖利率"),
    "查美債30Y": ("^TYX",   "美國30年期公債殖利率"),
}


def get_taiwan_gold_price() -> dict:
    """抓台灣黃金價格（每兩 = 37.5 公克）
    v10.9.32 修正：用正確的台銀網址 + 多重備援
    1. 台銀金鑽條塊（直接每兩價）→ 最準
    2. 黃金存摺（每公克 × 37.5）
    3. XAU/USD × 美元台幣匯率（估算）
    """
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
    import re as _re

    # 嘗試 1：台銀牌價頁面（正確網址：/gold/quote/recent）
    try:
        url = "https://rate.bot.com.tw/gold/quote/recent"
        r = requests.get(url, headers=headers, timeout=10, verify=False)
        if r.status_code == 200:
            html = r.text
            # 解析「掛牌時間」
            time_match = _re.search(r'掛牌時間[：:\s]*([\d/:\s]+)', html)
            quote_time = time_match.group(1).strip() if time_match else ""

            # 策略 A：找「臺銀金鑽條塊」每台兩價（最準，直接每兩）
            # 結構：「1 台兩」標題 + 本行賣出 XXX,XXX
            tael_section = _re.search(
                r'臺銀金鑽條塊.*?本行賣出[\s\S]*?([\d,]{5,})',
                html, _re.DOTALL
            )
            if tael_section:
                price_str = tael_section.group(1).replace(",", "").strip()
                try:
                    price_per_tael = float(price_str)
                    # 合理性檢查：每兩應在 50,000 ~ 500,000 之間
                    if 50000 <= price_per_tael <= 500000:
                        gram_price = price_per_tael / 37.5
                        dlog("GOLD", f"✅ 台銀金鑽條塊：{price_per_tael:,.0f}/兩")
                        return {
                            "price": price_per_tael,
                            "gram_price": gram_price,
                            "source": "台銀金鑽條塊牌價",
                            "currency": "TWD",
                            "quote_time": quote_time,
                            "est": False
                        }
                except: pass

            # 策略 B：找「黃金存摺」每公克價 × 37.5
            # 結構：「1 公克」標題 + 黃金存摺 + 本行賣出 X,XXX
            gram_section = _re.search(
                r'黃金存摺.*?本行賣出[\s\S]*?([\d,]{3,7})',
                html, _re.DOTALL
            )
            if gram_section:
                price_str = gram_section.group(1).replace(",", "").strip()
                try:
                    gram_price = float(price_str)
                    # 合理性檢查：每公克應在 1,000 ~ 10,000 之間
                    if 1000 <= gram_price <= 10000:
                        price_per_tael = gram_price * 37.5
                        dlog("GOLD", f"✅ 台銀黃金存摺：{gram_price:,.0f}/克")
                        return {
                            "price": price_per_tael,
                            "gram_price": gram_price,
                            "source": "台銀黃金存摺牌價",
                            "currency": "TWD",
                            "quote_time": quote_time,
                            "est": False
                        }
                except: pass

            dlog("GOLD", "台銀頁面成功取得但解析失敗")
        else:
            dlog("GOLD", f"台銀牌價 HTTP {r.status_code}")
    except Exception as e:
        dlog("GOLD", f"台銀牌價失敗：{e}")

    # 嘗試 2：用 XAU/USD * 美元台幣匯率估算（國際金價方式）
    try:
        xau = get_yahoo_quote("GC=F")  # 黃金期貨（比 XAU/USD 穩定）
        if not xau:
            xau = get_yahoo_quote("XAUUSD=X")
        usdtwd = get_yahoo_quote("TWD=X")
        if xau and usdtwd:
            # 黃金期貨是每盎司美元，1盎司 ≈ 31.1035 公克，1台兩 = 37.5 公克
            usd_per_oz = xau["price"]
            usd_per_gram = usd_per_oz / 31.1035
            twd_per_gram = usd_per_gram * usdtwd["price"]
            twd_per_tael = twd_per_gram * 37.5
            dlog("GOLD", f"✅ XAU 估算：{twd_per_tael:,.0f}/兩")
            return {
                "price": twd_per_tael,
                "gram_price": twd_per_gram,
                "source": f"國際金價 ${usd_per_oz:.0f}/盎司 × USD/TWD {usdtwd['price']:.2f}",
                "currency": "TWD",
                "est": True
            }
    except Exception as e:
        dlog("GOLD", f"估算失敗：{e}")

    return {}


def get_market_strength_label(pct: float) -> tuple:
    """根據漲跌幅判斷強弱方向，回傳 (icon, label, color)"""
    if pct >= 1.5:
        return ("📈", "強勢", "#E89B82")
    elif pct >= 0.3:
        return ("↗️", "偏多", "#E8B8A8")
    elif pct >= -0.3:
        return ("➡️", "持平", "#888888")
    elif pct >= -1.5:
        return ("↘️", "偏空", "#7AABBE")
    else:
        return ("📉", "弱勢", "#5B8DB8")


def get_yield_analysis() -> dict:
    """殖利率 AI 解讀（v10.9.32 新增，規則式判讀）"""
    headers = {"User-Agent": "Mozilla/5.0"}

    # 抓 2Y、10Y、30Y
    def get_yld(sym):
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}?interval=1d&range=5d"
            r = requests.get(url, headers=headers, timeout=8)
            result = r.json()["chart"]["result"][0]
            meta = result["meta"]
            quotes = result.get("indicators",{}).get("quote",[{}])[0]
            closes = [c for c in quotes.get("close",[]) if c is not None]
            price = meta.get("regularMarketPrice") or (closes[-1] if closes else 0)
            prev = closes[-2] if len(closes)>=2 else price
            chg = price - prev
            pct = chg / prev * 100 if prev else 0
            return {"yield": price, "chg": chg, "pct": pct}
        except Exception as e:
            dlog("YIELD", f"{sym} 失敗：{e}")
            return None

    y2 = get_yld("^IRX")   # 2Y (用短期國庫券近似)
    y10 = get_yld("^TNX")  # 10Y
    y30 = get_yld("^TYX")  # 30Y

    if not (y2 and y10):
        return {}

    # 判斷殖利率倒掛
    spread_2_10 = y10["yield"] - y2["yield"]
    inverted = spread_2_10 < 0

    # AI 解讀邏輯
    interpretations = []

    # 殖利率方向
    if y10["pct"] > 1:
        interpretations.append("📉 10年期殖利率上升 → 科技股、成長股壓力增加")
    elif y10["pct"] < -1:
        interpretations.append("📈 10年期殖利率下降 → 成長股、AI 類股可能受惠")

    # 倒掛警告
    if inverted:
        interpretations.append(f"⚠️ 殖利率倒掛 2Y > 10Y（差距 {abs(spread_2_10):.2f}％）→ 經濟衰退預警訊號")
    elif spread_2_10 < 0.5:
        interpretations.append(f"🟡 殖利率曲線平坦（差距僅 {spread_2_10:.2f}％）→ 市場對長期經濟保守")
    else:
        interpretations.append(f"🟢 殖利率曲線正常（10Y-2Y = {spread_2_10:.2f}％）→ 經濟結構健康")

    # 2 年期解讀（Fed 政策）
    if y2["pct"] > 2:
        interpretations.append("🔴 2年期急升 → 市場預期 Fed 升息壓力增加")
    elif y2["pct"] < -2:
        interpretations.append("🟢 2年期急跌 → 市場預期 Fed 可能降息")

    # 30 年期解讀（通膨）
    if y30 and y30["pct"] > 1.5:
        interpretations.append("📈 30年期上升 → 長期通膨與財政赤字疑慮")
    elif y30 and y30["pct"] < -1.5:
        interpretations.append("📉 30年期下降 → 長期通膨壓力舒緩")

    return {
        "y2": y2,
        "y10": y10,
        "y30": y30,
        "spread": spread_2_10,
        "inverted": inverted,
        "interpretations": interpretations,
    }


def make_yield_analysis_flex(data: dict) -> dict:
    """殖利率 AI 解讀 Flex 卡片（v10.9.32 新增）"""
    if not data: return None
    y2 = data.get("y2", {})
    y10 = data.get("y10", {})
    y30 = data.get("y30", {})
    inverted = data.get("inverted", False)
    spread = data.get("spread", 0)
    interpretations = data.get("interpretations", [])

    # 倒掛時用淺紅警示色
    header_color = "#D49B9B" if inverted else "#C9B0DB"
    title = "⚠️ 殖利率倒掛警示" if inverted else "📉 美債殖利率 AI 解讀"

    def yield_row(label, yld_data, hint):
        if not yld_data:
            return {"type":"box","layout":"horizontal","contents":[
                {"type":"text","text":label,"size":"sm","color":"#A07560","flex":3},
                {"type":"text","text":"--","size":"sm","color":"#888","flex":2,"align":"end"},
            ]}
        is_up = yld_data["chg"] >= 0
        c = "#D97A5C" if is_up else "#7AABBE"
        arrow = "▲" if is_up else "▼"
        return {"type":"box","layout":"vertical","spacing":"none","contents":[
            {"type":"box","layout":"horizontal","contents":[
                {"type":"text","text":label,"size":"sm","color":"#A07560","weight":"bold","flex":3},
                {"type":"text","text":f"{yld_data['yield']:.3f}%","size":"sm","color":c,"weight":"bold","flex":2,"align":"end"},
                {"type":"text","text":f"{arrow}{abs(yld_data['pct']):.2f}%","size":"xxs","color":c,"flex":2,"align":"end"},
            ]},
            {"type":"text","text":hint,"size":"xxs","color":"#B89BC4","margin":"none"}
        ]}

    interp_contents = []
    for line in interpretations[:6]:  # 最多 6 條解讀
        interp_contents.append({
            "type":"text","text":line,"size":"xs","color":"#5D3F75","wrap":True
        })

    return {
        "type":"bubble","size":"mega",
        "header":{
            "type":"box","layout":"vertical","backgroundColor":header_color,"paddingAll":"14px",
            "contents":[
                {"type":"text","text":title,"size":"lg","color":"#FFFFFF","weight":"bold"},
                {"type":"text","text":now_taipei().strftime("%m/%d %H:%M"),"size":"xxs","color":"#FFFFFF"}
            ]
        },
        "body":{
            "type":"box","layout":"vertical","spacing":"md","paddingAll":"14px",
            "contents":[
                # 殖利率數值
                yield_row("📊 2 年期", y2, "→ 短期利率/Fed 政策預期"),
                {"type":"separator","color":"#F0D5C0"},
                yield_row("📉 10 年期", y10, "→ 長期經濟/全球資金成本"),
                {"type":"separator","color":"#F0D5C0"},
                yield_row("📈 30 年期", y30, "→ 長期通膨/財政風險"),
                {"type":"separator","color":"#F0D5C0"},
                # 曲線狀態
                {"type":"box","layout":"horizontal","contents":[
                    {"type":"text","text":"曲線狀態","size":"xs","color":"#A07560","flex":1},
                    {"type":"text","text":f"{'⚠️ 倒掛' if inverted else '✅ 正常'} ({spread:+.2f}％)",
                     "size":"xs","color":("#D97A5C" if inverted else "#5D8B6B"),"weight":"bold","flex":2,"align":"end"}
                ]},
                {"type":"separator","color":"#F0D5C0"},
                # AI 解讀
                {"type":"text","text":"🧠 AI 市場解讀","size":"sm","color":"#A05A48","weight":"bold"},
            ] + interp_contents + [
                {"type":"separator","color":"#F0D5C0"},
                {"type":"text","text":"⚠️ 僅供參考，非投資建議","size":"xxs","color":"#B89BC4"}
            ]
        }
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
    c     = "#D97A5C" if is_up else "#7AABBE"
    arrow = "▲" if is_up else "▼"
    sign  = "+" if is_up else ""
    # v10.9.29：加強弱方向標籤
    str_icon, str_label, str_color = get_market_strength_label(pct)
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
                {"type":"box","layout":"horizontal","spacing":"xs","contents":[
                    {"type":"text","text":f"{str_icon} {str_label}","size":"xs","color":str_color,"weight":"bold","flex":0},
                    {"type":"text","text":now_taipei().strftime("%m/%d %H:%M"),
                     "size":"xxs","color":"#AAAAAA","align":"end","flex":1,"gravity":"bottom"}
                ]}
            ]
        }
    }


def make_taiwan_gold_flex(data: dict) -> dict:
    """台灣金價專用 Flex（v10.9.32 加掛牌時間）"""
    if not data: return None
    price = data.get("price", 0)
    gram_price = data.get("gram_price", 0)
    source = data.get("source", "")
    quote_time = data.get("quote_time", "")
    est = data.get("est", False)

    # 來源說明
    src_label = f"💡 {source}"
    if est:
        src_label += "（估算）"
    if quote_time:
        src_label += f"\n📅 掛牌：{quote_time}"

    return {
        "type":"bubble","size":"kilo",
        "header":{
            "type":"box","layout":"vertical","backgroundColor":"#E8C99B","paddingAll":"10px",
            "contents":[{"type":"text","text":"🥇 台灣金價","size":"sm","color":"#FFFFFF","weight":"bold"}]
        },
        "body":{
            "type":"box","layout":"vertical","paddingAll":"12px","spacing":"sm",
            "contents":[
                {"type":"text","text":"每兩（37.5 克）","size":"xxs","color":"#A07560"},
                {"type":"text","text":f"NT$ {price:,.0f}","size":"xxl","weight":"bold","color":"#E89B82"},
                {"type":"separator","color":"#F0D5C0"},
                {"type":"text","text":"每公克","size":"xxs","color":"#A07560"},
                {"type":"text","text":f"NT$ {gram_price:,.2f}","size":"md","color":"#E89B82","weight":"bold"},
                {"type":"separator","color":"#F0D5C0"},
                {"type":"text","text":src_label,"size":"xxs","color":"#A07560","wrap":True},
                {"type":"text","text":f"⏰ 查詢：{now_taipei().strftime('%m/%d %H:%M')}",
                 "size":"xxs","color":"#B8B8B8"}
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
        r   = requests.get(url, headers=headers, timeout=5, verify=False)
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
            r   = requests.get(url, headers=headers, timeout=5, verify=False)
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
            r   = requests.get(url, headers=headers, timeout=8, verify=False)
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
        r   = requests.get(url, headers=headers, timeout=8, verify=False)
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
        r=requests.get(url,headers=headers,timeout=8, verify=False)
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
        r=requests.get(url,headers=headers,timeout=8, verify=False)
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
        r=requests.get(url,headers=headers,timeout=8,verify=False); data=r.json()
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
            r=requests.get(url,headers=headers,timeout=10,verify=False); data=r.json()
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
                    if   dd==ts:                sn=f"✅ 已使用當日法人資料（{dd}）"
                    elif weekday<5 and not afc: sn=f"📅 今日法人資料尚未公布，暫用 {dd} 資料"
                    else:                       sn=f"📅 使用 {dd} 前交易日資料"
                    return candidates,dd,sn
        except Exception as e: dlog("REC", f"法人資料失敗：{e}")
    return [],"","⚠️ 法人資料來源連線失敗"

def fetch_tpex_institution_data()->list:
    headers={"User-Agent":"Mozilla/5.0"}; candidates=[]
    try:
        url="https://www.tpex.org.tw/openapi/v1/tpex_mainboard_institution_trading"
        r=requests.get(url,headers=headers,timeout=10,verify=False); data=r.json()
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
    except Exception as e: dlog("REC", f"TPEx法人失敗：{e}")
    return candidates


# ══════════════════════════════════════════
#  大盤
# ══════════════════════════════════════════
def get_market_status()->dict:
    headers={"User-Agent":"Mozilla/5.0"}
    result={"price":0,"pct":0,"ok":True,"str":"⚪ 大盤資料取得中"}
    try:
        url="https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=tse_t00.tw&json=1&delay=0"
        r=requests.get(url,headers=headers,timeout=8, verify=False)
        d=r.json().get("msgArray",[{}])[0]
        price=float(d.get("z",0) or d.get("y",0)); prev=float(d.get("y",price))
        pct=(price-prev)/prev*100 if prev else 0
        icon="🟢" if pct>=0 else "🔴"
        result={"price":price,"pct":pct,"ok":pct>=-2,"str":f"{icon} 加權 {price:,.0f}（{pct:+.2f}%）"}
    except: pass
    return result

def get_market_summary()->str:
    headers={"User-Agent":"Mozilla/5.0"}
    msg=(f"🌐 全球大盤\n━━━━━━━━━━━━━━\n"
         f"　{now_taipei().strftime('%m/%d %H:%M')} 更新\n━━━━━━━━━━━━━━\n")
    try:
        url="https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch=tse_t00.tw&json=1&delay=0"
        r=requests.get(url,headers=headers,timeout=8, verify=False)
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
            state="（盤後）" if ms=="POST" else ""
            msg+=f"{'🟢' if pct>=0 else '🔴'} {name}　{price:,.2f}　{pct:+.2f}%{state}\n"
        except: msg+=f"⚪ {name}　--\n"
    msg+="━━━━━━━━━━━━━━\n⚠️ 僅供參考，非投資建議"
    return msg


# ══════════════════════════════════════════
#  推薦股 Flex
# ══════════════════════════════════════════
def make_rec_card(rank:int, s:dict)->dict:
    is_up=s["pct"]>=0; color="#D97A5C" if is_up else "#7AABBE"
    arrow="▲" if is_up else "▼"; pct_str=f"{arrow} {abs(s['pct']):.2f}%"
    filled=s["score"]//10; bar="█"*filled+"░"*(10-filled)
    tech_sig="　".join(s.get("tech_signals",[])[:2]) or "--"
    chip_sig="　".join(s.get("chip_signals",[])[:2]) or "--"
    return {
        "type":"bubble","size":"mega",
        "header":{"type":"box","layout":"horizontal","backgroundColor":"#E89B82","paddingAll":"12px",
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
                    {"type":"text","text":f"{bar} {s['score']}/100","size":"xxs","color":"#E89B82","weight":"bold","flex":5}
                ]}
            ]}
    }

def make_rec_flex(scored:list, mkt:dict, source_note:str)->dict:
    now_str=now_taipei().strftime("%m/%d %H:%M")
    overview={
        "type":"bubble","size":"mega",
        "header":{"type":"box","layout":"vertical","backgroundColor":"#E89B82","paddingAll":"14px",
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
                {"type":"text","text":"📊 評分維度","size":"sm","color":"#A05A48","weight":"bold"},
                {"type":"box","layout":"vertical","spacing":"xs","contents":[
                    {"type":"box","layout":"horizontal","contents":[
                        {"type":"text","text":"技術面","size":"xs","color":"#9B6B5A","flex":2},
                        {"type":"text","text":"均線 RSI 漲幅","size":"xs","color":"#5B4040","flex":3},
                        {"type":"text","text":"40分","size":"xs","color":"#E89B82","flex":1,"align":"end"}
                    ]},
                    {"type":"box","layout":"horizontal","contents":[
                        {"type":"text","text":"籌碼面","size":"xs","color":"#9B6B5A","flex":2},
                        {"type":"text","text":"外資 投信 同買","size":"xs","color":"#5B4040","flex":3},
                        {"type":"text","text":"30分","size":"xs","color":"#E89B82","flex":1,"align":"end"}
                    ]},
                    {"type":"box","layout":"horizontal","contents":[
                        {"type":"text","text":"新聞情緒","size":"xs","color":"#9B6B5A","flex":2},
                        {"type":"text","text":"白名單財經媒體","size":"xs","color":"#5B4040","flex":3},
                        {"type":"text","text":"30分","size":"xs","color":"#E89B82","flex":1,"align":"end"}
                    ]},
                ]},
                {"type":"separator","color":"#E8C4B4"},
                {"type":"text","text":"⚠️ 僅供參考，非投資建議","size":"xxs","color":"#E8B8A8","wrap":True}
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
        dlog("REC", f"推薦股運算失敗：{e}")
        push_message(user_id,"⭐ 推薦股\n━━━━━━━━━━━━━━\n　系統處理中發生錯誤\n　請稍後再試")


# ══════════════════════════════════════════
#  持股
# ══════════════════════════════════════════
def get_portfolio_summary(user_id:str)->str:
    portfolio=load_portfolio()
    up={k:v for k,v in portfolio.items() if v.get("user_id")==user_id}
    if not up:
        return "📋 持股清單是空的\n━━━━━━━━━━━━━━\n新增方式：\n　新增 2330 100 200\n　（代碼 股數 買入均價）"
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
            msg+=f"{icon} {symbol}｜{name}\n　現價 {price:.2f}　買入 {bp:.2f}\n　{shares}股　損益 {profit:+,.0f}（{pct:+.1f}%）\n\n"
        except: msg+=f"　{symbol}　查詢失敗\n\n"
    msg+=f"━━━━━━━━━━━━━━\n{'🟢' if total>=0 else '🔴'} 總損益　{total:+,.0f}"
    return msg


# ══════════════════════════════════════════
#  股票 Flex 卡片
# ══════════════════════════════════════════
def make_ma_row(label,value):
    vs=f"{value:.0f}" if value else "N/A"
    color="#E89B82" if value else "#E8B8A8"
    return {"type":"box","layout":"horizontal","contents":[
        {"type":"text","text":label,"size":"xs","color":"#9B6B5A","flex":4},
        {"type":"text","text":vs,"size":"xs","color":color,"flex":2,"weight":"bold","align":"end"},
    ]}

def make_stock_flex(symbol,name,market_type,status,source,
                    price,chg,pct,open_p,high,low,vol,
                    kline,news_list,query_time):
    is_up=chg>=0; color="#D97A5C" if is_up else "#7AABBE"
    arrow="▲" if is_up else "▼"; sign="+" if is_up else ""
    spark=kline.get("spark","▄▄▄▄▄▄▄▄▄▄"); trend=kline.get("trend","--")
    ma5=kline.get("ma5"); ma20=kline.get("ma20"); ma60=kline.get("ma60")
    ma120=kline.get("ma120"); ma240=kline.get("ma240")
    rsi=kline.get("rsi",0); rl=kline.get("rsi_label","--")
    rc="#E89B82" if rsi>70 else ("#5B8DB8" if rsi<30 else "#8B6B5A")
    dn=f"{symbol} {name}" if name and name!=symbol else symbol
    nc=[]
    for t,u in news_list[:4]:
        if u: nc.append({"type":"button","style":"link","height":"sm",
            "action":{"type":"uri","label":f"📰 {t}","uri":u}})
        else: nc.append({"type":"text","text":f"📰 {t}","size":"xs","color":"#B06050","wrap":True})
    if not nc: nc=[{"type":"text","text":"暫無相關新聞","size":"xs","color":"#E8B8A8"}]
    return {
        "type":"bubble","size":"mega",
        "header":{"type":"box","layout":"vertical","backgroundColor":"#E89B82","paddingAll":"16px","contents":[
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
                        {"type":"text","text":str(high),"size":"sm","color":"#E89B82","weight":"bold"}
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
                {"type":"text","text":"📊 技術分析","size":"sm","weight":"bold","color":"#A05A48"},
                {"type":"text","text":spark,"size":"xl","color":color},
                {"type":"text","text":f"趨勢　{trend}","size":"sm","color":"#A05A48"},
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
                {"type":"text","text":"📰 相關新聞","size":"sm","weight":"bold","color":"#A05A48"},
            ]+nc+[
                {"type":"separator","color":"#E8C4B4"},
                {"type":"box","layout":"horizontal","contents":[
                    {"type":"text","text":f"🕐 {query_time}　{status}","size":"xxs","color":"#E8B8A8","flex":1},
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


# ══════════════════════════════════════════════════════════
#  Postback Handler v10.9.25（接收按鈕點擊）
# ══════════════════════════════════════════════════════════
@handler.add(PostbackEvent)
def handle_postback(event):
    """處理 Postback 按鈕點擊"""
    user_id = event.source.user_id
    data = event.postback.data
    dlog("POSTBACK", f"user={user_id[:10]}... data='{data}'")

    if is_blocked_user(user_id):
        reply_text(event.reply_token, "⛔ 此帳號已停止使用權限")
        return

    # 解析 data: action=xxx&param=yyy
    params = {}
    for pair in data.split("&"):
        if "=" in pair:
            k, v = pair.split("=", 1)
            params[k] = v
    action = params.get("action", "")

    try:
        # ── 點名字 → 顯示單人操作 Flex 卡片（v10.9.28 新增）
        if action == "user_card" and is_admin(user_id):
            name = params.get("name", "")
            dlog("POSTBACK", f"→ 用戶操作卡片 {name}")
            flex = make_single_user_action_flex(name)
            if flex:
                reply_flex(event.reply_token, flex, f"操作 {name}")
            else:
                reply_text(event.reply_token, f"❌ 找不到用戶：{name}")
            return

        # ── 用戶搜尋分批 / 篩選（v10.9.28 新增）
        if action == "user_search" and is_admin(user_id):
            filter_status = params.get("filter", "all")
            batch = int(params.get("batch", "0") or "0")
            dlog("POSTBACK", f"→ 用戶搜尋 filter={filter_status} batch={batch}")
            msg, total = make_user_text_list(filter_status)
            items, _, _ = make_user_search_quickreply(filter_status, batch=batch)
            # 若已是篩選模式，加「回到全部」按鈕
            if filter_status != "all":
                items.append(("🔙 回到全部", "action=user_search&filter=all&batch=0"))
            qr_items = items[:13]
            if total > 0:
                with ApiClient(configuration) as api_client:
                    MessagingApi(api_client).reply_message(
                        ReplyMessageRequest(reply_token=event.reply_token,
                            messages=[TextMessage(text=msg,
                                quick_reply=make_postback_quick_reply(qr_items))]))
            else:
                reply_text(event.reply_token, msg)
            return

        # ── 舊版 user_list_page（向下相容保留）
        if action == "user_list_page" and is_admin(user_id):
            page = int(params.get("page", "0") or "0")
            dlog("POSTBACK", f"→ 使用者列表第 {page+1} 頁（舊版）")
            flex = make_user_list_carousel(page=page)
            if flex:
                with ApiClient(configuration) as api_client:
                    MessagingApi(api_client).reply_message(
                        ReplyMessageRequest(reply_token=event.reply_token,
                            messages=[FlexMessage(alt_text=f"使用者列表 第{page+1}頁",
                                contents=FlexContainer.from_dict(flex))]))
            else:
                reply_text(event.reply_token, "📋 已經到最後一頁了")
            return

        # ── 查使用者詳情
        if action == "user_detail":
            name = params.get("name", "")
            if name:
                reply_text(event.reply_token, get_user_detail(name))
            return

        # ── 開始封鎖流程（Step 1：選原因）
        if action == "block_start" and is_admin(user_id):
            target_name = params.get("name", "")
            if target_name:
                WAITING_BLOCK_REASON[user_id] = target_name
                dlog("POSTBACK", f"等待封鎖原因 user={user_id[:10]} target={target_name}")
                qr_items = [
                    ("⚠️ 違規", f"action=block_do&name={target_name}&reason=違規"),
                    ("📢 廣告", f"action=block_do&name={target_name}&reason=廣告"),
                    ("😡 騷擾", f"action=block_do&name={target_name}&reason=騷擾"),
                    ("🤬 不當言論", f"action=block_do&name={target_name}&reason=不當言論"),
                    ("❓ 其他", f"action=block_do&name={target_name}&reason=其他"),
                    ("❌ 取消", f"action=block_cancel"),
                ]
                with ApiClient(configuration) as api_client:
                    MessagingApi(api_client).reply_message(
                        ReplyMessageRequest(reply_token=event.reply_token,
                            messages=[TextMessage(
                                text=f"⚠️ 確定要封鎖「{target_name}」嗎？\n請選擇原因：",
                                quick_reply=make_postback_quick_reply(qr_items))]))
            return

        # ── 執行封鎖（Step 2）
        if action == "block_do" and is_admin(user_id):
            name = params.get("name", "")
            reason = params.get("reason", "未說明")
            WAITING_BLOCK_REASON.pop(user_id, None)
            if name:
                reply_text(event.reply_token, block_user_by_name(name, reason))
            return

        # ── 取消封鎖
        if action == "block_cancel":
            WAITING_BLOCK_REASON.pop(user_id, None)
            reply_text(event.reply_token, "✅ 已取消封鎖操作")
            return

        # ── 解除封鎖
        if action == "unblock" and is_admin(user_id):
            name = params.get("name", "")
            if name:
                reply_text(event.reply_token, unblock_user_by_name(name))
            return

        # ── 移除管理者
        if action == "remove_admin" and is_owner(user_id):
            name = params.get("name", "")
            if name:
                reply_text(event.reply_token, remove_admin(name))
            return

        # ── 新增管理者
        if action == "add_admin" and is_owner(user_id):
            name = params.get("name", "")
            if name:
                reply_text(event.reply_token, add_admin_by_name(name))
            return

        # ── 刪除持股
        if action == "del_portfolio":
            symbol = params.get("symbol", "")
            if symbol:
                p = load_portfolio()
                if symbol in p and p[symbol].get("user_id") == user_id:
                    del p[symbol]
                    save_portfolio(p)
                    delete_portfolio_from_sheets(user_id, symbol)
                    reply_text(event.reply_token, f"✅ 已刪除持股：{symbol}")
                else:
                    reply_text(event.reply_token, f"❌ 找不到持股或無權限")
            return

        # 未知 action
        dlog("POSTBACK", f"未知 action：{action}")

    except Exception as e:
        dlog("POSTBACK", f"處理 postback 例外：{e}")
        try:
            reply_text(event.reply_token, f"❌ 操作失敗，請稍後再試")
        except: pass


@handler.add(MessageEvent,message=TextMessageContent)
def handle_message(event):
    text=event.message.text.strip(); user_id=event.source.user_id

    # ── Debug log：每筆訊息都記錄
    dlog("MSG", f"收到訊息 user={user_id[:10]}... text='{text}'")

    if is_blocked_user(user_id):
        dlog("MSG", f"封鎖用戶嘗試使用：{user_id[:10]}...")
        reply_text(event.reply_token,"⛔ 此帳號已停止使用權限\n如有疑問請聯繫管理員")
        return

    update_user_activity(user_id,text)

    # ══ 主選單觸發 ══
    if text=="查股票":
        dlog("HANDLER", "→ 查股票選單")
        reply_flex(event.reply_token, make_stock_menu_flex(), "查股票")
        return

    if text=="全球大盤":
        dlog("HANDLER", "→ 全球大盤選單")
        reply_flex_with_qr(event.reply_token, make_market_menu_flex(), "全球大盤",
            [("台股加權","查台股加權"),("Nasdaq","查Nasdaq"),("S&P500","查SP500"),
             ("DAX","查DAX"),("日經","查日經"),("恆生","查恆生"),
             ("WTI原油","查WTI"),("台灣金價","查台灣金價"),("VIX","查VIX")])
        return

    if text=="外匯資金":
        dlog("HANDLER", "→ 外匯資金選單")
        reply_flex(event.reply_token, make_forex_menu_flex(), "全球外匯與資金市場")
        return

    if text=="AI分析":
        dlog("HANDLER", "→ AI分析選單")
        reply_flex_with_qr(event.reply_token, make_ai_menu_flex(), "AI分析",
            [("推薦股","推薦股"),("趨勢股","趨勢股"),("成長股","成長股"),
             ("存股","存股"),("波段股","波段股"),("AI概念股","AI概念股")])
        return

    if text=="財經新聞":
        dlog("HANDLER", "→ 財經新聞選單")
        reply_flex_with_qr(event.reply_token, make_news_menu_flex(), "財經新聞",
            [("台股新聞","台股新聞"),("美股新聞","美股新聞"),
             ("個股新聞","個股新聞"),("國際新聞","國際新聞"),("地緣政治","地緣政治新聞")])
        return

    if text=="持股管理":
        dlog("HANDLER", "→ 持股管理選單")
        reply_flex_with_qr(event.reply_token, make_portfolio_menu_flex(), "持股管理",
            [("查持股","持股"),("新增持股","新增持股說明"),
             ("損益分析","損益分析"),("停損提醒","停損提醒說明")])
        return

    # 打字「管理後台」→ 顯示 Flex（給沒切換 Rich Menu 的人用）
    if text=="管理後台" and is_admin(user_id):
        dlog("HANDLER", "→ 管理後台 Flex")
        reply_flex(event.reply_token, make_admin_menu_flex(user_id), "管理後台")
        return

    # ══ Rich Menu 切換後的按鈕（v10.9.21 新增）══
    if text=="使用者管理" and is_admin(user_id):
        dlog("HANDLER", "→ 使用者管理")
        reply_flex(event.reply_token, make_user_mgmt_flex(is_owner(user_id)), "使用者管理")
        return
    if text=="系統管理" and is_admin(user_id):
        dlog("HANDLER", "→ 系統管理")
        reply_flex(event.reply_token, make_system_mgmt_flex(), "系統管理")
        return
    if text=="推播管理" and is_owner(user_id):
        dlog("HANDLER", "→ 推播管理（開發中）")
        reply_text(event.reply_token,
            "📢 推播管理\n━━━━━━━━━━━━━━\n功能開發中 🚧\n\n後續版本將開放：\n　• 晨報推播\n　• 夜報推播\n　• 全體公告")
        return
    if text=="AI管理" and is_owner(user_id):
        dlog("HANDLER", "→ AI 管理（開發中）")
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

    # ══ 殖利率 AI 解讀（v10.9.32 新增）══
    if text == "殖利率分析" or text == "殖利率 AI 解讀" or text == "殖利率AI解讀":
        dlog("HANDLER", "→ 殖利率 AI 解讀")
        data = get_yield_analysis()
        if data:
            flex = make_yield_analysis_flex(data)
            if flex:
                reply_flex(event.reply_token, flex, "美債殖利率 AI 解讀")
                return
        reply_text(event.reply_token, "⚠️ 殖利率資料取得失敗\n請稍後再試")
        return

    # ══ 市場指數查詢 ══
    if text in MARKET_SYMBOLS:
        sym, name = MARKET_SYMBOLS[text]
        # v10.9.29：台灣金價特殊處理
        if sym == "__TWGOLD__":
            dlog("HANDLER", "→ 查台灣金價")
            gold_data = get_taiwan_gold_price()
            if gold_data:
                flex = make_taiwan_gold_flex(gold_data)
                if flex:
                    reply_flex(event.reply_token, flex, name)
                    return
            reply_text(event.reply_token, "⚠️ 台灣金價取得失敗\n請稍後再試")
            return
        # 一般指數
        data = get_yahoo_quote(sym)
        if data:
            # v10.9.29：根據商品類型用不同顏色
            if "黃金" in name or "白銀" in name:
                color = "#E8C99B"
            elif "原油" in name or "天然氣" in name:
                color = "#D9B8A8"
            elif "VIX" in name:
                color = "#D49B9B"
            elif "DAX" in name or "CAC" in name or "FTSE" in name or "STOXX" in name:
                color = "#B89BC4"
            elif "日經" in name or "恆生" in name or "上證" in name or "KOSPI" in name:
                color = "#E89B82"
            else:
                color = "#5B8DB8"
            flex = make_quote_flex(name, data, color)
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
            flex = make_quote_flex(name, data, "#B89BC4")
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
        dlog("HANDLER", "→ 推薦股（背景執行）")
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
            # 新版：「新增管理者 姓名」（用註冊姓名自動找 user_id）
            reg_name = text.replace("新增管理者 ", "").strip()
            if reg_name:
                reply_text(event.reply_token, add_admin_by_name(reg_name))
            else:
                reply_text(event.reply_token, "格式：新增管理者 註冊姓名\n例如：新增管理者 王小明")
            return
        elif text.startswith("移除管理者 "):
            name=text.replace("移除管理者 ","").strip()
            if name: reply_text(event.reply_token, remove_admin(name))
            return
        elif text=="管理者名單":
            # v10.9.25：改成可點選的 Flex carousel
            dlog("HANDLER", "→ 管理者名單（互動清單）")
            flex = make_admin_list_carousel()
            if flex:
                reply_flex(event.reply_token, flex, "管理者名單")
            else:
                reply_text(event.reply_token, get_admin_list())  # 退回文字版
            return
        elif text=="管理者名單文字":
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
            cached=NAME_CACHE.get(sid,"（無）")
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
        elif text=="重設選單":
            # Owner 專屬：手動重新建立 Rich Menu
            dlog("HANDLER", "→ Owner 手動重設 Rich Menu")
            reply_text(event.reply_token, "🔄 開始重新建立 Rich Menu，約 30 秒後完成...")
            t=threading.Thread(target=setup_rich_menus); t.daemon=True; t.start()
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
            # v10.9.28：文字總覽 + Quick Reply 點名字操作
            dlog("HANDLER", "→ 使用者列表（文字 + QR）")
            msg, total = make_user_text_list("all")
            items, _, _ = make_user_search_quickreply("all", batch=0)
            # 加狀態篩選按鈕
            filter_items = [
                ("🟢 只看正常", f"action=user_search&filter=正常&batch=0"),
                ("🔴 只看封鎖", f"action=user_search&filter=封鎖&batch=0"),
                ("⚪ 只看未註冊", f"action=user_search&filter=未註冊&batch=0"),
            ]
            qr_items = items + filter_items
            qr_items = qr_items[:13]  # Quick Reply 最多 13 個

            if total > 0:
                with ApiClient(configuration) as api_client:
                    MessagingApi(api_client).reply_message(
                        ReplyMessageRequest(reply_token=event.reply_token,
                            messages=[TextMessage(
                                text=msg,
                                quick_reply=make_postback_quick_reply(qr_items))]))
            else:
                reply_text(event.reply_token, msg)
            return
        elif text=="使用者列表文字":
            reply_text(event.reply_token, get_user_list()); return
        elif text=="搜尋使用者":
            # v10.9.28：保留舊指令，直接導向使用者列表
            dlog("HANDLER", "→ 搜尋使用者（導向列表）")
            msg, total = make_user_text_list("all")
            items, _, _ = make_user_search_quickreply("all", batch=0)
            filter_items = [
                ("🟢 只看正常", f"action=user_search&filter=正常&batch=0"),
                ("🔴 只看封鎖", f"action=user_search&filter=封鎖&batch=0"),
                ("⚪ 只看未註冊", f"action=user_search&filter=未註冊&batch=0"),
            ]
            qr_items = (items + filter_items)[:13]
            if total > 0:
                with ApiClient(configuration) as api_client:
                    MessagingApi(api_client).reply_message(
                        ReplyMessageRequest(reply_token=event.reply_token,
                            messages=[TextMessage(text=msg,
                                quick_reply=make_postback_quick_reply(qr_items))]))
            else:
                reply_text(event.reply_token, "📋 目前沒有使用者")
            return
        elif text.startswith("查使用者 "):
            name=text.replace("查使用者 ","").strip()
            if name: reply_text(event.reply_token, get_user_detail(name)); return

    # ══ 說明按鈕回應 ══
    # 🌸 自己查自己的註冊資料
    if text in ["我的名字", "我的資料", "我是誰"]:
        record = get_user_record(user_id)
        if record and record.get("註冊姓名"):
            msg = (f"🌸 您的註冊資料\n━━━━━━━━━━━━━━\n"
                   f"註冊姓名：{record.get('註冊姓名','')}\n"
                   f"LINE暱稱：{record.get('LINE暱稱','')}\n"
                   f"狀態：{record.get('狀態','正常')}\n"
                   f"註冊時間：{record.get('註冊時間','')}\n")
            if is_owner(user_id):
                msg += "身份：👑 Owner"
            elif is_admin(user_id):
                msg += "身份：🛡️ 管理者"
            else:
                msg += "身份：✨ 一般用戶"
            reply_text(event.reply_token, msg)
        else:
            reply_text(event.reply_token,
                "🌱 您還沒註冊喔！\n━━━━━━━━━━━━━━\n"
                "請輸入「註冊 您的姓名」完成註冊\n例如：註冊 王小明")
        return

    if text=="封鎖說明": reply_text(event.reply_token,"格式：封鎖 姓名 原因\n例如：封鎖 王小明 違規"); return
    if text=="解除封鎖說明": reply_text(event.reply_token,"格式：解除封鎖 姓名\n例如：解除封鎖 王小明"); return
    if text=="查使用者說明": reply_text(event.reply_token,"格式：查使用者 姓名\n例如:查使用者 王小明"); return
    if text=="新增管理者說明": reply_text(event.reply_token,"格式：新增管理者 註冊姓名\n例如：新增管理者 王小明\n\n（系統會自動從使用者名單找對應的 user_id）"); return
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

    # v10.9.25：可點選刪除的持股清單
    if text in ["我的持股", "持股清單"]:
        dlog("HANDLER", "→ 持股清單（互動）")
        flex = make_portfolio_action_carousel(user_id)
        if flex:
            reply_flex(event.reply_token, flex, "我的持股")
        else:
            reply_text(event.reply_token, get_portfolio_summary(user_id))
        return

    # v10.9.25：黑名單清單（管理者用）
    if text == "黑名單" and is_admin(user_id):
        dlog("HANDLER", "→ 黑名單清單")
        flex = make_blocked_list_carousel()
        if flex:
            reply_flex(event.reply_token, flex, "黑名單")
        else:
            reply_text(event.reply_token, "📋 目前沒有被封鎖的用戶")
        return

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
        dlog("HANDLER", f"→ 股票查詢 {t}")
        flex,err=get_stock_flex(t,user_id)
        if flex: reply_flex(event.reply_token,flex,f"{t} 股票資訊")
        else: reply_text(event.reply_token,err or "查詢失敗")
        return

    dlog("HANDLER", "→ 無匹配，回 HELP_MSG")
    reply_text(event.reply_token, HELP_MSG)


if __name__=="__main__":
    print("慧股拾光 Lumistock LINE Bot v10.9.32 啟動中...")
    for code,name in FALLBACK_NAMES.items():
        NAME_CACHE[code]=name
    t=threading.Thread(target=_bg_init); t.daemon=True; t.start()
    setup_rich_menus()
    port=int(os.environ.get("PORT",5001))
    app.run(host="0.0.0.0",port=port,debug=False)
