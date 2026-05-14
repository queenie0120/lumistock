"""
慧股拾光 Lumistock – Rich Menu 設定腳本
執行一次即可，之後不需要再執行
"""

import requests
import json
import os

CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", "你的TOKEN")

headers = {
    "Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

# ══════════════════════════════════════════
#  建立 Rich Menu
# ══════════════════════════════════════════
rich_menu_body = {
    "size": {"width": 2500, "height": 1686},
    "selected": True,
    "name": "慧股拾光選單",
    "chatBarText": "✨ 慧股拾光 功能選單",
    "areas": [
        {
            "bounds": {"x": 0, "y": 0, "width": 833, "height": 843},
            "action": {"type": "message", "text": "查股票"}
        },
        {
            "bounds": {"x": 833, "y": 0, "width": 834, "height": 843},
            "action": {"type": "message", "text": "大盤"}
        },
        {
            "bounds": {"x": 1667, "y": 0, "width": 833, "height": 843},
            "action": {"type": "message", "text": "推薦股"}
        },
        {
            "bounds": {"x": 0, "y": 843, "width": 833, "height": 843},
            "action": {"type": "message", "text": "持股"}
        },
        {
            "bounds": {"x": 833, "y": 843, "width": 834, "height": 843},
            "action": {"type": "message", "text": "新聞"}
        },
        {
            "bounds": {"x": 1667, "y": 843, "width": 833, "height": 843},
            "action": {"type": "message", "text": "建議"}
        }
    ]
}

# 建立選單
r = requests.post(
    "https://api.line.me/v2/bot/richmenu",
    headers=headers,
    json=rich_menu_body
)
print("建立選單：", r.status_code, r.text)
rich_menu_id = r.json().get("richMenuId")
print("Rich Menu ID：", rich_menu_id)

# 上傳圖片
with open("richmenu.png", "rb") as f:
    r2 = requests.post(
        f"https://api-data.line.me/v2/bot/richmenu/{rich_menu_id}/content",
        headers={
            "Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}",
            "Content-Type": "image/png"
        },
        data=f
    )
print("上傳圖片：", r2.status_code, r2.text)

# 設為預設選單
r3 = requests.post(
    f"https://api.line.me/v2/bot/user/all/richmenu/{rich_menu_id}",
    headers={"Authorization": f"Bearer {CHANNEL_ACCESS_TOKEN}"}
)
print("設為預設：", r3.status_code, r3.text)
print("✅ Rich Menu 設定完成！")
