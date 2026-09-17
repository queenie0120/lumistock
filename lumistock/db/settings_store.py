"""設定持久化介面（v10.9.188）。

背景：推播設定、自訂停損／目標只存在 Render /tmp，重啟即遺失。
設計：
  - KVStore 介面：load(namespace) -> dict | None；save(namespace, data) -> bool
  - JsonFileStore：現行 /tmp JSON（主要讀寫，快速）
  - SheetsKVStore：Google Sheets「系統設定KV」分頁，一個 namespace 一列（鏡像備份）
  - MirroredStore：寫入先存 JSON，再以背景執行緒寫 Sheets（不拖慢 webhook 回覆）；
                   開機時 restore() 從 Sheets 拉回
P1 導入 Postgres 時，只需新增 PostgresKVStore 並替換 mirror，呼叫端不變。
限制：Google Sheets 單格上限約 50,000 字元，超過時 save 回傳 False 並記錄。
"""
import json
import os
import threading
import time

SHEETS_CELL_LIMIT = 49000


class JsonFileStore:
    def __init__(self, path_for_namespace: dict):
        self.paths = dict(path_for_namespace)
        self._lock = threading.Lock()

    def load(self, namespace: str):
        path = self.paths.get(namespace)
        if not path or not os.path.exists(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    def save(self, namespace: str, data: dict) -> bool:
        path = self.paths.get(namespace)
        if not path:
            return False
        tmp = f"{path}.tmp"
        try:
            with self._lock:
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False)
                os.replace(tmp, path)  # 原子替換
            return True
        except Exception:
            return False


class SheetsKVStore:
    HEADERS = ["namespace", "json", "updated_at"]

    def __init__(self, get_or_create_sheet, sheet_name: str = "系統設定KV", log=None):
        self._get = get_or_create_sheet
        self.sheet_name = sheet_name
        self._log = log or (lambda *a: None)
        self._lock = threading.Lock()

    def _sheet(self):
        return self._get(self.sheet_name, headers=self.HEADERS)

    def load(self, namespace: str):
        try:
            sh = self._sheet()
            if not sh:
                return None
            for row in sh.get_all_values()[1:]:
                if row and row[0] == namespace and len(row) > 1 and row[1]:
                    return json.loads(row[1])
        except Exception as e:
            self._log("SETTINGS_STORE", f"Sheets load {namespace} 失敗：{type(e).__name__}: {e}")
        return None

    def save(self, namespace: str, data: dict) -> bool:
        payload = json.dumps(data, ensure_ascii=False)
        if len(payload) > SHEETS_CELL_LIMIT:
            self._log("SETTINGS_STORE", f"{namespace} 超過 Sheets 單格上限（{len(payload)} 字元），未備份")
            return False
        try:
            with self._lock:
                sh = self._sheet()
                if not sh:
                    return False
                now = time.strftime("%Y-%m-%d %H:%M:%S")
                rows = sh.get_all_values()
                for i, row in enumerate(rows[1:], start=2):
                    if row and row[0] == namespace:
                        sh.update_cell(i, 2, payload)
                        sh.update_cell(i, 3, now)
                        return True
                sh.append_row([namespace, payload, now])
                return True
        except Exception as e:
            self._log("SETTINGS_STORE", f"Sheets save {namespace} 失敗：{type(e).__name__}: {e}")
            return False


class MirroredStore:
    def __init__(self, primary, mirror=None, async_mirror: bool = True, log=None):
        self.primary = primary
        self.mirror = mirror
        self.async_mirror = async_mirror
        self._log = log or (lambda *a: None)

    def load(self, namespace: str):
        return self.primary.load(namespace)

    def save(self, namespace: str, data: dict) -> bool:
        ok = self.primary.save(namespace, data)
        if self.mirror is not None:
            snapshot = json.loads(json.dumps(data))  # 深拷貝，避免背景寫入時被修改
            if self.async_mirror:
                t = threading.Thread(target=self.mirror.save, args=(namespace, snapshot), daemon=True)
                t.start()
            else:
                self.mirror.save(namespace, snapshot)
        return ok

    def restore(self, namespace: str, target: dict = None, merge=None):
        """開機時從鏡像拉回。
        - 本機（/tmp）沒有資料：直接採用鏡像。
        - 本機已有資料（例如開機後幾秒內使用者已修改）：若提供 merge(mirror, local)，
          以合併結果為準並回寫鏡像；未提供 merge 則保留本機、不還原。
        target 若提供，就地 clear()+update()，讓持有該 dict 的全域變數同步。
        回傳採用的 dict 或 None。"""
        if self.mirror is None:
            return None
        remote = self.mirror.load(namespace)
        if remote is None:
            return None
        local = self.primary.load(namespace)
        if local is None:
            data = remote
            self.primary.save(namespace, data)
        elif merge is not None:
            data = merge(remote, local)
            self.save(namespace, data)
        else:
            return None
        if target is not None:
            target.clear()
            target.update(data)
        self._log("SETTINGS_STORE", f"已從鏡像還原 {namespace}")
        return data


def merge_shallow(remote: dict, local: dict) -> dict:
    """本機值優先的淺層合併（推播設定用）。"""
    out = dict(remote or {})
    out.update(local or {})
    return out


def merge_two_level(remote: dict, local: dict) -> dict:
    """{user: {symbol: {...}}} 兩層合併，本機優先（自訂停損／目標用）。"""
    out = {u: dict(v) for u, v in (remote or {}).items()}
    for u, per in (local or {}).items():
        out.setdefault(u, {}).update(per)
    return out
