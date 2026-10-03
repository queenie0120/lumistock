"""新聞議題分群（純函式，無外部相依）。

問題：舊版去重只比「標題字面相似度」。同一件事換個寫法就過關，例如
    台積電法說會釋出樂觀展望 / 台積電上調全年資本支出 / AI 需求強 台積電營收創高
三則字面相似度都低於門檻 → 全部保留 → 版面被同一個議題吃光，
真正不同的議題反而被擠掉。使用者看到的就是「新聞幾乎都是重複的議題」。

作法：議題签名 =（實體集合, 事件類別）。
  - 實體：股票代號、英文代號、以及出現在名稱表裡的公司名。
  - 事件類別：以關鍵詞字典歸類（財報、營收、擴產、評等、人事…）。
同實體 + 同事件類別 → 同議題，只留最佳一則，其餘往後排（不丟棄）。
兩邊都歸不出類別時才回退字面相似度，避免把未分類新聞全部併成一團。

輸出只做「重新排序與限量」，不改變資料結構，呼叫端與 UI 不受影響。
"""
import re

# 事件類別關鍵詞。順序有意義：先命中者優先（較具體的放前面）。
EVENT_LEXICON = (
    ("法說", ("法說", "法說會", "業績發表", "線上說明會")),
    ("財報", ("財報", "季報", "年報", "eps", "每股盈餘", "稅後", "淨利", "毛利率",
              "獲利", "虧損", "轉盈", "轉虧")),
    ("營收", ("營收", "營業額", "自結", "年增", "月增", "創高", "新高")),
    ("股利", ("股利", "配息", "除息", "除權", "股東會", "減資", "增資")),
    ("評等", ("目標價", "評等", "升評", "降評", "調升", "調降", "外資報告",
              "大摩", "小摩", "高盛", "美銀", "分析師")),
    ("法人", ("外資", "投信", "自營商", "買超", "賣超", "三大法人", "籌碼", "融資", "融券")),
    ("產能", ("擴廠", "建廠", "投產", "產能", "資本支出", "設廠", "量產", "產線")),
    ("訂單", ("訂單", "出貨", "拉貨", "急單", "砍單", "庫存", "交期")),
    ("價格", ("漲價", "調漲", "降價", "報價", "跌價", "漲幅收斂")),
    ("併購", ("併購", "收購", "合併", "入股", "轉投資", "分割", "私有化")),
    ("人事", ("董事長", "總經理", "執行長", "接任", "請辭", "人事", "裁員", "異動")),
    ("訴訟", ("訴訟", "控告", "侵權", "專利", "罰款", "調查", "起訴", "開罰")),
    ("產品", ("發表", "推出", "新品", "問世", "開發", "亮相")),
    ("政策", ("關稅", "管制", "禁令", "補貼", "法規", "制裁", "出口管制")),
    ("利率", ("升息", "降息", "利率", "央行", "聯準會", "fed", "通膨", "cpi", "縮表")),
    ("供應鏈", ("供應鏈", "代工", "轉單", "認證", "打入", "供應商", "獨家供應")),
    ("ai", ("ai", "人工智慧", "輝達", "nvidia", "伺服器", "hbm", "算力", "晶片荒")),
    ("股價", ("漲停", "跌停", "重挫", "大漲", "大跌", "走高", "走低", "飆漲", "崩跌")),
)

_CODE_RE = re.compile(r"\b\d{4,6}[A-Za-z]?\b")
_TICKER_RE = re.compile(r"\b[A-Z]{2,5}\b")
_CJK_RE = re.compile(r"[一-鿿]+")


def event_category(title: str) -> str:
    """歸類事件類別；歸不出來回傳空字串。"""
    if not title:
        return ""
    low = title.lower()
    for cat, words in EVENT_LEXICON:
        for w in words:
            if w in low:
                return cat
    return ""


def extract_entities(title: str, names=None) -> frozenset:
    """抽出標題中的實體：股票代號、英文代號、名稱表裡的公司名。

    names 為公司名集合（例如 STOCK_NAMES 的值）。以「標題中的中文子字串是否
    在集合裡」判斷，避免拿上千個名稱逐一做子字串比對。
    """
    if not title:
        return frozenset()
    ents = set(_CODE_RE.findall(title))
    ents |= {t for t in _TICKER_RE.findall(title) if t not in _TICKER_STOPWORDS}
    if names:
        for chunk in _CJK_RE.findall(title):
            n = len(chunk)
            for size in (4, 3, 2):
                for i in range(n - size + 1):
                    cand = chunk[i:i + size]
                    if cand in names:
                        ents.add(cand)
    return frozenset(ents)


# 常見縮寫，不是公司代號
_TICKER_STOPWORDS = frozenset({
    "AI", "ETF", "GDP", "CPI", "PPI", "FED", "ECB", "OPEC", "IPO", "ESG",
    "CEO", "CFO", "COO", "EPS", "ROE", "ROI", "USD", "TWD", "JPY", "EUR",
    "NEWS", "UPDATE", "US", "EU", "UK", "TW", "HBM", "PC", "EV", "IC",
})


def topic_signature(title: str, names=None) -> tuple:
    """回傳 (實體集合, 事件類別)。"""
    return extract_entities(title, names), event_category(title)


def same_topic(sig_a: tuple, sig_b: tuple, lexical_sim=None,
               lex_threshold: float = 0.45) -> bool:
    """兩則是否屬於同一議題。

    - 兩邊都有事件類別：類別需相同，且實體有交集（或兩邊都沒實體）。
    - 類別不同：不同議題。
    - 有一邊歸不出類別：回退字面相似度，避免把未分類新聞全部併成一團。
    """
    ents_a, cat_a = sig_a
    ents_b, cat_b = sig_b
    if cat_a and cat_b:
        if cat_a != cat_b:
            return False
        if ents_a & ents_b:
            return True
        if not ents_a and not ents_b:
            return True
        return False
    return lexical_sim is not None and lexical_sim >= lex_threshold


def cluster_by_topic(titles: list, names=None, sim_fn=None,
                     lex_threshold: float = 0.45) -> list:
    """把標題分群。回傳與輸入等長的 cluster id 清單（0-indexed，先出現者先編號）。

    sim_fn(a, b) -> float：字面相似度函式，供無法歸類時回退使用；None 則不回退。
    """
    sigs = [topic_signature(t or "", names) for t in titles]
    cluster_of = [-1] * len(titles)
    reps = []   # [(cluster_id, 代表索引)]
    for i, sig in enumerate(sigs):
        found = -1
        for cid, rep_i in reps:
            sim = None
            if sim_fn is not None:
                try:
                    sim = sim_fn(titles[i] or "", titles[rep_i] or "")
                except Exception:
                    sim = None
            if same_topic(sig, sigs[rep_i], sim, lex_threshold):
                found = cid
                break
        if found < 0:
            found = len(reps)
            reps.append((found, i))
        cluster_of[i] = found
    return cluster_of


def diversify(items: list, cluster_of: list, max_per_topic: int = 1) -> list:
    """依議題輪流取件，讓前面的名額涵蓋最多不同議題。

    不丟棄任何一則：超過每議題上限的往後排，議題不足時照樣把清單填滿。
    同議題內維持輸入順序（呼叫端通常已依權重排序）。
    """
    if not items:
        return []
    buckets = {}
    order = []
    for item, cid in zip(items, cluster_of):
        if cid not in buckets:
            buckets[cid] = []
            order.append(cid)
        buckets[cid].append(item)

    out, overflow = [], []
    round_no = 0
    while True:
        added = False
        for cid in order:
            bucket = buckets[cid]
            if round_no < len(bucket):
                (out if round_no < max_per_topic else overflow).append(bucket[round_no])
                added = True
        if not added:
            break
        round_no += 1
    return out + overflow


def diversify_titles(items: list, title_of, names=None, sim_fn=None,
                     max_per_topic: int = 1) -> list:
    """便利包裝：items 可為任意結構，title_of(item) 取出標題。"""
    if not items:
        return []
    titles = [title_of(it) or "" for it in items]
    return diversify(items, cluster_by_topic(titles, names, sim_fn), max_per_topic)
