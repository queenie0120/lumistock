"""
慧股拾光 Lumistock – by Hui
LINE Bot 模組 v10.9.89（截圖預覽 Flex 卡片 + 內嵌浮標確認）

【v10.9.89 更新】
使用者反映：
1. 「能用卡片顯示就盡量用卡片」（截圖辨識後的賣出/買進/庫存預覽目前是純文字）
2. 「確認賣出 / 取消賣出 這種再確認的事不要自行輸入而是用浮標的方式讓用戶點擊」
   → 不要打字，所有再確認都要浮標（除非像手續費折數那種人人不同的值）

改動：
1. 新增 make_sell_preview_flex(items, user_id)
   - 總覽 bubble（珊瑚粉 header）+ 個股 bubble（顯示 毛/費/稅/淨/成本/淨損益）
   - Footer 浮標：[✅ 確認賣出] [🚫 取消賣出]（message action，免打字）
2. 新增 make_buy_preview_flex(items, user_id)
   - 總覽 bubble（綠 header）+ 個股 bubble（新部位 / 加碼後新均價）
   - Footer 浮標：[✅ 確認加碼] [🚫 取消加碼]
3. 新增 make_holdings_preview_flex(items)
   - 薰衣草粉 header + 警告「將覆蓋既有部位」
   - Footer 浮標：[✅ 確認匯入] [🚫 取消匯入]
4. 圖片 handler 改用 push_flex，Flex 失敗 fallback 原文字版（不會修壞）
5. 既有的文字版 format_*_preview 保留作 fallback（不刪除）

【v10.9.88】意見回饋升級 Flex 卡片 + 簡化入口

【v10.9.88 更新】
使用者反映：
1. 建議推播希望用卡片整理（v10.9.87 是純文字 push）
2. 5 個入口字詞意思都一樣，太多冗餘

改動：
1. 新增 make_suggestion_flex()：粉嫩 Flex 卡片
   - Header 顏色依角色：Owner 珊瑚粉、Admin 薰衣草、User 杏粉
   - 身份區（姓名 / 角色 / user_id）獨立框
   - 內容區白底圓角，清楚易讀
2. 推播改用 push_flex；Flex 失敗 fallback 純文字
3. 簡化入口：只保留「意見回饋」「建議」（移除其他 4 個冗詞）

【v10.9.87】意見回饋系統

【v10.9.87 更新】
使用者要求：用戶/管理者建議自動傳 LINE 給 Owner，且顯示誰傳的。

問題：現有 WAITING_SUGGESTION 是死碼（沒有進入點，從沒被用到）。

新增：
1. get_user_display_info(user_id)：取得 註冊姓名 + 角色 + user_id
   role: 👑 Owner / 🛡️ 管理者 / 👤 使用者
2. 意見回饋進入指令：意見回饋 / 建議 / 回饋 / 回報問題 / 我要回報 / 我有建議
   → 設定 WAITING_SUGGESTION state（5 分鐘 TTL）+ Quick Reply「取消回饋」
3. 取消指令：取消回饋 / 取消建議
4. 提交後：
   - 寫 Google Sheets「系統記錄」
   - push 給 Owner（含 完整身份 + 時間 + 內容）
   - 回覆使用者「已收到」
5. 「💬 意見回饋」按鈕加進 AI 分析選單
6. HELP_MSG 加入意見回饋說明

【v10.9.86】移除加碼按鈕 + 智能手續費輸入

【v10.9.86 更新】
使用者反映：
1. 加碼按鈕現在跟新增重複（v10.9.84 智能模式之後）
2. 「沒折數」這種輸入應該被接受（沒人記得每次打「設定手續費」）
3. 點手續費按鈕後應該能直接輸入，且有「完成 / 重新輸入」浮標

改動：
1. 移除「📈 加碼登記」按鈕（已併入新增智能模式）
2. 新增 parse_fee_discount_input() 智能解析：
   - 數字：28 / 60 / 100 / 6.5
   - 折數：六折 / 6.5折 / 2.8折 / 28折（民間口語）
   - 國字數字 → 阿拉伯數字
   - 中文片語：無折數 / 全價 / 半折 / 沒有折扣 / 沒打折
3. 「手續費設定」/「查手續費」改為進入輸入模式：
   - 顯示目前設定 + Quick Reply「🚫 取消設定」
   - 5 分鐘內任何訊息都當折數輸入
   - 自動退出 AI 問答模式避免干擾
4. 設定成功後顯示 Quick Reply「✅ 完成」「🔁 重新輸入」
5. 「設定手續費 X」（一鍵指令）也用同一個 parser，向下相容

【v10.9.85】損益分析：已實現 vs 未實現分離

【v10.9.85 更新】
使用者反映：損益分析跟我的持股顯示一樣，浪費；應該分開呈現
已實現/未實現，且兩者不合計（會計上意義不同）。

改動：
1. 新增 read_sell_history_from_sheets(user_id)：讀「賣出紀錄」分頁
2. 新增 format_pnl_analysis(user_id)：完整損益報告
   - 📕 已實現損益（從 Sheets）：每筆細節 + 合計
   - 📘 未實現損益（即時報價）：每檔細節 + 合計
   - **明確標示「不合計」**
3. 「損益分析」改用新函式；「我的損益」「損益總覽」同義
4. 我的持股 Flex 維持不變（仍是 carousel + 系統建議 + 刪除按鈕）

已實現 = Sheets 賣出紀錄（含手續費 + 證交稅扣除）
未實現 = 即時持股 ×（現價 - 含費均價）

【v10.9.84】「新增」改為智能模式：既有部位自動加碼

【v10.9.84 更新】
使用者反映：沒有賣出又再買進不就是加碼嗎？系統應該自動分辨。

改動：
1. 「新增」指令改用 process_buy()，自動判斷：
   - 既有部位 → 加權平均（不覆蓋）
   - 新部位 → 建立
2. 新增「重設 代號 股數 價」指令給明確強制覆蓋情境（少用）
3. 「加碼」保留為別名（同新增）
4. 新增持股說明文字更新

99% 日常情境系統自動做對的事，
只有想刻意重設資料才用「重設」。

【v10.9.83】加碼 / 分批買進 + 買進截圖辨識

【v10.9.83 更新】
使用者要求：分批買進需求。

新增：
1. process_buy(user_id, stock_id, shares, buy_price)：
   - 既有部位 → 加權平均（保留原成本，加新買部位）
     new_avg = (old_avg×old_shares + buy_price×shares + 手續費) / 總股數
   - 新部位 → 等同新增（含費成本均價）
2. 「加碼 代號 股數 買價」手動指令
3. analyze_brokerage_screenshot 新增 type="buy" 偵測
   - 「現買 / 買進 / 沖買 / ROD買」+ 已成交
4. format_buy_import_preview：含預估加權均價
5. WAITING_BUY_IMPORT state + 「確認加碼」/「取消加碼」
6. 「📈 加碼登記」按鈕加進持股管理選單
7. 「加碼說明」handler 介紹兩種方式 + 加碼 vs 新增差異

【v10.9.82】手續費設定 + 含費損益計算

【v10.9.82 更新】
使用者要求：成本及損益應含買入手續費、賣出手續費、證交稅；
            使用者可自訂手續費折數（不一定都打折）。

新增：
1. USER_SETTINGS（/tmp + Sheets「使用者設定」分頁）
   每位使用者獨立的手續費折數（預設 6 折，可調 1-100%）
2. calc_buy_fee()：買入手續費 = max(20, 股數×價×0.001425×折數)
3. calc_sell_fee_tax()：手續費 + 證交稅 0.3%
4. 「新增」自動算含費成本均價：
   cost_avg = (股數×價 + 手續費) / 股數
5. 「賣出」完整明細：毛收入 / 手續費 / 證交稅 / 淨收入 / 淨損益
6. 截圖辨識賣出預覽也含費
7. 新指令：
   - 設定手續費 28　→ 2.8 折
   - 設定手續費 60　→ 6 折
   - 設定手續費 100 → 無折扣
   - 查手續費　　　→ 查目前設定
8. 「💰 手續費設定」按鈕加進持股管理選單
9. 開機從 Sheets 還原使用者設定

待 v10.9.83 做：分批買進「加碼」指令（加權平均）。

【v10.9.81】賣出指令 + 截圖辨識賣出

【v10.9.81 更新】
使用者要求：要有賣出指令計算已實現損益，且要能傳賣出截圖辨識。

新增：
1. analyze_brokerage_screenshot()：統一辨識券商截圖
   - 自動判斷類型：holdings（庫存）/ sell（賣出回報）/ unknown
   - 嚴格只取「已成交賣單」，忽略委託中/部份買單
2. process_sell(user_id, stock_id, shares, sell_price)：
   - 從持股扣股數，成本均價不變
   - 計算已實現損益（賣價 - 成本）× 股數
   - 寫入 Sheets「賣出紀錄」
3. save_sell_to_sheets()：紀錄欄位 date/user/symbol/name/shares/price/cost/pnl
4. format_sell_import_preview()：截圖辨識結果含預估損益
5. 手動指令「賣出 代號 股數 賣價」
6. 截圖確認流程：「確認賣出」/「取消賣出」（WAITING_SELL_IMPORT state）
7. 「💸 賣出登記」按鈕加進持股管理選單
8. 「賣出說明」handler 介紹兩種方式

合規：均不影響成本均價（賣出不改變剩餘部位的平均成本）。
       若賣超過持有量會擋下，不會出現負股數。

【v10.9.80】觀察清單專業 AI 分析

【v10.9.80 更新】
使用者要求：推薦股要給出專業分析原因及理由。

新增：
1. ai_analyze_top_picks_batch(stocks, mkt)：批次 Groq 分析 top 5 候選股
   - 一次 API call 處理 5 檔，節省配額
   - 嚴格 grounding：只能根據資料分析，不可編造
   - 用詞合規：禁「建議買進賣出/保證/明牌」，改「偏多/觀察重點/留意」
   - 輸出 JSON：reason / tech / chip / news / style / risk / confidence
2. build_and_push_recommendation 整合 AI 分析 + 計算支撐/壓力/停損/目標
   - 支撐 = 近 60 天最低點；壓力 = 近 60 天最高點
   - 停損 = 支撐 × 0.95；目標 = 壓力 × 1.05
3. make_rec_card 完整重設計：
   - 入選理由（AI 寫的具體 1 句）
   - 三面觀察：技術/籌碼/消息（AI 寫的）
   - 價位區間：支撐/壓力/停損/目標
   - 風險提醒（AI）+ 適合操作風格 + AI 信心
   - 評分條 + 免責聲明

【v10.9.79】未註冊者全面阻擋

【v10.9.79 更新】
使用者：未註冊的人完全不能點擊功能，不是只有查股票無法使用。

問題：之前 is_registered 檢查放在 handler 中段，前面的菜單按鈕、AI 問答、
      股票查詢、postback、截圖辨識都還是會通。

修法：三個入口統一在最前面 gate
1. handle_message（文字訊息）：blocked → registered → 主流程
2. handle_postback（按鈕點擊）：同上
3. handle_image（截圖上傳）：同上

Owner / Admin 自動視為已註冊，避免管理者被卡住。
未註冊者只能輸入「註冊 姓名」完成註冊，其他全部導向註冊提示。

【v10.9.78】修：TPEx 櫃買指數 SSL 連線失敗

【v10.9.78 修正】
今天 06:30 自動健檢 7/8 失敗，TPEx 櫃買指數錯誤：
  3x retry: SSLError: HTTPSConnectionPool ... Max retries exceeded

問題：v10.9.67 加的 retry 只能救「空回應」，救不了 SSL 握手失敗。

修法（三層保險）：
1. SSL 寬容 verify=False（TPEx server 偶爾 SSL 不穩）
2. 多 endpoint：HTTPS 失敗試 HTTP（仍是 TPEx 官方）
3. 全失敗時 fallback 到 Yahoo ^TWOII（已知偏移 1 天 → 標記為「備援」）
   - 仍套 sanity range 過濾離譜值（50-2000）
   - meta is_fallback=True，使用者看到「⚠ 備援來源」

結果：TPEx 一掛掉，立刻自動切備援，使用者不會看到空白；
      健檢仍會記錄主來源失敗，可追蹤 TPEx server 健康度。

【v10.9.77】地緣政治改用話題層級關鍵字

【v10.9.77 更新】
使用者反映：地緣政治寫死「台海/美中/美伊/俄烏」是壞設計——
未來新衝突出現時 query 會漏，依賴開發者人工更新。

修法：改用話題層級關鍵字，Google News 自動抓今天熱門：
- q1: 地緣政治 OR 國際衝突 OR 戰爭
- q2: 制裁 OR 軍事行動 OR 外交危機 OR 邊境緊張
- q3: 國安 OR 聯合國決議 OR 政變 OR 國際情勢
三組聚合去重 → 全面覆蓋當下任何地緣熱點，不再依賴 hardcode。

【v10.9.76】新聞多來源

【v10.9.76 更新】
使用者反映：美股=國際內容一樣、地緣政治沒訊息、個股新聞重複、全來自 Yahoo。

改動：
1. 新增 get_google_news_multi()：Google News RSS 多媒體來源
   （UDN/中央社/中時/自由/cnyes/換日線…），顯示真實媒體名
2. 新增 get_category_news(category)：tw / us / intl / geo 分類查詢
   - 台股：Yahoo 直接連結 + Google 多來源
   - 美股：道瓊/Nasdaq/標普/費半/Fed（聚焦美國）
   - 國際：歐日陸/總經/油價/黃金（非美全球，與美股區隔）
   - 地緣政治：台海/美中/美伊中東/俄烏（修好「暫無新聞」）
3. 新增 _merge_dedup_news()：跨來源去重
4. 個股新聞 carousel：FinMind（直接連結）+ Google 多來源，去重補充更多（12 則）
5. 美股 ≠ 國際（不同 query，去重）

備註：Google 連結為跳轉但手機可開；換取的是多媒體來源多元性。

【v10.9.75】大盤新聞升級 Flex carousel + 直接連結

【v10.9.75 更新】
新聞分層補完：台股/美股/國際新聞從純文字 + Google News 跳轉
升級為 Yahoo RSS 直接連結 + Flex carousel。

改動：
1. 新增 get_yahoo_finance_rss(category)：Yahoo 台股 RSS（tw-market / intl-markets）
   - 直接連結（tw.news.yahoo.com），Safari 可開，無 Google 中介頁
   - 含來源、時間、去重、過濾非新聞
2. 新增 make_news_carousel()：通用新聞 carousel（標題/來源/時間/看完整按鈕）
3. 台股新聞 → tw-market、美股/國際新聞 → intl-markets
4. record_health 追蹤 Yahoo News
5. Yahoo RSS 失敗時 fallback 回原本 get_news 純文字

驗證：滿足願景條件「標示來源/標示時間/連結可在 Safari 打開」

備註：個股新聞（FinMind）本來就是直接連結，已滿足條件。
      地緣政治暫保留純文字（niche）。

【v10.9.74】AI 問答全面性服務 — 連續對話 + 結束浮標

【v10.9.74 更新】
使用者要求：AI 助理要全面性服務，不主動結束、隨時換話題、不用重新點選單、
            每次回答下方顯示「結束問答」浮標可點。

改動：
1. 移除 5 分鐘自動結束限制 → 進入問答後持續開著，直到使用者點「結束問答」
2. 連續對話：問完一題直接問下一題，不需重新點 AI 助理選單
3. 隨時換話題：任何即時問題都即時回答（每題獨立 grounding）
4. 新增 push_text_with_qr()：push 訊息附 Quick Reply 浮標
5. 每次 AI 回答下方顯示「🔚 結束問答」浮標，點一下即離開（不用打字）
6. 「問 XXX」前綴問完也進入連續模式
7. 純 4-6 位數字仍走個股卡片（保留快速查股）

【v10.9.73】持股多使用者隔離 — 複合 key

【v10.9.73 更新（重要：個人投資助理原則）】
原則：個人投資助理，使用者之間的資料不可互相參雜（除非系統層級問題）。
Bug：持股原本用「代號」當唯一 key，多人持有同一檔會互相覆蓋。
修法：
1. 新增 _pf_key(user_id, symbol) / _pf_symbol(key) 複合 key 工具
2. 持股 key 改為「user_id|symbol」，每人每檔獨立
3. 全面更新 8 個 touch point：
   - 還原 / 新增 / 截圖匯入 / 刪除（postback + 指令）
   - 我的持股 carousel / 文字版 / action carousel
   - 持股警報掃描 / AI 問答帶持倉
4. 相容舊 symbol-only key（刪除時多重比對）
5. 台股代號統一不帶 .TW

結果：你跟家人朋友各自的 2330 完全獨立，不再互相覆蓋。

【v10.9.72】合併「查持股」與「我的持股」重複按鈕

【v10.9.72 更新】
使用者反映：「查持股」和「我的持股（可刪除）」其實是同一件事。
修法：
1. make_portfolio_flex_carousel 每張卡片加「🗑️ 刪除這檔」按鈕
2. 「我的持股」改用 make_portfolio_flex_carousel（與「持股」同一 view）
3. 選單移除重複按鈕，「查持股」更名「我的持股」
結果：一個「我的持股」= 分析 + 刪除，不再重複

【v10.9.71】修持股重啟後遺失 — 從 Sheets 還原

【v10.9.71 修正（嚴重 bug）】
現象：截圖匯入 7 檔持股成功，但服務重啟後「持股」變空。
原因：持股存 /tmp/lumistock_portfolio.json，Render /tmp 重啟即清空；
      雖有同步寫 Google Sheets，但 load_portfolio() 只讀 /tmp 沒從 Sheets 還原。
修法：
1. 新增 restore_portfolio_from_sheets()：開機從「自選股」Sheet 重建 /tmp
2. _bg_init() 啟動時呼叫（名稱快取載入後）
3. 正規化台股代號（去 .TW）避免「2330」與「2330.TW」重複
   - 手動新增存 .TW、截圖匯入存純代號，現在統一

【v10.9.70】修 AI 問答模式被股票查詢誤攔

【v10.9.70 修正】
Bug：在 AI 問答模式打「0050與0056差在哪」回「查無此股票」。
原因：中文字在 Python isalnum() 回 True，被股票代號查詢分支誤判攔截。
修法：AI 問答模式檢查移到股票查詢之前；純 4-6 位數字仍走個股卡片，
      含中文/其他字元的問題才進 AI 問答。

【v10.9.69】AI 智能問答系統 — grounded chat

【v10.9.69 更新】
新增 AI 智能問答：有資料佐證、不亂答、合規、會做商品比較與族群分析。

1. AI_QA_SYSTEM_PROMPT：master prompt 編入所有規則
   - 最高原則：資料區沒給的數字絕不編造、資料不足要誠實說
   - 合規：禁用「建議買進/賣出/保證/明牌」，改「偏多/偏空/觀察重點」，結尾免責
   - 商品比較從「人」的角度：適合族群 / 投資心態 / 投資人格 / 目標導向
   - 不說「哪個比較好」，說「哪個比較適合什麼樣的人」
   - AI 信心評分（高/中/低）
2. 問題類型自動判斷 + grounding：
   - 個股 → 即時報價 + 均線/RSI + 區間 + 除權息 + 新聞（真實資料）
   - 大盤/國際 → 指數 + 匯率快照
   - 商品比較/知識 → LLM 知識 + 標示具體數字需查證
   - 功能/排錯 → App 內建說明
   - 持股問答 → 帶入使用者持倉
3. 觸發方式：
   - 「💬 問 AI 助理」按鈕（加進 AI 分析選單）→ 5 分鐘問答模式
   - 「問 XXX」前綴 → 直接問答
   - 「結束問答」離開模式
4. 背景執行 + push，避免卡住 webhook

【v10.9.68】立即持股警報 + 按鈕

【v10.9.68 更新】
使用者提問：警報 dedup 加進選單會奇怪嗎？

問題：v10.9.65 dedup 連手動觸發都擋住 → 使用者按「立即測試」看不到已被抑制的警報。

修法：
1. run_portfolio_alerts(force=False) 加 force 參數
   - 06:30 自動跑：force=False，維持 dedup 防轟炸
   - 手動觸發：force=True，跳過 dedup，重發所有觸發
2. 「持股警報測試 / 持股警報 / 立即持股警報」改用 force=True
3. 「💗 立即持股警報」按鈕加進「持股管理」Flex 選單
   - 一鍵點開不必打字
   - 放在「查持股」下方，最常用位置

不加「重置警報紀錄」按鈕：使用者誤觸機率高且 Render /tmp 機制讓重置變相自動。

【v10.9.67】TPEx 櫃買指數加 retry

【v10.9.67 更新】
使用者觀察：今天 06:30 自動健檢報告 7/8 通過，TPEx 櫃買指數又「空回應」失敗。
這是 v10.9.55 起多次出現的 transient 問題（不是 endpoint 壞了，是 TPEx server 一時不穩）。

修法：
- get_taiwan_otc_index 加最多 3 次重試，間隔 2 秒
- 任一次成功就用，全失敗才回 {}
- 錯誤訊息含「3x retry」標籤，未來辨識更容易

【v10.9.66】美股強化 + 外匯 metadata 確認

【v10.9.66 更新】
使用者反映：「美股的狀況還有許多要更新及修正，資料不一定每一檔都正確」
Audit 後發現 get_us_stock 沒有 v10.9.50 的 stale rmp 防禦、沒有 meta、沒有 record_health。

改動：
1. get_us_stock 重構（套用 v10.9.50 同等防禦）
   - 偵測 regularMarketPrice 過時（>24h）或偏離 close >5%
   - 異常時改用日線 close，標記為「Yahoo Finance（日線備援）」
   - POST/PRE/REGULAR 三種市場狀態各自處理
   - 回傳值加 meta 欄位（source / is_realtime / is_fallback / delay_min）
2. record_health：呼叫成功/失敗都記錄到健檢
3. get_stock_flex 美股流向傳遞 meta 給 make_stock_flex
4. 外匯：v10.9.52 已自動含 metadata（透過 make_quote_flex 既有顯示），確認無需改動

如果未來 Yahoo 對任何美股給出像櫃買 -34% 那種異常值，會立刻被偵測並降級。

【v10.9.65】持股警報 24h dedup

【v10.9.65 更新】
問題：v10.9.63 之後若同一檔股票跌破停損，每天 06:30 都會推播一次 → 轟炸感

解法：加 24h dedup
1. 新增 ALERT_HISTORY 字典 + 持久化到 /tmp/lumistock_alert_history.json
2. 新增 _should_alert(uid, sid, type) 守門員
   - 同一 (user, stock, type) 在 24h 內已通報過 → 跳過
   - 自動清掉超過 7 天的舊紀錄，避免無限長大
3. run_portfolio_alerts 每個觸發點都用 _should_alert wrap
   - stop_loss / near_target / ex_dividend 三類各自獨立 dedup
4. 24h 後若依然觸發 → 重新通報（提醒使用者狀況持續）

【v10.9.64】截圖辨識持股

【v10.9.64 更新】
觸及使用者願景：方便、即時、減少手動輸入

1. 使用者直接傳券商庫存截圖 → 30 秒內辨識完成
2. 新增 ImageMessageContent handler（事件流：圖片 → 下載 → vision → 預覽）
3. 新增 analyze_portfolio_screenshot()
   - 用 Groq Llama 4 Scout vision model
   - prompt 要求回傳 JSON：[{stock_id, shares, avg_price}]
   - 驗證：代號 4-6 位數字 / 股數 / 均價皆 > 0
   - 強制換算「張 → 股」（× 1000）
4. 新增確認流程：
   - 收圖 → 預覽 → 使用者打「確認匯入」/「取消匯入」
   - 5 分鐘內有效（WAITING_PORTFOLIO_IMPORT state）
5. 新增隱私提示：「截圖會送雲端，建議遮蔽帳號餘額」
6. 「新增持股說明」更新文字，介紹兩種方式

【v10.9.63】持股自動警報推播

【v10.9.63 更新】
觸及使用者願景紅線：持股提醒實用、即時通知風險

1. 新增 run_portfolio_alerts()：掃描所有 user 持股，三類觸發
   a) 現價 ≤ 系統建議停損 → ⚠️ 跌破停損警告
   b) 現價 ≥ 系統建議目標 × 0.95 → 📈 接近目標提示
   c) 今日 = 除權息日 → 💰 今日除息提醒
2. 新增 format_portfolio_alerts_msg() 組合 LINE 推播文字
3. 整合進每日 06:30 自動健檢：
   - 健檢 push 後，順便掃 portfolio
   - 每位有警報的 user 收到一則整合訊息
4. 新增 Owner 指令「持股警報測試」/「持股警報」/「立即持股警報」
   - 背景跑、推播完整結果
   - 給 Owner 報告：N 位 user / M 則訊息

【v10.9.62】持股管理升級 Flex carousel

【v10.9.62 更新】
觸及使用者願景紅線（11-12）：持股管理 / 停損目標提醒

1. 新增 _get_portfolio_advice(stock_id) → 系統建議：
   - 停損 = 近 60 天最低點 × 0.95
   - 目標 = 近 60 天最高點 × 1.05
   - 下次除權息（用 v10.9.60 的 lazy load 機制）
2. 新增 make_portfolio_flex_carousel(user_id) → 粉嫩持股 carousel
   - Overview bubble：總損益 + 檔數 + 免責聲明
   - 每檔一張 bubble：
     * Header（漲粉 / 跌藍）：代碼名稱 + 損益 + 損益%
     * Body：現價、成本、股數
     * 💡 系統建議價：停損 / 目標（含距現價%）
     * 💰 下次除息：日期 + 配息金額（若有）
3. 「持股」handler 改為 Flex carousel（Flex 失敗 fallback 純文字）
4. 新增「持股文字」指令保留純文字版

【v10.9.61】健檢合理範圍改為動態

【v10.9.61 更新】
使用者反映：「現在市場很多股票漲很兇，似乎要看市場」
問題：hardcode SANITY_RANGES（2454 設 300-3000）跟不上實際 3,230 元 → 誤報

改動：
1. 刪除 SANITY_RANGES 硬編碼字典
2. 新增 _get_dynamic_sanity_range(key, source) + 6 小時快取
   - finmind_tw：抓還原股價近 60 天 close
   - yahoo_index：抓 Yahoo chart 近 3 個月 close
3. 範圍公式：(近 60 天 min × 0.7, 近 60 天 max × 1.3)
   - 30% 餘裕給市場波動（台股單日 ±10%）
   - 只有真的異常（如 Yahoo 給 269 vs 實際 400）才警報
4. run_healthcheck_tests 重構改用動態範圍
   - 報告中顯示範圍：「範圍外 (1,500-2,800，近 60 天)」
   - 沒歷史資料時 fallback 到「price > 0」基本檢查

【v10.9.60】除權息改為 lazy load per-stock

【v10.9.60 修正】
- 發現 FinMind TaiwanStockDividend 不支援全市場 broad query（只回 3 筆/年）
- 但 per-stock query 回完整歷史（2330 25 筆 / 0050 13 筆）
- 改用 lazy load：使用者查某檔股票時才抓該檔除權息
- _lazy_load_exdiv_for_stock() 在 get_ex_dividend_info 第一次查時自動執行
- 結果存入 EX_DIVIDEND_CALENDAR 供後續快取

【v10.9.59】A 計畫：除權息 + 三大法人 + 還原股價 全部接 FinMind

【v10.9.59 三大改動】

1. 除權息 → FinMind 主來源（_load_exdiv_finmind）
   - 抓近 180 天公告，篩「除息/除權日 ≥ 今天-1」的資料
   - 之前 4 層 fallback（TWSE OpenAPI / TPEx / 內建 2 筆）降為備援
   - 觸及願景紅線：除權息日防誤判

2. 三大法人 → FinMind 主來源（_fetch_finmind_institution）
   - 聚合 Foreign_Investor + Foreign_Dealer_Self → 外資
   - Investment_Trust → 投信
   - 之前 TWSE T86 endpoint（海外 IP 可能被擋）降為備援
   - 觀察清單評分立刻變更穩

3. K 線收盤序列 → FinMind 還原股價（_load_finmind_closes_adj）
   - TaiwanStockPriceAdj 已扣除權息影響
   - 之前 Yahoo（未還原，除權息日會跳水）降為備援
   - 觸及願景紅線：K 線防誤判

【v10.9.58】個股新聞 carousel

【v10.9.58 更新】
1. 新增 get_finmind_news_enriched()：回傳完整 dict（含 date / source / link / title），近 14 天
2. 新增 make_stock_news_carousel()：粉嫩 Flex carousel，每張卡 1 則新聞
   - Header（杏粉）：#N + 股票代號名稱 + 媒體 + 時間
   - Body：完整標題
   - Footer：📖 看完整按鈕（直接連去原文）
3. 新增 WAITING_STOCK_NEWS state 字典 (user_id → timestamp，TTL 5 分鐘)
4. 「個股新聞」設 state；下次輸入股票代號 → 回 carousel
   - 個股查詢卡片仍維持 4 則（不變）
   - 想看更多 → 走「財經新聞 → 個股新聞 → 輸入代號」路徑

【v10.9.57】個股新聞接 FinMind — Phase 2 #5A

【v10.9.57 更新】
1. 新增 _load_finmind_news()：個股新聞主來源改為 FinMind TaiwanStockNews
   - 你已付費（Backer tier）含此 dataset
   - 資料結構化，含日期、媒體名、連結
   - 比 Google News RSS 權威穩定
2. get_tw_stock_news() 改為多層 fallback：
   FinMind → Google News（白名單）→ Google News（一般）
3. 新增 _merge_news_lists() 工具函式做跨來源去重
4. FinMind 新聞呼叫一律 record_health（健檢面板會顯示）

【v10.9.56】健檢升級為 Flex 卡片 — Phase 1 #3A

【v10.9.56 更新】
1. 新增 make_health_flex()：粉嫩 Flex dashboard，比純文字更專業
   - Header（杏粉 #E8B8A8）：💗 系統健檢 / 版本 / 運行時長 / 啟動時間
   - Body：資料源狀態列（icon + 名稱 + 成功率 + 最後活動 + 錯誤訊息）
   - 系統資訊：名稱快取、Groq AI、FinMind token 狀態
2. 「健檢」/「系統健檢」/「health」改為回 Flex（Flex 失敗時 fallback 純文字）
3. 新增「健檢文字」指令保留純文字版（給 debug 用）

【v10.9.55】每日自動健檢 + 立即測試 — Phase 1 #4

【v10.9.55 更新】
1. 新增 run_healthcheck_tests()：依序測試 8 個關鍵項目
   - FinMind StockInfo
   - TPEx 官方櫃買指數
   - Yahoo 加權指數 / 道瓊 / Nasdaq
   - 台股 2330 台積電 / 0050 / 2454 聯發科
   每項含合理性範圍檢查（例：櫃買 50-1000，加權 10000-60000）
2. 新增 format_healthcheck_report()：成功時 brief 早安、失敗時詳細報告
3. 新增 _run_daily_healthcheck() + scheduler，每天台北時間 06:30 自動跑
   - 通過 → push「🌸 早安，系統一切正常」
   - 失敗 → push「🚨 自動健檢報告」含每項細節
4. 新增 Owner 指令「立即測試」/「立即健檢」/「ping」→ 背景跑 + push 完整報告
5. 「立即測試所有 API」按鈕加進「系統管理」Flex 選單

【v10.9.54】系統健檢按鈕加入選單，方便一鍵呼叫

【v10.9.54 更新】
1. make_admin_menu_flex（管理後台）最上方加「💗 系統健檢」按鈕
2. make_system_mgmt_flex（系統管理）最上方加「💗 系統健檢」按鈕
3. 使用者不需要再手動打字「健檢」，從 Rich Menu 進去即可一鍵點開

【v10.9.53】Owner 系統健檢面板 — Phase 1 #3

【v10.9.53 更新】
1. 新增 HEALTH_STATE 全域字典追蹤每個資料源的健康狀態
2. 新增 record_health(source, ok, error) 與 get_health_summary()
3. 已 instrument 的資料源（呼叫即記錄成功 / 失敗 + 錯誤訊息）：
   - FinMind（_load_finmind_taiwan_stock_info）
   - Yahoo Finance（get_yahoo_quote）
   - TPEx 官方（get_taiwan_otc_index）
   - Google Sheets（log_to_sheets）
   - Groq AI（groq_chat）
4. 新增 Owner 指令「健檢」/「系統健檢」/「health」→ 純文字面板
   - 各資料源 icon（✅/⚠️/❌）、最後成功 / 失敗時間、累計次數、成功率、最近錯誤
   - 系統資訊：版本、名稱快取大小、啟動時間、運行時間、Groq / FinMind token 狀態
5. 命中願景紅線「我要知道哪個 API 壞了」

【v10.9.52】資料來源 metadata 套用到大盤指數卡片（PR #2B）

【v10.9.52 更新】
1. 新增 fmt_data_meta_full()：含來源名稱的完整 metadata 字串
   例：「📡 TPEx 官方 ‧ ✓ 即時資料」
   例：「📡 Yahoo Finance（日線備援）‧ ⚠ 收盤資料」
2. make_quote_flex（大盤/外匯/商品/指數卡片）底部加 metadata 行
3. 使用者可以一眼判斷：資料是即時 / 收盤 / 延遲 / 備援，避免跟其他 App 比對時混淆

【v10.9.51】修：櫃買指數改用 TPEx 官方來源

【v10.9.51 更新】
1. 新增 get_taiwan_otc_index()：用 TPEx 官方 openapi/v1/tpex_index 抓櫃買指數
   - 問題深挖：Yahoo ^TWOII 不只 rmp 卡 2024 舊值，連 close 陣列日期也偏移 1 天
     （Yahoo 把 5/19 的值標成 5/20，5/18 的標成 5/19，依此類推）
   - 結果：使用者跟富邦比對發現 Lumistock 顯示 398.18 但實際 5/20 應為 396.42
   - 修法：櫃買指數完全棄用 Yahoo，改用 TPEx 自家 OpenAPI，權威且資料對齊
2. MARKET_SYMBOLS["查櫃買指數"] 用 __TPEXIDX__ 標記，handler 走特殊分支
3. 回傳含 meta（source="TPEx 官方"）→ 卡片底部會顯示「✓ 主來源」

【v10.9.50】
1. 修正 get_yahoo_quote：偵測 regularMarketPrice 過時或異常時改用日線 close
   - 觸發：櫃買指數 ^TWOII 的 regularMarketPrice 卡在 2024-10-12 舊值 269.45，
     但實際近日收盤是 400+，導致漲跌幅出現 -34% 等荒謬數字
   - 修法：rmp 與 latest_close 差 > 5% 或時間 > 24hr 前 → 改用 close 並標記為備援
   - 影響所有 Yahoo 指數/商品/外匯查詢，但僅在 Yahoo 給出異常資料時生效
2. get_yahoo_quote 回傳值同步加入 meta 欄位
3. 加 dlog 警告，方便日後追蹤類似問題

【v10.9.49】
1. 新增 VERSION 常數，啟動 log 不再寫死版號
2. 新增 build_data_meta() / fmt_data_meta() 統一資料來源描述格式
3. get_tw_stock() 各層回傳 meta 欄位（source / is_realtime / is_fallback / delay_min / fetched_at）
4. make_stock_flex() 卡片底部加 metadata 行：主來源/備援標示、即時/延遲標示
5. 解決使用者願景紅線之一：資料抓不到要誠實顯示，資料來源/時間/延遲要透明

【v10.9.48】
1. 合規用詞：所有使用者可見的「推薦股 / 慧股推薦榜 / 智慧選股推薦」改為「觀察清單 / 慧股觀察榜 / 智慧選股觀察」
2. Quick Reply 按鈕用詞同步調整（趨勢觀察 / 成長觀察 / 存股觀察 / 波段觀察 / AI概念觀察）
3. 訊息匹配保留向後相容：舊指令「推薦股 / 趨勢股 / 成長股」等仍可觸發
4. 每張個股卡片底部加入「⚠ 僅供參考，非投資建議」免責提示
5. 內部 log / Sheets 紀錄同步調整，方便後續分析

【v10.9.47】
1. 新增 _load_finmind_taiwan_stock_info()：用 FinMind TaiwanStockInfo 取代被擋的 TWSE ISIN
2. 新增環境變數 FINMIND_TOKEN（FinMind API token，可選但建議設定以提高額度）
3. init_name_cache 載入順序：內建表 → FinMind → TPEx → TWSE ISIN（備援）→ ETF → OpenData
4. TWSE ISIN 保留為備援（海外 IP 通常會回 "FOR SECURITY REASONS..."）

【v10.9.46】
1. Rich Menu 從 3 張圖升級為 5 張圖 Alias 切換
   - richmenu_user.png      一般用戶（玫瑰金）
   - richmenu_owner_main.png  Owner 主頁（粉白少女）
   - richmenu_owner_admin.png Owner 管理頁
   - richmenu_admin_main.png  管理者主頁（粉紫）
   - richmenu_admin_mgmt.png  管理者管理頁
2. 圖片路徑改為 static/richmenu/
3. 新增 debug log：每筆訊息、每個 handler 都會記錄
4. 完整保留 v10.9.20 所有功能（查股票/觀察清單/新聞/權限/Sheets/Flex/Quick Reply）
"""

from flask import Flask, request, abort
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi, MessagingApiBlob,
    ReplyMessageRequest, TextMessage, PushMessageRequest,
    FlexMessage, FlexContainer, QuickReply, QuickReplyItem,
    MessageAction, PostbackAction
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent, ImageMessageContent, PostbackEvent
import base64
import requests
import json, os, re, threading, time
from datetime import datetime, timezone, timedelta
import xml.etree.ElementTree as ET
import gspread
from google.oauth2.service_account import Credentials
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)

VERSION              = "10.9.89"
CHANNEL_SECRET       = os.environ.get("LINE_CHANNEL_SECRET")
CHANNEL_ACCESS_TOKEN = os.environ.get("LINE_CHANNEL_ACCESS_TOKEN")
OWNER_USER_ID        = "U972c7aec7b6628d70f52bc0bcbb4bf4a"
SHEETS_ID            = os.environ.get("GOOGLE_SHEETS_ID")
FINMIND_TOKEN        = os.environ.get("FINMIND_TOKEN", "")

configuration = Configuration(access_token=CHANNEL_ACCESS_TOKEN)
handler       = WebhookHandler(CHANNEL_SECRET)

WAITING_SUGGESTION = {}  # v10.9.87: user_id -> ts（5 分鐘內任何訊息當作建議）
WAITING_STOCK_NEWS = {}  # v10.9.58: user_id -> set_at_timestamp（5 分鐘內輸入股票代號 → 回新聞 carousel）
WAITING_PORTFOLIO_IMPORT = {}  # v10.9.64: user_id -> {"items": [...], "ts": ts}（5 分鐘內確認）
WAITING_SELL_IMPORT      = {}  # v10.9.81: user_id -> {"items":[{stock_id,shares,sell_price}], "ts":ts}
WAITING_BUY_IMPORT       = {}  # v10.9.83: user_id -> {"items":[{stock_id,shares,buy_price}], "ts":ts}
WAITING_FEE_INPUT        = {}  # v10.9.86: user_id -> ts（5 分鐘內任何訊息當手續費輸入）
WAITING_AI_QA = {}  # v10.9.69: user_id -> set_at_ts（AI 問答模式，5 分鐘內任何訊息當問題）
PORTFOLIO_FILE     = "/tmp/lumistock_portfolio.json"
USER_SETTINGS_FILE = "/tmp/lumistock_user_settings.json"  # v10.9.82：手續費折數等
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


# ══════════════════════════════════════════
#  資料來源 metadata 系統（v10.9.49）
#  讓每筆給使用者看到的資料都帶來源 / 時間 / 延遲 / 是否備援
# ══════════════════════════════════════════
def build_data_meta(source: str, is_realtime: bool = False,
                    is_fallback: bool = False, delay_min: int = 0) -> dict:
    """建立統一的資料來源 metadata。所有對外資料來源回傳值都帶這個欄位。"""
    return {
        "source": source,
        "is_realtime": is_realtime,
        "is_fallback": is_fallback,
        "delay_min": delay_min,
        "fetched_at": now_taipei().strftime("%m/%d %H:%M"),
    }

def fmt_data_meta(meta: dict) -> str:
    """格式化 metadata 成單行文字，用於 UI 顯示。
    範例：
        ✓ 主來源 ‧ 即時資料
        ✓ 主來源 ‧ 收盤資料
        ⚠ 備援來源 ‧ 延遲約 15 分
    """
    if not isinstance(meta, dict):
        return ""
    tag = "⚠ 備援來源" if meta.get("is_fallback") else "✓ 主來源"
    if meta.get("is_realtime"):
        state = "即時資料"
    elif meta.get("delay_min", 0) > 0:
        state = f"延遲約 {meta['delay_min']} 分"
    else:
        state = "收盤資料"
    return f"{tag} ‧ {state}"

def fmt_data_meta_full(meta: dict) -> str:
    """完整版：包含來源名稱。例如：
        📡 TPEx 官方 ‧ ✓ 即時資料
        📡 Yahoo Finance ‧ ✓ 收盤資料
        📡 Yahoo Finance（日線備援）‧ ⚠ 收盤資料
    """
    if not isinstance(meta, dict):
        return ""
    source = meta.get("source", "")
    flag = "⚠" if meta.get("is_fallback") else "✓"
    if meta.get("is_realtime"):
        state = "即時資料"
    elif meta.get("delay_min", 0) > 0:
        state = f"延遲約 {meta['delay_min']} 分"
    else:
        state = "收盤資料"
    return f"📡 {source} ‧ {flag} {state}"


# ══════════════════════════════════════════
#  系統健檢（v10.9.53）
#  追蹤每個資料源的健康狀態，Owner 打「健檢」可看
# ══════════════════════════════════════════
SYSTEM_START_AT = now_taipei()
HEALTH_STATE: dict = {}  # source_key -> {ok_count, fail_count, last_ok_at, last_fail_at, last_error}

def record_health(source: str, ok: bool, error: str = "") -> None:
    """記錄一次資料源呼叫的成功 / 失敗。任何 except 都應呼叫 record_health(..., ok=False)。"""
    s = HEALTH_STATE.setdefault(source, {
        "ok_count": 0, "fail_count": 0,
        "last_ok_at": None, "last_fail_at": None, "last_error": "",
    })
    if ok:
        s["ok_count"] += 1
        s["last_ok_at"] = now_taipei()
    else:
        s["fail_count"] += 1
        s["last_fail_at"] = now_taipei()
        if error:
            s["last_error"] = str(error)[:120]

def _fmt_health_ts(ts) -> str:
    """格式化時間戳為「X 分前」或「MM/DD HH:MM」。"""
    if not ts:
        return "從未"
    delta = (now_taipei() - ts).total_seconds()
    if delta < 60:
        return f"{int(delta)} 秒前"
    if delta < 3600:
        return f"{int(delta/60)} 分前"
    if delta < 86400:
        return f"{int(delta/3600)} 小時前"
    return ts.strftime("%m/%d %H:%M")

def _fmt_uptime() -> str:
    sec = int((now_taipei() - SYSTEM_START_AT).total_seconds())
    if sec < 60: return f"{sec} 秒"
    if sec < 3600: return f"{sec//60} 分"
    if sec < 86400: return f"{sec//3600} 小時 {(sec%3600)//60} 分"
    return f"{sec//86400} 天 {(sec%86400)//3600} 小時"

def make_health_flex() -> dict:
    """v10.9.56：Flex 版健檢面板（粉嫩 dashboard 風格）。
    Header：💗 系統健檢 / 版本 / 運行時長
    Body：各資料源狀態列（icon + 名稱 + 最後活動 + 成功率）
    Footer：系統資訊（名稱快取、token 狀態）
    """
    # 整理資料源狀態
    if HEALTH_STATE:
        items = sorted(
            HEALTH_STATE.items(),
            key=lambda kv: (kv[1].get("last_ok_at") or kv[1].get("last_fail_at") or now_taipei()),
            reverse=True,
        )
    else:
        items = []

    source_boxes = []
    for source, s in items:
        ok_n = s["ok_count"]
        fail_n = s["fail_count"]
        total = ok_n + fail_n
        ok_at = s.get("last_ok_at")
        fail_at = s.get("last_fail_at")
        if fail_n == 0 and ok_n > 0:
            icon, color = "✅", "#A05A48"
        elif ok_n == 0 and fail_n > 0:
            icon, color = "❌", "#D97A5C"
        elif fail_at and ok_at and fail_at > ok_at:
            icon, color = "⚠️", "#E89B82"
        else:
            icon, color = "✅", "#A05A48"
        rate = (ok_n / total * 100) if total else 0
        last_activity = _fmt_health_ts(ok_at) if ok_at else (_fmt_health_ts(fail_at) if fail_at else "從未")
        source_boxes.append({
            "type":"box","layout":"horizontal","spacing":"sm",
            "contents":[
                {"type":"text","text":f"{icon} {source}","size":"xs","color":color,"weight":"bold","flex":4,"wrap":True},
                {"type":"text","text":f"{rate:.0f}%","size":"xs","color":color,"flex":1,"align":"end"},
            ]
        })
        source_boxes.append({
            "type":"text","text":f"　最後活動 {last_activity}　成功 {ok_n} ／失敗 {fail_n}",
            "size":"xxs","color":"#9B6B5A","wrap":True,
        })
        if fail_n and s.get("last_error"):
            source_boxes.append({
                "type":"text","text":f"　錯誤：{s['last_error']}",
                "size":"xxs","color":"#D97A5C","wrap":True,
            })

    if not source_boxes:
        source_boxes = [{"type":"text","text":"尚無資料源被呼叫過","size":"xs","color":"#9B6B5A","align":"center"}]

    return {
        "type":"bubble","size":"mega",
        "header":{"type":"box","layout":"vertical","backgroundColor":"#E8B8A8","paddingAll":"14px",
            "contents":[
                {"type":"box","layout":"horizontal","contents":[
                    {"type":"text","text":"💗 系統健檢","size":"lg","color":"#FFFFFF","weight":"bold","flex":1},
                    {"type":"text","text":f"v{VERSION}","size":"xs","color":"#FDF6F0","align":"end","gravity":"bottom","flex":1},
                ]},
                {"type":"text","text":f"運行 {_fmt_uptime()} ‧ 啟動 {SYSTEM_START_AT.strftime('%m/%d %H:%M')}",
                 "size":"xxs","color":"#F0D5C0","margin":"sm"},
            ]},
        "body":{"type":"box","layout":"vertical","backgroundColor":"#FDF6F0","paddingAll":"14px","spacing":"sm",
            "contents":[
                {"type":"text","text":"📊 資料源狀態","size":"sm","color":"#A05A48","weight":"bold"},
                *source_boxes,
                {"type":"separator","color":"#E8C4B4","margin":"md"},
                {"type":"text","text":"📌 系統資訊","size":"sm","color":"#A05A48","weight":"bold","margin":"sm"},
                {"type":"box","layout":"horizontal","contents":[
                    {"type":"text","text":"名稱快取","size":"xxs","color":"#9B6B5A","flex":2},
                    {"type":"text","text":f"{len(NAME_CACHE):,} 筆","size":"xxs","color":"#5B4040","flex":3,"weight":"bold"},
                ]},
                {"type":"box","layout":"horizontal","contents":[
                    {"type":"text","text":"Groq AI","size":"xxs","color":"#9B6B5A","flex":2},
                    {"type":"text","text":("啟用" if GROQ_AVAILABLE else "未設定"),
                     "size":"xxs","color":("#A05A48" if GROQ_AVAILABLE else "#D97A5C"),"flex":3,"weight":"bold"},
                ]},
                {"type":"box","layout":"horizontal","contents":[
                    {"type":"text","text":"FinMind","size":"xxs","color":"#9B6B5A","flex":2},
                    {"type":"text","text":("已設 token" if FINMIND_TOKEN else "未設（免費 600/hr）"),
                     "size":"xxs","color":("#A05A48" if FINMIND_TOKEN else "#9B6B5A"),"flex":3,"weight":"bold"},
                ]},
                {"type":"separator","color":"#E8C4B4","margin":"md"},
                {"type":"text","text":"輸入「立即測試」可主動 ping 所有 API","size":"xxs","color":"#C9A89A","align":"center","margin":"sm"},
            ]}
    }


def get_health_summary() -> str:
    """產生系統健檢純文字訊息（Owner / Admin 用）。
    每個資料源顯示：icon、最後成功時間、最後失敗時間、累計成功 / 失敗次數。
    """
    lines = []
    lines.append(f"📊 系統健檢  v{VERSION}")
    lines.append("━━━━━━━━━━━━━━")
    if not HEALTH_STATE:
        lines.append("（尚無資料源被呼叫）")
    else:
        # 依「最近一次成功時間」排序，最近的在前
        items = sorted(
            HEALTH_STATE.items(),
            key=lambda kv: (kv[1].get("last_ok_at") or kv[1].get("last_fail_at") or now_taipei()),
            reverse=True,
        )
        for source, s in items:
            ok_n   = s["ok_count"]
            fail_n = s["fail_count"]
            total  = ok_n + fail_n
            ok_at  = s.get("last_ok_at")
            fail_at = s.get("last_fail_at")
            # 健康狀態判斷
            if fail_n == 0 and ok_n > 0:
                icon = "✅"
            elif ok_n == 0 and fail_n > 0:
                icon = "❌"
            elif fail_at and ok_at and fail_at > ok_at:
                icon = "⚠️"  # 最近一次是失敗
            else:
                icon = "✅"
            rate = (ok_n / total * 100) if total else 0
            lines.append(f"{icon} {source}")
            lines.append(f"　最後成功：{_fmt_health_ts(ok_at)}　成功 {ok_n} 次")
            if fail_n:
                lines.append(f"　最後失敗：{_fmt_health_ts(fail_at)}　失敗 {fail_n} 次")
                if s.get("last_error"):
                    lines.append(f"　錯誤：{s['last_error']}")
            lines.append(f"　成功率：{rate:.1f}%")
    lines.append("━━━━━━━━━━━━━━")
    lines.append("📌 系統資訊")
    lines.append(f"　版本：v{VERSION}")
    lines.append(f"　名稱快取：{len(NAME_CACHE):,} 筆")
    lines.append(f"　啟動時間：{SYSTEM_START_AT.strftime('%m/%d %H:%M')}")
    lines.append(f"　運行時間：{_fmt_uptime()}")
    lines.append(f"　Groq AI：{'啟用' if GROQ_AVAILABLE else '未設定'}")
    lines.append(f"　FinMind：{'已設 token' if FINMIND_TOKEN else '未設 token（免費 600 次/小時）'}")
    return "\n".join(lines)

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
    # v10.9.71：從 Sheets 還原持股（Render /tmp 重啟會清空）
    try:
        restore_portfolio_from_sheets()
    except Exception as e:
        dlog("PORTFOLIO", f"開機還原持股失敗：{e}")
    # v10.9.82：還原使用者手續費設定
    try:
        n = restore_user_settings_from_sheets()
        if n: dlog("SETTINGS", f"還原使用者設定：{n} 位")
    except Exception as e:
        dlog("SETTINGS", f"開機還原設定失敗：{e}")


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
    """上櫃 ETF 名單（v10.9.27 新增，v10.9.37 改快速失敗）"""
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get("https://www.tpex.org.tw/openapi/v1/tpex_etf_summary_quotes",
                       headers=headers, timeout=5, verify=False)
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
    except: pass  # 安靜失敗
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
    """上市 ETF 名單（v10.9.27 新增，v10.9.37 改快速失敗）"""
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get("https://www.twse.com.tw/rwd/zh/ETFortune/ETFRanking?response=json",
                       headers=headers, timeout=5, verify=False)
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
    except: pass  # 安靜失敗
    return 0


# ══════════════════════════════════════════
#  除權息日曆系統（v10.9.45 重構為多層備援架構）
#
#  設計理念：
#    Lumistock 在除權息日不能誤判。
#    不卡死在單一 API，採多層 fallback。
#
#  資料來源層級（後寫不覆蓋先寫）：
#    Layer 1：TWSE OpenAPI opendata 路徑（最權威）
#    Layer 2：TPEx OpenAPI（補上櫃股）
#    Layer 4：EX_DIVIDEND_FALLBACK 內建表（保底）
#    Layer 3（Phase 2 預留）：Yahoo Finance 隱含偵測
#
#  資料結構（含 Phase 2 預留欄位）：
#    EX_DIVIDEND_CALENDAR[code] = {
#        "date": "20260519",              # 除權息日（YYYYMMDD）
#        "cash": 0.66,                    # 現金股利
#        "stock": 0.0,                    # 股票股利（Phase 2 用）
#        "adjusted_reference_price": None,# 除權息參考價（Phase 2 用）
#        "source": "fallback",            # 資料來源（除錯用）
#        "note": "",                      # 註記（如「ETF 季配息」）
#    }
# ══════════════════════════════════════════

EX_DIVIDEND_CALENDAR = {}
EX_DIVIDEND_LAST_UPDATE = 0
EX_DIVIDEND_TTL = 12 * 3600  # 12 小時更新一次


# ── 內建保底表（Layer 4）
# 維護規則：
#   1. 每月底維護一次，更新未來 30-60 天的已知除權息
#   2. 資料來源：TWSE 公開資料 / 公司公告
#   3. 欄位完整，含 Phase 2 預留
#   4. 維護紀錄寫在表格上方註解
#
# 維護紀錄：
#   2026/05/19 - Queenie 初始建立（v10.9.45）
#   下次更新建議：2026/06/01（補 6 月除權息）
EX_DIVIDEND_FALLBACK = {
    # ─────── 2026/05 ───────
    "00878": {"date": "20260519", "cash": 0.66, "stock": 0.0,
              "adjusted_reference_price": None, "source": "fallback",
              "note": "ETF 季配息"},
    "00904": {"date": "20260519", "cash": 0.20, "stock": 0.0,
              "adjusted_reference_price": None, "source": "fallback",
              "note": "ETF 季配息"},
    # TODO: 未來 30 天其他除權息（每月初手動更新）

    # ─────── 2026/06 ───────（預留位置）
    # 待 2026/06/01 從 TWSE 公開資料補入
}


def _load_exdiv_finmind() -> int:
    """Layer 0：FinMind TaiwanStockDividend（v10.9.59 新增主來源）
    付費 Backer tier 已包含此 dataset。
    抓近 180 天的公告，挑「除息/除權交易日 ≥ 今天-1 天」的資料，
    涵蓋今日 + 未來 60-90 天的除權息。
    """
    if not FINMIND_TOKEN:
        return 0
    now = now_taipei()
    start_date = (now - timedelta(days=180)).strftime("%Y-%m-%d")
    end_date   = (now + timedelta(days=60)).strftime("%Y-%m-%d")
    url = "https://api.finmindtrade.com/api/v4/data"
    params = {
        "dataset": "TaiwanStockDividend",
        "start_date": start_date,
        "end_date": end_date,
        "token": FINMIND_TOKEN,
    }
    added = 0
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code != 200:
            dlog("EXDIV", f"  [FinMind] HTTP {r.status_code}")
            record_health("FinMind", False, f"Dividend HTTP {r.status_code}")
            return 0
        payload = r.json()
        if payload.get("status") != 200:
            dlog("EXDIV", f"  [FinMind] {payload.get('msg','')[:80]}")
            record_health("FinMind", False, f"Dividend {payload.get('msg','')[:80]}")
            return 0
        rows = payload.get("data") or []
        record_health("FinMind", True)
        today_dt = now.replace(hour=0,minute=0,second=0,microsecond=0) - timedelta(days=1)
        for row in rows:
            try:
                code = str(row.get("stock_id","")).strip()
                if not code:
                    continue
                cash  = float(row.get("CashEarningsDistribution", 0) or 0)
                stock = float(row.get("StockEarningsDistribution", 0) or 0)
                if cash <= 0 and stock <= 0:
                    continue
                # 優先用現金除息日；無則用股票除權日
                date_str = (row.get("CashExDividendTradingDate") or row.get("StockExDividendTradingDate") or "").strip()
                if not date_str or "-" not in date_str:
                    continue
                # 「2025-04-18」→「20250418」
                try:
                    parts = date_str.split("-")
                    yr, mo, dy = int(parts[0]), int(parts[1]), int(parts[2])
                    ex_dt = datetime(yr, mo, dy, tzinfo=TZ_TAIPEI)
                except: continue
                # 只保留「今天 -1 天」以後的（過時的不要）
                if ex_dt < today_dt:
                    continue
                date_norm = f"{yr:04d}{mo:02d}{dy:02d}"
                if code not in EX_DIVIDEND_CALENDAR:
                    EX_DIVIDEND_CALENDAR[code] = {
                        "date": date_norm,
                        "cash": cash,
                        "stock": stock,
                        "adjusted_reference_price": None,
                        "source": "finmind",
                        "note": f"年度 {row.get('year','')}",
                    }
                    added += 1
            except: continue
        return added
    except Exception as e:
        dlog("EXDIV", f"  [FinMind] 例外：{type(e).__name__}: {e}")
        record_health("FinMind", False, f"Dividend {type(e).__name__}")
        return 0


def _load_exdiv_twse_opendata() -> int:
    """Layer 1：TWSE OpenAPI（opendata 路徑試找除權息資料）

    嘗試多個可能 endpoint：
    - exchangeReport/TWT48U_ALL（已知會被 Render IP 擋）
    - opendata/t187ap37 系列（待測試）

    Render 環境多半失敗，所以這層失敗不影響其他層。
    """
    headers = {"User-Agent": "Mozilla/5.0"}
    added = 0
    endpoints = [
        ("https://openapi.twse.com.tw/v1/exchangeReport/TWT48U_ALL", "TWT48U_ALL"),
    ]
    for url, label in endpoints:
        try:
            r = requests.get(url, headers=headers, timeout=10, verify=False)
            if r.status_code != 200:
                dlog("EXDIV", f"  [TWSE OpenAPI] {label} HTTP {r.status_code}")
                continue
            text = r.text
            if not text.strip().startswith("["):
                if "SECURITY" in text.upper():
                    dlog("EXDIV", f"  [TWSE OpenAPI] {label} 被 IP 安全阻擋")
                else:
                    dlog("EXDIV", f"  [TWSE OpenAPI] {label} 回傳非 JSON：{text[:80]}")
                continue
            data = r.json()
            if not isinstance(data, list):
                continue
            for item in data:
                try:
                    date_str = str(item.get("Date", "")).strip()
                    code = str(item.get("Code", "")).strip()
                    cash_str = str(item.get("CashDividend", "0")).replace(",", "").strip()
                    stock_str = str(item.get("StockDividend", "0")).replace(",", "").strip()
                    cash = float(cash_str) if cash_str and cash_str not in ("-", "") else 0.0
                    stock = float(stock_str) if stock_str and stock_str not in ("-", "") else 0.0
                    if len(date_str) == 7 and date_str.isdigit():
                        yr = int(date_str[:3]) + 1911
                        date_norm = f"{yr}{date_str[3:]}"
                    elif len(date_str) == 8 and date_str.isdigit():
                        date_norm = date_str
                    else:
                        continue
                    if not (code and date_norm):
                        continue
                    if code not in EX_DIVIDEND_CALENDAR:
                        EX_DIVIDEND_CALENDAR[code] = {
                            "date": date_norm,
                            "cash": cash,
                            "stock": stock,
                            "adjusted_reference_price": None,
                            "source": "twse_opendata",
                            "note": "",
                        }
                        added += 1
                except: continue
        except Exception as e:
            dlog("EXDIV", f"  [TWSE OpenAPI] {label} 例外：{type(e).__name__}")
    return added


def _load_exdiv_tpex() -> int:
    """Layer 2：TPEx OpenAPI（補上櫃股）

    嘗試 endpoint：
    - tpex_ex_dividend_announcement（除權息公告）

    TPEx 對 Render 比較友善，但 endpoint 名稱要確認，所以可能失敗。
    """
    headers = {"User-Agent": "Mozilla/5.0"}
    added = 0
    endpoints = [
        ("https://www.tpex.org.tw/openapi/v1/tpex_ex_dividend_announcement", "tpex_exdiv"),
    ]
    for url, label in endpoints:
        try:
            r = requests.get(url, headers=headers, timeout=10, verify=False)
            if r.status_code != 200:
                dlog("EXDIV", f"  [TPEx OpenAPI] {label} HTTP {r.status_code}")
                continue
            text = r.text
            if not text.strip().startswith("["):
                dlog("EXDIV", f"  [TPEx OpenAPI] {label} 回傳非 JSON：{text[:80]}")
                continue
            data = r.json()
            if not isinstance(data, list):
                continue
            for item in data:
                try:
                    date_str = str(item.get("Date") or item.get("ExDate") or "").strip()
                    code = str(item.get("Code") or item.get("StockCode") or "").strip()
                    cash_str = str(item.get("CashDividend") or item.get("Cash") or "0").replace(",", "").strip()
                    cash = float(cash_str) if cash_str and cash_str not in ("-", "") else 0.0
                    date_norm = re.sub(r"[^\d]", "", date_str)
                    if len(date_norm) == 7:
                        yr = int(date_norm[:3]) + 1911
                        date_norm = f"{yr}{date_norm[3:]}"
                    if len(date_norm) != 8:
                        continue
                    if not (code and date_norm):
                        continue
                    if code not in EX_DIVIDEND_CALENDAR:
                        EX_DIVIDEND_CALENDAR[code] = {
                            "date": date_norm,
                            "cash": cash,
                            "stock": 0.0,
                            "adjusted_reference_price": None,
                            "source": "tpex_openapi",
                            "note": "",
                        }
                        added += 1
                except: continue
        except Exception as e:
            dlog("EXDIV", f"  [TPEx OpenAPI] {label} 例外：{type(e).__name__}")
    return added


def _load_exdiv_fallback() -> int:
    """Layer 4：內建保底表（最穩定的最後一道防線）
    從 EX_DIVIDEND_FALLBACK 字典載入，後寫不覆蓋前面層。
    """
    added = 0
    for code, info in EX_DIVIDEND_FALLBACK.items():
        if code not in EX_DIVIDEND_CALENDAR:
            EX_DIVIDEND_CALENDAR[code] = dict(info)
            added += 1
    return added


def load_ex_dividend_calendar() -> int:
    """除權息日曆載入協調器（v10.9.45 重構）

    多層備援架構：
      Layer 1 TWSE OpenAPI    → Render 多半被擋
      Layer 2 TPEx OpenAPI    → 補上櫃
      Layer 4 內建保底表      → 最終防線

    每層獨立執行，失敗不影響其他層。
    後寫不覆蓋先寫（官方資料優先）。

    回傳總筆數。即使所有官方層失敗，至少還有內建表保底。
    """
    global EX_DIVIDEND_LAST_UPDATE
    EX_DIVIDEND_CALENDAR.clear()

    dlog("EXDIV", "🌟 開始載入除權息日曆（多層備援架構 v10.9.59）")

    # v10.9.59：Layer 0 FinMind 變成主來源（你已付費 Backer tier）
    l0 = _load_exdiv_finmind()
    dlog("EXDIV", f"  Layer 0 FinMind：{l0} 筆")

    l1 = _load_exdiv_twse_opendata()
    dlog("EXDIV", f"  Layer 1 TWSE OpenAPI：{l1} 筆")

    l2 = _load_exdiv_tpex()
    dlog("EXDIV", f"  Layer 2 TPEx OpenAPI：{l2} 筆")

    l4 = _load_exdiv_fallback()
    dlog("EXDIV", f"  Layer 4 內建保底：{l4} 筆")

    today = now_taipei().strftime("%Y%m%d")
    today_count = sum(1 for info in EX_DIVIDEND_CALENDAR.values()
                      if info.get("date") == today and info.get("cash", 0) > 0)

    EX_DIVIDEND_LAST_UPDATE = int(time.time())
    total = len(EX_DIVIDEND_CALENDAR)

    if l0 == 0 and l1 == 0 and l2 == 0:
        dlog("EXDIV", f"⚠️ 所有官方來源全失敗，僅用內建保底表（{total} 筆，今日 {today_count} 檔）")
    else:
        dlog("EXDIV", f"✅ 合併總計：{total} 筆（今日除權息：{today_count} 檔）")

    return total


def _lazy_load_exdiv_for_stock(stock_id: str) -> None:
    """v10.9.60：lazy load 個股除權息（FinMind dataset 不支援全市場 broad query，
    只支援 per-stock，所以使用者查某檔時才補抓該檔的近期除權息）。
    抓到後寫入 EX_DIVIDEND_CALENDAR 供 get_ex_dividend_info 使用。
    """
    if not FINMIND_TOKEN:
        return
    if stock_id in EX_DIVIDEND_CALENDAR:
        return  # 已有資料就不重抓
    now = now_taipei()
    start_date = (now - timedelta(days=400)).strftime("%Y-%m-%d")
    url = "https://api.finmindtrade.com/api/v4/data"
    params = {
        "dataset": "TaiwanStockDividend",
        "data_id": stock_id,
        "start_date": start_date,
        "token": FINMIND_TOKEN,
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code != 200:
            return
        payload = r.json()
        if payload.get("status") != 200:
            return
        rows = payload.get("data") or []
        record_health("FinMind", True)
        today_dt = now.replace(hour=0,minute=0,second=0,microsecond=0) - timedelta(days=1)
        # 挑「除息日 ≥ 今天-1」最近的一筆
        best = None
        for row in rows:
            try:
                cash  = float(row.get("CashEarningsDistribution", 0) or 0)
                stock = float(row.get("StockEarningsDistribution", 0) or 0)
                if cash <= 0 and stock <= 0:
                    continue
                date_str = (row.get("CashExDividendTradingDate") or row.get("StockExDividendTradingDate") or "").strip()
                if not date_str or "-" not in date_str:
                    continue
                parts = date_str.split("-")
                yr, mo, dy = int(parts[0]), int(parts[1]), int(parts[2])
                ex_dt = datetime(yr, mo, dy, tzinfo=TZ_TAIPEI)
                if ex_dt < today_dt:
                    continue
                # 取最近未來的那筆
                if not best or ex_dt < best["_dt"]:
                    best = {
                        "date": f"{yr:04d}{mo:02d}{dy:02d}",
                        "cash": cash, "stock": stock,
                        "adjusted_reference_price": None,
                        "source": "finmind",
                        "note": f"年度 {row.get('year','')}",
                        "_dt": ex_dt,
                    }
            except: continue
        if best:
            best.pop("_dt", None)
            EX_DIVIDEND_CALENDAR[stock_id] = best
            dlog("EXDIV", f"lazy load {stock_id} → 除息日 {best['date']} 現金 {best['cash']}")
    except Exception as e:
        record_health("FinMind", False, f"Dividend lazy {type(e).__name__}")


def get_ex_dividend_info(stock_id: str) -> dict:
    """查某檔股票是否「今天」是除權息日（v10.9.45 擴充：回傳 Phase 2 欄位）
    v10.9.60：若 EX_DIVIDEND_CALENDAR 沒這檔，先 lazy load FinMind 補抓

    回傳 None = 今天不是除權息日
    回傳 dict = 完整除權息資訊（含 Phase 2 預留欄位）
    """
    # v10.9.60：lazy load
    if stock_id not in EX_DIVIDEND_CALENDAR:
        _lazy_load_exdiv_for_stock(stock_id)
    if not EX_DIVIDEND_CALENDAR:
        return None
    info = EX_DIVIDEND_CALENDAR.get(stock_id)
    if not info:
        return None
    today = now_taipei().strftime("%Y%m%d")
    if info.get("date") != today:
        return None
    cash = info.get("cash", 0)
    stock = info.get("stock", 0)
    if cash <= 0 and stock <= 0:
        return None
    return {
        "cash": cash,
        "stock": stock,
        "adjusted_reference_price": info.get("adjusted_reference_price"),
        "source": info.get("source", "unknown"),
        "note": info.get("note", ""),
        "date": info.get("date"),
    }


# ══════════════════════════════════════════
#  除權息自動更新排程器（v10.9.46 新增）
#
#  設計：
#    每天台北時間 06:00 自動執行 load_ex_dividend_calendar()
#    使用 Python 內建 threading.Timer（不需新增依賴）
#    daemon=True：主程式結束時自動清理
#
#  失敗保護：
#    一次失敗不影響下次（finally 一定排下次）
#    Render 睡眠時排程暫停，醒來時繼續
# ══════════════════════════════════════════

_EXDIV_SCHEDULER_STARTED = False  # 防止重複啟動


def _run_exdiv_update():
    """執行除權息更新 + 排下一次"""
    dlog("EXDIV-AUTO", "🔄 排程自動更新除權息日曆")
    try:
        load_ex_dividend_calendar()
    except Exception as e:
        dlog("EXDIV-AUTO", f"❌ 自動更新例外：{type(e).__name__}: {e}")
    finally:
        # 不管成功失敗，都排下一次（持續運作）
        _schedule_next_exdiv_update()


def _schedule_next_exdiv_update():
    """計算到下一個台北時間 06:00 的秒數，設定 Timer"""
    try:
        now = now_taipei()
        # 下一個 06:00（如果現在過 06:00，就是明天的 06:00）
        next_run = now.replace(hour=6, minute=0, second=0, microsecond=0)
        if now >= next_run:
            next_run += timedelta(days=1)
        delay_seconds = (next_run - now).total_seconds()
        
        timer = threading.Timer(delay_seconds, _run_exdiv_update)
        timer.daemon = True  # 主程式結束時自動清掉
        timer.start()
        hours = delay_seconds / 3600
        dlog("EXDIV-AUTO", f"⏰ 下次自動更新：{next_run.strftime('%m/%d %H:%M')}（{hours:.1f} 小時後）")
    except Exception as e:
        dlog("EXDIV-AUTO", f"❌ 排程設定失敗：{type(e).__name__}: {e}")


def start_ex_dividend_scheduler():
    """啟動除權息自動更新排程器（每天 06:00 執行一次）
    多次呼叫只會啟動一次（防止重複）
    """
    global _EXDIV_SCHEDULER_STARTED
    if _EXDIV_SCHEDULER_STARTED:
        return
    _EXDIV_SCHEDULER_STARTED = True
    _schedule_next_exdiv_update()
    dlog("EXDIV-AUTO", "✅ 除權息自動更新排程器已啟動")


# ══════════════════════════════════════════
#  自動化健檢測試（v10.9.55）— Phase 1 #4
#  每天台北時間 06:30 自動跑：
#    - 各資料源 ping 測試
#    - 指數合理性檢查（櫃買 / 加權 / 道瓊 / Nasdaq）
#    - 熱門股查詢測試（2330 / 0050 / 2454）
#  失敗→push 詳細報告給 Owner；成功→push 簡短早安通知
# ══════════════════════════════════════════
_HEALTHCHECK_SCHEDULER_STARTED = False

# v10.9.61：合理性範圍改為「從歷史收盤動態計算」而非寫死
# 公式：(近 60 天 min × 0.7, 近 60 天 max × 1.3)
# 餘裕 30% 給市場波動（台股單日 ±10%，留 3 倍 buffer）
SANITY_RANGE_CACHE = {}  # key -> (range_tuple, computed_at_ts)
SANITY_RANGE_TTL = 6 * 3600  # 6 小時重算


def _get_dynamic_sanity_range(key: str, source: str = "finmind_tw") -> tuple:
    """從近 60 天歷史收盤動態計算合理範圍。
    key: stock_id (TW) 或 Yahoo symbol（^TWII / ^DJI / ^IXIC）
    source: "finmind_tw" / "yahoo_index"
    回傳 (low, high) 或 None（無歷史資料）
    """
    now_ts = time.time()
    cached = SANITY_RANGE_CACHE.get(key)
    if cached and (now_ts - cached[1]) < SANITY_RANGE_TTL:
        return cached[0]
    closes = []
    try:
        if source == "finmind_tw":
            closes = _load_finmind_closes_adj(key)
        elif source == "yahoo_index":
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{key}?interval=1d&range=3mo"
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            data = r.json()["chart"]["result"][0]
            quotes = data.get("indicators", {}).get("quote", [{}])[0]
            closes = [c for c in quotes.get("close", []) if c is not None]
    except Exception as e:
        dlog("HEALTHCHECK", f"動態範圍 {key} 失敗：{type(e).__name__}")
    if not closes or len(closes) < 5:
        return None
    closes = closes[-60:]
    lo, hi = min(closes), max(closes)
    rng = (lo * 0.7, hi * 1.3)
    SANITY_RANGE_CACHE[key] = (rng, now_ts)
    return rng


def _format_range(rng: tuple) -> str:
    if not rng: return "N/A"
    return f"{rng[0]:,.0f}–{rng[1]:,.0f}"

def run_healthcheck_tests() -> list:
    """逐項測試關鍵資料源，回傳 [(name, ok, value_or_msg, detail), ...]"""
    results = []

    # === FinMind StockInfo ===
    try:
        url = "https://api.finmindtrade.com/api/v4/data"
        params = {"dataset": "TaiwanStockInfo"}
        if FINMIND_TOKEN:
            params["token"] = FINMIND_TOKEN
        r = requests.get(url, params=params, timeout=15)
        payload = r.json() if r.status_code == 200 else {}
        rows = payload.get("data") or []
        ok = (r.status_code == 200 and payload.get("status") == 200 and len(rows) > 100)
        results.append(("FinMind StockInfo", ok,
                        f"{len(rows)} 筆" if ok else f"HTTP {r.status_code} / msg: {payload.get('msg','')[:80]}",
                        ""))
        record_health("FinMind", ok, "" if ok else payload.get("msg","")[:80])
    except Exception as e:
        results.append(("FinMind StockInfo", False, f"{type(e).__name__}: {e}", ""))
        record_health("FinMind", False, f"{type(e).__name__}: {e}")

    # === TPEx 官方櫃買指數（動態範圍：用 ^TWOII Yahoo close 算）===
    try:
        d = get_taiwan_otc_index()
        price = d.get("price", 0) if d else 0
        rng = _get_dynamic_sanity_range("^TWOII", source="yahoo_index")
        if rng:
            lo, hi = rng
            ok = bool(d) and lo <= price <= hi
            detail = "" if ok else (f"範圍外 ({_format_range(rng)}，近 60 天)" if d else "空回應")
        else:
            ok = bool(d) and price > 0
            detail = "" if ok else "空回應或 price<=0"
        results.append(("TPEx 櫃買指數", ok, f"{price:,.2f}" if d else "無資料", detail))
    except Exception as e:
        results.append(("TPEx 櫃買指數", False, f"{type(e).__name__}: {e}", ""))

    # === Yahoo 指數動態範圍批次測 ===
    for sym, label in [("^TWII", "Yahoo 加權指數"), ("^DJI", "Yahoo 道瓊"),
                        ("^IXIC", "Yahoo Nasdaq")]:
        try:
            d = get_yahoo_quote(sym)
            price = d.get("price", 0) if d else 0
            rng = _get_dynamic_sanity_range(sym, source="yahoo_index")
            if rng:
                lo, hi = rng
                ok = bool(d) and lo <= price <= hi
                detail = "" if ok else (f"範圍外 ({_format_range(rng)}，近 60 天)" if d else "空回應")
            else:
                ok = bool(d) and price > 0
                detail = "" if ok else "空回應或 price<=0"
            results.append((label, ok, f"{price:,.2f}" if d else "無資料", detail))
        except Exception as e:
            results.append((label, False, f"{type(e).__name__}: {e}", ""))

    # === 熱門個股動態範圍 ===
    for sid, label in [("2330", "2330 台積電"), ("0050", "0050"),
                        ("2454", "2454 聯發科")]:
        try:
            d = get_tw_stock(sid)
            price = d.get("price", 0) if d else 0
            rng = _get_dynamic_sanity_range(sid, source="finmind_tw")
            if rng:
                lo, hi = rng
                ok = bool(d) and lo <= price <= hi
                detail = "" if ok else (f"範圍外 ({_format_range(rng)}，近 60 天)" if d else "查無")
            else:
                ok = bool(d) and price > 0
                detail = "" if ok else "查無或 price<=0"
            results.append((f"台股 {label}", ok, f"{price:,.2f}" if d else "無資料", detail))
        except Exception as e:
            results.append((f"台股 {label}", False, f"{type(e).__name__}: {e}", ""))

    return results


def format_healthcheck_report(results: list, *, brief_on_success: bool = True) -> str:
    """組裝健檢報告純文字。
    全綠時回 brief 早安；有任何失敗則回完整報告（含每項細節）。
    """
    total = len(results)
    ok_n  = sum(1 for r in results if r[1])
    fail_n = total - ok_n
    ts = now_taipei().strftime("%m/%d %H:%M")

    if fail_n == 0 and brief_on_success:
        return (f"🌸 早安，系統一切正常\n━━━━━━━━━━━━━━\n"
                f"自動健檢時間：{ts}\n"
                f"通過：{ok_n}/{total} ✅\n"
                f"全部資料源運作正常 💗")

    icon = "🚨" if fail_n else "✨"
    lines = [f"{icon} 自動健檢報告  {ts}",
             "━━━━━━━━━━━━━━",
             f"通過：{ok_n}/{total}　失敗：{fail_n}",
             ""]
    for name, ok, value, detail in results:
        sym = "✅" if ok else "❌"
        lines.append(f"{sym} {name}")
        lines.append(f"　值：{value}")
        if detail:
            lines.append(f"　問題：{detail}")
    lines.append("")
    lines.append("（輸入「健檢」查詢累積統計）")
    return "\n".join(lines)


# v10.9.65：警報 24h dedup —— 同一筆 (user, stock, type) 24 小時內只發一次
ALERT_HISTORY_FILE = "/tmp/lumistock_alert_history.json"
ALERT_DEDUP_TTL = 86400  # 24 小時

def _load_alert_history() -> dict:
    try:
        if os.path.exists(ALERT_HISTORY_FILE):
            with open(ALERT_HISTORY_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except: pass
    return {}

def _save_alert_history(history: dict) -> None:
    try:
        with open(ALERT_HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(history, f)
    except Exception as e:
        dlog("PORTFOLIO-ALERT", f"alert history 寫入失敗：{e}")

ALERT_HISTORY = _load_alert_history()

def _should_alert(uid: str, sid: str, alert_type: str) -> bool:
    """24h dedup：同一 (user, stock, type) 24 小時內只通報一次。"""
    key = f"{uid}|{sid}|{alert_type}"
    last = ALERT_HISTORY.get(key, 0)
    now_ts = time.time()
    if (now_ts - last) < ALERT_DEDUP_TTL:
        return False
    ALERT_HISTORY[key] = now_ts
    # 順便清掉超過 7 天的舊紀錄，避免無限長大
    cutoff = now_ts - 7 * 86400
    stale_keys = [k for k, v in ALERT_HISTORY.items() if v < cutoff]
    for k in stale_keys:
        ALERT_HISTORY.pop(k, None)
    _save_alert_history(ALERT_HISTORY)
    return True


def run_portfolio_alerts(force: bool = False) -> dict:
    """v10.9.63：掃描所有使用者持股，收集警報。
    v10.9.65：加 24h dedup（同一筆 user × stock × type 一日只發一次）。
    v10.9.68：加 force 參數，True 時忽略 dedup（手動觸發用）。
    回傳 {user_id: [alert_msg, ...]}
    觸發條件：
      a) 現價 ≤ 系統建議停損價 → ⚠️ 跌破（dedup key: stop_loss）
      b) 現價 ≥ 系統建議目標價 × 0.95 → 📈 接近目標（dedup key: near_target）
      c) 今日 = 除權息日 → 💰 今日除息（dedup key: ex_dividend）

    force=True：跳過 _should_alert 檢查，所有觸發條件都會 push。
    """
    alerts_by_user = {}
    try:
        portfolio = load_portfolio()
    except:
        return alerts_by_user
    by_user = {}
    for key, data in portfolio.items():
        uid = data.get("user_id")
        if uid:
            # v10.9.73：複合 key 取 symbol
            by_user.setdefault(uid, []).append((_pf_symbol(key), data))
    today = now_taipei().strftime("%Y%m%d")

    for uid, holdings in by_user.items():
        user_alerts = []
        for symbol, data in holdings:
            sid = symbol.replace(".TW", "")
            if not sid.isdigit():
                continue  # 暫不處理美股（之後再加）
            try:
                tw = get_tw_stock(sid)
                if not tw: continue
                price = tw.get("price", 0)
                name  = tw.get("name", sid)
                if not price: continue
                adv = _get_portfolio_advice(sid)
                # 觸發 a) 跌破停損
                sl = adv.get("stop_loss")
                if sl and price <= sl and (force or _should_alert(uid, sid, "stop_loss")):
                    pct_below = (price - sl) / sl * 100
                    user_alerts.append(
                        f"⚠️ {symbol} {name}\n"
                        f"　現價 {price:,.2f} 跌破系統停損 {sl:,.2f}\n"
                        f"　已破位 {pct_below:+.1f}%，建議檢視持倉"
                    )
                # 觸發 b) 接近目標（不重疊 a）
                tg = adv.get("target")
                if (tg and not (sl and price <= sl)
                    and price >= tg * 0.95
                    and (force or _should_alert(uid, sid, "near_target"))):
                    diff = (tg - price) / tg * 100
                    user_alerts.append(
                        f"📈 {symbol} {name}\n"
                        f"　現價 {price:,.2f} 接近系統目標 {tg:,.2f}\n"
                        f"　距目標 {abs(diff):.1f}%，可考慮停利"
                    )
                # 觸發 c) 今日除息
                if (adv.get("ex_div_date") == today
                    and (force or _should_alert(uid, sid, "ex_dividend"))):
                    cash = adv.get("ex_div_cash", 0)
                    stock = adv.get("ex_div_stock", 0)
                    div_text = f"現金 {cash} 元"
                    if stock > 0:
                        div_text += f" + 配股 {stock}"
                    user_alerts.append(
                        f"💰 {symbol} {name}\n"
                        f"　今日除息：{div_text}\n"
                        f"　卡片漲跌已自動修正"
                    )
            except Exception as e:
                dlog("PORTFOLIO-ALERT", f"檢查 {symbol} 失敗：{type(e).__name__}: {e}")
        if user_alerts:
            alerts_by_user[uid] = user_alerts
    return alerts_by_user


def format_portfolio_alerts_msg(alerts: list) -> str:
    """組 LINE 推播文字。"""
    ts = now_taipei().strftime("%m/%d %H:%M")
    lines = [f"💗 持股提醒  {ts}", "━━━━━━━━━━━━━━"]
    for i, a in enumerate(alerts):
        if i > 0: lines.append("")
        lines.append(a)
    lines.append("")
    lines.append("━━━━━━━━━━━━━━")
    lines.append("⚠ 系統建議僅供參考，非投資建議")
    return "\n".join(lines)


def _run_daily_healthcheck():
    """執行每日自動健檢 + 持股警報 + 排下一次"""
    dlog("HEALTHCHECK-AUTO", "🔄 開始每日自動健檢")
    try:
        results = run_healthcheck_tests()
        ok_n = sum(1 for r in results if r[1])
        total = len(results)
        report = format_healthcheck_report(results, brief_on_success=True)
        dlog("HEALTHCHECK-AUTO", f"完成：{ok_n}/{total} 通過")
        try:
            push_to_owner(report)
        except Exception as e:
            dlog("HEALTHCHECK-AUTO", f"推播失敗：{e}")
        # v10.9.63：持股警報
        try:
            alerts_by_user = run_portfolio_alerts()
            for uid, alerts in alerts_by_user.items():
                msg = format_portfolio_alerts_msg(alerts)
                push_message(uid, msg)
            dlog("HEALTHCHECK-AUTO",
                 f"持股警報：{len(alerts_by_user)} 位 user / "
                 f"{sum(len(a) for a in alerts_by_user.values())} 則訊息")
        except Exception as e:
            dlog("HEALTHCHECK-AUTO", f"持股警報執行失敗：{type(e).__name__}: {e}")
    except Exception as e:
        dlog("HEALTHCHECK-AUTO", f"❌ 例外：{type(e).__name__}: {e}")
        try:
            push_to_owner(f"🚨 自動健檢執行例外\n{type(e).__name__}: {e}")
        except: pass
    finally:
        _schedule_next_healthcheck()


def _schedule_next_healthcheck():
    """計算到下一個台北時間 06:30 的秒數，設定 Timer"""
    try:
        now = now_taipei()
        next_run = now.replace(hour=6, minute=30, second=0, microsecond=0)
        if now >= next_run:
            next_run += timedelta(days=1)
        delay_seconds = (next_run - now).total_seconds()
        timer = threading.Timer(delay_seconds, _run_daily_healthcheck)
        timer.daemon = True
        timer.start()
        hours = delay_seconds / 3600
        dlog("HEALTHCHECK-AUTO", f"⏰ 下次自動健檢：{next_run.strftime('%m/%d %H:%M')}（{hours:.1f} 小時後）")
    except Exception as e:
        dlog("HEALTHCHECK-AUTO", f"❌ 排程設定失敗：{type(e).__name__}: {e}")


def start_healthcheck_scheduler():
    """啟動每日健檢排程器（每天 06:30 執行）。多次呼叫只會啟動一次。"""
    global _HEALTHCHECK_SCHEDULER_STARTED
    if _HEALTHCHECK_SCHEDULER_STARTED:
        return
    _HEALTHCHECK_SCHEDULER_STARTED = True
    _schedule_next_healthcheck()
    dlog("HEALTHCHECK-AUTO", "✅ 自動健檢排程器已啟動")


def _load_finmind_taiwan_stock_info() -> int:
    """FinMind TaiwanStockInfo：全市場股票名稱對照（上市+上櫃，約 4100 筆）
    解決 Render 海外 IP 被 TWSE ISIN 擋住的問題。
    免費版即可用；無 token 也能呼叫（但有額度限制，建議設環境變數 FINMIND_TOKEN）。
    """
    url = "https://api.finmindtrade.com/api/v4/data"
    params = {"dataset": "TaiwanStockInfo"}
    if FINMIND_TOKEN:
        params["token"] = FINMIND_TOKEN
    try:
        r = requests.get(url, params=params, timeout=30)
        if r.status_code != 200:
            dlog("CACHE", f"FinMind StockInfo HTTP {r.status_code}")
            record_health("FinMind", False, f"HTTP {r.status_code}")
            return 0
        payload = r.json()
        if payload.get("status") != 200:
            dlog("CACHE", f"FinMind StockInfo 回應錯誤：{payload.get('msg','')[:120]}")
            record_health("FinMind", False, payload.get("msg", "")[:80])
            return 0
        data = payload.get("data") or []
        added = 0
        for row in data:
            code = (row.get("stock_id") or "").strip()
            name = (row.get("stock_name") or "").strip()
            if not code or not name:
                continue
            if not code[:1].isdigit():
                continue
            if not has_chinese(name):
                continue
            if not has_chinese(NAME_CACHE.get(code, "")):
                NAME_CACHE[code] = name
                added += 1
        dlog("CACHE", f"FinMind StockInfo：{len(data)} 筆原始 / 新增 {added} 筆")
        record_health("FinMind", True)
        return added
    except requests.Timeout:
        dlog("CACHE", "FinMind StockInfo 超時（30秒）")
        record_health("FinMind", False, "timeout 30s")
    except Exception as e:
        dlog("CACHE", f"FinMind StockInfo 失敗：{type(e).__name__}: {e}")
        record_health("FinMind", False, f"{type(e).__name__}: {e}")
    return 0


def _load_twse_securities_list() -> int:
    """證券基本資料（包含全部上市股票 + ETF，最完整）
    v10.9.27 新增 / v10.9.39 修正：處理 9MB 大檔案
    v10.9.40 修正：強化 HTTP headers（之前回傳 765 字元，是 headers 不夠完整）
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://isin.twse.com.tw/isin/single_main.jsp",
        "Upgrade-Insecure-Requests": "1",
    }
    import re as _re
    urls = [
        ("https://isin.twse.com.tw/isin/C_public.jsp?strMode=2", "上市/ETF"),
        ("https://isin.twse.com.tw/isin/C_public.jsp?strMode=4", "上櫃/ETF"),
    ]
    total_added = 0
    for url, market_label in urls:
        try:
            r = requests.get(url, headers=headers, timeout=30, verify=False)
            if r.status_code != 200:
                dlog("CACHE", f"TWSE ISIN [{market_label}] HTTP {r.status_code}")
                continue
            r.encoding = "big5"  # 重要：必須 big5（MS950）
            html_text = r.text

            if len(html_text) < 5000:
                # v10.9.40：失敗時印出實際內容前 200 字以便診斷
                preview = html_text[:200].replace("\n"," ").replace("\r"," ")
                dlog("CACHE", f"TWSE ISIN [{market_label}] 內容太短（{len(html_text)} 字元）")
                dlog("CACHE", f"  └─ 預覽：{preview}")
                continue

            count = 0
            # 抓取 <table class='h4'> 中的 <tr>
            # 結構：<td>2330　台積電</td><td>ISIN</td><td>日期</td><td>市場別</td>...
            rows = _re.findall(r"<tr[^>]*>(.*?)</tr>", html_text, _re.DOTALL)
            for row in rows:
                cells = _re.findall(r"<td[^>]*>(.*?)</td>", row, _re.DOTALL)
                if len(cells) < 2:
                    continue
                # 第 0 格是「2330　台積電」（全形空格 \u3000 分隔）
                first = _re.sub(r"<[^>]+>", "", cells[0]).strip()
                # 全形空格 + 半形空格都試
                parts = first.replace("\u3000", " ").split()
                if len(parts) < 2:
                    continue
                code = parts[0].strip()
                name = " ".join(parts[1:]).strip()  # 名字可能含空格
                # 條件：code 是數字 + name 包含中文
                if code and name and has_chinese(name) and code[:1].isdigit():
                    NAME_CACHE[code] = name
                    count += 1

            if count > 0:
                dlog("CACHE", f"TWSE ISIN [{market_label}]：{count} 筆")
                total_added += count
            else:
                dlog("CACHE", f"TWSE ISIN [{market_label}] 解析後 0 筆（HTML 結構可能改變）")
        except requests.Timeout:
            dlog("CACHE", f"TWSE ISIN [{market_label}] 超時（30秒）")
        except Exception as e:
            dlog("CACHE", f"TWSE ISIN [{market_label}] 失敗：{type(e).__name__}: {e}")
    return total_added


def init_name_cache():
    """名稱快取載入（v10.9.41：先載內建表 2,352 筆，再從 API 補新股）"""
    global NAME_CACHE_LOADING, NAME_CACHE_LOADED
    if NAME_CACHE_LOADING: return
    NAME_CACHE_LOADING = True

    dlog("CACHE", "🌟 開始載入名稱快取...")
    initial_size = len(NAME_CACHE)

    # ── 內建名稱表（v10.9.41 新增：最穩定的保底，API 失敗也不怕）
    try:
        from stock_names import STATIC_STOCK_NAMES
        static_added = 0
        for code, name in STATIC_STOCK_NAMES.items():
            if not has_chinese(NAME_CACHE.get(code, "")):
                NAME_CACHE[code] = name
                static_added += 1
        dlog("CACHE", f"🌟 內建名稱表載入：{static_added} 筆")
    except Exception as e:
        dlog("CACHE", f"⚠️ 內建名稱表載入失敗：{e}（會 fallback 用 API）")

    # ── 主要來源（v10.9.47：FinMind 取代被 IP 擋住的 TWSE ISIN）
    _load_finmind_taiwan_stock_info()  # FinMind 全市場 ~4100 筆（海外 IP 可用）
    _load_tpex_quotes()              # TPEx 上櫃報價（含名稱）— 1004 筆
    _load_tpex_emerging()            # 興櫃 — 347 筆
    _load_twse_securities_list()     # TWSE ISIN（海外 IP 通常被擋，保留為備援）

    # ── ETF 補充（v10.9.37 還原，快速失敗模式）
    _load_twse_etf()                 # 失敗會安靜略過
    _load_tpex_etf()                 # 失敗會安靜略過

    # ── 嘗試 OpenData（容錯：失敗不影響整體）
    _load_opendata("https://openapi.twse.com.tw/v1/opendata/t187ap03_L","上市公司")
    _load_opendata("https://openapi.twse.com.tw/v1/opendata/t187ap03_O","上櫃公司")
    _load_opendata("https://openapi.twse.com.tw/v1/opendata/t187ap03_R","興櫃公司")

    # ── 嘗試 STOCK_DAY_ALL（每日成交資料含名稱）
    _load_twse_stock_day_all()

    # ── 補保底
    fallback_added = 0
    for code, name in FALLBACK_NAMES.items():
        if not has_chinese(NAME_CACHE.get(code,"")):
            NAME_CACHE[code] = name
            fallback_added += 1
    if fallback_added > 0:
        dlog("CACHE", f"保底名稱補入：{fallback_added} 筆")

    NAME_CACHE_LOADING = False
    NAME_CACHE_LOADED  = True
    total = len(NAME_CACHE)
    new_loaded = total - initial_size
    dlog("CACHE", f"✅ 名稱快取完整載入：{total} 筆（本次新增 {new_loaded} 筆）")

    # v10.9.42：順便載入除權息日曆（失敗不影響其他功能）
    try:
        load_ex_dividend_calendar()
    except Exception as e:
        dlog("EXDIV", f"❌ 啟動載入失敗：{e}")

    # v10.9.46：啟動除權息自動更新排程器（每天台北時間 06:00 自動更新）
    try:
        start_ex_dividend_scheduler()
    except Exception as e:
        dlog("EXDIV-AUTO", f"❌ 排程器啟動失敗：{e}")

    # v10.9.55：啟動每日自動健檢（每天 06:30，除權息更新後 30 分鐘）
    try:
        start_healthcheck_scheduler()
    except Exception as e:
        dlog("HEALTHCHECK-AUTO", f"❌ 排程器啟動失敗：{e}")

    try:
        push_to_owner(f"✅ Lumistock 啟動完成\n名稱快取：{total} 筆\n{now_taipei().strftime('%m/%d %H:%M')}")
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
    dlog("RICHMENU", f"🌸 開始建立 Rich Menu (v{VERSION} - 5張圖 Alias)")
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
        for key, data in list(up.items())[:10]:
            symbol = _pf_symbol(key)
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
            record_health("Google Sheets", True)
    except Exception as e:
        record_health("Google Sheets", False, f"{type(e).__name__}: {e}")

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

def make_suggestion_flex(info: dict, suggestion_text: str, ts: str) -> dict:
    """v10.9.88：意見回饋 Flex 卡片（給 Owner 看的，粉嫩風格）"""
    # 用顏色區分角色
    role = info.get("role", "")
    if "Owner" in role:
        header_color = "#E89B82"  # 珊瑚粉（自己人）
    elif "管理者" in role:
        header_color = "#C9B0DB"  # 薰衣草粉
    else:
        header_color = "#E8B8A8"  # 杏粉（一般使用者）
    return {
        "type": "bubble", "size": "mega",
        "header": {
            "type": "box", "layout": "vertical",
            "backgroundColor": header_color, "paddingAll": "14px",
            "contents": [
                {"type": "text", "text": "💬 收到新建議",
                 "size": "lg", "color": "#FFFFFF", "weight": "bold"},
                {"type": "text", "text": ts,
                 "size": "xxs", "color": "#FDF6F0", "margin": "xs"},
            ]
        },
        "body": {
            "type": "box", "layout": "vertical",
            "backgroundColor": "#FDF6F0", "paddingAll": "14px", "spacing": "sm",
            "contents": [
                # 身份區
                {"type": "box", "layout": "vertical", "spacing": "xs",
                 "backgroundColor": "#FAE6DE", "cornerRadius": "8px", "paddingAll": "10px",
                 "contents": [
                    {"type": "box", "layout": "horizontal", "contents": [
                        {"type": "text", "text": "👤 來自", "size": "xxs",
                         "color": "#9B6B5A", "flex": 2},
                        {"type": "text", "text": info.get("name", ""),
                         "size": "md", "color": "#5B4040",
                         "weight": "bold", "flex": 5, "align": "end", "wrap": True}
                    ]},
                    {"type": "box", "layout": "horizontal", "contents": [
                        {"type": "text", "text": "角色", "size": "xxs",
                         "color": "#9B6B5A", "flex": 2},
                        {"type": "text", "text": info.get("role", ""),
                         "size": "xs", "color": "#A05A48", "flex": 5, "align": "end"}
                    ]},
                    {"type": "box", "layout": "horizontal", "contents": [
                        {"type": "text", "text": "user_id", "size": "xxs",
                         "color": "#9B6B5A", "flex": 2},
                        {"type": "text", "text": f"{info.get('user_id','')[:12]}...",
                         "size": "xxs", "color": "#9B6B5A", "flex": 5, "align": "end"}
                    ]}
                 ]},
                # 內容區
                {"type": "text", "text": "📝 建議內容", "size": "xs",
                 "color": "#A05A48", "weight": "bold", "margin": "md"},
                {"type": "box", "layout": "vertical",
                 "backgroundColor": "#FFFFFF", "cornerRadius": "8px",
                 "paddingAll": "12px", "margin": "sm",
                 "contents": [
                    {"type": "text", "text": suggestion_text,
                     "size": "sm", "color": "#5B4040", "wrap": True}
                 ]},
                {"type": "separator", "color": "#E8C4B4", "margin": "md"},
                {"type": "text",
                 "text": "完整紀錄已寫入 Google Sheets「系統記錄」",
                 "size": "xxs", "color": "#C9A89A", "align": "center", "margin": "sm"},
            ]
        }
    }


def get_user_display_info(user_id: str) -> dict:
    """v10.9.87：取得使用者顯示資訊（給意見回饋等情境用）。
    回傳：{"name", "role", "user_id"}
    role: "Owner" / "Admin" / "User"
    name: 註冊姓名 > LINE displayName > user_id 簡寫
    """
    rec = get_user_record(user_id)
    name = (rec.get("註冊姓名") or rec.get("displayName") or "").strip()
    if not name:
        name = f"{user_id[:8]}..."
    if is_owner(user_id):
        role = "👑 Owner"
    elif is_admin(user_id):
        role = "🛡️ 管理者"
    else:
        role = "👤 使用者"
    return {"name": name, "role": role, "user_id": user_id}

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
def _pf_key(user_id: str, symbol: str) -> str:
    """v10.9.73：持股複合 key（多使用者隔離，個人投資助理不可互相參雜）。"""
    return f"{user_id}|{symbol}"

def _pf_symbol(key: str) -> str:
    """從複合 key 取出 symbol；相容舊的 symbol-only key。"""
    return key.split("|", 1)[1] if "|" in key else key

def load_portfolio():
    if os.path.exists(PORTFOLIO_FILE):
        with open(PORTFOLIO_FILE,"r",encoding="utf-8") as f: return json.load(f)
    return {}

def save_portfolio(p):
    with open(PORTFOLIO_FILE,"w",encoding="utf-8") as f:
        json.dump(p, f, ensure_ascii=False, indent=2)


# ══════════════════════════════════════════
#  使用者設定 — 手續費折數 (v10.9.82)
# ══════════════════════════════════════════
DEFAULT_FEE_DISCOUNT = 0.6   # 預設 6 折（一般券商常見優惠）
TW_COMMISSION_RATE   = 0.001425  # 0.1425% 手續費標準
TW_TAX_RATE          = 0.003     # 0.3% 證交稅（一般股，ETF 為 0.001）
TW_MIN_COMMISSION    = 20        # 最低手續費 20 元

def _load_user_settings() -> dict:
    if os.path.exists(USER_SETTINGS_FILE):
        try:
            with open(USER_SETTINGS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except: pass
    return {}

def _save_user_settings(s: dict) -> None:
    try:
        with open(USER_SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(s, f, ensure_ascii=False, indent=2)
    except Exception as e:
        dlog("SETTINGS", f"寫入失敗：{e}")

USER_SETTINGS = _load_user_settings()

def get_user_fee_discount(user_id: str) -> float:
    s = USER_SETTINGS.get(user_id, {})
    d = s.get("fee_discount", DEFAULT_FEE_DISCOUNT)
    try:
        d = float(d)
        return max(0.1, min(1.0, d))
    except: return DEFAULT_FEE_DISCOUNT

def set_user_fee_discount(user_id: str, discount: float) -> None:
    s = USER_SETTINGS.setdefault(user_id, {})
    s["fee_discount"] = round(float(discount), 4)
    _save_user_settings(USER_SETTINGS)
    # 同步寫到 Sheets「使用者設定」（失敗不影響）
    try:
        sheet = get_sheet("使用者設定")
        if sheet:
            records = sheet.get_all_values()
            updated = False
            for i, row in enumerate(records[1:], start=2):
                if row and row[0] == user_id:
                    sheet.update_cell(i, 2, s["fee_discount"])
                    updated = True
                    break
            if not updated:
                sheet.append_row([user_id, s["fee_discount"],
                                  now_taipei().strftime("%Y-%m-%d %H:%M")])
    except: pass

def restore_user_settings_from_sheets() -> int:
    """v10.9.82：開機從 Sheets 還原使用者設定（手續費折數）。"""
    try:
        sheet = get_sheet("使用者設定")
        if not sheet: return 0
        rows = sheet.get_all_values()
        if not rows or len(rows) < 2: return 0
        for row in rows[1:]:
            if len(row) < 2: continue
            uid = (row[0] or "").strip()
            try: disc = float(row[1])
            except: continue
            if uid:
                USER_SETTINGS.setdefault(uid, {})["fee_discount"] = disc
        _save_user_settings(USER_SETTINGS)
        return len(USER_SETTINGS)
    except Exception as e:
        dlog("SETTINGS", f"還原使用者設定失敗：{e}")
        return 0

def parse_fee_discount_input(text: str):
    """v10.9.86：解析使用者手續費輸入，支援數字 / 國字 / 中文片語。
    回傳 0.1~1.0 折數倍率，無法解析則回傳 None。
    例：
      28 / 二八 → 0.28
      6.5折 / 六五折 / 65 → 0.65
      6折 / 六折 / 60 → 0.6
      1折 / 一折 / 10 → 0.10
      無折數 / 沒有折扣 / 全價 / 100 → 1.0
      半折 / 半價 → 0.50
    """
    if not text:
        return None
    t = text.strip().replace(" ", "").replace("　", "")
    # 特殊片語
    no_discount_kws = ["無折", "沒折", "全價", "沒有折扣", "沒打折", "未打折",
                       "不打折", "原價", "標準"]
    if any(kw in t for kw in no_discount_kws):
        return 1.0
    if t in ("100", "100%", "百分百", "百分之百"):
        return 1.0
    if any(kw in t for kw in ["半折", "半價"]):
        return 0.5
    # 國字 → 阿拉伯
    cn_map = {"零":"0","一":"1","二":"2","三":"3","四":"4","五":"5",
              "六":"6","七":"7","八":"8","九":"9","壹":"1","貳":"2","參":"3",
              "肆":"4","伍":"5","陸":"6","柒":"7","捌":"8","玖":"9","拾":"10"}
    converted = t
    # 先處理「十」（單獨表 10 / 「X十」表 X*10）
    converted = re.sub(r'([零一二三四五六七八九])十([零一二三四五六七八九])', lambda m: f"{cn_map[m.group(1)]}{cn_map[m.group(2)]}", converted)
    converted = converted.replace("十", "10").replace("百", "100")
    for c, n in cn_map.items():
        converted = converted.replace(c, n)
    converted = converted.replace("百分之", "")
    # 「N折」模式
    m = re.match(r"^(\d+(?:\.\d+)?)\s*折$", converted)
    if m:
        n = float(m.group(1))
        # 「28折」民間口語 = 2.8 折
        if n > 10:
            n = n / 10.0
        if 0.5 <= n <= 10:
            return round(max(0.1, min(1.0, n / 10.0)), 4)
    # 純數字（含百分號可選）
    m = re.match(r"^(\d+(?:\.\d+)?)\s*%?$", converted)
    if m:
        n = float(m.group(1))
        if 0.5 <= n <= 10:    # 視為「X折」
            return round(max(0.1, min(1.0, n / 10.0)), 4)
        if 10 < n <= 100:     # 視為百分比
            return round(max(0.1, min(1.0, n / 100.0)), 4)
    return None


def calc_buy_fee(price: float, shares: int, user_id: str) -> int:
    """買入手續費（無條件捨去取整數，符合券商實務）。"""
    trade_value = price * shares
    discount = get_user_fee_discount(user_id)
    raw = trade_value * TW_COMMISSION_RATE * discount
    return max(TW_MIN_COMMISSION, int(raw))

def calc_sell_fee_tax(price: float, shares: int, user_id: str) -> tuple:
    """賣出手續費 + 證交稅。回傳 (fee, tax)。"""
    trade_value = price * shares
    discount = get_user_fee_discount(user_id)
    fee = max(TW_MIN_COMMISSION, int(trade_value * TW_COMMISSION_RATE * discount))
    tax = int(trade_value * TW_TAX_RATE)
    return fee, tax

def restore_portfolio_from_sheets() -> int:
    """v10.9.71：開機時從 Google Sheets「自選股」還原持股到 /tmp。
    解決 Render /tmp 重啟清空 → 持股遺失的問題。
    欄位（append_row 順序）：0用戶ID 1代號 2名稱 3市場 4股數 5買入價 ... 9建立 10更新
    同一 (user_id, symbol) 取最後一筆（最新）。
    """
    try:
        sheet = get_sheet("自選股")
        if not sheet:
            return 0
        rows = sheet.get_all_values()
        if not rows or len(rows) < 2:
            return 0
        portfolio = {}
        for row in rows[1:]:  # 跳過標題列
            if len(row) < 6:
                continue
            uid = str(row[0]).strip()
            symbol = str(row[1]).strip()
            shares_s = str(row[4]).strip().replace(",", "")
            price_s = str(row[5]).strip().replace(",", "")
            if not uid or not symbol or not shares_s or not price_s:
                continue
            try:
                shares = int(float(shares_s))
                buy_price = float(price_s)
                if shares <= 0 or buy_price <= 0:
                    continue
            except:
                continue
            # 正規化：台股代號統一去掉 .TW，避免「2330」與「2330.TW」重複
            norm_symbol = symbol.replace(".TW", "") if symbol.replace(".TW","").isdigit() else symbol
            # v10.9.73：複合 key（user_id|symbol）多使用者隔離，後寫覆蓋同一人同檔
            portfolio[_pf_key(uid, norm_symbol)] = {"user_id": uid, "shares": shares, "buy_price": buy_price}
        if portfolio:
            # 合併：Sheets 還原優先，但保留 /tmp 既有未同步的（理論上不該有）
            save_portfolio(portfolio)
            dlog("PORTFOLIO", f"從 Sheets 還原 {len(portfolio)} 檔持股")
        return len(portfolio)
    except Exception as e:
        dlog("PORTFOLIO", f"還原持股失敗：{type(e).__name__}: {e}")
        return 0


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

def push_text_with_qr(user_id: str, text: str, qr_pairs: list):
    """v10.9.74：push 文字訊息並附 Quick Reply 浮標。qr_pairs = [(label, text), ...]"""
    try:
        items = [QuickReplyItem(action=MessageAction(label=l, text=t)) for l, t in qr_pairs]
        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).push_message(
                PushMessageRequest(to=user_id, messages=[
                    TextMessage(text=text, quickReply=QuickReply(items=items))]))
    except Exception as e:
        dlog("PUSH", f"push_text_with_qr 失敗：{e}")
        push_message(user_id, text)  # fallback 純文字

def push_flex(user_id: str, flex_content: dict, alt_text: str = "觀察清單"):
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
    """全球外匯（v10.9.33 重構：分 3 區 carousel - 台灣關鍵 / 美元核心 / 國際主要）"""

    def section_bubble(title, subtitle, color, buttons, header_size="lg"):
        """共用區塊樣式（每個 carousel 卡片）"""
        btn_contents = []
        for label, text in buttons:
            btn_contents.append({
                "type":"button","style":"primary","height":"sm","color":color,
                "action":{"type":"message","label":label,"text":text}
            })
        return {
            "type":"bubble","size":"mega",
            "header":{
                "type":"box","layout":"vertical","backgroundColor":color,"paddingAll":"14px",
                "contents":[
                    {"type":"text","text":title,"size":header_size,"color":"#FFFFFF","weight":"bold"},
                    {"type":"text","text":subtitle,"size":"xs","color":"#FFFFFF","wrap":True}
                ]
            },
            "body":{
                "type":"box","layout":"vertical","spacing":"sm","paddingAll":"12px",
                "contents": btn_contents
            }
        }

    bubbles = [
        # 🇹🇼 台灣關鍵匯率（v10.9.33 新增區）
        section_bubble(
            "🇹🇼 台灣關鍵匯率",
            "真正影響台股、外資、資金流",
            "#E89B82",  # 珊瑚粉
            [
                ("🇹🇼 USD/TWD 美元台幣","查USDTWD"),
                ("🇯🇵 JPY/TWD 日圓台幣","查JPYTWD"),
                ("🇪🇺 EUR/TWD 歐元台幣","查EURTWD"),
                ("🇬🇧 GBP/TWD 英鎊台幣","查GBPTWD"),
                ("🇨🇳 CNY/TWD 人民幣台幣","查CNYTWD"),
                ("🇭🇰 HKD/TWD 港幣台幣","查HKDTWD"),
                ("🇰🇷 KRW/TWD 韓元台幣","查KRWTWD"),
                ("🇦🇺 AUD/TWD 澳幣台幣","查AUDTWD"),
            ]
        ),
        # 💵 美元核心（v10.9.33 新增區）
        section_bubble(
            "💵 美元核心",
            "全球資金市場核心指標",
            "#E8B8A8",  # 奶油杏粉
            [
                ("💵 DXY 美元指數","查DXY"),
                ("📉 10 年期美債殖利率","查美債"),
                ("📊 2 年期美債殖利率","查美債2Y"),
                ("🧠 殖利率 AI 解讀","殖利率分析"),
            ]
        ),
        # 🌏 國際主要貨幣對（保留）
        section_bubble(
            "🌏 國際主要貨幣對",
            "全球資金方向・市場風險偏好",
            "#C9B0DB",  # 薰衣草粉
            [
                ("💴 USD/JPY 美元日圓","查USDJPY"),
                ("💶 EUR/USD 歐元美元","查EURUSD"),
                ("💷 GBP/USD 英鎊美元","查GBPUSD"),
                ("🇨🇳 USD/CNH 離岸人民幣","查USDCNH"),
                ("🇰🇷 USD/KRW 美元韓元","查USDKRW"),
                ("🇦🇺 AUD/USD 澳幣美元","查AUDUSD"),
            ]
        ),
        # 📊 市場分析（保留，未來 AI 解讀區）
        section_bubble(
            "📊 市場分析",
            "AI 多空判讀・資金流向",
            "#B89BC4",  # 粉紫淺
            [
                ("📊 外匯市場分析","外匯市場分析"),
                ("🔗 市場連動分析","市場連動分析"),
                ("💸 全球資金流向","全球資金流向"),
            ]
        ),
    ]
    return {"type":"carousel","contents":bubbles}

def make_ai_menu_flex() -> dict:
    return make_menu_flex(
        "🤖 AI 分析", "智慧選股・多維度評分", "#E89B82",
        [("💬 問 AI 助理","問AI"),                # v10.9.69：AI 智能問答入口
         ("⭐ 觀察清單","觀察清單"), ("📈 趨勢觀察","趨勢觀察"),
         ("🌱 成長觀察","成長觀察"), ("💰 存股觀察","存股觀察"),
         ("🌊 波段觀察","波段觀察"), ("🤖 AI概念觀察","AI概念觀察"),
         ("💬 意見回饋","意見回饋")]            # v10.9.87：使用者建議直送 Owner
    )

def make_news_menu_flex() -> dict:
    return make_menu_flex(
        "📰 財經新聞", "AI 解讀・台股・美股・國際", "#D9C5B3",
        [("🤖 AI 新聞解讀","AI新聞"),
         ("📊 個股新聞","個股新聞"), ("🇹🇼 台股新聞","台股新聞"),
         ("🇺🇸 美股新聞","美股新聞"), ("🌐 國際新聞","國際新聞"),
         ("🌏 地緣政治","地緣政治新聞")]
    )

def make_portfolio_menu_flex() -> dict:
    # v10.9.86：移除「加碼登記」（已併入「新增」智能模式）
    return make_menu_flex(
        "📋 持股管理", "新增・賣出・損益・設定", "#5B8B6B",
        [("📋 我的持股","持股"),
         ("💗 立即持股警報","立即持股警報"),
         ("➕ 新增持股","新增持股說明"),
         ("💸 賣出登記","賣出說明"),
         ("💰 手續費設定","手續費設定"),           # v10.9.86：改用新 handler 進入直接輸入模式
         ("📊 損益分析","損益分析"),
         ("🔴 停損提醒","停損提醒說明"),
         ("🎯 目標價提醒","目標價提醒說明")]
    )

def make_admin_menu_flex(user_id: str) -> dict:
    """打字「管理後台」時用的 Flex（粉白少女系）"""
    owner = is_owner(user_id)
    color = "#E8B8A8"
    buttons = [
        ("💗 系統健檢","健檢"),                  # v10.9.54：放最上方，Owner 一鍵直達
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
        [("💗 系統健檢","健檢"),                  # v10.9.54：監測入口，放第一個
         ("🩺 立即測試所有 API","立即測試"),       # v10.9.55：手動觸發完整測試
         ("🔄 重新載入名稱快取","重載名稱"),
         ("📊 查看快取狀態","快取狀態"),
         ("🔍 查詢個別代號","查快取說明"),
         ("🌸 重設 Rich Menu","重設選單")]     # 緊急救援
    )


# ══════════════════════════════════════════
#  外匯/商品資料
# ══════════════════════════════════════════
FOREX_SYMBOLS = {
    # 🇹🇼 台灣關鍵匯率（v10.9.33 新增獨立區）— 真正影響台股、外資、資金流
    "查USDTWD": ("TWD=X",    "🇹🇼 USD/TWD 美元台幣"),
    "查JPYTWD": ("JPYTWD=X", "🇯🇵 JPY/TWD 日圓台幣"),
    "查EURTWD": ("EURTWD=X", "🇪🇺 EUR/TWD 歐元台幣"),
    "查GBPTWD": ("GBPTWD=X", "🇬🇧 GBP/TWD 英鎊台幣"),
    "查CNYTWD": ("CNYTWD=X", "🇨🇳 CNY/TWD 人民幣台幣"),
    "查HKDTWD": ("HKDTWD=X", "🇭🇰 HKD/TWD 港幣台幣"),
    "查KRWTWD": ("KRWTWD=X", "🇰🇷 KRW/TWD 韓元台幣"),
    "查AUDTWD": ("AUDTWD=X", "🇦🇺 AUD/TWD 澳幣台幣"),
    # 💵 美元核心
    "查DXY":    ("DX-Y.NYB", "💵 DXY 美元指數"),
    # 🌏 國際主要貨幣對（保留，代表全球資金方向）
    "查USDJPY": ("JPY=X",    "USD/JPY 美元日圓"),
    "查EURUSD": ("EURUSD=X", "EUR/USD 歐元美元"),
    "查GBPUSD": ("GBPUSD=X", "GBP/USD 英鎊美元"),
    "查USDCNY": ("CNY=X",    "USD/CNY 美元人民幣"),
    "查USDCNH": ("CNH=X",    "USD/CNH 美元離岸人民幣"),
    "查AUDUSD": ("AUDUSD=X", "AUD/USD 澳幣美元"),
    "查USDCHF": ("CHFUSD=X", "USD/CHF 美元瑞郎"),
    "查USDKRW": ("KRW=X",    "USD/KRW 美元韓元"),
    "查USDHKD": ("HKD=X",    "USD/HKD 美元港幣"),
}

MARKET_SYMBOLS = {
    # 🇹🇼 台股
    "查台股加權": ("^TWII",  "台股加權指數"),
    "查櫃買指數": ("__TPEXIDX__", "台灣櫃買指數"),  # v10.9.51：改用 TPEx 官方
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
    """抓 Yahoo Finance 指數/商品/外匯報價。
    v10.9.50：加入自我防禦——某些指數（例如 ^TWOII 櫃買指數）的 regularMarketPrice
    Yahoo 會卡在過期的舊值（甚至 1 年以上前），導致漲跌幅算出 -34% 等荒謬數字。
    防禦策略：當 regularMarketPrice 跟最近日線收盤偏離 > 5% 或時間 > 24 小時前，
    視為過時資料，改用 close 陣列的最新值。
    """
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=10d"
        r   = requests.get(url, headers=headers, timeout=10)
        result = r.json()["chart"]["result"][0]
        meta   = result["meta"]
        quotes = result.get("indicators",{}).get("quote",[{}])[0]
        closes = [c for c in quotes.get("close",[]) if c is not None]
        ms     = meta.get("marketState","")

        rmp = meta.get("regularMarketPrice")
        rmt = meta.get("regularMarketTime", 0)
        latest_close = closes[-1] if closes else 0

        # 偵測 regularMarketPrice 是否過時或異常
        is_stale  = False
        is_outlier = False
        try:
            if rmt:
                now_ts = datetime.now(timezone.utc).timestamp()
                is_stale = (now_ts - float(rmt)) > 86400  # 超過 24 小時
            if rmp and latest_close:
                is_outlier = abs(float(rmp) - latest_close) / latest_close > 0.05
        except: pass

        meta_source = "Yahoo Finance"
        meta_is_realtime = False
        meta_is_fallback = False
        meta_delay_min = 15

        if rmp and not is_stale and not is_outlier:
            price = float(rmp)
            meta_is_realtime = (ms == "REGULAR")
            meta_delay_min = 0 if meta_is_realtime else 15
        else:
            price = latest_close
            meta_is_fallback = True
            meta_source = "Yahoo Finance（日線備援）"
            if rmp and (is_stale or is_outlier):
                dlog("YAHOO", f"⚠️ {symbol} regularMarketPrice 異常（rmp={rmp}, latest_close={latest_close}, stale={is_stale}, outlier={is_outlier}）→ 改用日線 close")

        prev   = closes[-2] if len(closes) >= 2 else price
        chg    = price - prev
        pct    = chg / prev * 100 if prev else 0
        record_health("Yahoo Finance", True)
        return {
            "price":price, "chg":chg, "pct":pct, "ms":ms,
            "meta": build_data_meta(meta_source,
                                    is_realtime=meta_is_realtime,
                                    is_fallback=meta_is_fallback,
                                    delay_min=meta_delay_min),
        }
    except Exception as e:
        dlog("YAHOO", f"get_yahoo_quote({symbol}) 失敗：{type(e).__name__}: {e}")
        record_health("Yahoo Finance", False, f"{type(e).__name__}: {e}")
        return {}


def _fetch_tpex_index_raw():
    """v10.9.78：TPEx OpenAPI 抓取，加 SSL 寬容 + 較長 timeout，多 endpoint 嘗試。
    回傳 (rows or None, last_err)
    """
    endpoints = [
        "https://www.tpex.org.tw/openapi/v1/tpex_index",
        "http://www.tpex.org.tw/openapi/v1/tpex_index",  # SSL 掛掉時試 http
    ]
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    last_err = ""
    for url in endpoints:
        for attempt in range(3):
            try:
                # verify=False 讓 SSL 握手失敗時仍能取得資料（TPEx 偶爾 SSL 不穩）
                r = requests.get(url, headers=headers, timeout=15, verify=False)
                if r.status_code != 200:
                    last_err = f"HTTP {r.status_code}"
                else:
                    parsed = r.json()
                    if isinstance(parsed, list) and parsed:
                        return parsed, ""
                    last_err = "empty list"
            except Exception as e:
                last_err = f"{type(e).__name__}: {str(e)[:80]}"
            if attempt < 2:
                time.sleep(2)
    return None, last_err


def _fallback_otc_from_yahoo() -> dict:
    """v10.9.78：TPEx 全失敗時的最後備援 — Yahoo ^TWOII。
    Yahoo ^TWOII 已知問題：rmp 可能卡舊值、close 陣列日期可能偏移 1 天。
    所以這裡用 chartPreviousClose 對齊：取 closes[-1] 當「最近收盤」，差距大時直接放棄。
    回傳含 meta is_fallback=True、source 標記 Yahoo 備援。
    """
    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/^TWOII?interval=1d&range=10d"
        r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=10)
        if r.status_code != 200:
            return {}
        result = r.json()["chart"]["result"][0]
        quotes = result.get("indicators",{}).get("quote",[{}])[0]
        closes = [c for c in quotes.get("close",[]) if c is not None]
        if len(closes) < 2:
            return {}
        price = float(closes[-1])
        prev = float(closes[-2])
        chg = price - prev
        pct = chg/prev*100 if prev else 0
        # 合理範圍 sanity（櫃買大約 100-1000）
        if not (50 < price < 2000):
            return {}
        return {
            "price": price, "chg": chg, "pct": pct, "ms": "POST",
            "meta": build_data_meta("Yahoo ^TWOII（備援，日期可能偏移 1 天）",
                                    is_realtime=False, is_fallback=True, delay_min=1440),
        }
    except Exception as e:
        dlog("TPEX", f"Yahoo 備援也失敗：{type(e).__name__}: {e}")
        return {}


def get_taiwan_otc_index() -> dict:
    """抓台灣櫃買指數。
    v10.9.51：放棄 Yahoo 主來源（^TWOII rmp 卡舊值 + close 日期偏移 1 天）→ 改 TPEx 官方。
    v10.9.67：加 3x retry 解決 transient 空回應。
    v10.9.78：TPEx SSL 偶爾掛 → verify=False + 多 endpoint 嘗試 + Yahoo 備援保底。
    """
    rows, last_err = _fetch_tpex_index_raw()

    if not rows:
        dlog("TPEX", f"櫃買指數 TPEx 全失敗：{last_err}，嘗試 Yahoo 備援")
        record_health("TPEx 官方", False, f"primary fail: {last_err[:80]}")
        # v10.9.78：最後備援 Yahoo，至少給使用者有數字看
        fb = _fallback_otc_from_yahoo()
        if fb:
            dlog("TPEX", "已用 Yahoo 備援")
        return fb

    try:
        rows = sorted(rows, key=lambda x: x.get("Date",""))
        last = rows[-1]
        prev = rows[-2] if len(rows) >= 2 else last
        price = float(str(last.get("Close","0")).replace(",",""))
        prev_close = float(str(prev.get("Close","0")).replace(",",""))
        chg_str = str(last.get("Change","0")).strip().replace(",","")
        chg = float(chg_str) if chg_str else (price - prev_close)
        pct = chg / prev_close * 100 if prev_close else 0
        date_str = str(last.get("Date",""))
        today_str = now_taipei().strftime("%Y%m%d")
        is_today = (date_str == today_str)
        record_health("TPEx 官方", True)
        return {
            "price": price, "chg": chg, "pct": pct,
            "ms": "REGULAR" if is_today else "POST",
            "meta": build_data_meta("TPEx 官方", is_realtime=False,
                                    is_fallback=False,
                                    delay_min=0 if is_today else 1440),
        }
    except Exception as e:
        dlog("TPEX", f"櫃買指數解析失敗：{type(e).__name__}: {e}")
        record_health("TPEx 官方", False, f"parse {type(e).__name__}")
        return {}


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
    # v10.9.52：metadata 行
    meta = data.get("meta")
    meta_text = fmt_data_meta_full(meta) if isinstance(meta, dict) else ""
    meta_color = "#B89BC4" if (isinstance(meta, dict) and meta.get("is_fallback")) else "#A07560"
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
                ]},
                *([{"type":"text","text":meta_text,"size":"xxs","color":meta_color,"wrap":True,"margin":"sm"}] if meta_text else [])
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

            # v10.9.42：除權息日當天，MIS 的 y（昨收）沒扣股利，需修正
            ex_div = get_ex_dividend_info(stock_id)
            if ex_div:
                cash = ex_div["cash"]
                # 除息參考價 = 昨收 - 現金股利
                ref_price = prev - cash
                chg = price - ref_price
                pct = chg/ref_price*100 if ref_price else 0
                dlog("EXDIV", f"{stock_id} 今日除息 {cash} 元，修正漲跌：{price}-{ref_price:.2f}={chg:+.2f}({pct:+.2f}%)")

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
            result = {"name":name,"price":price,"chg":chg,"pct":pct,
                    "open":open_v,"high":high_v,"low":low_v,"vol":vol_str,
                    "market_type":"台股","status":"盤中" if is_rt else "試撮","source":"TWSE 即時",
                    "meta": build_data_meta("TWSE MIS", is_realtime=is_rt, is_fallback=False, delay_min=0)}
            # v10.9.42：除權息日，傳遞給 UI 顯示
            if ex_div:  # ex_div 在上方計算 chg 時已賦值（None 或 dict）
                result["ex_dividend"] = ex_div["cash"]
            return result
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
                    "market_type":"台股","status":"收盤","source":"TWSE",
                    "meta": build_data_meta("TWSE 日線", is_realtime=False, is_fallback=True, delay_min=0)}
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
                    "market_type":"台股","status":"收盤","source":"TPEx",
                    "meta": build_data_meta("TPEx 日線", is_realtime=False, is_fallback=True, delay_min=0)}
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
                    "market_type":"台股","status":"收盤","source":"Yahoo Finance",
                    "meta": build_data_meta("Yahoo Finance", is_realtime=False, is_fallback=True, delay_min=15)}
        except: pass
    return None


# ══════════════════════════════════════════
#  美股資料
# ══════════════════════════════════════════
def get_us_stock(symbol: str) -> dict:
    """美股報價（v10.9.66：補上 v10.9.50 同等的 stale rmp 防禦 + meta + record_health）。
    跟 get_yahoo_quote 同樣的偵測邏輯：
      若 regularMarketPrice 過時（> 24hr）或偏離 latest_close > 5%，改用日線 close 並標記為備援。
    """
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=10d"
        r = requests.get(url, headers=headers, timeout=10)
        result = r.json()["chart"]["result"][0]
        meta_raw = result["meta"]
        quotes = result.get("indicators",{}).get("quote",[{}])[0]
        opens  = [o for o in quotes.get("open",[]) if o is not None]
        highs  = [h for h in quotes.get("high",[]) if h is not None]
        lows   = [l for l in quotes.get("low",[]) if l is not None]
        vols   = [v for v in quotes.get("volume",[]) if v is not None]
        closes = [c for c in quotes.get("close",[]) if c is not None]
        ms = meta_raw.get("marketState","")

        rmp = meta_raw.get("regularMarketPrice")
        rmt = meta_raw.get("regularMarketTime", 0)
        latest_close = closes[-1] if closes else 0

        is_stale = False
        is_outlier = False
        try:
            if rmt:
                is_stale = (datetime.now(timezone.utc).timestamp() - float(rmt)) > 86400
            if rmp and latest_close:
                is_outlier = abs(float(rmp) - latest_close) / latest_close > 0.05
        except: pass

        meta_source = "Yahoo Finance"
        meta_is_realtime = False
        meta_is_fallback = False
        meta_delay_min = 15

        # 依市場狀態 + stale 偵測決定 price / prev
        if ms == "POST":
            post_price = meta_raw.get("postMarketPrice")
            if post_price:
                price = float(post_price)
                meta_delay_min = 0
            elif rmp and not is_stale and not is_outlier:
                price = float(rmp)
                meta_delay_min = 0
            else:
                price = latest_close
                meta_is_fallback = True
                meta_source = "Yahoo Finance（日線備援）"
            prev = closes[-1] if closes else price
        elif ms == "PRE":
            pre_price = meta_raw.get("preMarketPrice")
            if pre_price:
                price = float(pre_price)
                meta_delay_min = 0
            elif rmp and not is_stale and not is_outlier:
                price = float(rmp)
                meta_delay_min = 0
            else:
                price = latest_close
                meta_is_fallback = True
                meta_source = "Yahoo Finance（日線備援）"
            prev = closes[-2] if len(closes) >= 2 else price
        else:
            if rmp and not is_stale and not is_outlier:
                price = float(rmp)
                meta_is_realtime = (ms == "REGULAR")
                meta_delay_min = 0 if meta_is_realtime else 15
            else:
                price = latest_close
                meta_is_fallback = True
                meta_source = "Yahoo Finance（日線備援）"
                if rmp and (is_stale or is_outlier):
                    dlog("YAHOO", f"⚠️ US {symbol} rmp 異常（rmp={rmp}, latest={latest_close}, stale={is_stale}, outlier={is_outlier}）→ 改用日線 close")
            prev = closes[-2] if len(closes) >= 2 else price

        chg = price - prev
        pct = chg / prev * 100 if prev else 0
        name = meta_raw.get("shortName") or meta_raw.get("longName") or symbol
        sl = {"POST":"盤後","PRE":"盤前","REGULAR":"盤中","CLOSED":"收盤"}.get(ms,"")

        record_health("Yahoo Finance", True)
        return {
            "name": name[:20], "price": price, "chg": chg, "pct": pct,
            "open": f"{opens[-1]:.2f}" if opens else "N/A",
            "high": f"{highs[-1]:.2f}" if highs else "N/A",
            "low": f"{lows[-1]:.2f}" if lows else "N/A",
            "vol": format_us_volume(vols[-1]) if vols else "N/A",
            "status": sl, "closes": [],
            "meta": build_data_meta(meta_source,
                                    is_realtime=meta_is_realtime,
                                    is_fallback=meta_is_fallback,
                                    delay_min=meta_delay_min),
        }
    except Exception as e:
        dlog("YAHOO", f"get_us_stock({symbol}) 失敗：{type(e).__name__}: {e}")
        record_health("Yahoo Finance", False, f"US {type(e).__name__}: {e}")
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

def _load_finmind_closes_adj(stock_id: str) -> list:
    """v10.9.59：用 FinMind TaiwanStockPriceAdj 抓還原股價收盤序列（近 1 年）。
    還原股價已扣除權息影響，K 線分析（均線/RSI）不會因除權息日出現大幅跳水誤判。
    """
    if not FINMIND_TOKEN:
        return []
    end_date   = now_taipei().strftime("%Y-%m-%d")
    start_date = (now_taipei() - timedelta(days=400)).strftime("%Y-%m-%d")
    url = "https://api.finmindtrade.com/api/v4/data"
    params = {
        "dataset": "TaiwanStockPriceAdj",
        "data_id": stock_id,
        "start_date": start_date,
        "end_date": end_date,
        "token": FINMIND_TOKEN,
    }
    try:
        r = requests.get(url, params=params, timeout=15)
        if r.status_code != 200:
            record_health("FinMind", False, f"PriceAdj HTTP {r.status_code}")
            return []
        payload = r.json()
        if payload.get("status") != 200:
            return []
        rows = payload.get("data") or []
        if not rows:
            return []
        record_health("FinMind", True)
        rows.sort(key=lambda x: x.get("date",""))  # asc
        closes = [float(r.get("close", 0)) for r in rows if r.get("close") is not None]
        return closes
    except Exception as e:
        record_health("FinMind", False, f"PriceAdj {type(e).__name__}")
        return []


def get_tw_closes(stock_id: str) -> list:
    """個股近 1 年收盤序列（v10.9.59：FinMind 還原股價主來源 → Yahoo → TWSE 備援）。
    Layer 0 FinMind 還原股價：除權息日不會誤判暴跌（K 線、均線、RSI 都更準確）。
    """
    # Layer 0：FinMind 還原股價（v10.9.59 新增主來源）
    closes = _load_finmind_closes_adj(stock_id)
    if len(closes) >= 20:
        return closes

    headers={"User-Agent":"Mozilla/5.0"}
    # Layer 1：Yahoo Finance（未還原，除權息日會跳水）
    for suffix in [".TW",".TWO"]:
        try:
            url=f"https://query1.finance.yahoo.com/v8/finance/chart/{stock_id}{suffix}?interval=1d&range=1y"
            r=requests.get(url,headers=headers,timeout=10)
            closes=r.json()["chart"]["result"][0]["indicators"]["quote"][0]["close"]
            closes=[c for c in closes if c is not None]
            if len(closes)>=20: return closes
        except: pass
    # Layer 2：TWSE STOCK_DAY 備援
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
#  Groq AI 模組（v10.9.35 新增）
#  用途：新聞語意去重 + 情緒分析 + AI 摘要
#  模型：Llama 3.3 70B Versatile（免費、夠強）
# ══════════════════════════════════════════
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "")
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "llama-3.3-70b-versatile"
GROQ_AVAILABLE = bool(GROQ_API_KEY)

# 新聞 AI 結果快取（key=normalized_title, value=(result, timestamp)）
NEWS_AI_CACHE = {}
NEWS_AI_CACHE_TTL = 3600  # 1 小時


def groq_chat(messages: list, max_tokens: int = 1500, temperature: float = 0.2, timeout: int = 8) -> str:
    """呼叫 Groq Chat API，回傳純文字。失敗時回傳空字串"""
    if not GROQ_AVAILABLE:
        return ""
    try:
        headers = {
            "Authorization": f"Bearer {GROQ_API_KEY}",
            "Content-Type": "application/json"
        }
        payload = {
            "model": GROQ_MODEL,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        r = requests.post(GROQ_API_URL, headers=headers, json=payload, timeout=timeout)
        if r.status_code == 200:
            data = r.json()
            record_health("Groq AI", True)
            return data["choices"][0]["message"]["content"].strip()
        elif r.status_code == 429:
            dlog("GROQ", f"⏸️ 達到速率限制（429），暫時降級為規則式")
            record_health("Groq AI", False, "rate limit 429")
            return ""
        elif r.status_code == 413:
            dlog("GROQ", f"📏 請求太大（413），可能批次太多新聞")
            record_health("Groq AI", False, "payload too large 413")
            return ""
        else:
            dlog("GROQ", f"❌ API 錯誤 HTTP {r.status_code}: {r.text[:200]}")
            record_health("Groq AI", False, f"HTTP {r.status_code}")
            return ""
    except requests.Timeout:
        dlog("GROQ", "⏱️ API 超時（>8s）")
        record_health("Groq AI", False, "timeout")
        return ""
    except Exception as e:
        dlog("GROQ", f"❌ 呼叫失敗：{e}")
        record_health("Groq AI", False, f"{type(e).__name__}: {e}")
        return ""


def analyze_brokerage_screenshot(image_bytes: bytes, mime: str = "image/jpeg") -> dict:
    """v10.9.81：統一辨識券商截圖（庫存 OR 賣出回報）。
    回傳：
      {"type":"holdings", "items":[{stock_id, shares, avg_price}]}  ← 庫存頁
      {"type":"sell",     "items":[{stock_id, shares, sell_price}]}  ← 賣出/委成回（只取已成交賣單）
      {"type":"unknown",  "items":[]}                                 ← 無法判斷
    """
    if not GROQ_AVAILABLE:
        return {"type": "unknown", "items": []}
    try:
        b64 = base64.b64encode(image_bytes).decode()
    except Exception as e:
        dlog("VISION", f"base64 失敗：{e}")
        return {"type": "unknown", "items": []}

    prompt = """這是台灣證券戶的截圖。請判斷類型並辨識資料。

【類型 1：庫存頁】顯示「目前持有的股票」，欄位有股數、平均成本/均價、現價/損益等
→ 回傳 {"type":"holdings","items":[{"stock_id":"2330","shares":100,"avg_price":2010.0}, ...]}

【類型 2：賣出/委成回】顯示「賣出/現賣/沖賣/沖售」字樣，且為「已成交」
→ 只取「已成交的賣出」（忽略委託中、買單、未成交）
→ 回傳 {"type":"sell","items":[{"stock_id":"6742","shares":1000,"sell_price":59.5}]}

【類型 3：買進/成交回報】顯示「買進/現買/沖買/ROD買」字樣，且為「已成交」
→ 只取「已成交的買進」
→ 回傳 {"type":"buy","items":[{"stock_id":"2330","shares":1000,"buy_price":2010.0}]}

【類型 4：無法判斷】
→ 回傳 {"type":"unknown","items":[]}

判斷優先序：
- 同時混合買賣 → 看哪邊筆數多就回哪邊；但若使用者明顯只截某一邊（如標題顯示「賣出回報」）以該邊為準
- 純庫存（沒交易動作字樣，多欄損益/現價）→ holdings

規則：
- stock_id：4-6 位字元（2330/00878/6446）
- shares：統一換算成「股數」（張 × 1000）
- 庫存 avg_price = 平均成本；買進 buy_price = 成交價；賣出 sell_price = 成交價
- 看不清楚的整筆略過
- 只回純 JSON，不要 markdown、不要其他文字
- 忽略「委託中」「部份成交（未完成）」等未實際成交的列
"""

    payload = {
        "model": "meta-llama/llama-4-scout-17b-16e-instruct",
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": prompt},
                {"type": "image_url",
                 "image_url": {"url": f"data:{mime};base64,{b64}"}},
            ]
        }],
        "max_tokens": 2048,
        "temperature": 0.0,
    }

    try:
        r = requests.post(GROQ_API_URL, headers={
            "Authorization": f"Bearer {GROQ_API_KEY}",
            "Content-Type": "application/json"
        }, json=payload, timeout=30)
        if r.status_code != 200:
            dlog("VISION", f"vision HTTP {r.status_code}: {r.text[:200]}")
            record_health("Groq AI", False, f"vision HTTP {r.status_code}")
            return {"type": "unknown", "items": []}
        data = r.json()
        text = data["choices"][0]["message"]["content"].strip()
        record_health("Groq AI", True)
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        try:
            obj = json.loads(text)
        except Exception as e:
            dlog("VISION", f"JSON 解析失敗：{e} / raw={text[:200]}")
            return {"type": "unknown", "items": []}
        if not isinstance(obj, dict):
            return {"type": "unknown", "items": []}
        t = obj.get("type", "unknown")
        items = obj.get("items", [])
        if not isinstance(items, list):
            items = []
        # 過濾驗證
        valid = []
        for h in items:
            if not isinstance(h, dict): continue
            sid = str(h.get("stock_id", "")).strip().upper()
            if not re.match(r"^[0-9]{4,6}[A-Z]?$", sid): continue
            sh = h.get("shares")
            if sh is None: continue
            try:
                shares = int(float(sh))
                if shares <= 0: continue
            except: continue
            if t == "holdings":
                bp = h.get("avg_price")
                if bp is None: continue
                try:
                    price = float(bp)
                    if price <= 0: continue
                    valid.append({"stock_id": sid, "shares": shares, "avg_price": price})
                except: continue
            elif t == "sell":
                sp = h.get("sell_price")
                if sp is None: continue
                try:
                    price = float(sp)
                    if price <= 0: continue
                    valid.append({"stock_id": sid, "shares": shares, "sell_price": price})
                except: continue
            elif t == "buy":
                bp = h.get("buy_price")
                if bp is None: continue
                try:
                    price = float(bp)
                    if price <= 0: continue
                    valid.append({"stock_id": sid, "shares": shares, "buy_price": price})
                except: continue
        dlog("VISION", f"辨識：type={t}, raw {len(items)} → valid {len(valid)}")
        if t not in ("holdings", "sell", "buy"):
            t = "unknown"
        return {"type": t, "items": valid}
    except Exception as e:
        dlog("VISION", f"vision 例外：{type(e).__name__}: {e}")
        record_health("Groq AI", False, f"vision {type(e).__name__}")
        return {"type": "unknown", "items": []}


def analyze_portfolio_screenshot(image_bytes: bytes, mime: str = "image/jpeg") -> list:
    """v10.9.64：用 Groq Llama 4 Scout vision 辨識券商庫存截圖。
    回傳 [{"stock_id":"2330","shares":100,"avg_price":2010.0}, ...]
    """
    if not GROQ_AVAILABLE:
        dlog("VISION", "Groq 未設定，無法辨識")
        return []
    try:
        b64 = base64.b64encode(image_bytes).decode()
    except Exception as e:
        dlog("VISION", f"base64 編碼失敗：{e}")
        return []

    prompt = """這是台灣證券戶的庫存截圖。請仔細辨識所有持股，回傳純 JSON array（不要 markdown）：

[{"stock_id": "2330", "shares": 100, "avg_price": 2010.0}, ...]

規則：
- stock_id 是 4-6 位字元（例：2330、00878、6446、2330R）
- shares 一律換算成「股數」（若截圖是「張」則 ×1000）
- avg_price 是平均買進價（每股新台幣）
- 看不清楚的欄位填 null（整筆會被略過）
- 只回 JSON array，不要其他文字、不要 markdown
- 若無法辨識任何持股，回傳 []
"""

    payload = {
        "model": "meta-llama/llama-4-scout-17b-16e-instruct",
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": prompt},
                {"type": "image_url",
                 "image_url": {"url": f"data:{mime};base64,{b64}"}},
            ]
        }],
        "max_tokens": 2048,
        "temperature": 0.0,
    }

    try:
        r = requests.post(GROQ_API_URL, headers={
            "Authorization": f"Bearer {GROQ_API_KEY}",
            "Content-Type": "application/json"
        }, json=payload, timeout=30)
        if r.status_code != 200:
            dlog("VISION", f"Groq vision 失敗 HTTP {r.status_code}: {r.text[:200]}")
            record_health("Groq AI", False, f"vision HTTP {r.status_code}")
            return []
        data = r.json()
        text = data["choices"][0]["message"]["content"].strip()
        record_health("Groq AI", True)
        # 去除 markdown 包裝
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        try:
            holdings = json.loads(text)
        except Exception as e:
            dlog("VISION", f"JSON 解析失敗：{e} / raw={text[:200]}")
            return []
        if not isinstance(holdings, list):
            return []
        # validation
        valid = []
        for h in holdings:
            if not isinstance(h, dict): continue
            sid = str(h.get("stock_id", "")).strip().upper()
            sh = h.get("shares")
            bp = h.get("avg_price")
            if not sid or not re.match(r"^[0-9]{4,6}[A-Z]?$", sid): continue
            if sh is None or bp is None: continue
            try:
                shares = int(float(sh))
                price = float(bp)
                if shares <= 0 or price <= 0: continue
                valid.append({"stock_id": sid, "shares": shares, "avg_price": price})
            except: continue
        dlog("VISION", f"辨識完成：raw {len(holdings)} 項 → valid {len(valid)} 項")
        return valid
    except Exception as e:
        dlog("VISION", f"vision 例外：{type(e).__name__}: {e}")
        record_health("Groq AI", False, f"vision {type(e).__name__}")
        return []


def read_sell_history_from_sheets(user_id: str) -> list:
    """v10.9.85：讀 Sheets「賣出紀錄」分頁，取該使用者所有賣出紀錄。
    欄位順序（append_row 寫入時）：時間、用戶ID、代號、名稱、賣股數、賣價、成本均價、已實現損益
    回傳 [{date, symbol, name, shares, price, cost, pnl}, ...]，依時間 desc 排序。
    """
    try:
        sheet = get_sheet("賣出紀錄")
        if not sheet: return []
        rows = sheet.get_all_values()
        if not rows or len(rows) < 2: return []
        out = []
        for row in rows[1:]:
            if len(row) < 8: continue
            if str(row[1]).strip() != user_id: continue
            try:
                out.append({
                    "date": (row[0] or "").strip(),
                    "symbol": (row[2] or "").strip(),
                    "name": (row[3] or "").strip(),
                    "shares": int(float(row[4])),
                    "price": float(row[5]),
                    "cost": float(row[6]),
                    "pnl": float(row[7]),
                })
            except: continue
        out.sort(key=lambda r: r["date"], reverse=True)
        return out
    except Exception as e:
        dlog("PORTFOLIO", f"讀賣出紀錄失敗：{e}")
        return []


def format_pnl_analysis(user_id: str) -> str:
    """v10.9.85：損益分析 — 已實現（Sheets）+ 未實現（即時持股）分開呈現。
    取代原本 get_portfolio_summary（與「我的持股」重複）。
    """
    realized = read_sell_history_from_sheets(user_id)
    portfolio = load_portfolio()
    up = {k: v for k, v in portfolio.items() if v.get("user_id") == user_id}

    lines = ["📈 損益分析", "━━━━━━━━━━━━━━"]

    # ── 已實現損益（從 Sheets 賣出紀錄） ──
    lines.append("")
    lines.append("📕 已實現損益（賣出紀錄）")
    if not realized:
        lines.append("　（尚無賣出紀錄）")
    else:
        total_realized = 0
        for r in realized:
            total_realized += r["pnl"]
        # 顯示最近 10 筆細節
        for r in realized[:10]:
            sign = "🟢" if r["pnl"] >= 0 else "🔴"
            d_short = r["date"][:10] if len(r["date"]) >= 10 else r["date"]
            lines.append(f"　{d_short}　{sign} {r['symbol']} {r['name']}")
            lines.append(f"　　賣 {r['shares']:,} 股 @ {r['price']:,.2f}　{r['pnl']:+,.0f} 元")
        if len(realized) > 10:
            lines.append(f"　... 另有 {len(realized)-10} 筆較早紀錄")
        sign = "🟢" if total_realized >= 0 else "🔴"
        lines.append("")
        lines.append(f"　合計（{len(realized)} 筆）：{sign} {total_realized:+,.0f} 元")

    # ── 未實現損益（從目前持股 + 即時報價） ──
    lines.append("")
    lines.append("📘 未實現損益（目前持股）")
    if not up:
        lines.append("　（持股清單是空的）")
    else:
        total_unrealized = 0
        ok_count = 0
        for k, data in up.items():
            symbol = _pf_symbol(k)
            sid = symbol.replace(".TW", "")
            try:
                if sid.isdigit():
                    tw = get_tw_stock(sid)
                    price = tw["price"] if tw else 0
                    name = tw["name"] if tw else sid
                else:
                    us = get_us_stock(symbol)
                    price = us["price"] if us else 0
                    name = us["name"] if us else symbol
                shares = int(data.get("shares", 0))
                bp = float(data.get("buy_price", 0))
                if price > 0 and bp > 0:
                    pnl = (price - bp) * shares
                    pct = (price - bp) / bp * 100
                    sign = "🟢" if pnl >= 0 else "🔴"
                    lines.append(f"　{sign} {sid} {name}（{shares:,} 股）")
                    lines.append(f"　　均 {bp:,.2f} → 現 {price:,.2f}　{pnl:+,.0f}（{pct:+.2f}%）")
                    total_unrealized += pnl
                    ok_count += 1
                else:
                    lines.append(f"　⚠ {sid} 報價暫無")
            except: pass
        if ok_count:
            sign = "🟢" if total_unrealized >= 0 else "🔴"
            lines.append("")
            lines.append(f"　合計（{ok_count} 檔）：{sign} {total_unrealized:+,.0f} 元")

    lines.append("")
    lines.append("━━━━━━━━━━━━━━")
    lines.append("⚠ 已實現與未實現分開計算，不合計")
    lines.append("　未實現用即時報價（可能延遲 15 分）")
    lines.append("　已實現已扣手續費 + 證交稅")
    return "\n".join(lines)


def save_sell_to_sheets(user_id, symbol, name, sell_shares, sell_price, cost_avg, realized_pnl):
    """v10.9.81：賣出紀錄寫入 Google Sheets「賣出紀錄」分頁。"""
    try:
        sheet = get_sheet("賣出紀錄")
        if sheet:
            now = now_taipei().strftime("%Y-%m-%d %H:%M")
            sheet.append_row([now, user_id, symbol, name,
                              sell_shares, sell_price, cost_avg, realized_pnl])
    except Exception as e:
        dlog("PORTFOLIO", f"save_sell_to_sheets 失敗：{e}")


def process_sell(user_id: str, stock_id: str, sell_shares: int, sell_price: float) -> dict:
    """v10.9.81：處理賣出 — 從持股扣股數、算已實現損益、寫 Sheets。
    v10.9.82：含手續費 + 證交稅計算。
    回傳 {"ok": bool, "msg": str, "realized_pnl": float, ..., "fee": int, "tax": int}
    """
    portfolio = load_portfolio()
    norm = stock_id.replace(".TW", "")
    candidates = [
        _pf_key(user_id, norm),
        _pf_key(user_id, stock_id),
        norm, stock_id, stock_id + ".TW",
    ]
    target_key = None
    for k in candidates:
        if k in portfolio:
            v = portfolio[k]
            if v.get("user_id") == user_id:
                target_key = k
                break
    if not target_key:
        return {"ok": False, "msg": f"❌ 持股清單裡找不到 {norm}\n請先確認你有持有這檔，或用「新增 代號 股數 均價」建立"}

    data = portfolio[target_key]
    held_shares = int(data.get("shares", 0))
    cost_avg = float(data.get("buy_price", 0))

    if sell_shares > held_shares:
        return {"ok": False,
                "msg": f"❌ 賣出股數 {sell_shares:,} 超過持有 {held_shares:,} 股\n請確認"}

    # v10.9.82：含手續費 + 證交稅
    gross = sell_price * sell_shares
    fee, tax = calc_sell_fee_tax(sell_price, sell_shares, user_id)
    net_proceeds = gross - fee - tax
    cost_total = cost_avg * sell_shares
    realized_pnl = net_proceeds - cost_total
    remaining = held_shares - sell_shares
    name = NAME_CACHE.get(norm, norm)

    if remaining > 0:
        portfolio[target_key] = {
            "user_id": user_id,
            "shares": remaining,
            "buy_price": cost_avg,  # 賣出不影響成本均價
        }
    else:
        del portfolio[target_key]

    save_portfolio(portfolio)
    save_sell_to_sheets(user_id, norm, name, sell_shares, sell_price, cost_avg, realized_pnl)

    sign = "🟢 賺" if realized_pnl >= 0 else "🔴 虧"
    pct = realized_pnl / cost_total * 100 if cost_total else 0
    discount = get_user_fee_discount(user_id)
    disc_str = f"{int(discount*100)}%" if discount < 1.0 else "無折扣"
    msg = (f"✅ 賣出完成：{norm} {name}\n"
           f"━━━━━━━━━━━━━━\n"
           f"　賣出 {sell_shares:,} 股 @ {sell_price:,.2f}\n"
           f"　毛收入　{gross:>10,.0f}\n"
           f"　手續費　-{fee:>9,}（折數 {disc_str}）\n"
           f"　證交稅　-{tax:>9,}（0.3%）\n"
           f"　淨收入　{net_proceeds:>10,.0f}\n"
           f"　成本　　{cost_total:>10,.0f}（均價 {cost_avg:,.2f}）\n"
           f"　{sign}　{realized_pnl:+,.0f} 元（{pct:+.2f}%）\n"
           f"　剩餘庫存 {remaining:,} 股")
    return {"ok": True, "msg": msg, "realized_pnl": realized_pnl,
            "remaining_shares": remaining, "name": name,
            "fee": fee, "tax": tax, "gross": gross, "net": net_proceeds}


def process_buy(user_id: str, stock_id: str, buy_shares: int, buy_price: float) -> dict:
    """v10.9.83：分批買進（加碼）— 既有部位做加權平均，新部位則建立。
    含買入手續費納入成本均價。
    回傳 {"ok": bool, "msg": str, "is_new": bool, ...}
    """
    norm = stock_id.replace(".TW", "")
    portfolio = load_portfolio()

    # 找該使用者既有部位
    target_key = None
    for k in (_pf_key(user_id, norm), _pf_key(user_id, stock_id),
              norm, stock_id, stock_id + ".TW"):
        v = portfolio.get(k)
        if v and v.get("user_id") == user_id:
            target_key = k
            break

    # 計算這次買入的手續費 + 真實成本
    is_tw = norm.isdigit()
    buy_fee = calc_buy_fee(buy_price, buy_shares, user_id) if is_tw else 0
    new_cost_total = buy_price * buy_shares + buy_fee  # 含費總成本

    if target_key:
        # 加碼：加權平均
        old = portfolio[target_key]
        old_shares = int(old.get("shares", 0))
        old_avg = float(old.get("buy_price", 0))
        old_cost_total = old_avg * old_shares
        total_shares = old_shares + buy_shares
        new_avg = (old_cost_total + new_cost_total) / total_shares if total_shares else buy_price
        # 統一搬到複合 key
        if target_key != _pf_key(user_id, norm):
            portfolio.pop(target_key, None)
        portfolio[_pf_key(user_id, norm)] = {
            "user_id": user_id,
            "shares": total_shares,
            "buy_price": new_avg,
        }
        is_new = False
        sign = "📈"
        action = "加碼"
        change_str = (f"原 {old_shares:,} 股 ‧ 均價 {old_avg:,.2f}\n"
                      f"加碼 {buy_shares:,} 股 @ {buy_price:,.2f}\n"
                      f"手續費 {buy_fee:,}\n"
                      f"加權後 {total_shares:,} 股 ‧ 均價 {new_avg:,.4f}")
        msg_payload = {"total_shares": total_shares, "new_avg": new_avg}
    else:
        # 新部位
        new_avg = new_cost_total / buy_shares if buy_shares else buy_price
        portfolio[_pf_key(user_id, norm)] = {
            "user_id": user_id,
            "shares": buy_shares,
            "buy_price": new_avg,
        }
        is_new = True
        sign = "🆕"
        action = "新增"
        change_str = (f"新增 {buy_shares:,} 股 @ {buy_price:,.2f}\n"
                      f"手續費 {buy_fee:,}\n"
                      f"含費成本均價 {new_avg:,.4f}")
        msg_payload = {"total_shares": buy_shares, "new_avg": new_avg}

    save_portfolio(portfolio)
    # Sheets 同步（用 append_row 紀錄這次交易；保留歷史）
    try:
        name = NAME_CACHE.get(norm, norm)
        market = "台股" if is_tw else "美股"
        save_portfolio_to_sheets(user_id, norm, name, market,
                                 msg_payload["total_shares"], msg_payload["new_avg"])
    except: pass

    name = NAME_CACHE.get(norm, norm)
    discount = get_user_fee_discount(user_id)
    disc_str = f"{int(discount*100)}%" if discount < 1.0 else "無折扣"
    msg = (f"{sign} {action}成功：{norm} {name}\n"
           f"━━━━━━━━━━━━━━\n"
           f"{change_str}\n"
           f"（折數 {disc_str}）")
    return {"ok": True, "msg": msg, "is_new": is_new, **msg_payload}


def make_sell_preview_flex(items: list, user_id: str) -> dict:
    """v10.9.89：賣出預覽 Flex carousel — 帶確認/取消按鈕，使用者不用打字。"""
    portfolio = load_portfolio()
    discount = get_user_fee_discount(user_id)
    disc_str = f"{int(discount*100)}%" if discount < 1.0 else "無折扣"
    parsed = []
    total_pnl = 0
    can_process = 0
    for h in items:
        sid = h["stock_id"]; shares = h["shares"]; price = h["sell_price"]
        norm = sid.replace(".TW", "")
        name = NAME_CACHE.get(norm, "")
        held = None
        for k in (_pf_key(user_id, norm), _pf_key(user_id, sid), norm, sid, sid + ".TW"):
            v = portfolio.get(k)
            if v and v.get("user_id") == user_id:
                held = v; break
        rec = {"sid": sid, "name": name, "shares": shares, "price": price, "error": None}
        if held:
            cost = float(held.get("buy_price", 0))
            held_n = int(held.get("shares", 0))
            if shares <= held_n:
                gross = price * shares
                fee, tax = calc_sell_fee_tax(price, shares, user_id)
                net = gross - fee - tax
                pnl = net - cost * shares
                rec.update({"cost": cost, "gross": gross, "fee": fee, "tax": tax,
                            "net": net, "pnl": pnl, "held": held_n})
                total_pnl += pnl
                can_process += 1
            else:
                rec["error"] = f"持有僅 {held_n:,} 股"
        else:
            rec["error"] = "持股清單沒這檔"
        parsed.append(rec)

    pnl_color = "#D97A5C" if total_pnl >= 0 else "#7AABBE"
    pnl_sign = "🟢 賺" if total_pnl >= 0 else "🔴 虧"
    overview = {
        "type": "bubble", "size": "mega",
        "header": {"type":"box","layout":"vertical","backgroundColor":"#E89B82","paddingAll":"14px",
            "contents":[
                {"type":"text","text":"💸 賣出預覽","size":"lg","color":"#FFFFFF","weight":"bold"},
                {"type":"text","text":f"手續費 {disc_str} ‧ 證交稅 0.3%","size":"xxs","color":"#FDF6F0","margin":"xs"}
            ]},
        "body": {"type":"box","layout":"vertical","backgroundColor":"#FDF6F0","paddingAll":"14px","spacing":"sm",
            "contents":[
                {"type":"text","text":"預估合計淨損益","size":"xs","color":"#9B6B5A"},
                {"type":"text","text":f"{pnl_sign}　{total_pnl:+,.0f} 元",
                 "size":"xl","color":pnl_color,"weight":"bold"},
                {"type":"text","text":f"{can_process}/{len(items)} 筆可處理",
                 "size":"xs","color":"#A07560","margin":"sm"},
                {"type":"separator","color":"#E8C4B4","margin":"md"},
                {"type":"text","text":"⚠ 請確認後按下方按鈕","size":"xxs","color":"#C9A89A","align":"center","margin":"sm"}
            ]},
        "footer": {"type":"box","layout":"vertical","spacing":"sm","paddingAll":"8px","contents":[
            {"type":"button","style":"primary","color":"#D97A5C","height":"sm",
             "action":{"type":"message","label":"✅ 確認賣出","text":"確認賣出"}},
            {"type":"button","style":"secondary","height":"sm",
             "action":{"type":"message","label":"🚫 取消賣出","text":"取消賣出"}}
        ]}
    }

    bubbles = [overview]
    for i, r in enumerate(parsed[:11], 1):
        if r["error"]:
            header_color = "#C9B0DB"
            body_contents = [
                {"type":"text","text":f"{r['sid']} {r['name']}".rstrip(),"size":"sm","color":"#5B4040","weight":"bold","wrap":True},
                {"type":"text","text":f"賣 {r['shares']:,} 股 @ {r['price']:,.2f}","size":"xs","color":"#9B6B5A","margin":"sm"},
                {"type":"separator","color":"#E8C4B4","margin":"md"},
                {"type":"text","text":f"⚠ {r['error']}","size":"xs","color":"#D97A5C","weight":"bold","margin":"sm","wrap":True}
            ]
        else:
            pnl_c = "#D97A5C" if r["pnl"] >= 0 else "#7AABBE"
            pnl_s = "🟢" if r["pnl"] >= 0 else "🔴"
            header_color = pnl_c
            body_contents = [
                {"type":"text","text":f"{r['sid']} {r['name']}".rstrip(),"size":"sm","color":"#5B4040","weight":"bold","wrap":True},
                {"type":"text","text":f"賣 {r['shares']:,} 股 @ {r['price']:,.2f}","size":"xs","color":"#9B6B5A","margin":"sm"},
                {"type":"separator","color":"#E8C4B4","margin":"sm"},
                {"type":"box","layout":"horizontal","contents":[
                    {"type":"text","text":"毛","size":"xxs","color":"#9B6B5A","flex":1},
                    {"type":"text","text":f"{r['gross']:,.0f}","size":"xxs","color":"#5B4040","flex":2,"align":"end"}]},
                {"type":"box","layout":"horizontal","contents":[
                    {"type":"text","text":"費","size":"xxs","color":"#9B6B5A","flex":1},
                    {"type":"text","text":f"-{r['fee']}","size":"xxs","color":"#7AABBE","flex":2,"align":"end"}]},
                {"type":"box","layout":"horizontal","contents":[
                    {"type":"text","text":"稅","size":"xxs","color":"#9B6B5A","flex":1},
                    {"type":"text","text":f"-{r['tax']}","size":"xxs","color":"#7AABBE","flex":2,"align":"end"}]},
                {"type":"box","layout":"horizontal","contents":[
                    {"type":"text","text":"淨","size":"xxs","color":"#9B6B5A","flex":1,"weight":"bold"},
                    {"type":"text","text":f"{r['net']:,.0f}","size":"xs","color":"#5B4040","weight":"bold","flex":2,"align":"end"}]},
                {"type":"separator","color":"#E8C4B4","margin":"sm"},
                {"type":"box","layout":"horizontal","contents":[
                    {"type":"text","text":"成本均","size":"xxs","color":"#9B6B5A","flex":1},
                    {"type":"text","text":f"{r['cost']:,.2f}","size":"xxs","color":"#5B4040","flex":2,"align":"end"}]},
                {"type":"box","layout":"horizontal","contents":[
                    {"type":"text","text":"淨損益","size":"xs","color":"#A05A48","flex":1,"weight":"bold"},
                    {"type":"text","text":f"{pnl_s} {r['pnl']:+,.0f}","size":"sm","color":pnl_c,"weight":"bold","flex":2,"align":"end"}]}
            ]
        bubbles.append({
            "type":"bubble","size":"kilo",
            "header":{"type":"box","layout":"horizontal","backgroundColor":header_color,"paddingAll":"10px",
                "contents":[{"type":"text","text":f"#{i}","size":"sm","color":"#FFFFFF","weight":"bold"}]},
            "body":{"type":"box","layout":"vertical","backgroundColor":"#FDF6F0","paddingAll":"12px","spacing":"xs",
                "contents":body_contents}
        })
    return {"type":"carousel","contents":bubbles}


def make_buy_preview_flex(items: list, user_id: str) -> dict:
    """v10.9.89：買進預覽 Flex carousel — 帶確認/取消按鈕。"""
    portfolio = load_portfolio()
    discount = get_user_fee_discount(user_id)
    disc_str = f"{int(discount*100)}%" if discount < 1.0 else "無折扣"
    parsed = []
    for h in items:
        sid = h["stock_id"]; shares = h["shares"]; price = h["buy_price"]
        norm = sid.replace(".TW", "")
        name = NAME_CACHE.get(norm, "")
        is_tw = norm.isdigit()
        fee = calc_buy_fee(price, shares, user_id) if is_tw else 0
        held = None
        for k in (_pf_key(user_id, norm), _pf_key(user_id, sid), norm, sid, sid + ".TW"):
            v = portfolio.get(k)
            if v and v.get("user_id") == user_id:
                held = v; break
        rec = {"sid": sid, "name": name, "shares": shares, "price": price, "fee": fee}
        if held:
            old_shares = int(held.get("shares", 0))
            old_avg = float(held.get("buy_price", 0))
            new_total = old_shares + shares
            new_avg = (old_avg*old_shares + price*shares + fee) / new_total
            rec.update({"is_new": False, "old_shares": old_shares, "old_avg": old_avg,
                        "new_total": new_total, "new_avg": new_avg})
        else:
            new_avg = (price * shares + fee) / shares if shares else price
            rec.update({"is_new": True, "new_avg": new_avg, "new_total": shares})
        parsed.append(rec)

    overview = {
        "type":"bubble","size":"mega",
        "header":{"type":"box","layout":"vertical","backgroundColor":"#5B8B6B","paddingAll":"14px",
            "contents":[
                {"type":"text","text":"📈 買進預覽","size":"lg","color":"#FFFFFF","weight":"bold"},
                {"type":"text","text":f"手續費 {disc_str}","size":"xxs","color":"#FDF6F0","margin":"xs"}
            ]},
        "body":{"type":"box","layout":"vertical","backgroundColor":"#FDF6F0","paddingAll":"14px","spacing":"sm",
            "contents":[
                {"type":"text","text":f"共 {len(parsed)} 筆","size":"sm","color":"#5B4040","weight":"bold"},
                {"type":"text","text":"既有部位 → 自動加權平均，新部位 → 建立","size":"xxs","color":"#A07560","margin":"sm","wrap":True},
                {"type":"separator","color":"#E8C4B4","margin":"md"},
                {"type":"text","text":"⚠ 請確認後按下方按鈕","size":"xxs","color":"#C9A89A","align":"center","margin":"sm"}
            ]},
        "footer":{"type":"box","layout":"vertical","spacing":"sm","paddingAll":"8px","contents":[
            {"type":"button","style":"primary","color":"#5B8B6B","height":"sm",
             "action":{"type":"message","label":"✅ 確認加碼","text":"確認加碼"}},
            {"type":"button","style":"secondary","height":"sm",
             "action":{"type":"message","label":"🚫 取消加碼","text":"取消加碼"}}
        ]}
    }
    bubbles = [overview]
    for i, r in enumerate(parsed[:11], 1):
        tag = "🆕 新增" if r["is_new"] else "📈 加碼"
        header_color = "#E89B82" if r["is_new"] else "#5B8B6B"
        body_contents = [
            {"type":"text","text":f"{r['sid']} {r['name']}".rstrip(),"size":"sm","color":"#5B4040","weight":"bold","wrap":True},
            {"type":"text","text":f"{tag} {r['shares']:,} 股 @ {r['price']:,.2f}","size":"xs","color":"#9B6B5A","margin":"sm"},
            {"type":"text","text":f"手續費 {r['fee']:,}","size":"xxs","color":"#9B6B5A"},
            {"type":"separator","color":"#E8C4B4","margin":"sm"},
        ]
        if r["is_new"]:
            body_contents += [
                {"type":"text","text":"建立新部位","size":"xxs","color":"#A05A48","weight":"bold"},
                {"type":"box","layout":"horizontal","contents":[
                    {"type":"text","text":"含費均價","size":"xxs","color":"#9B6B5A","flex":1},
                    {"type":"text","text":f"{r['new_avg']:,.4f}","size":"sm","color":"#A05A48","weight":"bold","flex":2,"align":"end"}]},
            ]
        else:
            body_contents += [
                {"type":"box","layout":"horizontal","contents":[
                    {"type":"text","text":"原","size":"xxs","color":"#9B6B5A","flex":1},
                    {"type":"text","text":f"{r['old_shares']:,} 股均 {r['old_avg']:,.2f}","size":"xxs","color":"#5B4040","flex":3,"align":"end"}]},
                {"type":"box","layout":"horizontal","contents":[
                    {"type":"text","text":"→ 新","size":"xxs","color":"#A05A48","flex":1,"weight":"bold"},
                    {"type":"text","text":f"{r['new_total']:,} 股均 {r['new_avg']:,.4f}","size":"xs","color":"#A05A48","weight":"bold","flex":3,"align":"end"}]},
            ]
        bubbles.append({
            "type":"bubble","size":"kilo",
            "header":{"type":"box","layout":"horizontal","backgroundColor":header_color,"paddingAll":"10px",
                "contents":[{"type":"text","text":f"#{i}","size":"sm","color":"#FFFFFF","weight":"bold"}]},
            "body":{"type":"box","layout":"vertical","backgroundColor":"#FDF6F0","paddingAll":"12px","spacing":"xs",
                "contents":body_contents}
        })
    return {"type":"carousel","contents":bubbles}


def make_holdings_preview_flex(items: list) -> dict:
    """v10.9.89：庫存匯入預覽 Flex carousel — 帶確認/取消按鈕。"""
    overview = {
        "type":"bubble","size":"mega",
        "header":{"type":"box","layout":"vertical","backgroundColor":"#C9B0DB","paddingAll":"14px",
            "contents":[
                {"type":"text","text":"📋 庫存匯入預覽","size":"lg","color":"#FFFFFF","weight":"bold"},
                {"type":"text","text":"⚠ 將覆蓋既有部位（重設用）","size":"xxs","color":"#FDF6F0","margin":"xs"}
            ]},
        "body":{"type":"box","layout":"vertical","backgroundColor":"#FDF6F0","paddingAll":"14px","spacing":"sm",
            "contents":[
                {"type":"text","text":f"共 {len(items)} 檔","size":"sm","color":"#5B4040","weight":"bold"},
                {"type":"text","text":"確認後一鍵儲存到「我的持股」","size":"xxs","color":"#A07560","margin":"sm"},
                {"type":"separator","color":"#E8C4B4","margin":"md"},
                {"type":"text","text":"⚠ 請核對辨識結果","size":"xxs","color":"#C9A89A","align":"center","margin":"sm"}
            ]},
        "footer":{"type":"box","layout":"vertical","spacing":"sm","paddingAll":"8px","contents":[
            {"type":"button","style":"primary","color":"#C9B0DB","height":"sm",
             "action":{"type":"message","label":"✅ 確認匯入","text":"確認匯入"}},
            {"type":"button","style":"secondary","height":"sm",
             "action":{"type":"message","label":"🚫 取消匯入","text":"取消匯入"}}
        ]}
    }
    bubbles = [overview]
    for i, h in enumerate(items[:11], 1):
        sid = h["stock_id"]; shares = h["shares"]; price = h["avg_price"]
        name = NAME_CACHE.get(sid, "")
        bubbles.append({
            "type":"bubble","size":"kilo",
            "header":{"type":"box","layout":"horizontal","backgroundColor":"#E8B8A8","paddingAll":"10px",
                "contents":[{"type":"text","text":f"#{i}","size":"sm","color":"#FFFFFF","weight":"bold"}]},
            "body":{"type":"box","layout":"vertical","backgroundColor":"#FDF6F0","paddingAll":"12px","spacing":"xs",
                "contents":[
                    {"type":"text","text":f"{sid} {name}".rstrip(),"size":"sm","color":"#5B4040","weight":"bold","wrap":True},
                    {"type":"separator","color":"#E8C4B4","margin":"sm"},
                    {"type":"box","layout":"horizontal","contents":[
                        {"type":"text","text":"股數","size":"xxs","color":"#9B6B5A","flex":1},
                        {"type":"text","text":f"{shares:,}","size":"sm","color":"#5B4040","weight":"bold","flex":2,"align":"end"}]},
                    {"type":"box","layout":"horizontal","contents":[
                        {"type":"text","text":"均價","size":"xxs","color":"#9B6B5A","flex":1},
                        {"type":"text","text":f"{price:,.2f}","size":"sm","color":"#A05A48","weight":"bold","flex":2,"align":"end"}]},
                ]}
        })
    return {"type":"carousel","contents":bubbles}


def format_buy_import_preview(items: list, user_id: str) -> str:
    """v10.9.83：買進截圖辨識結果預覽，含加碼後預估均價。"""
    if not items:
        return "❌ 沒有辨識到任何買進交易"
    portfolio = load_portfolio()
    discount = get_user_fee_discount(user_id)
    disc_str = f"{int(discount*100)}%" if discount < 1.0 else "無折扣"
    lines = ["📈 辨識結果 — 買進交易",
             f"（手續費折數 {disc_str}）",
             "━━━━━━━━━━━━━━"]
    for i, h in enumerate(items, 1):
        sid = h["stock_id"]; shares = h["shares"]; price = h["buy_price"]
        norm = sid.replace(".TW", "")
        name = NAME_CACHE.get(norm, "")
        is_tw = norm.isdigit()
        fee = calc_buy_fee(price, shares, user_id) if is_tw else 0
        # 找既有部位
        held = None
        for k in (_pf_key(user_id, norm), _pf_key(user_id, sid), norm, sid, sid + ".TW"):
            v = portfolio.get(k)
            if v and v.get("user_id") == user_id:
                held = v; break
        lines.append(f"{i}. {sid} {name}".rstrip())
        if held:
            old_shares = int(held.get("shares", 0))
            old_avg = float(held.get("buy_price", 0))
            new_total = old_shares + shares
            new_avg = (old_avg*old_shares + price*shares + fee) / new_total
            lines.append(f"　🔁 加碼 {shares:,} 股 @ {price:,.2f}　費 {fee:,}")
            lines.append(f"　原 {old_shares:,} 股 均 {old_avg:,.2f}")
            lines.append(f"　→ {new_total:,} 股 均 {new_avg:,.4f}")
        else:
            new_avg = (price * shares + fee) / shares if shares else price
            lines.append(f"　🆕 新增 {shares:,} 股 @ {price:,.2f}　費 {fee:,}")
            lines.append(f"　→ 含費均價 {new_avg:,.4f}")
    lines.append("━━━━━━━━━━━━━━")
    lines.append("　• 輸入「確認加碼」執行")
    lines.append("　• 輸入「取消加碼」放棄")
    lines.append("　• 5 分鐘後自動取消")
    return "\n".join(lines)


def format_sell_import_preview(items: list, user_id: str) -> str:
    """v10.9.81：賣出截圖辨識結果預覽，含預估已實現損益。
    v10.9.82：扣手續費 + 證交稅後的淨損益。
    """
    if not items:
        return "❌ 沒有辨識到任何賣出交易"
    portfolio = load_portfolio()
    discount = get_user_fee_discount(user_id)
    disc_str = f"{int(discount*100)}%" if discount < 1.0 else "無折扣"
    lines = ["💸 辨識結果 — 賣出交易",
             f"（手續費折數 {disc_str}　證交稅 0.3%）",
             "━━━━━━━━━━━━━━"]
    total_pnl = 0
    can_process = 0
    for i, h in enumerate(items, 1):
        sid = h["stock_id"]
        shares = h["shares"]
        price = h["sell_price"]
        name = NAME_CACHE.get(sid, "")
        # 查現有持股算預估損益
        norm = sid.replace(".TW", "")
        held = None
        for k in (_pf_key(user_id, norm), _pf_key(user_id, sid), norm, sid, sid + ".TW"):
            v = portfolio.get(k)
            if v and v.get("user_id") == user_id:
                held = v; break
        lines.append(f"{i}. {sid} {name}".rstrip())
        lines.append(f"　賣出 {shares:,} 股 @ {price:,.2f}")
        if held:
            cost = float(held.get("buy_price", 0))
            held_n = int(held.get("shares", 0))
            if shares <= held_n:
                gross = price * shares
                fee, tax = calc_sell_fee_tax(price, shares, user_id)
                net = gross - fee - tax
                pnl = net - cost * shares
                total_pnl += pnl
                sign = "🟢" if pnl >= 0 else "🔴"
                lines.append(f"　毛 {gross:,.0f} ‧ 費 -{fee} ‧ 稅 -{tax}")
                lines.append(f"　成本 {cost:,.2f} ‧ {sign} 淨 {pnl:+,.0f} 元")
                can_process += 1
            else:
                lines.append(f"　⚠ 持有僅 {held_n:,} 股，無法賣出 {shares:,}")
        else:
            lines.append(f"　⚠ 持股清單沒這檔")
    lines.append("━━━━━━━━━━━━━━")
    if can_process:
        sign = "🟢" if total_pnl >= 0 else "🔴"
        lines.append(f"預估合計淨損益：{sign} {total_pnl:+,.0f} 元（{can_process} 筆可處理）")
    lines.append("　• 輸入「確認賣出」執行")
    lines.append("　• 輸入「取消賣出」放棄")
    lines.append("　• 5 分鐘後自動取消")
    return "\n".join(lines)


def format_portfolio_import_preview(items: list) -> str:
    """組合「辨識結果預覽」訊息給使用者確認。"""
    if not items:
        return "❌ 沒有辨識到任何持股"
    lines = ["📋 辨識結果 — 確認後即可匯入",
             "━━━━━━━━━━━━━━"]
    for i, h in enumerate(items, 1):
        sid = h["stock_id"]
        name = NAME_CACHE.get(sid, "")
        lines.append(f"{i}. {sid} {name}".rstrip())
        lines.append(f"　{h['shares']:,} 股 ‧ 均價 {h['avg_price']:,.2f}")
    lines.append("━━━━━━━━━━━━━━")
    lines.append("⚠ 請核對辨識結果")
    lines.append("　• 輸入「確認匯入」儲存全部")
    lines.append("　• 輸入「取消匯入」放棄")
    lines.append("　• 5 分鐘後自動取消")
    return "\n".join(lines)


# ══════════════════════════════════════════
#  AI 智能問答系統（v10.9.69）
#  核心：grounding（先查資料再回答）+ 不亂答 + 合規 + 商品比較/族群分析
# ══════════════════════════════════════════

AI_QA_SYSTEM_PROMPT = """你是 Lumistock（慧股拾光）的 AI 投資研究助理，由 Hui 開發。
角色：AI 投資助理 / 市場研究員 / 商品比較顧問 / 投資教育助理 / 功能客服。
全程使用繁體中文，語氣專業但好懂。

【最高原則：絕不無根據亂答】
1. 只有「資料區」提供的數據才能引用為事實。
2. 絕對不可以憑空編造：股價、漲跌幅、成交量、法人買賣超、配息金額、費用率、成分股、新聞內容、財報數字。
3. 若「資料區」沒有某項資料，就明說「目前缺少 XX 資料，無法精準判斷」，不可以假裝知道。
4. 不確定就說不確定。寧可保守，不要硬掰。

【合規（重要）】
1. 禁用詞：建議買進、建議賣出、保證、一定漲、一定跌、明牌、必賺、現在買一定對。
2. 改用：偏多 / 偏空 / 短線有機會 / 需留意 / 可考慮觀察 / 接近壓力 / 跌破支撐要小心。
3. 不預測未來股價（「明天會漲嗎」→ 答「無法預測，只能整理目前技術/籌碼/消息面」）。
4. 每則投資相關回答結尾務必加一行：「⚠ 僅供參考，不構成投資建議」。

【商品比較必須從「人」的角度，不只比商品】
比較 ETF / 基金 / 股票 / 債券 時，除商品差異外，必須分析：
- 適合什麼樣的人、什麼投資心態、追求什麼目標
- 不適合什麼樣的人
- 投資前該有的觀念（配息≠獲利、總報酬比殖利率重要、除息會扣股價、要看填息）
- 不可以說「哪個比較好」，要說「哪個比較適合什麼樣的人」
- 提醒可以「核心（市值型）+ 衛星（高股息）」搭配，不一定二選一

【投資人格對應】依使用者描述判斷並對應合適方向：
- 保守穩健型（怕虧損、不想盯盤）→ 市值型 ETF 定期定額 + 債券搭配，避免槓桿/單壓
- 穩定現金流型（喜歡領息）→ 高股息 ETF / 債券 ETF，但提醒別只看殖利率
- 長期成長型（年輕、可承受波動）→ 市值型/全球型 ETF、優質成長股
- 積極研究型（願做功課）→ 個股/成長股/波段，但需停損與資金控管
- 短線交易型 → 流動性高標的，提醒高股息 ETF 不適合當短線主力
- 容易焦慮型 → 定期定額、分散、低頻檢視，避免槓桿/短線/重壓

【回答格式（依問題類型彈性運用，不要每次都套同一模板）】
- 個股：①現況（價/漲跌/量）②技術面（均線/RSI/趨勢）③籌碼面（法人）④消息面（新聞）⑤觀察重點 ⑥風險 ⑦AI 信心(高/中/低)
- 商品比較：①快速結論 ②商品定位 ③主要差異 ④適合族群與心態 ⑤優缺點 ⑥風險 ⑦總結（依目標）⑧AI 信心
- 知識教學：①白話解釋 ②投資上代表什麼 ③怎麼用 ④常見誤解 ⑤簡單範例
- 功能/排錯：①功能說明 ②操作步驟 ③可能異常原因 ④需要時提醒回報管理員

【避免】
- 不要只說「投資有風險請自行判斷」就結束（要有實質分析）
- 不要過度絕對
- 商品比較若缺最新具體數字（費用率/配息/成分股），標示「具體數字需查證最新資料」

【AI 信心評分依據】資料完整度、資料更新時間、指標是否一致、是否有重大事件干擾。資料嚴重不足時直接說「目前資料不足，AI 不做強判斷」。
"""


# 常見台股名稱 → 代號（補強：使用者打名稱也能對應）
_COMMON_NAME_TO_ID = {
    "台積電": "2330", "聯發科": "2454", "鴻海": "2317", "台達電": "2308",
    "廣達": "2382", "聯電": "2303", "日月光": "3711", "中華電": "2412",
    "富邦金": "2881", "國泰金": "2882", "中信金": "2891", "玉山金": "2884",
    "兆豐金": "2886", "元大金": "2885", "開發金": "2883", "第一金": "2892",
    "鈊象": "3293", "緯創": "3231", "緯穎": "6669", "技嘉": "2376",
    "華碩": "2357", "和碩": "4938", "大立光": "3008", "智原": "3035",
    "創意": "3443", "世芯": "3661", "長榮": "2603", "陽明": "2609",
    "台塑": "1301", "南亞": "1303", "中鋼": "2002", "台泥": "1101",
    "統一": "1216", "台塑化": "6505",
}

def _detect_stocks_in_question(text: str) -> list:
    """從問題中辨識股票代號（4-6 位數字）或常見名稱，回傳 [stock_id, ...]（最多 3 檔）。"""
    found = []
    # 1. 直接的數字代號
    for m in re.findall(r"\b(\d{4,6}[A-Za-z]?)\b", text):
        code = m.upper()
        if code not in found:
            found.append(code)
    # 2. 常見名稱
    for name, code in _COMMON_NAME_TO_ID.items():
        if name in text and code not in found:
            found.append(code)
    # 3. NAME_CACHE 反查（名稱 → 代號），但只比對 >= 2 字的名稱避免誤判
    if len(found) < 3:
        for code, nm in list(NAME_CACHE.items())[:6000]:
            if nm and len(nm) >= 2 and nm in text and code not in found:
                found.append(code)
                if len(found) >= 3:
                    break
    return found[:3]


def _build_stock_context(sid: str) -> str:
    """組單檔股票的資料區文字（供 AI grounding）。"""
    tw = get_tw_stock(sid)
    if not tw:
        return f"[{sid}] 查無即時報價資料"
    name = tw.get("name", sid)
    lines = [f"◆ {sid} {name}"]
    lines.append(f"　現價 {tw['price']:.2f}　漲跌 {tw['chg']:+.2f}（{tw['pct']:+.2f}%）　{tw.get('status','')}")
    lines.append(f"　開 {tw.get('open','N/A')} 高 {tw.get('high','N/A')} 低 {tw.get('low','N/A')} 量 {tw.get('vol','N/A')}")
    if tw.get("ex_dividend"):
        lines.append(f"　今日除息 {tw['ex_dividend']} 元（漲跌已修正）")
    # 技術面
    try:
        closes = get_tw_closes(sid)
        k = get_kline_analysis(closes)
        ma = lambda v: f"{v:.1f}" if v else "N/A"
        lines.append(f"　技術：趨勢 {k.get('trend','--')}　RSI {k.get('rsi',0):.0f}（{k.get('rsi_label','')}）")
        lines.append(f"　均線 MA5 {ma(k.get('ma5'))} / MA20 {ma(k.get('ma20'))} / MA60 {ma(k.get('ma60'))} / MA120 {ma(k.get('ma120'))} / MA240 {ma(k.get('ma240'))}")
    except: pass
    # 籌碼（用 advice 內含的近期 / 或法人，簡化用 advice 的支撐目標）
    try:
        adv = _get_portfolio_advice(sid)
        if adv.get("stop_loss") and adv.get("target"):
            lines.append(f"　近 60 天區間參考：支撐約 {adv['stop_loss']:.1f}　壓力約 {adv['target']:.1f}")
        if adv.get("ex_div_date"):
            lines.append(f"　下次除息日 {adv['ex_div_date']}　現金 {adv.get('ex_div_cash',0)} 元")
    except: pass
    # 新聞
    try:
        news = get_tw_stock_news(sid, name, count=3)
        if news:
            lines.append("　近期新聞：")
            for t, u in news[:3]:
                lines.append(f"　・{t}")
    except: pass
    return "\n".join(lines)


def _build_market_context() -> str:
    """組大盤 / 國際市場資料區。"""
    lines = ["◆ 市場概況"]
    try:
        mkt = get_market_status()
        if mkt.get("ok"):
            lines.append(f"　台股加權 {mkt.get('price',0):.0f}（{mkt.get('pct',0):+.2f}%）")
    except: pass
    # 關鍵指數
    for sym, label in [("^TWII","加權"), ("^IXIC","Nasdaq"), ("^DJI","道瓊"),
                       ("^SOX","費半"), ("^VIX","VIX")]:
        try:
            d = get_yahoo_quote(sym)
            if d and d.get("price"):
                lines.append(f"　{label} {d['price']:,.2f}（{d.get('pct',0):+.2f}%）")
        except: pass
    # 匯率
    try:
        fx = get_yahoo_quote("TWD=X")
        if fx and fx.get("price"):
            lines.append(f"　USD/TWD {fx['price']:.3f}（{fx.get('pct',0):+.2f}%）")
    except: pass
    return "\n".join(lines)


def _is_function_question(text: str) -> bool:
    kws = ["怎麼", "如何", "為什麼", "沒顯示", "沒更新", "設定", "新增持股",
           "停損", "怎麼用", "功能", "操作", "壞了", "錯誤", "當機", "卡住"]
    invest_kws = ["買", "賣", "加碼", "停利", "適合", "差在哪", "比較", "報酬", "配息"]
    has_func = any(k in text for k in kws)
    has_invest = any(k in text for k in invest_kws)
    # 同時有投資詞時優先當投資問題
    return has_func and not has_invest


def _is_compare_or_knowledge(text: str) -> bool:
    kws = ["差在哪", "差別", "差異", "比較", "vs", "VS", "跟", "和",
           "什麼是", "解釋", "教學", "意思", "適合", "新手", "保守", "積極",
           "領息", "成長", "存股", "ETF", "基金", "債券", "0050", "0056",
           "高股息", "市值型"]
    return any(k in text for k in kws)


def ai_qa_answer(user_id: str, question: str) -> str:
    """v10.9.69：AI 智能問答主函式。
    流程：偵測問題類型 → 抓對應真實資料 → 組 context → Groq 推理 → 回答。
    """
    if not GROQ_AVAILABLE:
        return ("🤖 AI 問答功能未啟用\n"
                "需設定 GROQ_API_KEY，請聯繫管理員")

    q = question.strip()
    if not q:
        return "請輸入你的問題，例如：\n　問 2330 現在怎麼看\n　問 0050 跟 0056 差在哪\n　問 怎麼設定停損"

    context_parts = []
    qtype = "general"

    # 1) 功能問題
    if _is_function_question(q):
        qtype = "function"
        context_parts.append("【App 功能說明區】")
        context_parts.append(HELP_MSG[:1500])
        context_parts.append(
            "常見排錯：\n"
            "・股票名稱沒顯示 → 名稱快取尚未載入完成 / 該代號不在名稱表 / API 缺名稱欄位。可請管理員「重載名稱」。\n"
            "・股價沒更新 → 可能 Yahoo 延遲 15 分，或盤後；卡片底部 metadata 會標來源與時間。\n"
            "・新增持股 → 打「新增 代碼 股數 均價」，或直接傳券商庫存截圖 AI 辨識。\n"
            "・設定停損 → 持股卡片有系統建議停損；自訂功能開發中。")
    else:
        # 2) 個股偵測
        stocks = _detect_stocks_in_question(q)
        if stocks:
            qtype = "stock"
            context_parts.append("【即時資料區（僅能引用以下數據，未列出的不可編造）】")
            for sid in stocks:
                context_parts.append(_build_stock_context(sid))
            # 持股問答：附上「該使用者自己」的持倉（v10.9.73：複合 key 隔離）
            try:
                portfolio = load_portfolio()
                up = {k: v for k, v in portfolio.items()
                      if v.get("user_id") == user_id
                      and _pf_symbol(k).replace(".TW","") in stocks}
                if up:
                    context_parts.append("【使用者持倉】")
                    for k, data in up.items():
                        context_parts.append(f"　{_pf_symbol(k)}：{data['shares']} 股，成本 {data['buy_price']}")
            except: pass

        # 3) 市場 / 國際關鍵字
        market_kws = ["大盤", "台股", "美股", "道瓊", "nasdaq", "Nasdaq", "費半", "費城",
                      "VIX", "美元", "匯率", "黃金", "原油", "Fed", "升息", "降息",
                      "殖利率", "美債", "國際", "盤勢", "今天為什麼", "昨晚"]
        if any(k in q for k in market_kws):
            if qtype == "general":
                qtype = "market"
            context_parts.append(_build_market_context())

        # 4) 商品比較 / 知識（不需即時資料，靠 LLM 知識 + 規則）
        if _is_compare_or_knowledge(q) and qtype in ("general",):
            qtype = "compare_knowledge"
            context_parts.append(
                "【商品比較 / 知識題：可用你的既有金融知識回答，"
                "但具體數字（費用率、配息金額、成分股、規模）若無即時資料須標示『需查證最新資料』。"
                "務必依系統 prompt 的『從人的角度 + 適合族群 + 投資人格』框架回答。】")

    now_str = now_taipei().strftime("%Y-%m-%d %H:%M")
    context = "\n".join(context_parts) if context_parts else "（本題無即時資料，請用概念性方式回答並提醒資料限制）"

    user_msg = (
        f"現在時間：{now_str}（台北）\n"
        f"問題類型偵測：{qtype}\n\n"
        f"{context}\n\n"
        f"───────────\n"
        f"使用者問題：{q}\n\n"
        f"請依系統規則回答。投資相關務必附資料來源與免責聲明、AI 信心。"
    )

    messages = [
        {"role": "system", "content": AI_QA_SYSTEM_PROMPT},
        {"role": "user", "content": user_msg},
    ]
    answer = groq_chat(messages, max_tokens=1800, temperature=0.3, timeout=30)
    if not answer:
        return ("🤖 AI 暫時無法回答（可能是 API 忙碌或額度限制）\n"
                "請稍後再試，或換個方式提問")
    # 保險：確保有免責聲明
    if "不構成投資建議" not in answer and qtype in ("stock", "market", "compare_knowledge"):
        answer += "\n\n⚠ 僅供參考，不構成投資建議"
    return answer


def ai_analyze_news_batch(news_list: list, stock_name: str = "", market_type: str = "tw") -> list:
    """一次呼叫 Groq 處理整批新聞：語意去重 + 情緒 + 摘要（v10.9.35）

    Args:
        news_list: [(title, url), ...]
        stock_name: 股票名稱（如「台積電」）幫助 AI 判斷上下文
        market_type: "tw" / "us" / "global"

    Returns:
        [{"title": str, "url": str, "keep": bool, "sentiment": str,
          "summary": str, "duplicate_of": int or None}, ...]
        失敗時回傳原始 list（不影響 fallback）
    """
    if not news_list:
        return []
    if not GROQ_AVAILABLE:
        # 沒 API key → 直接回傳原始結果
        return [{"title": t, "url": u, "keep": True, "sentiment": "🟡中性",
                 "summary": "", "duplicate_of": None} for t, u in news_list]

    # 檢查快取：每則新聞獨立快取
    now_ts = int(time.time())
    cached_results = []
    uncached_news = []
    uncached_indices = []
    for i, (t, u) in enumerate(news_list):
        cache_key = normalize_title(t)
        if cache_key in NEWS_AI_CACHE:
            cached_result, cached_ts = NEWS_AI_CACHE[cache_key]
            if now_ts - cached_ts < NEWS_AI_CACHE_TTL:
                cached_results.append((i, cached_result))
                continue
        uncached_news.append((i, t, u))
        uncached_indices.append(i)

    if not uncached_news:
        # 全部都有快取
        results = [None] * len(news_list)
        for i, r in cached_results: results[i] = r
        return results

    # 構造 prompt
    market_hint = {
        "tw": "台股市場",
        "us": "美股市場",
        "global": "全球市場",
    }.get(market_type, "金融市場")
    context = f"關於{stock_name}的" if stock_name else ""

    news_text = "\n".join([f"{idx+1}. {t}" for idx, (_, t, _) in enumerate(uncached_news)])

    system_prompt = f"""你是專業金融新聞分析師。請分析以下{context}{market_hint}新聞，輸出 JSON 陣列。

對每則新聞回傳：
- "keep": true/false（是否保留，重複的標 false）
- "sentiment": 必須是 "🟢偏多" / "🟡中性" / "🔴偏空" 之一
- "summary": 一句話 15 字以內精華（描述對市場/股價的影響，不要重複標題）
- "duplicate_of": 如果是重複某則，填那則的編號（1-indexed），否則 null

重複判斷標準：
- 同一事件不同說法 → 重複
- 不同事件、不同公司 → 不重複
- 只要有任一面向不同（時間、人物、地點），就視為不重複

只輸出 JSON 陣列，不要其他文字。"""

    user_prompt = f"新聞列表：\n{news_text}"

    response = groq_chat([
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ], max_tokens=1500, temperature=0.2, timeout=8)

    # 解析 JSON
    try:
        # 抓 JSON 陣列（容錯：有時 LLM 會加 ```json 包裝）
        json_match = re.search(r'\[.*\]', response, re.DOTALL)
        if not json_match:
            raise ValueError("找不到 JSON 陣列")
        ai_results = json.loads(json_match.group(0))
        if not isinstance(ai_results, list) or len(ai_results) != len(uncached_news):
            raise ValueError(f"AI 回傳數量不對：{len(ai_results)} vs {len(uncached_news)}")
    except Exception as e:
        dlog("GROQ", f"❌ JSON 解析失敗：{e} / response={response[:200]}")
        # Fallback：全部保留、中性、無摘要
        ai_results = [{"keep": True, "sentiment": "🟡中性", "summary": "", "duplicate_of": None}
                     for _ in uncached_news]

    # 組合完整結果並寫入快取
    results = [None] * len(news_list)
    for i, r in cached_results:
        results[i] = r
    for ai_idx, (orig_idx, title, url) in enumerate(uncached_news):
        ai_item = ai_results[ai_idx] if ai_idx < len(ai_results) else {}
        result = {
            "title": title,
            "url": url,
            "keep": ai_item.get("keep", True),
            "sentiment": ai_item.get("sentiment", "🟡中性"),
            "summary": ai_item.get("summary", "")[:50],  # 限制 50 字防爆
            "duplicate_of": ai_item.get("duplicate_of"),
        }
        results[orig_idx] = result
        # 寫入快取
        NEWS_AI_CACHE[normalize_title(title)] = (result, now_ts)

    # 清理過期快取（順手做）
    if len(NEWS_AI_CACHE) > 500:
        expired = [k for k, (_, ts) in NEWS_AI_CACHE.items() if now_ts - ts >= NEWS_AI_CACHE_TTL]
        for k in expired:
            del NEWS_AI_CACHE[k]

    kept = sum(1 for r in results if r and r.get("keep"))
    dlog("GROQ", f"✅ 新聞 AI 分析：{len(news_list)} → 保留 {kept}（其中 {len(uncached_news)} 則新分析、{len(cached_results)} 則快取）")
    return results


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

# v10.9.34 新增：來源權重（分數越高越優先顯示）
SOURCE_WEIGHTS = {
    # 🌟 頂級台股（鉅亨/MoneyDJ/工商/經濟日報/中央社/財訊）
    "cnyes.com": 100, "anue.com": 100, "moneydj.com": 95,
    "ctee.com.tw": 90, "money.udn.com": 90, "cna.com.tw": 90,
    "wealth.com.tw": 85,
    # 🌟 頂級美股
    "reuters.com": 100, "bloomberg.com": 100, "cnbc.com": 95,
    "marketwatch.com": 90, "wsj.com": 95, "barrons.com": 85,
    "investing.com": 80,
    # 🟡 一般可信
    "udn.com": 70, "technews.tw": 65, "bnext.com.tw": 65,
    "stockfeel.com.tw": 60, "tw.stock.yahoo.com": 60,
    "finance.yahoo.com": 65,
}

NON_NEWS_KEYWORDS=[
    "股票價格","股價圖","圖表","K線圖","個股概覽","個股頁",
    "持倉","ETF持股","成分股","歷史資料","歷史股價","技術圖",
    "stock price","stock chart","chart","quote","overview",
    "portfolio","historical data","price history","TSM股票","TSMC股票",
]

def is_trusted_source(url:str)->bool: return any(s in url for s in STRICT_TRUSTED) if url else False
def is_real_news(title:str)->bool: return not any(kw in title for kw in NON_NEWS_KEYWORDS)


def get_news_domain(url: str) -> str:
    """從 URL 提取主網域（v10.9.34 新增）"""
    if not url: return ""
    try:
        # 簡單匹配 - 抓 ://xxx.yyy.zz/
        m = re.search(r'https?://(?:www\.|m\.)?([^/]+)', url)
        if m:
            domain = m.group(1).lower()
            # 對應到白名單裡的主域名
            for trusted in STRICT_TRUSTED:
                if trusted in domain:
                    return trusted
            return domain
    except: pass
    return ""


def get_source_weight(url: str) -> int:
    """取得來源權重分數（v10.9.34 新增）"""
    domain = get_news_domain(url)
    return SOURCE_WEIGHTS.get(domain, 30 if is_trusted_source(url) else 10)


def normalize_title(title: str) -> str:
    """標準化標題用來比對相似度（v10.9.34 升級）
    - 移除標點/空白/全形符號
    - 移除新聞常見冗詞
    - 轉小寫"""
    t = title.lower()
    # 移除常見冗詞
    for word in ["獨家", "快訊", "即時", "更新", "突發", "重磅", "焦點",
                 "重要", "盤前", "盤後", "盤中", "速報", "本日", "今日",
                 "exclusive", "breaking", "update", "alert", "report"]:
        t = t.replace(word, "")
    # 只留中英數字
    t = re.sub(r'[^\u4e00-\u9fffa-z0-9]', '', t)
    return t


def title_similarity(a: str, b: str) -> float:
    """計算兩個標題的相似度 0~1（v10.9.34）
    結合 char-bigram + 中文關鍵詞重疊度（更適合中文新聞）"""
    a_norm, b_norm = normalize_title(a), normalize_title(b)
    if not a_norm or not b_norm: return 0.0
    if a_norm == b_norm: return 1.0
    # 如果一個包含另一個（短的長度至少 6 字）
    if len(a_norm) >= 6 and len(b_norm) >= 6:
        if a_norm in b_norm or b_norm in a_norm: return 0.95

    # 方法 1：char bigram Jaccard
    def bigrams(s):
        return set(s[i:i+2] for i in range(len(s)-1))
    ba, bb = bigrams(a_norm), bigrams(b_norm)
    bigram_sim = len(ba & bb) / len(ba | bb) if (ba | bb) else 0.0

    # 方法 2：中文 trigram 重疊（抓「台積電」「法說會」「特斯拉」這類專有名詞）
    def trigrams(s):
        return set(s[i:i+3] for i in range(len(s)-2))
    ta, tb = trigrams(a_norm), trigrams(b_norm)
    if ta and tb:
        overlap = len(ta & tb)
        min_len = min(len(ta), len(tb))
        trigram_sim = overlap / min_len if min_len else 0.0
    else:
        trigram_sim = 0.0

    # 綜合分數：兩種方法取較高（讓任一強訊號就能判定重複）
    return max(bigram_sim, trigram_sim * 0.85)


def deduplicate_news(nl: list, similarity_threshold: float = 0.5, max_per_source: int = 2) -> list:
    """新聞去重 + 同媒體限制（v10.9.34 完全升級）
    nl: [(title, url), ...]
    similarity_threshold: 標題相似度閾值（>= 此值視為重複）
    max_per_source: 同一媒體最多顯示幾則
    """
    if not nl: return []
    original_count = len(nl)

    # 先按來源權重排序（高權重優先保留）
    sorted_nl = sorted(nl, key=lambda x: -get_source_weight(x[1]))

    result = []
    source_count = {}  # {domain: count}
    skipped_similar = 0
    skipped_max_source = 0

    for title, url in sorted_nl:
        domain = get_news_domain(url)

        # 同媒體限制
        if source_count.get(domain, 0) >= max_per_source:
            skipped_max_source += 1
            continue

        # 相似度去重 — 跟已收的每篇比對
        is_duplicate = False
        for existing_title, _ in result:
            if title_similarity(title, existing_title) >= similarity_threshold:
                is_duplicate = True
                break
        if is_duplicate:
            skipped_similar += 1
            continue

        result.append((title, url))
        source_count[domain] = source_count.get(domain, 0) + 1

    if original_count > len(result):
        dlog("NEWS", f"去重 {original_count}→{len(result)}（過濾相似{skipped_similar} 同媒體{skipped_max_source}）")
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

def _load_finmind_news(stock_id: str, count: int = 4) -> list:
    """v10.9.57：用 FinMind TaiwanStockNews 抓個股新聞。
    回傳 [(title, url), ...] 與既有格式一致。
    FinMind 已付費（Backer tier）含此 dataset，資料較 Google News RSS 結構化、權威。
    """
    if not FINMIND_TOKEN:
        return []
    # 抓近 7 天
    end_date = now_taipei().strftime("%Y-%m-%d")
    start_date = (now_taipei() - timedelta(days=7)).strftime("%Y-%m-%d")
    url = "https://api.finmindtrade.com/api/v4/data"
    params = {
        "dataset": "TaiwanStockNews",
        "data_id": stock_id,
        "start_date": start_date,
        "end_date": end_date,
        "token": FINMIND_TOKEN,
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code != 200:
            record_health("FinMind", False, f"News HTTP {r.status_code}")
            return []
        payload = r.json()
        if payload.get("status") != 200:
            record_health("FinMind", False, f"News {payload.get('msg','')[:80]}")
            return []
        rows = payload.get("data") or []
        record_health("FinMind", True)
        # 依日期 desc 排序，取近的
        rows.sort(key=lambda x: x.get("date",""), reverse=True)
        results = []
        seen_titles = set()
        for r in rows:
            title = clean_title(r.get("title", "")).strip()
            link  = (r.get("link") or "").strip()
            if not title or not link:
                continue
            if not is_real_news(title):
                continue
            # 標題去重（normalize 後比對）
            norm = normalize_title(title)
            if norm in seen_titles:
                continue
            seen_titles.add(norm)
            results.append((title, link))
            if len(results) >= count:
                break
        return results
    except requests.Timeout:
        dlog("NEWS", f"FinMind 新聞超時：{stock_id}")
        record_health("FinMind", False, "news timeout")
    except Exception as e:
        dlog("NEWS", f"FinMind 新聞失敗：{type(e).__name__}: {e}")
        record_health("FinMind", False, f"news {type(e).__name__}")
    return []


def get_tw_stock_news(stock_id:str, cn_name:str, count:int=4)->list:
    """個股新聞抓取（v10.9.57：FinMind 主來源 + Google News 備援）。
    層級：
      1. FinMind TaiwanStockNews（結構化、含日期/媒體名）— 付費版資料源
      2. Google News RSS（中文 query 「[名] 台股 財經」）
      3. Google News RSS（「[id] [名] 股票」）
      4. Google News RSS（「[id] 台股」，trusted_only=False）
    """
    # 第一層：FinMind（v10.9.57 新增）
    results = _load_finmind_news(stock_id, count=count)
    if len(results) >= count:
        return results[:count]
    # 第二層：Google News（中文名 query）
    if has_chinese(cn_name) and cn_name!=stock_id:
        more = get_news(f"{cn_name} 台股 財經",count=count,trusted_only=True)
        results = _merge_news_lists(results, more, max_count=count)
    if len(results) >= count:
        return results[:count]
    # 第三層：[id] [name] 股票
    more = get_news(f"{stock_id} {cn_name} 股票",count=count,trusted_only=True)
    results = _merge_news_lists(results, more, max_count=count)
    if len(results) >= count:
        return results[:count]
    # 第四層：放寬到非白名單
    more = get_news(f"{stock_id} 台股",count=count,trusted_only=False)
    results = _merge_news_lists(results, more, max_count=count)
    return results[:count]


def get_finmind_news_enriched(stock_id: str, count: int = 10) -> list:
    """v10.9.58：抓 FinMind 個股新聞，回傳完整 dict（含 date / source / link / title）。
    用於「個股新聞」carousel：要顯示媒體名 + 時間，所以不能簡化成 tuple。
    """
    if not FINMIND_TOKEN:
        return []
    end_date = now_taipei().strftime("%Y-%m-%d")
    start_date = (now_taipei() - timedelta(days=14)).strftime("%Y-%m-%d")
    url = "https://api.finmindtrade.com/api/v4/data"
    params = {
        "dataset": "TaiwanStockNews",
        "data_id": stock_id,
        "start_date": start_date,
        "end_date": end_date,
        "token": FINMIND_TOKEN,
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code != 200:
            record_health("FinMind", False, f"News HTTP {r.status_code}")
            return []
        payload = r.json()
        if payload.get("status") != 200:
            record_health("FinMind", False, f"News {payload.get('msg','')[:80]}")
            return []
        rows = payload.get("data") or []
        record_health("FinMind", True)
        rows.sort(key=lambda x: x.get("date",""), reverse=True)
        seen_titles = set()
        out = []
        for r in rows:
            title  = clean_title(r.get("title", "")).strip()
            link   = (r.get("link") or "").strip()
            source = (r.get("source") or "").strip()
            date   = (r.get("date") or "").strip()
            if not title or not link:
                continue
            if not is_real_news(title):
                continue
            norm = normalize_title(title)
            if norm in seen_titles:
                continue
            seen_titles.add(norm)
            out.append({"title": title, "url": link, "source": source, "date": date})
            if len(out) >= count:
                break
        return out
    except Exception as e:
        dlog("NEWS", f"FinMind enriched 失敗：{type(e).__name__}: {e}")
        record_health("FinMind", False, f"news enriched {type(e).__name__}")
        return []


def make_stock_news_carousel(stock_id: str, name: str, news_dicts: list) -> dict:
    """v10.9.58：個股新聞 carousel — 每張卡 1 則新聞含媒體 / 時間 / 連結。粉嫩風格。"""
    bubbles = []
    for i, n in enumerate(news_dicts[:12], start=1):
        title  = n.get("title", "")
        url    = n.get("url", "")
        source = n.get("source", "未知來源")
        date   = n.get("date", "")
        # 日期格式化：「2026-05-20 14:30:00」→「05/20 14:30」
        date_short = date
        try:
            if len(date) >= 16:
                date_short = date[5:16].replace("-", "/")
        except: pass

        bubble = {
            "type": "bubble", "size": "kilo",
            "header": {
                "type": "box", "layout": "vertical",
                "backgroundColor": "#E8B8A8", "paddingAll": "10px",
                "contents": [
                    {"type": "box", "layout": "horizontal", "contents": [
                        {"type": "text", "text": f"#{i}", "size": "xs",
                         "color": "#FDF6F0", "flex": 0, "weight": "bold"},
                        {"type": "text", "text": f"{stock_id} {name}", "size": "xs",
                         "color": "#FFFFFF", "weight": "bold", "flex": 1, "margin": "sm"},
                    ]},
                    {"type": "text", "text": f"📰 {source}　{date_short}",
                     "size": "xxs", "color": "#FDF6F0", "margin": "xs", "wrap": True},
                ]
            },
            "body": {
                "type": "box", "layout": "vertical",
                "backgroundColor": "#FDF6F0", "paddingAll": "12px",
                "contents": [
                    {"type": "text", "text": title, "size": "sm",
                     "color": "#5B4040", "weight": "bold", "wrap": True},
                ]
            },
            "footer": {
                "type": "box", "layout": "vertical", "paddingAll": "8px",
                "contents": [
                    {"type": "button", "style": "primary", "color": "#E89B82",
                     "height": "sm",
                     "action": {"type": "uri", "label": "📖 看完整", "uri": url}}
                ]
            }
        }
        bubbles.append(bubble)

    if not bubbles:
        return None
    return {"type": "carousel", "contents": bubbles}


def _merge_news_lists(existing: list, new: list, max_count: int = 4) -> list:
    """合併新聞清單，依 normalize_title 去重，已存在的不加入。"""
    seen = {normalize_title(t) for t, _ in existing}
    out = list(existing)
    for t, u in new:
        norm = normalize_title(t)
        if norm in seen:
            continue
        seen.add(norm)
        out.append((t, u))
        if len(out) >= max_count:
            break
    return out


def get_news_with_ai(query: str, stock_name: str = "", count: int = 4,
                    market_type: str = "tw", trusted_only: bool = True) -> list:
    """取新聞並用 AI 分析（v10.9.35 新增）

    Returns:
        [{"title", "url", "keep", "sentiment", "summary", "duplicate_of"}, ...]
        如果 Groq 沒設定，會回傳 keep=True、sentiment=🟡中性、summary="" 的結果（不影響使用）
    """
    # 抓比 count 多 50% 的新聞給 AI 篩
    raw_results = get_news(query, count=int(count * 1.5) + 2, trusted_only=trusted_only)
    if not raw_results:
        return []

    # AI 分析（已內含快取機制）
    ai_results = ai_analyze_news_batch(raw_results, stock_name=stock_name, market_type=market_type)

    # 篩掉 AI 標記為重複的、保留 keep=True
    kept = [r for r in ai_results if r and r.get("keep")]
    return kept[:count]


def get_tw_stock_news_with_ai(stock_id: str, cn_name: str, count: int = 4) -> list:
    """台股 AI 新聞分析（v10.9.35 新增）"""
    raw_news = get_tw_stock_news(stock_id, cn_name, count=int(count * 1.5) + 2)
    if not raw_news:
        return []
    stock_name = cn_name if has_chinese(cn_name) else stock_id
    ai_results = ai_analyze_news_batch(raw_news, stock_name=stock_name, market_type="tw")
    kept = [r for r in ai_results if r and r.get("keep")]
    return kept[:count]


# ══════════════════════════════════════════
#  AI 新聞解讀模組（v10.9.38 新增）
#  獨立模組，不影響主流程查詢
#  特色：原文 + 中文翻譯（美股）+ 情緒分析 + 影響台股
# ══════════════════════════════════════════

def get_google_news_multi(query: str, count: int = 10) -> list:
    """v10.9.76：Google News RSS 多來源新聞，回傳 [{title,url,source,date}]。
    特色：一個 query 涵蓋多家媒體（UDN/中央社/中時/自由/cnyes/換日線…），來源多元。
    連結為 Google 跳轉，但在手機瀏覽器會正常導向原文。
    """
    url = (f"https://news.google.com/rss/search?q={requests.utils.quote(query)}"
           f"&hl=zh-TW&gl=TW&ceid=TW:zh-Hant")
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        if r.status_code != 200:
            record_health("Google News", False, f"HTTP {r.status_code}")
            return []
        root = ET.fromstring(r.content)
        items = root.findall(".//item")
        out, seen = [], set()
        for it in items:
            raw_title = clean_title(it.findtext("title", "") or "")
            link = (it.findtext("link", "") or "").strip()
            if not raw_title or not link:
                continue
            # 來源：<source> 元素，或標題尾「… - 媒體名」
            src_el = it.find("source")
            source = (src_el.text if src_el is not None else "") or ""
            title = raw_title
            if " - " in raw_title:
                head, tail = raw_title.rsplit(" - ", 1)
                if not source:
                    source = tail.strip()
                title = head.strip()
            if not source:
                source = "綜合報導"
            if not is_real_news(title):
                continue
            norm = normalize_title(title)
            if norm in seen:
                continue
            seen.add(norm)
            pub = (it.findtext("pubDate", "") or "").strip()
            date_short = pub
            try:
                from email.utils import parsedate_to_datetime
                dt = parsedate_to_datetime(pub)
                date_short = dt.astimezone(TZ_TAIPEI).strftime("%m/%d %H:%M")
            except: pass
            out.append({"title": title, "url": link, "source": source, "date": date_short})
            if len(out) >= count:
                break
        if out:
            record_health("Google News", True)
        return out
    except Exception as e:
        dlog("NEWS", f"Google News 失敗：{type(e).__name__}: {e}")
        record_health("Google News", False, f"{type(e).__name__}")
        return []


def _merge_dedup_news(*lists, count: int = 12) -> list:
    """合併多個新聞 dict 清單，依 normalize_title 去重，保留先到的。"""
    out, seen = [], set()
    for lst in lists:
        for n in lst:
            t = n.get("title", "")
            norm = normalize_title(t)
            if not t or norm in seen:
                continue
            seen.add(norm)
            out.append(n)
            if len(out) >= count:
                return out
    return out


def get_category_news(category: str, count: int = 10) -> list:
    """v10.9.76：分類大盤新聞，多來源。
    category: tw / us / intl / geo
    """
    if category == "tw":
        # 台股：Yahoo（直接連結）優先 + Google 補多來源
        y = get_yahoo_finance_rss("tw-market", count=count)
        g = get_google_news_multi("台股 加權指數 台積電 上市櫃", count=count)
        return _merge_dedup_news(y, g, count=count)
    if category == "us":
        # 美股：聚焦美國市場
        return get_google_news_multi(
            "美股 道瓊 那斯達克 標普500 費城半導體 Fed 聯準會", count=count)
    if category == "intl":
        # 國際：聚焦非美國的全球（歐日陸 + 總經 + 商品）
        return get_google_news_multi(
            "國際財經 歐洲股市 日本股市 全球經濟 油價 黃金 IMF", count=count)
    if category == "geo":
        # v10.9.77：用「話題層級關鍵字」而非寫死國家/事件
        # Google News 會自動抓「今天最熱的衝突/制裁/外交」，不用人工維護地點清單
        # 多 query 聚合：覆蓋衝突、制裁、外交、軍事、政變、戰爭等通用面向
        q1 = get_google_news_multi("地緣政治 OR 國際衝突 OR 戰爭", count=count)
        q2 = get_google_news_multi("制裁 OR 軍事行動 OR 外交危機 OR 邊境緊張", count=count)
        q3 = get_google_news_multi("國安 OR 聯合國決議 OR 政變 OR 國際情勢", count=count)
        return _merge_dedup_news(q1, q2, q3, count=count)
    return []


def get_yahoo_finance_rss(category: str, count: int = 10) -> list:
    """v10.9.75：抓 Yahoo 台灣股市 RSS（直接連結，Safari 可開）。
    category: tw-market（台股）/ intl-markets（國際/美股）。
    回傳 [{"title","url","source","date"}, ...]
    """
    url = f"https://tw.stock.yahoo.com/rss?category={category}"
    try:
        r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        if r.status_code != 200:
            record_health("Yahoo News", False, f"RSS HTTP {r.status_code}")
            return []
        root = ET.fromstring(r.content)
        items = root.findall(".//item")
        out = []
        seen = set()
        for it in items:
            title = clean_title(it.findtext("title", "") or "")
            link = (it.findtext("link", "") or "").strip()
            pub = (it.findtext("pubDate", "") or "").strip()
            if not title or not link:
                continue
            if not is_real_news(title):
                continue
            norm = normalize_title(title)
            if norm in seen:
                continue
            seen.add(norm)
            # pubDate: "Fri, 22 May 2026 10:30:00 +0800" → "05/22 10:30"
            date_short = pub
            try:
                from email.utils import parsedate_to_datetime
                dt = parsedate_to_datetime(pub)
                date_short = dt.astimezone(TZ_TAIPEI).strftime("%m/%d %H:%M")
            except: pass
            out.append({"title": title, "url": link, "source": "Yahoo 股市", "date": date_short})
            if len(out) >= count:
                break
        if out:
            record_health("Yahoo News", True)
        return out
    except Exception as e:
        dlog("NEWS", f"Yahoo RSS {category} 失敗：{type(e).__name__}: {e}")
        record_health("Yahoo News", False, f"RSS {type(e).__name__}")
        return []


def make_news_carousel(title: str, color: str, items: list) -> dict:
    """v10.9.75：通用新聞 carousel（台股/美股/國際大盤新聞用）。
    每張卡：標題、來源、時間、看完整按鈕（直接連結）。
    """
    bubbles = []
    for i, n in enumerate(items[:10], start=1):
        t = n.get("title", "")
        u = n.get("url", "")
        src = n.get("source", "")
        date = n.get("date", "")
        bubble = {
            "type": "bubble", "size": "kilo",
            "header": {"type": "box", "layout": "vertical",
                       "backgroundColor": color, "paddingAll": "10px",
                       "contents": [
                           {"type": "box", "layout": "horizontal", "contents": [
                               {"type": "text", "text": f"#{i}", "size": "xs",
                                "color": "#FDF6F0", "flex": 0, "weight": "bold"},
                               {"type": "text", "text": title, "size": "xs",
                                "color": "#FFFFFF", "weight": "bold", "flex": 1, "margin": "sm"},
                           ]},
                           {"type": "text", "text": f"📰 {src}　{date}",
                            "size": "xxs", "color": "#FDF6F0", "margin": "xs", "wrap": True},
                       ]},
            "body": {"type": "box", "layout": "vertical",
                     "backgroundColor": "#FDF6F0", "paddingAll": "12px",
                     "contents": [
                         {"type": "text", "text": t, "size": "sm",
                          "color": "#5B4040", "weight": "bold", "wrap": True},
                     ]},
            "footer": {"type": "box", "layout": "vertical", "paddingAll": "8px",
                       "contents": [
                           {"type": "button", "style": "primary", "color": color,
                            "height": "sm",
                            "action": {"type": "uri", "label": "📖 看完整", "uri": u}}
                       ]},
        }
        bubbles.append(bubble)
    if not bubbles:
        return None
    return {"type": "carousel", "contents": bubbles}


def get_us_news_english(count: int = 8) -> list:
    """抓美股英文新聞（規格書要求：Reuters/CNBC/Bloomberg/MarketWatch/Benzinga）"""
    queries = [
        "US stock market today",
        "S&P 500 Nasdaq Dow",
        "Fed interest rate news",
        "AI semiconductor earnings",
    ]
    all_results = []
    seen_titles = set()
    for q in queries[:2]:  # 抓 2 個 query
        results = get_news(q, count=count, trusted_only=False)
        for t, u in results:
            # 篩選只留英文新聞來源
            if any(s in u for s in ["reuters.com", "cnbc.com", "bloomberg.com",
                                     "marketwatch.com", "wsj.com", "barrons.com",
                                     "investing.com", "benzinga.com", "seekingalpha.com",
                                     "finance.yahoo.com", "ft.com"]):
                key = normalize_title(t)[:20]
                if key and key not in seen_titles:
                    seen_titles.add(key)
                    all_results.append((t, u))
    return all_results[:count]


def ai_translate_and_analyze(news_list: list, market_type: str = "us") -> list:
    """美股新聞：翻譯+摘要+情緒+影響台股，一次 Groq 呼叫做 4 件事

    Args:
        news_list: [(title, url), ...]
        market_type: "us" (有翻譯) / "tw" (純中文) / "global"

    Returns:
        [{"title": str, "url": str, "translation": str, "summary": str,
          "sentiment": str, "impact": str, "keep": bool}, ...]
    """
    if not news_list:
        return []
    if not GROQ_AVAILABLE:
        # 沒 API key → 退回普通結果
        return [{"title": t, "url": u, "translation": "",
                 "summary": "", "sentiment": "🟡中性", "impact": "", "keep": True}
                for t, u in news_list]

    # 檢查快取
    now_ts = int(time.time())
    cached_results = []
    uncached_news = []
    for i, (t, u) in enumerate(news_list):
        cache_key = f"trans_{normalize_title(t)}"
        if cache_key in NEWS_AI_CACHE:
            cached_result, cached_ts = NEWS_AI_CACHE[cache_key]
            if now_ts - cached_ts < NEWS_AI_CACHE_TTL:
                cached_results.append((i, cached_result))
                continue
        uncached_news.append((i, t, u))

    if not uncached_news:
        results = [None] * len(news_list)
        for i, r in cached_results: results[i] = r
        return results

    # 構造 prompt
    news_text = "\n".join([f"{idx+1}. {t}" for idx, (_, t, _) in enumerate(uncached_news)])

    if market_type == "us":
        system_prompt = """你是專業金融新聞分析師。請分析美股新聞，輸出 JSON 陣列。

對每則新聞回傳：
- "translation": 中文翻譯（保留專有名詞英文，例如 NVIDIA、Fed、CPI）
- "summary": 20 字內精華摘要（描述對市場的影響，不重複標題）
- "sentiment": 必須是 "🟢偏多" / "🟡中性" / "🔴偏空" 之一
- "impact": 影響哪些台股族群（例如「台積電供應鏈/AI 概念股」），10 字內
- "keep": true/false（是否保留，重複的標 false）

只輸出 JSON 陣列，不要其他文字。"""
    else:
        # 台股/國際：純中文，不需翻譯
        system_prompt = """你是專業金融新聞分析師。請分析新聞，輸出 JSON 陣列。

對每則新聞回傳：
- "translation": 空字串
- "summary": 20 字內精華摘要（描述對市場的影響）
- "sentiment": 必須是 "🟢偏多" / "🟡中性" / "🔴偏空" 之一
- "impact": 影響哪些族群（10 字內）
- "keep": true/false（是否保留，重複的標 false）

只輸出 JSON 陣列，不要其他文字。"""

    response = groq_chat([
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"新聞列表：\n{news_text}"}
    ], max_tokens=2000, temperature=0.2, timeout=15)  # AI 新聞給長一點 timeout

    # 解析
    try:
        json_match = re.search(r'\[.*\]', response, re.DOTALL)
        if not json_match:
            raise ValueError("找不到 JSON 陣列")
        ai_results = json.loads(json_match.group(0))
        if not isinstance(ai_results, list):
            raise ValueError("不是陣列")
    except Exception as e:
        dlog("GROQ", f"❌ AI 新聞解析失敗：{e} / response={response[:200]}")
        ai_results = [{"translation": "", "summary": "", "sentiment": "🟡中性",
                       "impact": "", "keep": True} for _ in uncached_news]

    # 組合結果
    results = [None] * len(news_list)
    for i, r in cached_results:
        results[i] = r
    for ai_idx, (orig_idx, title, url) in enumerate(uncached_news):
        ai_item = ai_results[ai_idx] if ai_idx < len(ai_results) else {}
        result = {
            "title": title,
            "url": url,
            "translation": ai_item.get("translation", "")[:100],
            "summary": ai_item.get("summary", "")[:50],
            "sentiment": ai_item.get("sentiment", "🟡中性"),
            "impact": ai_item.get("impact", "")[:30],
            "keep": ai_item.get("keep", True),
        }
        results[orig_idx] = result
        NEWS_AI_CACHE[f"trans_{normalize_title(title)}"] = (result, now_ts)

    kept = sum(1 for r in results if r and r.get("keep"))
    dlog("GROQ", f"✅ AI 新聞解讀：{len(news_list)} → 保留 {kept}（{len(uncached_news)} 新分析 + {len(cached_results)} 快取）")
    return results


def make_ai_news_carousel(news_results: list, title: str, header_color: str = "#C9B0DB") -> dict:
    """AI 新聞 carousel（v10.9.38 新增）
    每則新聞一張卡片：原文標題 + 中文翻譯 + 摘要 + 情緒 + 影響台股
    """
    if not news_results:
        return None

    bubbles = []
    for item in news_results[:8]:  # carousel 最多 10 張，留 2 張給「未來功能」
        if not item or not item.get("keep"):
            continue
        t = item.get("title", "")
        u = item.get("url", "")
        translation = item.get("translation", "")
        summary = item.get("summary", "")
        sentiment = item.get("sentiment", "🟡中性")
        impact = item.get("impact", "")

        # 情緒色
        if "🟢" in sentiment:
            sent_color = "#D97A5C"
            sent_bg = "#FAE6DE"
        elif "🔴" in sentiment:
            sent_color = "#7AABBE"
            sent_bg = "#DEE8FA"
        else:
            sent_color = "#B89BC4"
            sent_bg = "#EFE5F5"

        body_contents = [
            # 原文標題
            {"type":"text","text":t,"size":"sm","weight":"bold","color":"#5D3F75","wrap":True},
        ]

        # 中文翻譯（美股才有）
        if translation:
            body_contents.append({"type":"separator","color":"#F0D5C0","margin":"sm"})
            body_contents.append({"type":"text","text":"🇹🇼 中文翻譯","size":"xxs","color":"#A07560","margin":"sm"})
            body_contents.append({"type":"text","text":translation,"size":"xs","color":"#7A5040","wrap":True})

        # AI 摘要
        if summary:
            body_contents.append({"type":"separator","color":"#F0D5C0","margin":"sm"})
            body_contents.append({"type":"text","text":"💡 AI 摘要","size":"xxs","color":"#A07560","margin":"sm"})
            body_contents.append({"type":"text","text":summary,"size":"xs","color":"#7A5040","wrap":True})

        # 情緒 + 影響
        body_contents.append({"type":"separator","color":"#F0D5C0","margin":"sm"})
        body_contents.append({"type":"box","layout":"horizontal","spacing":"sm","margin":"sm","contents":[
            {"type":"box","layout":"vertical","backgroundColor":sent_bg,"cornerRadius":"6px","paddingAll":"6px","contents":[
                {"type":"text","text":sentiment,"size":"xs","color":sent_color,"weight":"bold","align":"center"}
            ], "flex":0}
        ]})

        if impact:
            body_contents.append({"type":"text","text":f"🎯 {impact}","size":"xxs","color":"#A05A48","margin":"xs","wrap":True})

        # URL 按鈕
        footer = None
        if u:
            footer = {
                "type":"box","layout":"vertical","paddingAll":"8px","contents":[
                    {"type":"button","style":"link","height":"sm","action":{
                        "type":"uri","label":"📖 看原文","uri":u}}
                ]
            }

        bubble = {
            "type":"bubble","size":"kilo",
            "header":{
                "type":"box","layout":"vertical","backgroundColor":header_color,"paddingAll":"10px",
                "contents":[
                    {"type":"text","text":title,"size":"xs","color":"#FFFFFF","weight":"bold"}
                ]
            },
            "body":{
                "type":"box","layout":"vertical","spacing":"none","paddingAll":"12px",
                "contents": body_contents
            }
        }
        if footer:
            bubble["footer"] = footer
        bubbles.append(bubble)

    if not bubbles:
        return None
    return {"type":"carousel","contents":bubbles}


def build_and_push_ai_news(user_id: str, news_type: str):
    """背景執行 AI 新聞分析並推送（v10.9.38 新增）
    news_type: "tw" / "us" / "global"
    """
    try:
        if news_type == "tw":
            # 抓台股一般新聞
            news = get_news("台股 大盤 加權", count=8, trusted_only=True)
            title = "🇹🇼 台股 AI 新聞解讀"
            header_color = "#E89B82"  # 珊瑚粉
            market_type = "tw"
        elif news_type == "us":
            # 抓英文美股新聞
            news = get_us_news_english(count=8)
            title = "🇺🇸 美股 AI 新聞解讀"
            header_color = "#C9B0DB"  # 薰衣草粉
            market_type = "us"
        elif news_type == "global":
            # 抓國際新聞
            news = get_news("global market geopolitics", count=8, trusted_only=True)
            if not news:
                news = get_news("國際財經 全球市場", count=8, trusted_only=True)
            title = "🌍 國際 AI 新聞解讀"
            header_color = "#E8B8A8"  # 奶油杏粉
            market_type = "global"
        else:
            push_message(user_id, "⚠️ 未知的新聞類型")
            return

        if not news:
            push_message(user_id, f"⚠️ {title}\n暫無相關新聞，請稍後再試")
            return

        # 用 Groq 分析
        ai_results = ai_translate_and_analyze(news, market_type=market_type)

        # 構造 carousel
        flex = make_ai_news_carousel(ai_results, title, header_color)
        if not flex:
            push_message(user_id, f"⚠️ {title}\nAI 分析失敗，請稍後再試")
            return

        # 推送
        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).push_message(
                PushMessageRequest(to=user_id, messages=[
                    FlexMessage(alt_text=title, contents=FlexContainer.from_dict(flex))
                ])
            )
        dlog("AI_NEWS", f"✅ 已推送 {title} 給 {user_id[:8]}...")
    except Exception as e:
        dlog("AI_NEWS", f"❌ 推送失敗：{e}")
        try:
            push_message(user_id, f"⚠️ AI 新聞分析失敗：{e}")
        except: pass


def make_ai_news_menu_flex() -> dict:
    """AI 新聞解讀子選單（v10.9.38 新增）"""
    return {
        "type":"bubble","size":"mega",
        "header":{
            "type":"box","layout":"vertical","backgroundColor":"#C9B0DB","paddingAll":"14px",
            "contents":[
                {"type":"text","text":"🤖 AI 新聞解讀","size":"lg","color":"#FFFFFF","weight":"bold"},
                {"type":"text","text":"Groq AI 即時情緒分析・影響台股","size":"xs","color":"#FFFFFF"}
            ]
        },
        "body":{
            "type":"box","layout":"vertical","spacing":"sm","paddingAll":"12px",
            "contents":[
                {"type":"text","text":"⏱️ 分析約需 5-10 秒\n結果會自動推送到聊天","size":"xxs","color":"#A07560","wrap":True},
                {"type":"separator","color":"#F0D5C0","margin":"sm"},
                {"type":"button","style":"primary","height":"sm","color":"#E89B82",
                 "action":{"type":"message","label":"🇹🇼 台股 AI 分析","text":"AI台股新聞"}},
                {"type":"button","style":"primary","height":"sm","color":"#C9B0DB",
                 "action":{"type":"message","label":"🇺🇸 美股 AI 分析（含中文翻譯）","text":"AI美股新聞"}},
                {"type":"button","style":"primary","height":"sm","color":"#E8B8A8",
                 "action":{"type":"message","label":"🌍 國際 AI 分析","text":"AI國際新聞"}},
                {"type":"separator","color":"#F0D5C0","margin":"sm"},
                {"type":"text","text":"💡 由 Groq Llama 3.3 70B 提供","size":"xxs","color":"#B89BC4","align":"center"}
            ]
        }
    }


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
#  觀察清單評分
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

def _fetch_finmind_institution(target_date) -> list:
    """v10.9.59：用 FinMind 抓特定日期的三大法人。
    回傳 [(stock_id, name, total_lots, foreign_lots, trust_lots), ...]，過濾 total > 500。
    target_date: datetime 物件（用台北時區）。
    """
    if not FINMIND_TOKEN:
        return []
    ds = target_date.strftime("%Y-%m-%d")
    url = "https://api.finmindtrade.com/api/v4/data"
    params = {
        "dataset": "TaiwanStockInstitutionalInvestorsBuySell",
        "start_date": ds,
        "end_date": ds,
        "token": FINMIND_TOKEN,
    }
    try:
        r = requests.get(url, params=params, timeout=20)
        if r.status_code != 200:
            record_health("FinMind", False, f"Institution HTTP {r.status_code}")
            return []
        payload = r.json()
        if payload.get("status") != 200:
            return []
        rows = payload.get("data") or []
        if not rows:
            return []
        record_health("FinMind", True)
        # 聚合：每檔股票 → foreign_net, trust_net（張數 = 股數 // 1000）
        agg = {}  # stock_id -> {"foreign_net": int, "trust_net": int}
        for row in rows:
            sid  = (row.get("stock_id") or "").strip()
            name = (row.get("name") or "")
            buy  = int(row.get("buy", 0) or 0)
            sell = int(row.get("sell", 0) or 0)
            net  = buy - sell  # 股數
            if not sid: continue
            d = agg.setdefault(sid, {"foreign_net":0, "trust_net":0})
            if name.startswith("Foreign_"):
                d["foreign_net"] += net
            elif name == "Investment_Trust":
                d["trust_net"] += net
        candidates = []
        for sid, d in agg.items():
            fl = d["foreign_net"] // 1000  # 張
            il = d["trust_net"]   // 1000
            tl = fl + il
            if tl > 500:
                cname = NAME_CACHE.get(sid, sid)
                candidates.append((sid, cname, tl, fl, il))
        return candidates
    except Exception as e:
        dlog("REC", f"FinMind 法人失敗：{type(e).__name__}: {e}")
        record_health("FinMind", False, f"Institution {type(e).__name__}")
        return []


def fetch_institution_data()->tuple:
    """三大法人資料（v10.9.59：FinMind 主來源 + TWSE T86 備援）。
    回傳 (candidates_list, data_date_str, source_note)。
    """
    now=now_taipei(); weekday=now.weekday(); afc=is_after_close()
    dq=[]
    if weekday<5 and afc: dq.append((now,True))
    for i in range(1,10):
        d=now-timedelta(days=i)
        if d.weekday()<5: dq.append((d,False))
        if len(dq)>=7: break

    # === Layer 0：FinMind（v10.9.59）===
    for cd, is_today in dq:
        candidates = _fetch_finmind_institution(cd)
        if candidates:
            dd = cd.strftime("%Y/%m/%d"); ts = now.strftime("%Y/%m/%d")
            if   dd==ts:                sn=f"✅ FinMind 當日法人（{dd}）"
            elif weekday<5 and not afc: sn=f"📅 今日法人尚未公布，FinMind 暫用 {dd}"
            else:                       sn=f"📅 FinMind 使用 {dd} 前交易日資料"
            return candidates, dd, sn

    # === Layer 1：TWSE T86 備援（v10.9.59 降為備援，Render 海外 IP 可能被擋）===
    headers={"User-Agent":"Mozilla/5.0"}
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
                    if   dd==ts:                sn=f"⚠ TWSE 備援 當日法人（{dd}）"
                    elif weekday<5 and not afc: sn=f"⚠ TWSE 備援 暫用 {dd}"
                    else:                       sn=f"⚠ TWSE 備援 {dd}"
                    return candidates,dd,sn
        except Exception as e: dlog("REC", f"TWSE 法人失敗：{e}")
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
#  觀察清單 Flex
# ══════════════════════════════════════════
def make_rec_card(rank:int, s:dict)->dict:
    """v10.9.80：觀察清單卡片 — AI 深度分析版。"""
    is_up=s["pct"]>=0; color="#D97A5C" if is_up else "#7AABBE"
    arrow="▲" if is_up else "▼"; pct_str=f"{arrow} {abs(s['pct']):.2f}%"
    filled=s["score"]//10; bar="█"*filled+"░"*(10-filled)
    ai = s.get("ai") or {}
    # AI 分析 fallback：若 AI 未回，用 signals 拼湊
    reason = ai.get("reason") or "—（AI 分析暫缺）"
    tech_txt = ai.get("tech") or ("、".join(s.get("tech_signals",[])[:3]) or "—")
    chip_txt = ai.get("chip") or ("、".join(s.get("chip_signals",[])[:3]) or "—")
    news_txt = ai.get("news") or s.get("sentiment","中性")
    style = ai.get("style", "波段")
    risk = ai.get("risk") or "短線波動仍需留意大盤與國際雜訊"
    confidence = ai.get("confidence", "中")
    # 價位區間（v10.9.80 新增）
    sl = s.get("stop_loss"); tg = s.get("target")
    sup = s.get("support"); res = s.get("resistance")
    has_levels = bool(sl and tg)
    return {
        "type":"bubble","size":"mega",
        "header":{"type":"box","layout":"horizontal","backgroundColor":"#E89B82","paddingAll":"12px",
            "contents":[
                {"type":"box","layout":"vertical","flex":0,
                 "contents":[{"type":"text","text":f"#{rank}","size":"xl","color":"#FFFFFF","weight":"bold"}]},
                {"type":"box","layout":"vertical","flex":1,"paddingStart":"10px",
                 "contents":[
                     {"type":"text","text":f"{s['sid']} {s['name']}","size":"md","color":"#FFFFFF","weight":"bold","wrap":True},
                     {"type":"text","text":f"{s.get('category','綜合')} ‧ 適合 {style}","size":"xs","color":"#F0D0C0"}
                 ]}
            ]},
        "body":{"type":"box","layout":"vertical","backgroundColor":"#FDF6F0","paddingAll":"12px","spacing":"sm",
            "contents":[
                # 價格 + 漲跌
                {"type":"box","layout":"horizontal","contents":[
                    {"type":"text","text":f"{s['price']:.2f}","size":"xxl","weight":"bold","color":color,"flex":1},
                    {"type":"text","text":pct_str,"size":"sm","color":color,"align":"end","flex":1,"gravity":"bottom"}
                ]},
                {"type":"separator","color":"#E8C4B4"},
                # 入選理由（AI）
                {"type":"text","text":"💡 入選理由","size":"xxs","color":"#A05A48","weight":"bold"},
                {"type":"text","text":reason,"size":"xs","color":"#5B4040","wrap":True},
                {"type":"separator","color":"#E8C4B4"},
                # 三面觀察（AI）
                {"type":"box","layout":"vertical","spacing":"xs","contents":[
                    {"type":"box","layout":"horizontal","contents":[
                        {"type":"text","text":"📊 技術","size":"xxs","color":"#9B6B5A","flex":2},
                        {"type":"text","text":tech_txt,"size":"xxs","color":"#5B4040","flex":5,"wrap":True}
                    ]},
                    {"type":"box","layout":"horizontal","contents":[
                        {"type":"text","text":"💰 籌碼","size":"xxs","color":"#9B6B5A","flex":2},
                        {"type":"text","text":chip_txt,"size":"xxs","color":"#5B4040","flex":5,"wrap":True}
                    ]},
                    {"type":"box","layout":"horizontal","contents":[
                        {"type":"text","text":"📰 消息","size":"xxs","color":"#9B6B5A","flex":2},
                        {"type":"text","text":news_txt,"size":"xxs","color":"#5B4040","flex":5,"wrap":True}
                    ]},
                ]},
                # 支撐 / 壓力 / 停損 / 目標
                *([{"type":"separator","color":"#E8C4B4"},
                   {"type":"text","text":"📍 價位區間（近 60 天）","size":"xxs","color":"#A05A48","weight":"bold"},
                   {"type":"box","layout":"horizontal","contents":[
                       {"type":"text","text":"支撐","size":"xxs","color":"#9B6B5A","flex":1},
                       {"type":"text","text":f"{sup:.0f}","size":"xxs","color":"#5B8DB8","flex":2,"weight":"bold"},
                       {"type":"text","text":"壓力","size":"xxs","color":"#9B6B5A","flex":1},
                       {"type":"text","text":f"{res:.0f}","size":"xxs","color":"#D97A5C","flex":2,"weight":"bold","align":"end"},
                   ]},
                   {"type":"box","layout":"horizontal","contents":[
                       {"type":"text","text":"停損","size":"xxs","color":"#9B6B5A","flex":1},
                       {"type":"text","text":f"{sl:.0f}","size":"xxs","color":"#7AABBE","flex":2,"weight":"bold"},
                       {"type":"text","text":"目標","size":"xxs","color":"#9B6B5A","flex":1},
                       {"type":"text","text":f"{tg:.0f}","size":"xxs","color":"#E89B82","flex":2,"weight":"bold","align":"end"},
                   ]}] if has_levels else []),
                # 風險
                {"type":"separator","color":"#E8C4B4"},
                {"type":"box","layout":"horizontal","backgroundColor":"#FAE6DE","cornerRadius":"6px","paddingAll":"6px","contents":[
                    {"type":"text","text":f"⚠ {risk}","size":"xxs","color":"#A05A48","wrap":True}
                ]},
                # 評分 + AI 信心
                {"type":"box","layout":"horizontal","contents":[
                    {"type":"text","text":f"{bar} {s['score']}/100","size":"xxs","color":"#E89B82","weight":"bold","flex":3},
                    {"type":"text","text":f"AI 信心 {confidence}","size":"xxs","color":"#9B6B5A","align":"end","flex":2}
                ]},
                {"type":"text","text":"⚠ 僅供參考，非投資建議","size":"xxs","color":"#C9A89A","align":"center","margin":"sm"}
            ]}
    }

def make_rec_flex(scored:list, mkt:dict, source_note:str)->dict:
    now_str=now_taipei().strftime("%m/%d %H:%M")
    overview={
        "type":"bubble","size":"mega",
        "header":{"type":"box","layout":"vertical","backgroundColor":"#E89B82","paddingAll":"14px",
            "contents":[
                {"type":"text","text":"⭐ 慧股觀察榜","size":"xl","color":"#FFFFFF","weight":"bold"},
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

def ai_analyze_top_picks_batch(stocks: list, mkt: dict) -> dict:
    """v10.9.80：批次 AI 分析 top 5 候選股，回傳 {sid: {reason, tech, chip, news, style, risk, confidence}}
    一次 Groq call 處理 5 檔，節省 API 配額。
    嚴格 grounding：只能根據提供的資料分析，不可編造。
    """
    if not GROQ_AVAILABLE or not stocks:
        return {}
    # 組資料區
    lines = [f"【市場狀態】{mkt.get('str', '')}", "", "【候選股資料】"]
    for i, s in enumerate(stocks, 1):
        tech_sig = "、".join(s.get("tech_signals", [])[:3]) or "--"
        chip_sig = "、".join(s.get("chip_signals", [])[:3]) or "--"
        news_titles = "、".join([t for t, _ in s.get("news_list", [])[:3]]) or "--"
        rng_str = ""
        if s.get("support") and s.get("resistance"):
            rng_str = f" 近 60 天區間 {s['support']:.0f}-{s['resistance']:.0f}"
        lines.append(f"{i}. {s['sid']} {s['name']}")
        lines.append(f"   現價 {s['price']:.2f}（{s['pct']:+.2f}%）{rng_str}")
        lines.append(f"   技術訊號：{tech_sig}")
        lines.append(f"   籌碼訊號：{chip_sig}")
        lines.append(f"   近期新聞標題：{news_titles}")
        lines.append(f"   分類：{s.get('category', '綜合')}　評分 {s.get('score', 0)}/100")
    data_block = "\n".join(lines)

    system_prompt = """你是台股觀察清單分析師。對每檔候選股做專業觀察分析。

【最高原則：不可編造】
- 只能根據「候選股資料」分析，不可虛構數字、新聞、財報。
- 資料不足就說「資料不足」。

【用詞合規】
- 禁用：建議買進、建議賣出、保證、明牌、必賺、一定漲跌
- 改用：偏多、偏空、觀察重點、需留意、可考慮觀察
- 不預測股價。

【輸出格式：純 JSON array，沒有 markdown 包裝】
[
  {
    "sid": "2330",
    "reason": "入選理由 1 句（為何在觀察清單，需具體）",
    "tech": "技術面觀察 1-2 句",
    "chip": "籌碼面觀察 1-2 句",
    "news": "消息面觀察 1 句",
    "style": "短線/波段/長期 其中一種",
    "risk": "主要風險 1 句",
    "confidence": "高/中/低"
  },
  ...
]

順序需與輸入相同。每欄位簡潔但具體，避免空泛。
"""
    user_msg = data_block + "\n\n請依規則輸出每檔的分析（JSON array）。"
    answer = groq_chat(
        messages=[{"role": "system", "content": system_prompt},
                  {"role": "user", "content": user_msg}],
        max_tokens=2500, temperature=0.3, timeout=30)
    if not answer:
        return {}
    # 嘗試解析 JSON
    try:
        cleaned = re.sub(r"^```(?:json)?\s*", "", answer.strip())
        cleaned = re.sub(r"\s*```$", "", cleaned)
        arr = json.loads(cleaned)
        if not isinstance(arr, list):
            return {}
        out = {}
        for item in arr:
            if isinstance(item, dict) and item.get("sid"):
                out[str(item["sid"])] = item
        dlog("REC", f"AI 批次分析：{len(out)} 檔 OK")
        return out
    except Exception as e:
        dlog("REC", f"AI 分析 JSON 解析失敗：{e} / raw={answer[:200]}")
        return {}


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
            push_message(user_id,"⭐ 觀察清單\n━━━━━━━━━━━━━━\n　目前無法取得資料\n　請稍後再試"); return
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
            # v10.9.80：加支撐壓力停損目標（從近 60 天動態範圍）
            support = resistance = stop_loss = target = None
            try:
                if closes and len(closes) >= 5:
                    recent = closes[-60:]
                    lo, hi = min(recent), max(recent)
                    support, resistance = lo, hi
                    stop_loss = lo * 0.95
                    target    = hi * 1.05
            except: pass
            scored.append({
                "sid": sid, "name": tw["name"], "price": tw["price"], "pct": tw["pct"],
                "sentiment": sentiment["label"],
                "tech_signals": tech.get("signals", []),
                "chip_signals": chip.get("signals", []),
                "news_list": nl,
                "category": classify_stock(tech, chip, tw["pct"]),
                "score": ts,
                "support": support, "resistance": resistance,
                "stop_loss": stop_loss, "target": target,
            })
        scored.sort(key=lambda x:x["score"],reverse=True)
        top5=scored[:5]
        if not top5:
            push_message(user_id,"⭐ 觀察清單\n━━━━━━━━━━━━━━\n　目前無符合條件個股"); return
        # v10.9.80：AI 批次分析 top5
        ai_map = ai_analyze_top_picks_batch(top5, mkt)
        for s in top5:
            s["ai"] = ai_map.get(s["sid"], {})
        push_flex(user_id,make_rec_flex(top5,mkt,source_note),"慧股觀察榜")
    except Exception as e:
        dlog("REC", f"觀察清單運算失敗：{e}")
        push_message(user_id,"⭐ 觀察清單\n━━━━━━━━━━━━━━\n　系統處理中發生錯誤\n　請稍後再試")


# ══════════════════════════════════════════
#  持股
# ══════════════════════════════════════════
def _get_portfolio_advice(stock_id: str) -> dict:
    """v10.9.62：根據近 60 天還原股價算系統建議停損/目標 + 下次除權息。
    回傳 {stop_loss, target, ex_div_date, ex_div_cash, ex_div_stock} 或 None。
    """
    advice = {"stop_loss": None, "target": None,
              "ex_div_date": None, "ex_div_cash": 0, "ex_div_stock": 0}
    # 動態合理範圍 → 取低點 × 0.95 當停損、高點 × 1.05 當目標
    rng = _get_dynamic_sanity_range(stock_id, source="finmind_tw")
    if rng:
        lo, hi = rng
        # rng 已是 (60 天 min × 0.7, 60 天 max × 1.3)，反推回 raw min/max
        raw_lo = lo / 0.7
        raw_hi = hi / 1.3
        advice["stop_loss"] = raw_lo * 0.95
        advice["target"] = raw_hi * 1.05
    # 除權息：lazy load 該股，取近期未來那筆
    try:
        _lazy_load_exdiv_for_stock(stock_id)
        info = EX_DIVIDEND_CALENDAR.get(stock_id)
        if info:
            date = info.get("date", "")
            # 只顯示未來的（>= 今天）
            today = now_taipei().strftime("%Y%m%d")
            if date >= today:
                advice["ex_div_date"] = date
                advice["ex_div_cash"] = info.get("cash", 0)
                advice["ex_div_stock"] = info.get("stock", 0)
    except: pass
    return advice


def _fmt_exdiv_date(date_str: str) -> str:
    """20260613 → 06/13"""
    if not date_str or len(date_str) < 8:
        return date_str
    return f"{date_str[4:6]}/{date_str[6:8]}"


def make_portfolio_flex_carousel(user_id: str) -> dict:
    """v10.9.62：持股 Flex carousel。每檔一張卡 + 開頭一張 overview 總損益。"""
    portfolio = load_portfolio()
    up = {k: v for k, v in portfolio.items() if v.get("user_id") == user_id}
    if not up:
        return None

    # 先算每檔現價、損益
    rows = []
    total_profit = 0
    total_cost = 0
    for key, data in up.items():
        symbol = _pf_symbol(key)  # v10.9.73：從複合 key 取 symbol
        sid = symbol.replace(".TW", "")
        try:
            if sid.isdigit():
                tw = get_tw_stock(sid)
                price = tw["price"] if tw else 0
                name  = tw["name"] if tw else sid
            else:
                us = get_us_stock(symbol)
                price = us["price"] if us else 0
                name  = us["name"] if us else symbol
        except:
            price = 0; name = sid
        shares = data["shares"]
        bp = data["buy_price"]
        profit = (price - bp) * shares if price else 0
        cost = bp * shares
        pct = (price - bp) / bp * 100 if price and bp else 0
        rows.append({
            "symbol": symbol, "sid": sid, "name": name,
            "price": price, "bp": bp, "shares": shares,
            "profit": profit, "pct": pct, "cost": cost,
        })
        total_profit += profit
        total_cost += cost
    total_pct = (total_profit / total_cost * 100) if total_cost else 0

    bubbles = []
    # Overview bubble
    is_up = total_profit >= 0
    overview_color = "#D97A5C" if is_up else "#7AABBE"
    bubbles.append({
        "type": "bubble", "size": "kilo",
        "header": {"type": "box", "layout": "vertical",
                   "backgroundColor": "#E8B8A8", "paddingAll": "14px",
                   "contents": [
                       {"type": "text", "text": "📋 我的持股總覽",
                        "size": "md", "color": "#FFFFFF", "weight": "bold"},
                       {"type": "text",
                        "text": f"共 {len(rows)} 檔 ‧ {now_taipei().strftime('%m/%d %H:%M')}",
                        "size": "xxs", "color": "#FDF6F0", "margin": "xs"},
                   ]},
        "body": {"type": "box", "layout": "vertical",
                 "backgroundColor": "#FDF6F0", "paddingAll": "16px", "spacing": "sm",
                 "contents": [
                     {"type": "text", "text": "總損益", "size": "xs", "color": "#9B6B5A"},
                     {"type": "text", "text": f"{total_profit:+,.0f}",
                      "size": "3xl", "color": overview_color, "weight": "bold"},
                     {"type": "text", "text": f"{'▲' if is_up else '▼'} {total_pct:+.2f}%",
                      "size": "sm", "color": overview_color},
                     {"type": "separator", "color": "#E8C4B4"},
                     {"type": "text", "text": "⚠ 系統建議僅供參考，非投資建議",
                      "size": "xxs", "color": "#C9A89A", "align": "center", "margin": "sm"},
                 ]},
    })

    # 每檔股票一張卡
    for r in rows:
        profit = r["profit"]
        pct = r["pct"]
        is_up = profit >= 0
        c = "#D97A5C" if is_up else "#7AABBE"
        arrow = "▲" if is_up else "▼"
        sign = "+" if is_up else ""

        # 系統建議
        advice_box = []
        if r["sid"].isdigit():
            adv = _get_portfolio_advice(r["sid"])
            if adv.get("stop_loss") and adv.get("target"):
                sl = adv["stop_loss"]
                tg = adv["target"]
                sl_pct = (sl - r["price"]) / r["price"] * 100 if r["price"] else 0
                tg_pct = (tg - r["price"]) / r["price"] * 100 if r["price"] else 0
                advice_box = [
                    {"type": "separator", "color": "#E8C4B4"},
                    {"type": "text", "text": "💡 系統建議價", "size": "xs",
                     "color": "#A05A48", "weight": "bold", "margin": "sm"},
                    {"type": "box", "layout": "horizontal", "contents": [
                        {"type": "text", "text": "停損", "size": "xxs",
                         "color": "#9B6B5A", "flex": 1},
                        {"type": "text", "text": f"{sl:,.2f}",
                         "size": "xs", "color": "#7AABBE", "weight": "bold", "flex": 2},
                        {"type": "text", "text": f"{sl_pct:+.1f}%",
                         "size": "xxs", "color": "#7AABBE", "align": "end", "flex": 2},
                    ]},
                    {"type": "box", "layout": "horizontal", "contents": [
                        {"type": "text", "text": "目標", "size": "xxs",
                         "color": "#9B6B5A", "flex": 1},
                        {"type": "text", "text": f"{tg:,.2f}",
                         "size": "xs", "color": "#D97A5C", "weight": "bold", "flex": 2},
                        {"type": "text", "text": f"{tg_pct:+.1f}%",
                         "size": "xxs", "color": "#D97A5C", "align": "end", "flex": 2},
                    ]},
                ]
            # 除權息
            if adv.get("ex_div_date"):
                d_short = _fmt_exdiv_date(adv["ex_div_date"])
                cash = adv.get("ex_div_cash", 0)
                stock = adv.get("ex_div_stock", 0)
                div_text = f"{d_short} ‧ 現金 {cash} 元"
                if stock > 0:
                    div_text += f" + 配股 {stock}"
                advice_box.extend([
                    {"type": "separator", "color": "#E8C4B4"},
                    {"type": "box", "layout": "horizontal", "spacing": "sm",
                     "backgroundColor": "#FAE6DE", "cornerRadius": "6px",
                     "paddingAll": "8px", "margin": "sm", "contents": [
                        {"type": "text", "text": "💰 下次除息",
                         "size": "xxs", "color": "#A05A48", "weight": "bold", "flex": 2},
                        {"type": "text", "text": div_text,
                         "size": "xxs", "color": "#5B4040", "flex": 5, "wrap": True},
                    ]},
                ])

        bubble = {
            "type": "bubble", "size": "kilo",
            "header": {"type": "box", "layout": "vertical",
                       "backgroundColor": c, "paddingAll": "12px",
                       "contents": [
                           {"type": "text",
                            "text": f"{r['symbol']} {r['name']}",
                            "size": "sm", "color": "#FFFFFF", "weight": "bold", "wrap": True},
                           {"type": "text",
                            "text": f"{arrow} {profit:+,.0f}　{sign}{pct:.1f}%",
                            "size": "md", "color": "#FFFFFF", "weight": "bold", "margin": "xs"},
                       ]},
            "body": {"type": "box", "layout": "vertical",
                     "backgroundColor": "#FDF6F0", "paddingAll": "12px", "spacing": "xs",
                     "contents": [
                         {"type": "box", "layout": "horizontal", "contents": [
                             {"type": "text", "text": "現價", "size": "xxs",
                              "color": "#9B6B5A", "flex": 2},
                             {"type": "text", "text": f"{r['price']:,.2f}" if r['price'] else "—",
                              "size": "sm", "color": "#5B4040", "weight": "bold",
                              "align": "end", "flex": 3},
                         ]},
                         {"type": "box", "layout": "horizontal", "contents": [
                             {"type": "text", "text": "成本", "size": "xxs",
                              "color": "#9B6B5A", "flex": 2},
                             {"type": "text", "text": f"{r['bp']:,.2f}",
                              "size": "xs", "color": "#5B4040", "align": "end", "flex": 3},
                         ]},
                         {"type": "box", "layout": "horizontal", "contents": [
                             {"type": "text", "text": "股數", "size": "xxs",
                              "color": "#9B6B5A", "flex": 2},
                             {"type": "text", "text": f"{r['shares']:,}",
                              "size": "xs", "color": "#5B4040", "align": "end", "flex": 3},
                         ]},
                         *advice_box,
                     ]},
            # v10.9.72：每張卡片加刪除按鈕（合併「我的持股」功能）
            "footer": {"type": "box", "layout": "vertical", "paddingAll": "8px",
                       "contents": [
                           {"type": "button", "style": "secondary", "height": "sm",
                            "action": {"type": "postback", "label": "🗑️ 刪除這檔",
                                       "data": f"action=del_portfolio&symbol={r['symbol']}"}}
                       ]},
        }
        bubbles.append(bubble)

    return {"type": "carousel", "contents": bubbles}


def get_portfolio_summary(user_id:str)->str:
    portfolio=load_portfolio()
    up={k:v for k,v in portfolio.items() if v.get("user_id")==user_id}
    if not up:
        return "📋 持股清單是空的\n━━━━━━━━━━━━━━\n新增方式：\n　新增 2330 100 200\n　（代碼 股數 買入均價）"
    msg="📋 我的持股\n━━━━━━━━━━━━━━\n"; total=0
    for key,data in up.items():
        symbol=_pf_symbol(key)  # v10.9.73：複合 key 取 symbol
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
                    kline,news_list,query_time,ex_dividend=None,meta=None):
    is_up=chg>=0; color="#D97A5C" if is_up else "#7AABBE"
    arrow="▲" if is_up else "▼"; sign="+" if is_up else ""
    spark=kline.get("spark","▄▄▄▄▄▄▄▄▄▄"); trend=kline.get("trend","--")
    ma5=kline.get("ma5"); ma20=kline.get("ma20"); ma60=kline.get("ma60")
    ma120=kline.get("ma120"); ma240=kline.get("ma240")
    rsi=kline.get("rsi",0); rl=kline.get("rsi_label","--")
    rc="#E89B82" if rsi>70 else ("#5B8DB8" if rsi<30 else "#8B6B5A")
    dn=f"{symbol} {name}" if name and name!=symbol else symbol
    nc=[]
    # v10.9.35：支援兩種格式 — 舊 (t, u) tuple、新 dict（含 sentiment+summary）
    for item in news_list[:4]:
        if isinstance(item, dict):
            # AI 新聞格式
            t = item.get("title", "")
            u = item.get("url", "")
            sentiment = item.get("sentiment", "")
            summary = item.get("summary", "")
            # 情緒顏色
            sent_color = "#D97A5C" if "🟢" in sentiment else ("#7AABBE" if "🔴" in sentiment else "#8B6B9B")
            if u:
                nc.append({"type":"box","layout":"vertical","spacing":"xxs","contents":[
                    {"type":"button","style":"link","height":"sm","action":{
                        "type":"uri","label":f"📰 {t}","uri":u}},
                    # 顯示 AI 摘要 + 情緒
                    {"type":"box","layout":"horizontal","contents":[
                        {"type":"text","text":sentiment,"size":"xxs","color":sent_color,"weight":"bold","flex":0},
                        {"type":"text","text":f"  {summary}" if summary else "",
                         "size":"xxs","color":"#A07560","wrap":True,"flex":1},
                    ]},
                ]})
            else:
                nc.append({"type":"text","text":f"📰 {t}","size":"xs","color":"#B06050","wrap":True})
        else:
            # 舊格式 (title, url) tuple
            t, u = item
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
                # v10.9.42：除權息日提醒
                *([{"type":"box","layout":"horizontal","backgroundColor":"#FAE6DE","cornerRadius":"6px","paddingAll":"8px","contents":[
                    {"type":"text","text":f"💰 今日除息 {ex_dividend} 元（漲跌已修正）","size":"xs","color":"#A05A48","weight":"bold","wrap":True}
                ]}] if ex_dividend else []),
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
                ]},
                # v10.9.49：資料來源 metadata（主來源/備援、即時/延遲）
                *([{"type":"text",
                    "text":f"📡 {fmt_data_meta(meta)}",
                    "size":"xxs",
                    "color":"#B89BC4" if meta.get("is_fallback") else "#C9A89A",
                    "align":"start","margin":"xs"}] if isinstance(meta, dict) else [])
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
        # v10.9.37 修：用一般新聞（規則式去重），避免 Groq API 卡住 LINE webhook
        # AI 新聞改為獨立指令未來使用
        news=get_tw_stock_news(symbol,tw["name"],count=4)
        update_tw_data_to_sheets(symbol,tw)
        log_to_sheets(user_id,"查詢台股",symbol,"成功")
        return make_stock_flex(symbol,tw["name"],tw.get("market_type","台股"),
                               tw.get("status",""),tw.get("source",""),
                               tw["price"],tw["chg"],tw["pct"],
                               tw.get("open","N/A"),tw["high"],tw["low"],tw["vol"],
                               kline,news,query_time,
                               ex_dividend=tw.get("ex_dividend"),
                               meta=tw.get("meta")),None
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
                               kline,news,query_time,
                               meta=us.get("meta")),None


HELP_MSG="""✨ 慧股拾光 Lumistock
━━━━━━━━━━━━━━
📌 功能說明

🔍 查股票　輸入代號即可
　台股：2330　美股：AAPL
　ETF：0050　00878

🌐 全球大盤　點選選單

💹 外匯資金　匯率與市場分析

🤖 AI分析　智慧選股觀察

📰 財經新聞　台股美股國際

📋 持股管理　損益追蹤

💬 意見回饋　輸入「意見回饋」
　直接告訴 Owner 你的想法或 bug
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

    # v10.9.79：未註冊者全面阻擋 postback
    if not is_owner(user_id) and not is_admin(user_id) and not is_registered(user_id):
        reply_text(event.reply_token,
            "🔒 請先註冊才能使用功能\n輸入「註冊 您的姓名」即可")
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

        # ── 刪除持股（v10.9.73：複合 key + 嚴格使用者隔離）
        if action == "del_portfolio":
            symbol = params.get("symbol", "")
            if symbol:
                p = load_portfolio()
                norm = symbol.replace(".TW","") if symbol.replace(".TW","").isdigit() else symbol
                ckey = _pf_key(user_id, norm)
                # 優先複合 key；相容舊 symbol-only key（但須確認 user_id 相符）
                target = None
                if ckey in p:
                    target = ckey
                elif symbol in p and p[symbol].get("user_id") == user_id:
                    target = symbol
                elif norm in p and p[norm].get("user_id") == user_id:
                    target = norm
                if target:
                    del p[target]
                    save_portfolio(p)
                    delete_portfolio_from_sheets(user_id, symbol)
                    delete_portfolio_from_sheets(user_id, norm)
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


@handler.add(MessageEvent, message=ImageMessageContent)
def handle_image(event):
    """v10.9.64：使用者傳券商庫存截圖 → Groq vision 辨識 → 確認後匯入持股。"""
    user_id = event.source.user_id
    msg_id = event.message.id
    dlog("IMAGE", f"收到圖片 user={user_id[:10]}... msg_id={msg_id}")
    if is_blocked_user(user_id):
        return
    # v10.9.79：未註冊者全面阻擋（含截圖辨識）
    if not is_owner(user_id) and not is_admin(user_id) and not is_registered(user_id):
        reply_text(event.reply_token,
            "🔒 請先註冊才能使用功能\n輸入「註冊 您的姓名」即可")
        return
    if not GROQ_AVAILABLE:
        reply_text(event.reply_token,
            "⚠️ AI 辨識功能未啟用（需 GROQ_API_KEY）\n"
            "請聯繫管理員")
        return
    reply_text(event.reply_token,
        "🔍 收到持股截圖，AI 辨識中...\n"
        "約 15-30 秒後回報結果\n\n"
        "⚠ 截圖將送到 AI 雲端辨識；\n建議事先遮蔽帳號、餘額等敏感資訊")

    def _bg():
        try:
            with ApiClient(configuration) as api_client:
                blob_api = MessagingApiBlob(api_client)
                content = blob_api.get_message_content(message_id=msg_id)
            # v10.9.81：統一辨識 — 庫存頁 OR 賣出回報
            result = analyze_brokerage_screenshot(content, mime="image/jpeg")
            rtype = result.get("type", "unknown")
            items = result.get("items", [])

            if rtype == "holdings" and items:
                WAITING_PORTFOLIO_IMPORT[user_id] = {"items": items, "ts": time.time()}
                # v10.9.89：改用 Flex 卡片預覽 + 內嵌浮標確認/取消
                try:
                    flex = make_holdings_preview_flex(items)
                    push_flex(user_id, flex, alt_text="庫存匯入預覽")
                except Exception as e:
                    dlog("IMAGE", f"庫存 Flex 失敗 fallback 文字：{type(e).__name__}: {e}")
                    push_message(user_id, format_portfolio_import_preview(items))
                return

            if rtype == "sell" and items:
                WAITING_SELL_IMPORT[user_id] = {"items": items, "ts": time.time()}
                # v10.9.89：改用 Flex 卡片預覽 + 內嵌浮標確認/取消
                try:
                    flex = make_sell_preview_flex(items, user_id)
                    push_flex(user_id, flex, alt_text="賣出預覽")
                except Exception as e:
                    dlog("IMAGE", f"賣出 Flex 失敗 fallback 文字：{type(e).__name__}: {e}")
                    push_message(user_id, format_sell_import_preview(items, user_id))
                return

            if rtype == "buy" and items:
                WAITING_BUY_IMPORT[user_id] = {"items": items, "ts": time.time()}
                # v10.9.89：改用 Flex 卡片預覽 + 內嵌浮標確認/取消
                try:
                    flex = make_buy_preview_flex(items, user_id)
                    push_flex(user_id, flex, alt_text="買進預覽")
                except Exception as e:
                    dlog("IMAGE", f"買進 Flex 失敗 fallback 文字：{type(e).__name__}: {e}")
                    push_message(user_id, format_buy_import_preview(items, user_id))
                return

            push_message(user_id,
                "❌ 無法辨識任何持股 / 買進 / 賣出資料\n"
                "可能原因：\n"
                "　• 截圖不清楚 / 字太小\n"
                "　• 不是券商庫存頁、也不是交易回報\n"
                "　• AI 服務暫時忙碌\n\n"
                "庫存：請截「庫存查詢 / 股票庫存」頁\n"
                "買進：請截「成交回報（買）」頁\n"
                "賣出：請截「成交回報（賣）」頁\n"
                "或手動：\n"
                "　・新增 代號 股數 均價（新部位）\n"
                "　・加碼 代號 股數 買價（既有部位加碼）\n"
                "　・賣出 代號 股數 賣價")
        except Exception as e:
            dlog("IMAGE", f"處理圖片失敗：{type(e).__name__}: {e}")
            push_message(user_id, f"❌ 處理圖片失敗：{type(e).__name__}")

    threading.Thread(target=_bg, daemon=True).start()


def is_registered_user(user_id: str) -> bool:
    """簡易註冊檢查（避免外人濫用 AI 配額）。Owner / Admin 自動算註冊。"""
    if is_owner(user_id) or is_admin(user_id):
        return True
    # 其他人視已加好友即可（暫不嚴格驗證）
    return True


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

    # ══ 未註冊使用者全面阻擋（v10.9.79）══
    # 未註冊者只能：1) 輸入「註冊 姓名」完成註冊  2) 看歡迎/說明
    # Owner / Admin 自動視為已註冊（避免管理者被卡住）
    if not is_owner(user_id) and not is_admin(user_id) and not is_registered(user_id):
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
        # 其他任何訊息一律導向註冊
        dlog("MSG", f"未註冊者嘗試使用：{user_id[:10]}... text='{text[:20]}'")
        reply_text(event.reply_token,
              "👋 歡迎使用慧股拾光 Lumistock！\n"
              "━━━━━━━━━━━━━━\n"
              "🔒 為了個人化投資助理體驗，\n"
              "全部功能須先完成註冊才能使用。\n\n"
              "📝 註冊方式：\n"
              "　輸入「註冊 您的姓名」\n\n"
              "　例如：\n"
              "　註冊 王小明\n\n"
              "註冊完成後，所有功能立即解鎖 🌸")
        return

    # ══ AI 智能問答（v10.9.69 / v10.9.74 升級為連續對話）══
    # 進入問答模式（按鈕）
    if text in ["問AI", "問 AI", "AI問答", "AI 問答", "問AI助理"]:
        WAITING_AI_QA[user_id] = time.time()
        reply_text(event.reply_token,
            "💬 AI 投資助理已就緒\n━━━━━━━━━━━━━━\n"
            "直接輸入你的問題即可，例如：\n"
            "　・2330 現在怎麼看\n"
            "　・0050 跟 0056 差在哪\n"
            "　・我是新手適合買 ETF 還是股票\n"
            "　・今天台股為什麼跌\n"
            "　・什麼是殖利率\n"
            "　・怎麼設定停損\n\n"
            "可以連續發問、隨時換話題，問完一題直接問下一題即可。\n"
            "結束時點下方「🔚 結束問答」即可離開。\n"
            "⚠ AI 只根據查到的資料回答，不會亂掰")
        return
    if text in ["結束問答", "離開問答", "退出問答", "🔚 結束問答"]:
        WAITING_AI_QA.pop(user_id, None)
        reply_text(event.reply_token, "✅ 已結束 AI 問答\n隨時想問再點「💬 問 AI 助理」就好 🌸")
        return
    # 「問 XXX」前綴：直接問答，並進入連續問答模式
    if text.startswith("問 ") or text.startswith("問："):
        q = text[2:].strip()
        dlog("HANDLER", f"→ AI 問答（前綴）：{q[:30]}")
        WAITING_AI_QA[user_id] = time.time()  # v10.9.74：進入模式，之後可連續問
        reply_text(event.reply_token, "🤖 AI 思考中...\n約 10-20 秒後回覆")
        def _bg_qa():
            try:
                ans = ai_qa_answer(user_id, q)
            except Exception as e:
                ans = f"🤖 回答失敗：{type(e).__name__}"
            push_text_with_qr(user_id, ans,
                [("🔚 結束問答", "結束問答")])
        threading.Thread(target=_bg_qa, daemon=True).start()
        return

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
            [("觀察清單","觀察清單"),("趨勢觀察","趨勢觀察"),("成長觀察","成長觀察"),
             ("存股觀察","存股觀察"),("波段觀察","波段觀察"),("AI概念觀察","AI概念觀察")])
        return

    if text=="財經新聞":
        dlog("HANDLER", "→ 財經新聞選單")
        reply_flex_with_qr(event.reply_token, make_news_menu_flex(), "財經新聞",
            [("AI 新聞","AI新聞"),("台股新聞","台股新聞"),("美股新聞","美股新聞"),
             ("個股新聞","個股新聞"),("國際新聞","國際新聞"),("地緣政治","地緣政治新聞")])
        return

    # ══ AI 新聞解讀（v10.9.38 新增，獨立功能） ══
    if text in ["AI新聞", "AI 新聞", "AI新聞解讀"]:
        dlog("HANDLER", "→ AI 新聞解讀選單")
        reply_flex(event.reply_token, make_ai_news_menu_flex(), "🤖 AI 新聞解讀")
        return

    if text == "AI台股新聞":
        dlog("HANDLER", "→ AI 台股新聞（背景執行）")
        reply_text(event.reply_token,
            "⏳ AI 正在分析台股新聞...\n━━━━━━━━━━━━━━\n"
            "🤖 Groq AI 正在進行：\n"
            "  • 新聞去重\n  • 情緒分析（多空判讀）\n  • 影響族群分析\n\n"
            "約 5-10 秒後將推送結果 🌸")
        t = threading.Thread(target=build_and_push_ai_news, args=(user_id, "tw"))
        t.daemon = True; t.start()
        return

    if text == "AI美股新聞":
        dlog("HANDLER", "→ AI 美股新聞（背景執行）")
        reply_text(event.reply_token,
            "⏳ AI 正在分析美股新聞...\n━━━━━━━━━━━━━━\n"
            "🤖 Groq AI 正在進行：\n"
            "  • 抓取 Reuters/CNBC/Bloomberg 英文新聞\n"
            "  • 翻譯成中文\n"
            "  • 情緒分析（多空判讀）\n"
            "  • 分析對台股影響\n\n"
            "約 5-10 秒後將推送結果 🌸")
        t = threading.Thread(target=build_and_push_ai_news, args=(user_id, "us"))
        t.daemon = True; t.start()
        return

    if text == "AI國際新聞":
        dlog("HANDLER", "→ AI 國際新聞（背景執行）")
        reply_text(event.reply_token,
            "⏳ AI 正在分析國際新聞...\n━━━━━━━━━━━━━━\n"
            "🤖 Groq AI 正在進行新聞分析\n"
            "約 5-10 秒後將推送結果 🌸")
        t = threading.Thread(target=build_and_push_ai_news, args=(user_id, "global"))
        t.daemon = True; t.start()
        return

    if text=="持股管理":
        dlog("HANDLER", "→ 持股管理選單")
        reply_flex_with_qr(event.reply_token, make_portfolio_menu_flex(), "持股管理",
            [("我的持股","持股"),("新增持股","新增持股說明"),
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
            "🤖 AI 管理\n━━━━━━━━━━━━━━\n功能開發中 🚧\n\n後續版本將開放：\n　• AI 模型參數調整\n　• 觀察演算法設定\n　• 評分權重配置")
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
            [("我的持股","持股"),("新增持股","新增持股說明"),("損益分析","損益分析")])
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
        # v10.9.51：櫃買指數改用 TPEx 官方（Yahoo ^TWOII 資料偏移 1 天且 rmp 卡舊值）
        if sym == "__TPEXIDX__":
            dlog("HANDLER", "→ 查櫃買指數（TPEx 官方）")
            data = get_taiwan_otc_index()
            if data:
                flex = make_quote_flex(name, data, "#5B8DB8")
                if flex:
                    reply_flex(event.reply_token, flex, name)
                    return
            reply_text(event.reply_token, "⚠️ 櫃買指數暫時無法取得\n請稍後再試")
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
            # v10.9.33：根據幣對類型用不同顏色
            if "TWD" in sym and sym != "TWD=X":
                # XXX/TWD 外幣對台幣 → 珊瑚粉
                color = "#E89B82"
            elif sym == "TWD=X":
                # USD/TWD 用台灣最重要的珊瑚粉
                color = "#E89B82"
            elif "DX-Y" in sym:
                # DXY 美元指數 → 奶油杏
                color = "#E8B8A8"
            else:
                # 國際貨幣對 → 薰衣草粉
                color = "#C9B0DB"
            flex = make_quote_flex(name, data, color)
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
        # v10.9.76：多來源（Yahoo 直接連結 + Google 多媒體）
        items = get_category_news("tw", count=10)
        if items:
            flex = make_news_carousel("🇹🇼 台股新聞", "#E89B82", items)
            if flex: reply_flex(event.reply_token, flex, "台股新聞"); return
        news=get_news("台股 股市 財經 今日",4,trusted_only=True)
        reply_text(event.reply_token, format_news_text(news,"🇹🇼 台股新聞"))
        return
    if text=="美股新聞":
        # v10.9.76：聚焦美股（道瓊/Nasdaq/標普/費半/Fed），多媒體
        items = get_category_news("us", count=10)
        if items:
            flex = make_news_carousel("🇺🇸 美股新聞", "#5B8DB8", items)
            if flex: reply_flex(event.reply_token, flex, "美股新聞"); return
        news=get_news("美股 華爾街 財經",4,trusted_only=True)
        reply_text(event.reply_token, format_news_text(news,"🇺🇸 美股新聞"))
        return
    if text=="國際新聞":
        # v10.9.76：聚焦非美全球（歐日陸/總經/商品），與美股區隔
        items = get_category_news("intl", count=10)
        if items:
            flex = make_news_carousel("🌐 國際財經", "#B89BC4", items)
            if flex: reply_flex(event.reply_token, flex, "國際新聞"); return
        news=get_news("國際財經 全球市場 Fed",4,trusted_only=True)
        reply_text(event.reply_token, format_news_text(news,"🌐 國際財經新聞"))
        return
    if text=="地緣政治新聞":
        # v10.9.76：多來源（台海/美中/美伊中東/俄烏），Flex carousel
        items = get_category_news("geo", count=10)
        if items:
            flex = make_news_carousel("🌏 地緣政治", "#A0809B", items)
            if flex: reply_flex(event.reply_token, flex, "地緣政治"); return
        news=get_news("地緣政治 美中 台海 俄烏 中東",4,trusted_only=False)
        reply_text(event.reply_token, format_news_text(news,"🌏 地緣政治"))
        return
    if text=="個股新聞":
        # v10.9.58：設定 5 分鐘內 state，下次輸入股票代號 → 回新聞 carousel（10 則）
        WAITING_STOCK_NEWS[user_id] = time.time()
        reply_text_with_qr(event.reply_token,
            "📰 個股新聞\n━━━━━━━━━━━━━━\n請輸入股票代號，將回傳近 14 天最多 10 則新聞\n例如：2330",
            [("台積電","2330"),("聯發科","2454"),("鴻海","2317"),("廣達","2382")])
        return

    # ══ AI 選股（v10.9.48：新詞優先，舊詞保留向後相容） ══
    if text in ["觀察清單","今日觀察","推薦股","今日推薦股"]:
        dlog("HANDLER", "→ 觀察清單（背景執行）")
        log_to_sheets(user_id,"查詢觀察清單","","成功")
        reply_text(event.reply_token,
              "⭐ 觀察清單分析中...\n━━━━━━━━━━━━━━\n"
              "正在整合法人籌碼、技術面、新聞情緒\n約 15～30 秒後將推送結果 📊\n\n⚠ 僅供參考，非投資建議")
        t=threading.Thread(target=build_and_push_recommendation,args=(user_id,))
        t.daemon=True; t.start()
        return

    if text in ["趨勢觀察","成長觀察","存股觀察","波段觀察","AI概念觀察",
                "趨勢股","成長股","存股","波段股","AI概念股"]:
        reply_text(event.reply_token,
            f"🤖 {text} 分析功能開發中...\n━━━━━━━━━━━━━━\n"
            f"目前請使用「觀察清單」功能\n後續版本將加入更多 AI 分析類別 🚀")
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
        elif text in ["健檢", "系統健檢", "health"]:
            # v10.9.56：Flex 版健檢面板（粉嫩 dashboard，比純文字更專業）
            dlog("HANDLER", "→ 系統健檢（Flex）")
            try:
                flex = make_health_flex()
                reply_flex(event.reply_token, flex, "系統健檢")
            except Exception as e:
                dlog("HEALTHCHECK", f"Flex 失敗 fallback 文字：{e}")
                reply_text(event.reply_token, get_health_summary())
            return
        elif text in ["健檢文字", "系統健檢文字"]:
            # 純文字版備用（給 debug 看完整資訊）
            dlog("HANDLER", "→ 系統健檢（文字版）")
            reply_text(event.reply_token, get_health_summary()); return
        elif text in ["立即測試", "立即健檢", "ping"]:
            # v10.9.55：手動觸發完整 API 測試（同 06:30 自動跑的內容）
            dlog("HANDLER", "→ 立即測試所有 API")
            reply_text(event.reply_token, "🩺 開始測試所有 API...\n約 10-20 秒後回報結果")
            def _bg():
                try:
                    results = run_healthcheck_tests()
                    report = format_healthcheck_report(results, brief_on_success=False)
                    push_to_owner(report)
                except Exception as e:
                    push_to_owner(f"🚨 立即測試例外\n{type(e).__name__}: {e}")
            threading.Thread(target=_bg, daemon=True).start()
            return
        elif text in ["持股警報測試", "持股警報", "立即持股警報"]:
            # v10.9.63：手動觸發持股警報掃描
            # v10.9.68：手動觸發 force=True，跳過 dedup，立即重發所有觸發條件
            dlog("HANDLER", "→ 立即持股警報（force=True）")
            reply_text(event.reply_token,
                "💗 立即掃描持股警報中...\n"
                "（會忽略 24h dedup，重發所有觸發）\n"
                "約 10-20 秒後推播")
            def _bg_alert():
                try:
                    alerts_by_user = run_portfolio_alerts(force=True)
                    if not alerts_by_user:
                        push_to_owner("✨ 所有 user 持股都在安全範圍內，無警報")
                        return
                    for uid, alerts in alerts_by_user.items():
                        push_message(uid, format_portfolio_alerts_msg(alerts))
                    total = sum(len(a) for a in alerts_by_user.values())
                    push_to_owner(f"✅ 立即警報完成\n{len(alerts_by_user)} 位 user / {total} 則訊息")
                except Exception as e:
                    push_to_owner(f"🚨 立即警報例外\n{type(e).__name__}: {e}")
            threading.Thread(target=_bg_alert, daemon=True).start()
            return
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
    if text=="新增持股說明":
        reply_text(event.reply_token,
            "📋 新增持股 — 兩種方式（v10.9.84 智能模式）\n━━━━━━━━━━━━━━\n"
            "1️⃣ 手動輸入\n"
            "　格式：新增 代碼 股數 買入價\n"
            "　例如：新增 2330 100 2010\n\n"
            "2️⃣ 截圖辨識 ✨\n"
            "　傳券商「庫存查詢」or「成交回報（買）」截圖\n"
            "　AI 自動辨識 → 確認 → 一鍵儲存\n\n"
            "🧠 系統會自動判斷：\n"
            "　• 已有部位 → 自動加碼（加權平均成本）\n"
            "　• 沒有部位 → 建立新部位\n"
            "　• 含買入手續費納入成本均價\n\n"
            "📌 特殊指令：\n"
            "　• 重設 代號 股數 價 → 強制覆蓋（資料錯誤時用）\n"
            "　• 加碼 代號 股數 價 → 等同新增（保留別名）\n"
            "　⚠ 截圖會送雲端，建議遮蔽帳號餘額")
        return
    if text=="賣出說明":
        reply_text(event.reply_token,
            "💸 登記賣出 — 兩種方式\n━━━━━━━━━━━━━━\n"
            "1️⃣ 手動輸入\n"
            "　格式：賣出 代碼 股數 賣價\n"
            "　例如：賣出 6742 1000 59.5\n\n"
            "2️⃣ 截圖辨識 ✨\n"
            "　傳券商「成交回報（賣）」截圖\n"
            "　AI 自動辨識 → 預估已實現損益 → 確認賣出\n\n"
            "系統會自動：\n"
            "　• 從庫存扣股數（成本均價不變）\n"
            "　• 計算已實現損益（含手續費 + 證交稅）\n"
            "　• 寫入 Google Sheets「賣出紀錄」")
        return
    if text=="加碼說明":
        # v10.9.86：加碼已併入「新增」智能模式，這裡保留為說明轉址
        reply_text(event.reply_token,
            "📈 加碼 / 分批買進\n━━━━━━━━━━━━━━\n"
            "v10.9.84 起「新增」已自動分辨：\n"
            "　• 已有部位 → 自動加權平均（加碼）\n"
            "　• 新部位 → 建立\n\n"
            "輸入方式：\n"
            "　・新增 代碼 股數 買價\n"
            "　・或傳「成交回報（買）」截圖\n\n"
            "例：新增 2330 500 2200")
        return
    # v10.9.64：持股截圖匯入確認
    if text in ["確認匯入", "確認", "yes", "Yes", "YES"]:
        record = WAITING_PORTFOLIO_IMPORT.get(user_id)
        if not record or (time.time() - record["ts"]) > 300:
            reply_text(event.reply_token, "⏰ 沒有待確認的匯入或已過期\n請重新傳截圖")
            return
        items = record["items"]
        portfolio = load_portfolio()
        added = 0
        for h in items:
            symbol = h["stock_id"]
            # v10.9.73：複合 key（多使用者隔離）
            portfolio[_pf_key(user_id, symbol)] = {
                "user_id": user_id,
                "shares": h["shares"],
                "buy_price": h["avg_price"],
            }
            try:
                name = NAME_CACHE.get(symbol, symbol)
                save_portfolio_to_sheets(user_id, symbol, name, "TW", h["shares"], h["avg_price"])
            except: pass
            added += 1
        save_portfolio(portfolio)
        WAITING_PORTFOLIO_IMPORT.pop(user_id, None)
        reply_text(event.reply_token,
            f"✅ 已匯入 {added} 檔持股\n輸入「持股」查看完整清單")
        return
    if text in ["取消匯入", "取消", "no", "No", "NO"]:
        if WAITING_PORTFOLIO_IMPORT.pop(user_id, None):
            reply_text(event.reply_token, "✅ 已取消匯入，原資料未變動")
        else:
            reply_text(event.reply_token, "⏰ 沒有待確認的匯入")
        return
    # v10.9.81：賣出截圖確認 / 取消
    if text in ["確認賣出"]:
        record = WAITING_SELL_IMPORT.get(user_id)
        if not record or (time.time() - record["ts"]) > 300:
            reply_text(event.reply_token, "⏰ 沒有待確認的賣出，或已過期\n請重新傳截圖或用「賣出 代號 股數 賣價」")
            return
        items = record["items"]
        ok_n = 0; total_pnl = 0
        lines = ["✅ 賣出執行結果", "━━━━━━━━━━━━━━"]
        for h in items:
            r = process_sell(user_id, h["stock_id"], int(h["shares"]), float(h["sell_price"]))
            if r["ok"]:
                ok_n += 1
                total_pnl += r["realized_pnl"]
                lines.append(f"✅ {h['stock_id']} {r.get('name','')}")
                lines.append(f"　{h['shares']:,} 股 @ {h['sell_price']:,.2f}　{r['realized_pnl']:+,.0f} 元")
                lines.append(f"　剩餘 {r['remaining_shares']:,} 股")
            else:
                lines.append(f"❌ {h['stock_id']}：{r['msg'].split(chr(10))[0]}")
        WAITING_SELL_IMPORT.pop(user_id, None)
        lines.append("━━━━━━━━━━━━━━")
        sign = "🟢" if total_pnl >= 0 else "🔴"
        lines.append(f"合計已實現損益：{sign} {total_pnl:+,.0f} 元（{ok_n}/{len(items)} 筆成功）")
        reply_text(event.reply_token, "\n".join(lines))
        return
    if text in ["取消賣出"]:
        if WAITING_SELL_IMPORT.pop(user_id, None):
            reply_text(event.reply_token, "✅ 已取消賣出，原資料未變動")
        else:
            reply_text(event.reply_token, "⏰ 沒有待確認的賣出")
        return
    # v10.9.83：加碼截圖確認 / 取消
    if text in ["確認加碼"]:
        record = WAITING_BUY_IMPORT.get(user_id)
        if not record or (time.time() - record["ts"]) > 300:
            reply_text(event.reply_token, "⏰ 沒有待確認的加碼，或已過期\n請重新傳截圖或用「加碼 代號 股數 買價」")
            return
        items = record["items"]
        ok_n = 0
        lines = ["✅ 加碼/新增執行結果", "━━━━━━━━━━━━━━"]
        for h in items:
            r = process_buy(user_id, h["stock_id"], int(h["shares"]), float(h["buy_price"]))
            if r["ok"]:
                ok_n += 1
                tag = "🆕 新增" if r.get("is_new") else "📈 加碼"
                lines.append(f"{tag} {h['stock_id']}")
                lines.append(f"　{h['shares']:,} 股 @ {h['buy_price']:,.2f}")
                lines.append(f"　→ {r['total_shares']:,} 股 均價 {r['new_avg']:,.4f}")
            else:
                lines.append(f"❌ {h['stock_id']}：{r['msg'].split(chr(10))[0]}")
        WAITING_BUY_IMPORT.pop(user_id, None)
        lines.append("━━━━━━━━━━━━━━")
        lines.append(f"完成：{ok_n}/{len(items)} 筆")
        reply_text(event.reply_token, "\n".join(lines))
        return
    if text in ["取消加碼"]:
        if WAITING_BUY_IMPORT.pop(user_id, None):
            reply_text(event.reply_token, "✅ 已取消加碼，原資料未變動")
        else:
            reply_text(event.reply_token, "⏰ 沒有待確認的加碼")
        return
    # v10.9.83：手動「加碼 代號 股數 買價」
    if text.startswith("加碼 "):
        parts = text.split()
        if len(parts) == 4:
            try:
                stock_id = parts[1].upper()
                shares = int(parts[2])
                buy_price = float(parts[3])
                if shares <= 0 or buy_price <= 0:
                    raise ValueError("invalid")
                r = process_buy(user_id, stock_id, shares, buy_price)
                reply_text(event.reply_token, r["msg"])
            except Exception:
                reply_text(event.reply_token,
                    "格式錯誤\n範例：加碼 2330 500 2200\n　（代號 股數 買價）")
        else:
            reply_text(event.reply_token,
                "格式：加碼 代號 股數 買價\n範例：加碼 2330 500 2200\n\n"
                "說明：\n　• 既有部位：自動加權平均成本（不覆蓋）\n"
                "　• 新部位：等同新增")
        return
    # v10.9.81：手動「賣出 代號 股數 賣價」
    if text.startswith("賣出 "):
        parts = text.split()
        if len(parts) == 4:
            try:
                stock_id = parts[1].upper()
                shares = int(parts[2])
                sell_price = float(parts[3])
                if shares <= 0 or sell_price <= 0:
                    raise ValueError("invalid")
                r = process_sell(user_id, stock_id, shares, sell_price)
                reply_text(event.reply_token, r["msg"])
            except Exception:
                reply_text(event.reply_token,
                    "格式錯誤\n範例：賣出 6742 1000 59.5\n　（代號 股數 賣價）")
        else:
            reply_text(event.reply_token,
                "格式：賣出 代號 股數 賣價\n範例：賣出 6742 1000 59.5")
        return
    if text=="停損提醒說明": reply_text(event.reply_token,"停損提醒功能開發中 🚧\n後續版本將開放設定"); return
    if text=="目標價提醒說明": reply_text(event.reply_token,"目標價提醒功能開發中 🚧\n後續版本將開放設定"); return
    if text in ["損益分析", "我的損益", "損益總覽"]:
        # v10.9.85：已實現 + 未實現分開（取代原本與「我的持股」重複的文字版）
        reply_text(event.reply_token, format_pnl_analysis(user_id)); return
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

    # v10.9.79：未註冊已在 handler 開頭擋掉，此處不再需要重複檢查

    # ══ 意見回饋（v10.9.87 / v10.9.88 升級 Flex）═══════════════════════════
    # 進入回饋模式（兩個入口：意見回饋 / 建議）
    if text in ["意見回饋", "建議"]:
        WAITING_SUGGESTION[user_id] = time.time()
        WAITING_AI_QA.pop(user_id, None)      # 退出 AI 問答避免干擾
        WAITING_FEE_INPUT.pop(user_id, None)
        reply_text_with_qr(event.reply_token,
            "💬 意見回饋\n━━━━━━━━━━━━━━\n"
            "請直接打下你的建議、想法或 bug 回報\n"
            "（最多 1000 字，5 分鐘內任何訊息都會送出）\n\n"
            "你的訊息會直接通知 Owner，\n"
            "並附上你的註冊姓名讓他知道是誰 🌸",
            [("🚫 取消回饋", "取消回饋")])
        return
    if text in ["取消回饋", "取消建議"]:
        if WAITING_SUGGESTION.pop(user_id, None):
            reply_text(event.reply_token, "✅ 已取消意見回饋")
        else:
            reply_text(event.reply_token, "⏰ 沒有進行中的意見回饋")
        return
    # 在回饋模式中送出
    sug_at = WAITING_SUGGESTION.get(user_id)
    if sug_at and (time.time() - sug_at) < 300:
        WAITING_SUGGESTION.pop(user_id, None)
        suggestion_text = text[:1000]
        info = get_user_display_info(user_id)
        ts = now_taipei().strftime("%Y-%m-%d %H:%M")
        # 寫 Sheets（保留歷史）
        try:
            save_suggestion_to_sheets(user_id, suggestion_text)
        except Exception as e:
            dlog("SUGGESTION", f"寫 Sheets 失敗：{e}")
        # v10.9.88：推播給 Owner（Flex 卡片）
        try:
            flex = make_suggestion_flex(info, suggestion_text, ts)
            push_flex(OWNER_USER_ID, flex, alt_text=f"新建議 — {info.get('name','')}")
        except Exception as e:
            dlog("SUGGESTION", f"Flex push 失敗 fallback text：{e}")
            try:
                push_to_owner(
                    f"💬 收到新建議\n━━━━━━━━━━━━━━\n"
                    f"{info['role']}  {info['name']}\n"
                    f"時間：{ts}\n\n📝 內容：\n{suggestion_text}")
            except: pass
        reply_text(event.reply_token,
            "✅ 已收到你的建議，謝謝！\n"
            "Owner 會盡快看到並評估 🌸")
        return

    # ══ 持股管理指令 ══
    if text=="持股":
        # v10.9.62：Flex carousel（含系統建議停損/目標 + 下次除權息）
        try:
            flex = make_portfolio_flex_carousel(user_id)
            if flex:
                reply_flex(event.reply_token, flex, "我的持股")
                return
        except Exception as e:
            dlog("PORTFOLIO", f"Flex 失敗 fallback 文字：{e}")
        reply_text(event.reply_token, get_portfolio_summary(user_id)); return
    if text=="持股文字":
        # v10.9.62 保留純文字版（debug 用）
        reply_text(event.reply_token, get_portfolio_summary(user_id)); return

    # v10.9.25：可點選刪除的持股清單
    if text in ["我的持股", "持股清單"]:
        # v10.9.72：合併為豐富卡片（含分析 + 刪除按鈕），與「持股」同一個 view
        dlog("HANDLER", "→ 持股清單（合併版）")
        try:
            flex = make_portfolio_flex_carousel(user_id)
            if flex:
                reply_flex(event.reply_token, flex, "我的持股")
                return
        except Exception as e:
            dlog("PORTFOLIO", f"Flex 失敗 fallback：{e}")
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

    # v10.9.84：「新增」改為智能模式 — 既有部位自動加碼，新部位自動建立
    if text.startswith("新增 "):
        parts=text.split()
        if len(parts)==4:
            try:
                stock_id = parts[1].upper()
                shares = int(parts[2])
                raw_price = float(parts[3])
                if shares <= 0 or raw_price <= 0:
                    raise ValueError("invalid")
                r = process_buy(user_id, stock_id, shares, raw_price)
                # process_buy 已根據是否有既有部位顯示「📈 加碼」或「🆕 新增」
                reply_text(event.reply_token, r["msg"] + "\n\n💡 想強制覆蓋既有部位請用「重設 代號 股數 價」")
            except Exception:
                reply_text(event.reply_token,
                    "格式錯誤\n範例：新增 2330 100 200")
        else:
            reply_text(event.reply_token,
                "格式：新增 代碼 股數 買入價\n範例：新增 2330 100 200\n\n"
                "說明：\n　• 既有部位 → 自動加碼（加權平均）\n"
                "　• 新部位 → 建立\n"
                "　• 強制覆蓋請用「重設」")
        return

    # v10.9.84：「重設」明確強制覆蓋既有部位（少用，用於資料錯誤想重設）
    if text.startswith("重設 "):
        parts = text.split()
        if len(parts) == 4:
            try:
                stock_id = parts[1].upper()
                shares = int(parts[2])
                raw_price = float(parts[3])
                if shares <= 0 or raw_price <= 0:
                    raise ValueError("invalid")
                norm = stock_id.replace(".TW", "")
                is_tw = norm.isdigit()
                buy_fee = calc_buy_fee(raw_price, shares, user_id) if is_tw else 0
                cost_avg = (raw_price * shares + buy_fee) / shares if shares else raw_price
                # 移除所有既有 key
                p = load_portfolio()
                for k in (_pf_key(user_id, norm), _pf_key(user_id, stock_id),
                          norm, stock_id, stock_id + ".TW"):
                    if k in p and p[k].get("user_id") == user_id:
                        p.pop(k, None)
                p[_pf_key(user_id, norm)] = {
                    "user_id": user_id,
                    "shares": shares,
                    "buy_price": cost_avg,
                }
                save_portfolio(p)
                name = NAME_CACHE.get(norm, norm)
                market = "台股" if is_tw else "美股"
                save_portfolio_to_sheets(user_id, norm, name, market, shares, cost_avg)
                log_to_sheets(user_id, "重設持股", norm, "成功")
                disc = get_user_fee_discount(user_id)
                disc_str = f"{int(disc*100)}%" if disc < 1.0 else "無折扣"
                reply_text(event.reply_token,
                    f"🔄 重設成功（既有部位被覆蓋）\n━━━━━━━━━━━━━━\n"
                    f"　{norm} {name}\n"
                    f"　{shares:,} 股 @ {raw_price:,.2f}\n"
                    f"　手續費 {buy_fee:,}（折數 {disc_str}）\n"
                    f"　含費成本均價 {cost_avg:,.4f}")
            except Exception:
                reply_text(event.reply_token,
                    "格式錯誤\n範例：重設 2330 100 200")
        else:
            reply_text(event.reply_token,
                "格式：重設 代碼 股數 買入價\n範例：重設 2330 100 200\n\n"
                "⚠ 重設會覆蓋既有部位（一般情境請用「新增」自動加碼）")
        return

    # v10.9.82：設定手續費折數 / 查詢
    # v10.9.86：手續費設定 — 進入直接輸入模式（任何後續輸入都當折數）
    if text in ["手續費設定", "查手續費", "手續費", "目前手續費"]:
        d = get_user_fee_discount(user_id)
        disc_str = f"{int(d*100)}%" if d < 1.0 else "無折扣（100%）"
        WAITING_FEE_INPUT[user_id] = time.time()
        WAITING_AI_QA.pop(user_id, None)  # 退出 AI 問答模式避免干擾
        reply_text_with_qr(event.reply_token,
            f"💸 你的手續費設定\n━━━━━━━━━━━━━━\n"
            f"目前折數：{disc_str}\n"
            f"標準費率：0.1425%\n"
            f"實際費率：{0.1425 * d:.4f}%\n"
            f"證交稅：0.3%（固定）\n"
            f"最低手續費：{TW_MIN_COMMISSION} 元\n\n"
            f"━━━━━━━━━━━━━━\n"
            f"📝 直接輸入新折數即可變更：\n"
            f"　• 數字：28 / 60 / 100\n"
            f"　• 折數：六折 / 6.5折 / 2.8折\n"
            f"　• 中文：無折數 / 半折 / 全價",
            [("🚫 取消設定", "取消設定")])
        return
    # 向下相容：「設定手續費 X」一鍵指令
    if text.startswith("設定手續費"):
        val = text.replace("設定手續費", "").strip()
        parsed = parse_fee_discount_input(val) if val else None
        if parsed is not None:
            set_user_fee_discount(user_id, parsed)
            n_pct = int(round(parsed * 100))
            disc_str = f"{n_pct}%" if parsed < 1.0 else "無折扣（100%）"
            reply_text_with_qr(event.reply_token,
                f"✅ 手續費已設為 {disc_str}\n━━━━━━━━━━━━━━\n"
                f"實際費率：{0.1425 * parsed:.4f}%\n"
                f"證交稅：0.3%（固定）\n\n"
                f"⚠ 之後所有新增 / 賣出皆按此折數計算",
                [("✅ 完成", "完成設定"), ("🔁 重新輸入", "重新輸入手續費")])
        else:
            reply_text(event.reply_token,
                "❓ 看不懂這個格式，請試試：\n"
                "　28 / 60 / 100\n"
                "　六折 / 6.5折 / 2.8折\n"
                "　無折數 / 半折 / 全價")
        return
    # 手續費輸入模式的浮標按鈕
    if text in ["取消設定", "取消手續費"]:
        WAITING_FEE_INPUT.pop(user_id, None)
        reply_text(event.reply_token, "✅ 已取消手續費設定，原設定未變動")
        return
    if text in ["完成設定", "完成"]:
        WAITING_FEE_INPUT.pop(user_id, None)
        reply_text(event.reply_token, "✅ 設定完成 🌸")
        return
    if text in ["重新輸入手續費", "重新輸入"]:
        WAITING_FEE_INPUT[user_id] = time.time()
        reply_text_with_qr(event.reply_token,
            "📝 請重新輸入手續費折數：\n"
            "　• 數字：28 / 60 / 100\n"
            "　• 折數：六折 / 6.5折 / 2.8折\n"
            "　• 中文：無折數 / 半折 / 全價",
            [("🚫 取消設定", "取消設定")])
        return

    if text.startswith("刪除 "):
        parts=text.split()
        if len(parts)==2:
            raw=parts[1].upper()
            norm=raw.replace(".TW","")
            # v10.9.73：複合 key + 相容舊 key
            p=load_portfolio()
            ckey=_pf_key(user_id, norm)
            target=None
            for cand in [ckey, norm, raw, raw+".TW"]:
                if cand in p and (("|" in cand) or p[cand].get("user_id")==user_id):
                    target=cand; break
            if target:
                del p[target]; save_portfolio(p)
                delete_portfolio_from_sheets(user_id,norm)
                delete_portfolio_from_sheets(user_id,raw)
                reply_text(event.reply_token,f"✅ 已刪除 {norm}")
            else: reply_text(event.reply_token,f"找不到 {norm}")
        else: reply_text(event.reply_token,"格式：刪除 代碼\n範例：刪除 2330")
        return

    # ══ 大盤快捷 ══
    if text in ["大盤","全球大盤行情"]:
        reply_text(event.reply_token, get_market_summary()); return

    # ══ 說明 ══
    if text in ["說明","help","Help","?"]:
        reply_text(event.reply_token, HELP_MSG); return

    # ══ 手續費輸入模式（v10.9.86）— 放在 AI 問答模式之前 ══
    fee_at = WAITING_FEE_INPUT.get(user_id)
    if fee_at and (time.time() - fee_at) < 300:
        parsed = parse_fee_discount_input(text)
        if parsed is not None:
            WAITING_FEE_INPUT.pop(user_id, None)
            set_user_fee_discount(user_id, parsed)
            n_pct = int(round(parsed * 100))
            disc_str = f"{n_pct}%" if parsed < 1.0 else "無折扣（100%）"
            reply_text_with_qr(event.reply_token,
                f"✅ 手續費已設為 {disc_str}\n━━━━━━━━━━━━━━\n"
                f"實際費率：{0.1425 * parsed:.4f}%\n"
                f"證交稅：0.3%（固定）\n\n"
                f"⚠ 之後所有新增 / 賣出皆按此折數計算",
                [("✅ 完成", "完成設定"), ("🔁 重新輸入", "重新輸入手續費")])
            return
        # 解析失敗 → 不消耗模式，提示重試
        reply_text_with_qr(event.reply_token,
            "❓ 看不懂這個格式，請試試：\n"
            "　• 數字：28 / 60 / 100\n"
            "　• 折數：六折 / 6.5折 / 2.8折\n"
            "　• 中文：無折數 / 半折 / 全價",
            [("🚫 取消設定", "取消設定")])
        return

    # ══ AI 問答模式（v10.9.74：連續對話，不自動結束，直到使用者點「結束問答」）══
    # 移到股票查詢之前，避免中文問題被 isalnum 誤判成股票代號
    if user_id in WAITING_AI_QA:
        # 純股票代號（4-6 位數字）仍走個股卡片；含中文 / 其他字元的問題才走 AI
        _t_check = text.upper().replace("查","").strip()
        if not (_t_check.isdigit() and 4 <= len(_t_check) <= 6):
            WAITING_AI_QA[user_id] = time.time()  # 更新活動時間
            dlog("HANDLER", f"→ AI 問答（連續模式）：{text[:30]}")
            reply_text(event.reply_token, "🤖 AI 思考中...\n約 10-20 秒後回覆")
            def _bg_qa_mode():
                try:
                    ans = ai_qa_answer(user_id, text)
                except Exception as e:
                    ans = f"🤖 回答失敗：{type(e).__name__}"
                # v10.9.74：每次回答附「結束問答」浮標，可連續問下一題
                push_text_with_qr(user_id, ans, [("🔚 結束問答", "結束問答")])
            threading.Thread(target=_bg_qa_mode, daemon=True).start()
            return

    # ══ 股票代號查詢 ══
    t=text.upper().replace("查","").strip()
    if t and (t.isdigit() or (t.isalpha() and len(t)>=1) or t.replace("-","").isalnum()):
        # v10.9.58：「個股新聞」模式 — 5 分鐘內輸入代號 → 回新聞 carousel（不是個股 Flex）
        waiting_at = WAITING_STOCK_NEWS.get(user_id)
        if waiting_at and (time.time() - waiting_at) < 300:
            WAITING_STOCK_NEWS.pop(user_id, None)
            dlog("HANDLER", f"→ 個股新聞 carousel {t}")
            # 嘗試解出中文名（先用快取，再用 fallback）
            name = NAME_CACHE.get(t, "")
            if not has_chinese(name):
                name = get_tw_stock_name_fallback(t) or ""
            # v10.9.76：多來源（FinMind 直接連結 + Google 多媒體），去重補充更多
            fm = get_finmind_news_enriched(t, count=10)
            gq = f"{t} {name}".strip() if has_chinese(name) else f"{t} 股票"
            gg = get_google_news_multi(gq, count=10)
            news_dicts = _merge_dedup_news(fm, gg, count=12)
            if news_dicts:
                carousel = make_stock_news_carousel(t, name or t, news_dicts)
                if carousel:
                    reply_flex(event.reply_token, carousel, f"{t} 個股新聞")
                    return
            reply_text(event.reply_token,
                f"📰 {t} {name}\n━━━━━━━━━━━━━━\n近期無相關新聞\n或資料源暫時無法取得")
            return
        dlog("HANDLER", f"→ 股票查詢 {t}")
        flex,err=get_stock_flex(t,user_id)
        if flex: reply_flex(event.reply_token,flex,f"{t} 股票資訊")
        else: reply_text(event.reply_token,err or "查詢失敗")
        return

    dlog("HANDLER", "→ 無匹配，回 HELP_MSG")
    reply_text(event.reply_token, HELP_MSG)


if __name__=="__main__":
    print(f"慧股拾光 Lumistock LINE Bot v{VERSION} 啟動中...")
    if GROQ_AVAILABLE:
        print(f"🤖 Groq AI：已啟用（AI 新聞解讀功能可用）")
    else:
        print("⚠️ Groq AI：未設定 GROQ_API_KEY，AI 新聞功能會降級")
    for code,name in FALLBACK_NAMES.items():
        NAME_CACHE[code]=name
    t=threading.Thread(target=_bg_init); t.daemon=True; t.start()
    setup_rich_menus()
    port=int(os.environ.get("PORT",5001))
    app.run(host="0.0.0.0",port=port,debug=False)
