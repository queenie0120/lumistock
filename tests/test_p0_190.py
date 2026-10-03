"""v10.9.190 新聞議題去重測試。

使用者回報：「新聞幾乎都是重複的議題」。
根因：去重只比標題字面相似度，同一件事換個寫法就過關，版面被單一議題吃光。
本檔先重現症狀，再驗證議題分群把不同議題排到前面。
"""
import unittest
from unittest import mock

from stubs import load_app
app = load_app()

from lumistock.calc.news_topics import (event_category, extract_entities,
                                        topic_signature, same_topic,
                                        cluster_by_topic, diversify,
                                        diversify_titles)

NAMES = frozenset({"台積電", "鴻海", "聯發科", "長榮"})

# 同一議題（台積電法說會）的三種寫法 —— 字面差很多，舊版全部放行
SAME_TOPIC = [
    "台積電法說會釋出樂觀展望",
    "台積電法說會：明年看好AI強勁需求",
    "台積電線上說明會 管理層談明年展望",
]
# 真正不同的議題
OTHER_TOPICS = [
    "台積電董事長宣布接任人事案",
    "台積電遭控專利侵權 對方求償百億",
    "外資大買台積電三萬張 籌碼轉強",
]


class T1_Category(unittest.TestCase):
    def test_basic_categories(self):
        self.assertEqual(event_category("台積電法說會釋出樂觀展望"), "法說")
        self.assertEqual(event_category("鴻海董事長請辭"), "人事")
        self.assertEqual(event_category("聯發科遭控專利侵權"), "訴訟")
        self.assertEqual(event_category("外資買超台股三萬張"), "法人")
        self.assertEqual(event_category("聯準會宣布降息一碼"), "利率")

    def test_uncategorised_returns_empty(self):
        self.assertEqual(event_category("今天天氣不錯"), "")
        self.assertEqual(event_category(""), "")


class T2_Entities(unittest.TestCase):
    def test_code_and_name(self):
        e = extract_entities("2330 台積電法說會", NAMES)
        self.assertIn("2330", e)
        self.assertIn("台積電", e)

    def test_name_needs_table(self):
        """沒有名稱表時仍可從代號辨識，不會整個失效。"""
        e = extract_entities("2330 台積電法說會", None)
        self.assertIn("2330", e)
        self.assertNotIn("台積電", e)

    def test_common_abbrev_not_treated_as_ticker(self):
        e = extract_entities("AI 與 ETF 熱潮推升 GDP", NAMES)
        self.assertNotIn("AI", e)
        self.assertNotIn("ETF", e)
        self.assertNotIn("GDP", e)

    def test_real_ticker_kept(self):
        self.assertIn("NVDA", extract_entities("NVDA 財報優於預期", NAMES))


class T3_SameTopic(unittest.TestCase):
    def sig(self, t):
        return topic_signature(t, NAMES)

    def test_same_entity_same_category(self):
        self.assertTrue(same_topic(self.sig(SAME_TOPIC[0]), self.sig(SAME_TOPIC[1])))

    def test_same_entity_different_category(self):
        self.assertFalse(same_topic(self.sig("台積電法說會登場"),
                                    self.sig("台積電董事長請辭")))

    def test_different_entity_same_category(self):
        self.assertFalse(same_topic(self.sig("台積電法說會登場"),
                                    self.sig("鴻海法說會登場")))

    def test_uncategorised_falls_back_to_lexical(self):
        a, b = self.sig("某某公司消息面觀察"), self.sig("某某公司消息面追蹤")
        self.assertTrue(same_topic(a, b, lexical_sim=0.9))
        self.assertFalse(same_topic(a, b, lexical_sim=0.1))
        self.assertFalse(same_topic(a, b, lexical_sim=None))


class T4_Cluster(unittest.TestCase):
    def test_same_topic_variants_group_together(self):
        cl = cluster_by_topic(SAME_TOPIC, NAMES)
        self.assertEqual(len(set(cl)), 1)

    def test_distinct_topics_stay_separate(self):
        cl = cluster_by_topic(OTHER_TOPICS, NAMES)
        self.assertEqual(len(set(cl)), 3)

    def test_mixed(self):
        cl = cluster_by_topic(SAME_TOPIC + OTHER_TOPICS, NAMES)
        self.assertEqual(len(set(cl)), 4)   # 1 個法說 + 3 個其他


class T5_Diversify(unittest.TestCase):
    def test_round_robin_puts_distinct_topics_first(self):
        titles = SAME_TOPIC + OTHER_TOPICS
        out = diversify_titles(titles, lambda t: t, names=NAMES)
        cats = [event_category(t) for t in out[:4]]
        self.assertEqual(len(set(cats)), 4)

    def test_nothing_is_dropped(self):
        titles = SAME_TOPIC + OTHER_TOPICS
        out = diversify_titles(titles, lambda t: t, names=NAMES)
        self.assertEqual(sorted(out), sorted(titles))

    def test_fills_up_when_topics_are_few(self):
        """只有一個議題時，清單照樣填滿，不可縮水。"""
        out = diversify_titles(SAME_TOPIC, lambda t: t, names=NAMES)
        self.assertEqual(sorted(out), sorted(SAME_TOPIC))

    def test_max_per_topic_two(self):
        items = list(range(6))
        clusters = [0, 0, 0, 1, 1, 1]
        out = diversify(items, clusters, max_per_topic=2)
        self.assertEqual(len(out), 6)
        self.assertEqual(set(out[:4]), {0, 1, 3, 4})

    def test_empty(self):
        self.assertEqual(diversify([], [], 1), [])
        self.assertEqual(diversify_titles([], lambda t: t), [])


class T6_AppIntegration(unittest.TestCase):
    """接回 app.py 的兩條實際路徑。"""

    def setUp(self):
        self._patch = mock.patch.dict(
            app.NAME_CACHE, {"2330": "台積電", "2317": "鴻海", "2454": "聯發科"},
            clear=True)
        self._patch.start()
        app._NEWS_NAME_SET["size"] = -1      # 讓名稱集合重算
    def tearDown(self):
        self._patch.stop()
        app._NEWS_NAME_SET["size"] = -1

    def test_entity_names_from_cache(self):
        self.assertIn("台積電", app.get_news_entity_names())

    def test_merge_dedup_news_diversifies(self):
        """個股新聞輪播：12 格不該被同一議題吃光。"""
        fm = [{"title": t, "url": f"https://a.com/{i}"} for i, t in enumerate(SAME_TOPIC)]
        gg = [{"title": t, "url": f"https://b.com/{i}"} for i, t in enumerate(OTHER_TOPICS)]
        out = app._merge_dedup_news(fm, gg, count=12)
        self.assertEqual(len(out), 6)                       # 一則都沒少
        cats = [event_category(n["title"]) for n in out[:4]]
        self.assertEqual(len(set(cats)), 4)                 # 前 4 格四個不同議題

    def test_merge_dedup_news_respects_count(self):
        fm = [{"title": t, "url": f"https://a.com/{i}"} for i, t in enumerate(SAME_TOPIC)]
        gg = [{"title": t, "url": f"https://b.com/{i}"} for i, t in enumerate(OTHER_TOPICS)]
        out = app._merge_dedup_news(fm, gg, count=3)
        self.assertEqual(len(out), 3)
        self.assertEqual(len({event_category(n["title"]) for n in out}), 3)

    def test_merge_dedup_news_exact_duplicates_still_removed(self):
        a = [{"title": "台積電法說會釋出樂觀展望", "url": "https://a.com/1"}]
        b = [{"title": "台積電法說會釋出樂觀展望", "url": "https://b.com/1"}]
        self.assertEqual(len(app._merge_dedup_news(a, b, count=12)), 1)

    def test_deduplicate_news_top_slots_cover_distinct_topics(self):
        nl = [(t, f"https://src{i}.com/x") for i, t in enumerate(SAME_TOPIC + OTHER_TOPICS)]
        out = app.deduplicate_news(nl)
        cats = [event_category(t) for t, _ in out[:4]]
        self.assertEqual(len(set(cats)), len(cats))   # 前段不重複議題

    def test_deduplicate_news_still_returns_tuples(self):
        """回傳結構不可變，呼叫端與 UI 依賴 (title, url)。"""
        nl = [(t, f"https://src{i}.com/x") for i, t in enumerate(OTHER_TOPICS)]
        out = app.deduplicate_news(nl)
        self.assertTrue(all(isinstance(x, tuple) and len(x) == 2 for x in out))

    def test_same_news_topic_helper(self):
        self.assertTrue(app.same_news_topic(SAME_TOPIC[0], SAME_TOPIC[1]))
        self.assertFalse(app.same_news_topic(OTHER_TOPICS[0], OTHER_TOPICS[1]))

    def test_diversify_failure_keeps_original_order(self):
        """分群出錯時必須原樣回傳，不可讓新聞消失。"""
        items = [("a", "u1"), ("b", "u2")]
        with mock.patch("lumistock.calc.news_topics.diversify_titles",
                        side_effect=RuntimeError("boom")):
            out = app.diversify_news_topics(items, lambda x: x[0])
        self.assertEqual(out, items)


if __name__ == "__main__":
    unittest.main()
