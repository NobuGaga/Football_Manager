from __future__ import annotations

import math
import re

from utils import clean_text, normalize_score, optional_float, outcome

NOT_UPSET = "🟢不爆冷"
SMALL_UPSET = "🟡小爆冷"
BIG_UPSET = "🔴大爆冷"
AI_HIT_LABELS = {0: "❌", 1: "1️⃣", 2: "2️⃣", 3: "3️⃣"}

OUTCOME_ALIASES = {
    "主胜": ("H", False), "胜": ("H", False), "H": ("H", False),
    "平局": ("D", False), "平": ("D", False), "D": ("D", False),
    "主负": ("A", False), "客胜": ("A", False), "负": ("A", False), "A": ("A", False),
    "让胜": ("H", True), "让球主胜": ("H", True),
    "让平": ("D", True), "让球平局": ("D", True),
    "让负": ("A", True), "让球主负": ("A", True), "让球客胜": ("A", True),
}

LEAGUE_ALIASES = {
    "亚洲冠军精英联赛": "亚冠精英", "亚洲冠军乙级联赛": "亚冠乙",
    "欧洲冠军联赛": "欧冠", "欧足联欧洲联赛": "欧罗巴", "欧洲协会联赛": "欧协联",
    "南美解放者杯": "解放者杯", "沙特职业联赛": "沙职", "沙特阿拉伯职业联赛": "沙职",
    "德国甲级联赛": "德甲", "德国乙级联赛": "德乙", "意大利甲级联赛": "意甲",
    "西班牙甲级联赛": "西甲", "法国甲级联赛": "法甲", "法国乙级联赛": "法乙",
    "英格兰超级联赛": "英超", "英格兰冠军联赛": "英冠", "荷兰甲级联赛": "荷甲",
    "荷兰乙级联赛": "荷乙", "葡萄牙超级联赛": "葡超", "瑞典超级联赛": "瑞超",
    "挪威超级联赛": "挪超", "芬兰超级联赛": "芬超", "日本职业联赛": "日职",
    "日本职业足球联赛": "日职", "韩国职业联赛": "韩职", "澳大利亚超级联赛": "澳超",
    "美国职业大联盟": "美职", "美国职业足球大联盟": "美职",
}


def normalize_league(value: object) -> str:
    text = clean_text(value)
    return LEAGUE_ALIASES.get(text, text)


def normalize_score_list(value: object) -> list[str]:
    if value is None:
        return []
    values = value if isinstance(value, (list, tuple)) else clean_text(value).split("/")
    normalized = []
    for item in values:
        text = clean_text(item)
        if text:
            score = normalize_score(text)
            if score not in normalized:
                normalized.append(score)
    return normalized


def parse_handicap(value: object) -> float | None:
    text = clean_text(value)
    if not text:
        return None
    text = text.split("/")[-1].replace("让", "").replace("球", "")
    number = optional_float(text)
    if number is None:
        raise ValueError(f"无效让球：{value}")
    return number


def handicap_outcome(home_goals: int, away_goals: int, handicap: object) -> str:
    value = parse_handicap(handicap)
    if value is None:
        raise ValueError("让球胜平负核对需要提供让球数")
    adjusted = home_goals + value
    if not math.isclose(adjusted, round(adjusted)):
        raise ValueError("当前仅支持整数让球核对")
    return outcome(int(round(adjusted)), away_goals)


def classify_result_type(odds: dict, home_goals: int, away_goals: int, handicap: object = None) -> str:
    normal = [optional_float(odds.get(key)) for key in ("odds_home", "odds_draw", "odds_away")]
    if all(value is not None and value > 0 for value in normal):
        values = normal
        actual_outcome = outcome(home_goals, away_goals)
    else:
        values = [optional_float(odds.get(key)) for key in (
            "odds_handicap_home", "odds_handicap_draw", "odds_handicap_away",
        )]
        if not all(value is not None and value > 0 for value in values):
            return ""
        actual_outcome = handicap_outcome(home_goals, away_goals, handicap)
    actual_odds = values[{"H": 0, "D": 1, "A": 2}[actual_outcome]]
    if actual_odds == min(values):
        return NOT_UPSET
    if actual_odds == max(values):
        return BIG_UPSET
    return SMALL_UPSET


def normalize_result_type(value: object) -> str:
    text = clean_text(value).replace(" ", "")
    aliases = {
        NOT_UPSET: NOT_UPSET, "不爆冷": NOT_UPSET, "🟢": NOT_UPSET,
        SMALL_UPSET: SMALL_UPSET, "小爆冷": SMALL_UPSET, "🟡": SMALL_UPSET,
        BIG_UPSET: BIG_UPSET, "大爆冷": BIG_UPSET, "🔴": BIG_UPSET,
    }
    return aliases.get(text, "")


def parse_outcome_selections(value: object) -> list[tuple[str, bool]]:
    text = clean_text(value)
    if not text:
        return []
    normalized = []
    for part in re.split(r"[/、,，]", text):
        selection = clean_text(part)
        if not selection:
            continue
        if selection not in OUTCOME_ALIASES:
            raise ValueError(f"无效胜平负选择：{selection}")
        parsed = OUTCOME_ALIASES[selection]
        if parsed not in normalized:
            normalized.append(parsed)
    return normalized


def outcome_selection_hit(value: object, home_goals: int, away_goals: int, handicap: object) -> bool | None:
    selections = parse_outcome_selections(value)
    if not selections:
        return None
    actual_normal = outcome(home_goals, away_goals)
    actual_handicap = None
    for selected, is_handicap in selections:
        if is_handicap:
            if actual_handicap is None:
                actual_handicap = handicap_outcome(home_goals, away_goals, handicap)
            if selected == actual_handicap:
                return True
        elif selected == actual_normal:
            return True
    return False


def ai_hit_index(scores: list[str], actual_score: object) -> int | None:
    actual = clean_text(actual_score)
    if not actual:
        return None
    normalized_actual = normalize_score(actual)
    return normalized_actual in scores and scores.index(normalized_actual) + 1 or 0


def ai_hit_label(scores: list[str], actual_score: object) -> str:
    index = ai_hit_index(scores, actual_score)
    return "" if index is None else AI_HIT_LABELS[index]


def parse_ai_hit(value: object) -> int | None:
    text = clean_text(value).replace(" ", "")
    if not text:
        return None
    aliases = {"❌": 0, "1️⃣": 1, "1⃣": 1, "1": 1, "2️⃣": 2, "2⃣": 2, "2": 2, "3️⃣": 3, "3⃣": 3, "3": 3}
    if text not in aliases:
        raise ValueError(f"无效 AI 比分预测结果：{value}")
    return aliases[text]
