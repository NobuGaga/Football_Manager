from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from database import Database
from importers import FixtureImporter, ResultImporter
from selections import (ai_hit_index, ai_hit_label, classify_result_type, normalize_result_type,
                        normalize_score_list, parse_ai_hit)
from utils import (clean_text, first, local_date, normalize_score, optional_float,
                   parse_datetime, read_structured_file, utc_now)

HISTORY_COLUMNS = [
    "场次", "联赛", "对阵", "比赛时间", "让球", "主胜", "平局", "主负",
    "让球主胜", "让球平局", "让球主负", "预测比分_Kimi", "实际比分", "结果类型", "AI比分预测结果",
]


def display_handicap(value: object) -> str:
    text = clean_text(value, "0")
    if "/" in text:
        return text
    return f"0/{text}"


def display_odds(value: object) -> str:
    number = optional_float(value)
    return "--" if number is None else f"{number:g}"


def display_match_time(start_time_utc: str, timezone: str) -> str:
    value = datetime.fromisoformat(start_time_utc).astimezone(ZoneInfo(timezone))
    return f"{value.year}/{value.month}/{value.day} {value.hour}:{value.minute:02d}"


def _match_sort_key(row: dict) -> tuple:
    number = re.search(r"(\d+)$", row["场次"])
    match_time = datetime.strptime(row["比赛时间"], "%Y/%m/%d %H:%M")
    return match_time, int(number.group(1)) if number else 0


class HistoryManager:
    def __init__(self, db_path: str | Path = "data/football.db"):
        self.db = Database(db_path)
        self.db.initialize()

    def import_file(self, path: str | Path, year: int | None = None) -> dict:
        source = Path(path)
        rows, _ = read_structured_file(source)
        if not rows:
            raise ValueError("历史数据为空")
        selected_year = year or self._infer_year(rows)
        fixture_rows = []
        normalized_rows = []
        for number, row in enumerate(rows, 2):
            try:
                start_utc, timezone = self._parse_time(first(row, "比赛时间", "开赛时间"), selected_year)
                home, away = FixtureImporter._teams(row)
                business_date = local_date(start_utc, timezone)
                fixture_row = {
                    "比赛编号": clean_text(first(row, "场次", "比赛编号"), "未编号"),
                    "比赛日期": business_date,
                    "联赛": clean_text(first(row, "联赛"), "未知联赛"),
                    "主队": home,
                    "客队": away,
                    "开赛时间": start_utc,
                    "执行时区": "Asia/Shanghai",
                    "让球": clean_text(first(row, "让球"), "0"),
                    "主胜": first(row, "主胜"),
                    "平局": first(row, "平局"),
                    "主负": first(row, "主负"),
                    "让球主胜": first(row, "让球主胜"),
                    "让球平局": first(row, "让球平局"),
                    "让球主负": first(row, "让球主负"),
                    "比赛状态": "finished" if clean_text(first(row, "实际比分")) else "pending",
                    "原始数据来源": str(source),
                }
                scores = normalize_score_list(first(row, "预测比分_Kimi", "预测比分 (Kimi)"))
                if not scores:
                    raise ValueError("预测比分_Kimi 不能为空")
                normalized_rows.append({
                    "source_row": number,
                    "fixture": fixture_row,
                    "home": home,
                    "away": away,
                    "start_utc": start_utc,
                    "scores": scores,
                    "actual_score": clean_text(first(row, "实际比分")),
                    "declared_result_type": clean_text(first(row, "结果类型")),
                    "declared_ai_hit": clean_text(first(row, "AI比分预测结果", "AI 比分预测结果")),
                })
                fixture_rows.append(fixture_row)
            except (ValueError, TypeError) as exc:
                raise ValueError(f"历史数据第 {number} 行：{exc}") from exc
        from utils import normalize_name

        deduplicated = {}
        for item in normalized_rows:
            key = (item["start_utc"], normalize_name(item["home"]), normalize_name(item["away"]))
            deduplicated[key] = item
        normalized_rows = list(deduplicated.values())
        fixture_rows = [item["fixture"] for item in normalized_rows]
        fixture_result = FixtureImporter(self.db.path).import_rows(fixture_rows, source="history_csv")
        if fixture_result["errors"]:
            raise ValueError(f"历史对阵导入失败：{fixture_result['errors'][:3]}")
        keys = []
        with self.db.connect() as conn:
            for item in normalized_rows:
                matches = conn.execute("""SELECT fixture_key FROM fixtures
                    WHERE home_team=? AND away_team=? AND start_time_utc=?""",
                    (item["home"], item["away"], item["start_utc"])).fetchall()
                if len(matches) != 1:
                    raise ValueError(f"历史第 {item['source_row']} 行无法唯一关联对阵")
                keys.append(matches[0]["fixture_key"])
        result_rows = [{"fixture_key": key, "实际比分": item["actual_score"]}
                       for key, item in zip(keys, normalized_rows) if item["actual_score"]]
        result_info = ResultImporter(self.db.path).import_rows(result_rows, source="history_csv")
        if result_info["errors"]:
            raise ValueError(f"历史赛果导入失败：{result_info['errors'][:3]}")
        warnings = []
        now = utc_now()
        last_positions = {key: index for index, key in enumerate(keys)}
        with self.db.connect() as conn:
            for position, (key, item) in enumerate(zip(keys, normalized_rows)):
                declared_hit = parse_ai_hit(item["declared_ai_hit"])
                declared_type = normalize_result_type(item["declared_result_type"])
                if item["declared_result_type"] and not declared_type:
                    raise ValueError(f"历史第 {item['source_row']} 行结果类型无效")
                conn.execute("""INSERT INTO legacy_score_predictions(
                    fixture_key,ai_model,predicted_scores_json,declared_result_type,declared_ai_hit,source,created_at
                ) VALUES(?,?,?,?,?,?,?) ON CONFLICT(fixture_key) DO UPDATE SET
                    predicted_scores_json=excluded.predicted_scores_json,
                    declared_result_type=excluded.declared_result_type,declared_ai_hit=excluded.declared_ai_hit,
                    source=excluded.source""", (
                    key, "kimi", json.dumps(item["scores"], ensure_ascii=False), declared_type,
                    declared_hit, str(source), now,
                ))
                if item["actual_score"] and last_positions[key] == position:
                    result = conn.execute("SELECT result_type FROM results WHERE fixture_key=?", (key,)).fetchone()
                    actual_hit = ai_hit_index(item["scores"], item["actual_score"])
                    if declared_type and result["result_type"] and declared_type != result["result_type"]:
                        warnings.append({"row": item["source_row"], "field": "结果类型",
                                         "declared": declared_type, "computed": result["result_type"]})
                    if declared_hit is not None and declared_hit != actual_hit:
                        warnings.append({"row": item["source_row"], "field": "AI比分预测结果",
                                         "declared": declared_hit, "computed": actual_hit})
        return {
            "rows": len(rows), "unique_matches": len(set(keys)), "results_imported": result_info["imported"],
            "validation_warnings": warnings, "source": str(source),
        }

    def analysis_records(self) -> list[dict]:
        with self.db.connect() as conn:
            rows = conn.execute("""SELECT f.fixture_key,f.match_code,f.league,f.home_team,f.away_team,
                f.start_time_utc,f.timezone,f.handicap,f.odds_json,r.actual_score,r.home_goals,r.away_goals,
                r.outcome,r.result_type,COALESCE(p.top_scores_json,l.predicted_scores_json) AS scores_json,
                CASE WHEN p.id IS NOT NULL THEN 'current' ELSE 'legacy' END AS prediction_source
                FROM results r JOIN fixtures f ON f.fixture_key=r.fixture_key
                LEFT JOIN legacy_score_predictions l ON l.fixture_key=f.fixture_key
                LEFT JOIN predictions p ON p.id=(
                    SELECT p2.id FROM predictions p2 WHERE p2.fixture_key=f.fixture_key AND p2.ai_model='kimi'
                        AND p2.predicted_at<=f.start_time_utc ORDER BY p2.predicted_at DESC,p2.id DESC LIMIT 1
                ) ORDER BY f.start_time_utc,f.match_code""").fetchall()
        records = []
        for row in rows:
            item = dict(row)
            item["odds"] = json.loads(item.pop("odds_json") or "{}")
            raw_scores = json.loads(item.pop("scores_json") or "[]")[:3]
            item["scores"] = [entry["score"] if isinstance(entry, dict) else str(entry) for entry in raw_scores]
            if not item["result_type"]:
                item["result_type"] = classify_result_type(item["odds"], item["home_goals"],
                                                            item["away_goals"], item["handicap"])
            records.append(item)
        return records

    def export(self, output: str | Path = "reports/足球预测历史数据.csv") -> dict:
        rows = []
        skipped = 0
        for item in self.analysis_records():
            if not item["scores"]:
                skipped += 1
                continue
            odds = item["odds"]
            rows.append({
                "场次": item["match_code"], "联赛": item["league"],
                "对阵": f"{item['home_team']} VS {item['away_team']}",
                "比赛时间": display_match_time(item["start_time_utc"], item["timezone"]),
                "让球": display_handicap(item["handicap"]),
                "主胜": display_odds(odds.get("odds_home")), "平局": display_odds(odds.get("odds_draw")),
                "主负": display_odds(odds.get("odds_away")),
                "让球主胜": display_odds(odds.get("odds_handicap_home")),
                "让球平局": display_odds(odds.get("odds_handicap_draw")),
                "让球主负": display_odds(odds.get("odds_handicap_away")),
                "预测比分_Kimi": "/".join(item["scores"][:3]), "实际比分": normalize_score(item["actual_score"]),
                "结果类型": item["result_type"], "AI比分预测结果": ai_hit_label(item["scores"][:3], item["actual_score"]),
            })
        rows.sort(key=_match_sort_key)
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows, columns=HISTORY_COLUMNS).to_csv(output_path, index=False, encoding="utf-8-sig")
        return {"csv": str(output_path), "rows": len(rows), "skipped_without_scores": skipped}

    @staticmethod
    def _infer_year(rows: list[dict]) -> int:
        years = []
        for row in rows:
            text = clean_text(first(row, "比赛时间", "开赛时间"))
            match = re.match(r"(20\d{2})[/-]", text)
            if match:
                years.append(int(match.group(1)))
        if not years:
            raise ValueError("历史比赛时间缺少年份，请使用 --year 显式指定")
        return Counter(years).most_common(1)[0][0]

    @staticmethod
    def _parse_time(value: object, year: int) -> tuple[str, str]:
        text = clean_text(value).replace("/", "-")
        if re.match(r"^\d{1,2}-\d{1,2}\s+\d{1,2}:\d{2}", text):
            text = f"{year}-{text}"
        return parse_datetime(text, tz_name="Asia/Shanghai")
