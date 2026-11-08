from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from database import Database
from history import display_handicap, display_match_time, display_odds
from importers import BaseImporter, ResultImporter
from reporter import ReportGenerator
from selections import (ai_hit_index, ai_hit_label, classify_result_type, normalize_result_type,
                        normalize_score_list, outcome_selection_hit, parse_ai_hit, parse_outcome_selections)
from utils import (clean_text, first, normalize_name, normalize_score, optional_float, parse_datetime,
                   read_structured_file, utc_now)

CHECKLIST_COLUMNS = [
    "场次", "联赛", "对阵", "比赛时间", "让球", "主胜", "平局", "主负",
    "让球主胜", "让球平局", "让球主负", "预测比分_Kimi", "我的预测 (胜平负)", "我的预测 (比分)",
    "实际比分", "结果类型", "AI 比分预测结果", "胜平负预测结果", "比分预测结果",
]


def _hit_mark(value: bool | None) -> str:
    return "" if value is None else "✅" if value else "❌"


class ChecklistGenerator:
    def __init__(self, db_path: str | Path = "data/football.db", output_dir: str | Path = "reports",
                 ai_model: str = "kimi"):
        self.db = Database(db_path)
        self.db.initialize()
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.ai_model = ai_model

    def generate(self, date: str, output: str | Path | None = None) -> dict:
        items = [item for item in ReportGenerator(self.db.path).daily_items(date)
                 if item["ai_model"] == self.ai_model]
        with self.db.connect() as conn:
            bets = {row["fixture_key"]: dict(row) for row in conn.execute("SELECT * FROM user_bets")}
        rows = [self._row(item, bets.get(item["fixture_key"])) for item in items]
        output_path = Path(output) if output else self.output_dir / f"预测核对表_{date}.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(rows, columns=CHECKLIST_COLUMNS).to_csv(output_path, index=False, encoding="utf-8-sig")
        return {"csv": str(output_path), "rows": len(rows)}

    @staticmethod
    def _row(item: dict, bet: dict | None) -> dict:
        import json

        odds = json.loads(item["odds_json"] or "{}")
        scores = [entry["score"] for entry in item["top_scores"][:3]]
        actual = clean_text(item["actual_score"])
        outcome_selection = clean_text(bet["outcome_selection"]) if bet else ""
        score_selections = normalize_score_list(json.loads(bet["score_selections_json"] or "[]")) if bet else []
        result_type = outcome_hit = score_hit = None
        if actual:
            actual = normalize_score(actual)
            home, away = map(int, actual.split(":"))
            result_type = classify_result_type(odds, home, away, item["handicap"])
            outcome_hit = outcome_selection_hit(outcome_selection, home, away, item["handicap"])
            if score_selections:
                score_hit = actual in score_selections
        return {
            "场次": item["match_code"], "联赛": item["league"],
            "对阵": f"{item['home_team']} VS {item['away_team']}",
            "比赛时间": display_match_time(item["start_time_utc"], item["timezone"]),
            "让球": display_handicap(item["handicap"]),
            "主胜": display_odds(odds.get("odds_home")), "平局": display_odds(odds.get("odds_draw")),
            "主负": display_odds(odds.get("odds_away")),
            "让球主胜": display_odds(odds.get("odds_handicap_home")),
            "让球平局": display_odds(odds.get("odds_handicap_draw")),
            "让球主负": display_odds(odds.get("odds_handicap_away")),
            "预测比分_Kimi": "/".join(scores), "我的预测 (胜平负)": outcome_selection,
            "我的预测 (比分)": "/".join(score_selections), "实际比分": actual,
            "结果类型": result_type or "", "AI 比分预测结果": ai_hit_label(scores, actual),
            "胜平负预测结果": _hit_mark(outcome_hit), "比分预测结果": _hit_mark(score_hit),
        }


class ChecklistValidator:
    def validate_file(self, path: str | Path) -> dict:
        rows, _ = read_structured_file(path)
        errors = []
        if not rows:
            errors.append({"row": 0, "error": "核对表没有数据行"})
        else:
            missing = [column for column in CHECKLIST_COLUMNS if column not in rows[0]]
            if missing:
                errors.append({"row": 0, "error": f"核对表缺少固定列：{', '.join(missing)}"})
        seen = {}
        for number, row in enumerate(rows, 2):
            try:
                self._validate_row(row)
                home, away = self._teams(row)
                key = (clean_text(first(row, "比赛时间", "开赛时间")), normalize_name(home), normalize_name(away))
                if key in seen:
                    raise ValueError(f"与第 {seen[key]} 行重复；应先按比赛时间和对阵去重")
                seen[key] = number
            except (ValueError, TypeError) as exc:
                errors.append({"row": number, "error": str(exc)})
        return {"file": str(path), "rows": len(rows), "valid": not errors,
                "error_count": len(errors), "errors": errors}

    def write_report(self, path: str | Path, output: str | Path) -> dict:
        result = self.validate_file(path)
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        return {**result, "report": str(output_path)}

    def _validate_row(self, row: dict) -> None:
        scores = normalize_score_list(first(row, "预测比分_Kimi", "预测比分 (Kimi)"))
        if not scores:
            raise ValueError("预测比分_Kimi 不能为空")
        actual_text = clean_text(first(row, "实际比分", "actual_score"))
        outcome_selection = clean_text(first(row, "我的预测 (胜平负)"))
        user_scores = normalize_score_list(first(row, "我的预测 (比分)"))
        parse_outcome_selections(outcome_selection)
        declared_result = clean_text(first(row, "结果类型"))
        declared_ai = clean_text(first(row, "AI 比分预测结果", "AI比分预测结果"))
        declared_outcome = clean_text(first(row, "胜平负预测结果"))
        declared_score = clean_text(first(row, "比分预测结果"))
        if not actual_text:
            for name, value in (("结果类型", declared_result), ("AI 比分预测结果", declared_ai),
                                ("胜平负预测结果", declared_outcome), ("比分预测结果", declared_score)):
                if value:
                    raise ValueError(f"实际比分为空时{name}也应为空")
            return
        actual = normalize_score(actual_text)
        home, away = map(int, actual.split(":"))
        odds = self._odds(row)
        handicap = first(row, "让球")
        expected_type = classify_result_type(odds, home, away, handicap)
        if expected_type:
            if normalize_result_type(declared_result) != expected_type:
                raise ValueError(f"结果类型应为 {expected_type}，实际填写 {declared_result or '空'}")
        elif declared_result and not normalize_result_type(declared_result):
            raise ValueError("结果类型无效")
        expected_ai = ai_hit_index(scores, actual)
        if parse_ai_hit(declared_ai) != expected_ai:
            raise ValueError(f"AI 比分预测结果应为 {ai_hit_label(scores, actual)}")
        expected_outcome = outcome_selection_hit(outcome_selection, home, away, handicap)
        self._validate_hit("胜平负预测结果", declared_outcome, expected_outcome)
        expected_score = actual in user_scores if user_scores else None
        self._validate_hit("比分预测结果", declared_score, expected_score)

    @staticmethod
    def _validate_hit(name: str, declared: str, expected: bool | None) -> None:
        if declared not in {"", "✅", "❌"}:
            raise ValueError(f"{name}只能为 ✅、❌ 或空白")
        if expected is None and declared:
            raise ValueError(f"未提供对应预测时{name}应为空")
        if expected is not None and declared != _hit_mark(expected):
            raise ValueError(f"{name}应为 {_hit_mark(expected)}，实际填写 {declared or '空'}")

    @staticmethod
    def _odds(row: dict) -> dict:
        return {key: optional_float(first(row, *names)) for key, names in {
            "odds_home": ("主胜",), "odds_draw": ("平局",), "odds_away": ("主负", "客胜"),
            "odds_handicap_home": ("让球主胜",), "odds_handicap_draw": ("让球平局",),
            "odds_handicap_away": ("让球主负",),
        }.items()}

    @staticmethod
    def _teams(row: dict) -> tuple[str, str]:
        pairing = clean_text(first(row, "对阵"))
        for separator in (" VS ", " vs ", "VS", "vs"):
            if separator in pairing:
                home, away = pairing.split(separator, 1)
                return home.strip(), away.strip()
        home = clean_text(first(row, "主队"))
        away = clean_text(first(row, "客队"))
        if not home or not away:
            raise ValueError("对阵双方不能为空")
        return home, away


class ChecklistImporter(BaseImporter):
    def import_file(self, path: str | Path, output_dir: str | Path = "reports") -> dict:
        from history import HistoryManager
        from result_checker import ResultChecker

        source = Path(path)
        rows, _ = read_structured_file(source)
        result_rows = [row for row in rows if clean_text(first(row, "实际比分", "actual_score"))]
        result_info = ResultImporter(self.db.path).import_rows(result_rows, source="prediction_checklist")
        if result_info["errors"]:
            raise ValueError(f"核对表赛果导入失败：{result_info['errors'][:3]}")
        bets_imported = 0
        affected_keys = set()
        with self.db.connect() as conn:
            for number, row in enumerate(rows, 2):
                try:
                    outcome_selection = clean_text(first(row, "我的预测 (胜平负)"))
                    score_selections = normalize_score_list(first(row, "我的预测 (比分)"))
                    metadata = [first(row, "下注金额", "投注金额", "stake"), first(row, "下注赔率", "odds"),
                                first(row, "票号", "ticket_id"), first(row, "下注时间", "placed_at")]
                    if not outcome_selection and not score_selections and not any(clean_text(value) for value in metadata):
                        continue
                    key = self._locate_checklist(conn, row)
                    parse_outcome_selections(outcome_selection)
                    placed_at = self._placed_at(row)
                    fixture = conn.execute("SELECT start_time_utc FROM fixtures WHERE fixture_key=?", (key,)).fetchone()
                    cutoff = placed_at or fixture["start_time_utc"]
                    prediction = conn.execute("""SELECT id FROM predictions
                        WHERE fixture_key=? AND ai_model='kimi' AND predicted_at<=?
                        ORDER BY predicted_at DESC,id DESC LIMIT 1""", (key, cutoff)).fetchone()
                    conn.execute("""INSERT INTO user_bets(
                        fixture_key,prediction_id,strategy_version,outcome_selection,score_selections_json,
                        stake,odds,ticket_id,placed_at,source,updated_at
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(fixture_key) DO UPDATE SET
                        prediction_id=excluded.prediction_id,strategy_version=excluded.strategy_version,
                        outcome_selection=excluded.outcome_selection,score_selections_json=excluded.score_selections_json,
                        stake=excluded.stake,odds=excluded.odds,ticket_id=excluded.ticket_id,
                        placed_at=excluded.placed_at,source=excluded.source,updated_at=excluded.updated_at""", (
                        key, prediction["id"] if prediction else None,
                        clean_text(first(row, "策略版本", "strategy_version")) or None,
                        outcome_selection, json.dumps(score_selections, ensure_ascii=False),
                        optional_float(first(row, "下注金额", "投注金额", "stake")),
                        optional_float(first(row, "下注赔率", "odds")),
                        clean_text(first(row, "票号", "ticket_id")) or None, placed_at,
                        str(source), utc_now(),
                    ))
                    bets_imported += 1
                    affected_keys.add(key)
                except (ValueError, TypeError) as exc:
                    raise ValueError(f"核对表第 {number} 行下注导入失败：{exc}") from exc
        with self.db.connect() as conn:
            for row in rows:
                if not clean_text(first(row, "实际比分", "actual_score")):
                    continue
                key = self._locate_checklist(conn, row)
                affected_keys.add(key)
            dates = [row["business_date"] for key in affected_keys
                     for row in conn.execute("SELECT business_date FROM fixtures WHERE fixture_key=?", (key,)).fetchall()]
        checked = len(ResultChecker(self.db.path).check_all())
        generator = ChecklistGenerator(self.db.path, output_dir)
        checklist_paths = [generator.generate(date)["csv"] for date in sorted(set(dates))]
        history = HistoryManager(self.db.path).export(Path(output_dir) / "足球预测历史数据.csv")
        return {"rows": len(rows), "bets_imported": bets_imported,
                "results_imported": result_info["imported"], "predictions_checked": checked,
                "checklists": checklist_paths, "history": history}

    def _locate_checklist(self, conn, row: dict) -> str:
        supplied = clean_text(first(row, "fixture_key", "比赛键"))
        if supplied:
            if conn.execute("SELECT 1 FROM fixtures WHERE fixture_key=?", (supplied,)).fetchone():
                return supplied
            raise ValueError(f"fixture_key 不存在：{supplied}")
        home, away = ChecklistValidator._teams(row)
        code = clean_text(first(row, "场次", "比赛编号"))
        candidates = conn.execute("""SELECT fixture_key,start_time_utc,timezone FROM fixtures
            WHERE match_code=? AND home_team=? AND away_team=?""", (code, home, away)).fetchall()
        if len(candidates) > 1:
            match_time = clean_text(first(row, "比赛时间", "开赛时间"))
            if match_time:
                start_utc, _ = parse_datetime(match_time, tz_name=candidates[0]["timezone"])
                candidates = [item for item in candidates if item["start_time_utc"] == start_utc]
        if len(candidates) != 1:
            raise ValueError(f"无法唯一关联对阵（候选 {len(candidates)} 个）：{code} {home} VS {away}")
        return candidates[0]["fixture_key"]

    @staticmethod
    def _placed_at(row: dict) -> str | None:
        value = clean_text(first(row, "下注时间", "placed_at"))
        if not value:
            return None
        return parse_datetime(value, tz_name="Asia/Shanghai")[0]
