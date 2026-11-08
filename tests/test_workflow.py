from __future__ import annotations

import csv
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from betting_policy import load_betting_policy
from checklist import CHECKLIST_COLUMNS, ChecklistGenerator, ChecklistImporter, ChecklistValidator
from database import Database
from history import HISTORY_COLUMNS, HistoryManager
from importers import FixtureImporter
from predictor import FootballPredictor
from selections import classify_result_type
from strategy_engine import StrategyEngine, StrategyValidator


class WorkflowTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "football.db"
        self.reports = self.root / "reports"
        self.policy = self.root / "policy.md"
        shutil.copy2(ROOT / "rules" / "football_manager_policy.md", self.policy)
        fixture_file = self.root / "fixtures.json"
        fixture_file.write_text(json.dumps([{
            "比赛编号": "周日001", "比赛日期": "2099-10-25", "联赛": "测试联赛",
            "主队": "主队A", "客队": "客队B", "开赛时间": "2099-10-25 18:00",
            "让球": "-1", "主胜": 1.8, "平局": 3.6, "主负": 4.2,
            "让球主胜": 2.8, "让球平局": 3.5, "让球主负": 2.2,
        }], ensure_ascii=False), encoding="utf-8")
        FixtureImporter(self.db).import_file(fixture_file)
        FootballPredictor(self.db).predict_all(as_of="2099-10-24T08:00:00+00:00")

    def tearDown(self):
        self.temp.cleanup()

    def test_bet_check_validation_history_and_next_strategy_version(self):
        first_strategy = StrategyEngine(self.db, self.reports, self.policy).generate("2099-10-25")
        strategy = json.loads(Path(first_strategy["json"]).read_text(encoding="utf-8"))
        self.assertEqual(len(strategy["matches"]), 1)
        self.assertEqual(len(strategy["matches"][0]["dual_scores"]), 2)
        self.assertEqual(strategy["matches"][0]["normal_outcome"], "主胜")
        self.assertEqual(strategy["matches"][0]["action"], "观望")

        initial = ChecklistGenerator(self.db, self.reports).generate("2099-10-25")
        self.assertTrue(ChecklistValidator().validate_file(initial["csv"])["valid"])
        with open(initial["csv"], encoding="utf-8-sig", newline="") as handle:
            row = next(csv.DictReader(handle))
        self.assertEqual(row["主胜"], "1.8")
        self.assertEqual(row["让球"], "0/-1")
        self.assertEqual(row["结果类型"], "")

        actual = row["预测比分_Kimi"].split("/")[0]
        home, away = map(int, actual.split(":"))
        row["我的预测 (胜平负)"] = "主胜" if home > away else "平局" if home == away else "主负"
        row["我的预测 (比分)"] = "/".join(row["预测比分_Kimi"].split("/")[:2])
        row["策略版本"] = "selection-v1"
        row["下注金额"] = "20"
        row["票号"] = "TEST-001"
        bet_file = self.root / "bet.csv"
        self._write_rows(bet_file, [row], CHECKLIST_COLUMNS + ["策略版本", "下注金额", "票号"])
        imported_bet = ChecklistImporter(self.db).import_file(bet_file, self.reports)
        self.assertEqual(imported_bet["bets_imported"], 1)
        with Database(self.db).connect() as conn:
            bet = conn.execute("SELECT stake,ticket_id,strategy_version FROM user_bets").fetchone()
            self.assertEqual(tuple(bet), (20.0, "TEST-001", "selection-v1"))

        row["实际比分"] = actual
        completed_file = self.root / "completed.csv"
        self._write_rows(completed_file, [row], CHECKLIST_COLUMNS + ["策略版本", "下注金额", "票号"])
        completed = ChecklistImporter(self.db).import_file(completed_file, self.reports)
        self.assertEqual(completed["results_imported"], 1)
        self.assertEqual(completed["history"]["rows"], 1)
        final_path = completed["checklists"][0]
        validation = ChecklistValidator().validate_file(final_path)
        self.assertTrue(validation["valid"], validation["errors"])
        with open(final_path, encoding="utf-8-sig", newline="") as handle:
            final_row = next(csv.DictReader(handle))
        expected_type = classify_result_type({
            "odds_home": 1.8, "odds_draw": 3.6, "odds_away": 4.2,
            "odds_handicap_home": 2.8, "odds_handicap_draw": 3.5, "odds_handicap_away": 2.2,
        }, home, away, "-1")
        self.assertEqual(final_row["结果类型"], expected_type)
        self.assertEqual(final_row["AI 比分预测结果"], "1️⃣")
        self.assertEqual(final_row["胜平负预测结果"], "✅")
        self.assertEqual(final_row["比分预测结果"], "✅")

        with open(completed["history"]["csv"], encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            self.assertEqual(reader.fieldnames, HISTORY_COLUMNS)
            self.assertEqual(len(list(reader)), 1)

        StrategyEngine(self.db, self.reports, self.policy).generate("2099-10-25")
        with Database(self.db).connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM strategy_decisions").fetchone()[0], 2)
            self.assertEqual(conn.execute("SELECT COUNT(DISTINCT history_sha256) FROM strategy_decisions").fetchone()[0], 2)
        calibrated = StrategyValidator(self.db, self.reports, self.policy).generate("2099-10-26", update_policy=True)
        self.assertTrue(calibrated["policy_updated"])
        self.assertEqual(load_betting_policy(self.policy)["config"]["calibration"]["samples"], 1)
        StrategyEngine(self.db, self.reports, self.policy).generate("2099-10-25")
        with Database(self.db).connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM strategy_decisions").fetchone()[0], 3)
        checked = StrategyValidator(self.db, self.reports, self.policy).generate("2099-10-26")
        report = json.loads(Path(checked["json"]).read_text(encoding="utf-8"))
        self.assertTrue(all(item["status"] == "通过" for item in report["checks"]))

        wrong_type = dict(final_row)
        wrong_type["结果类型"] = "🔴大爆冷" if expected_type != "🔴大爆冷" else "🟢不爆冷"
        wrong_outcome = dict(final_row)
        wrong_outcome["胜平负预测结果"] = "❌"
        invalid_file = self.root / "invalid.csv"
        self._write_rows(invalid_file, [wrong_type, wrong_outcome], CHECKLIST_COLUMNS)
        invalid = ChecklistValidator().validate_file(invalid_file)
        self.assertFalse(invalid["valid"])
        self.assertGreaterEqual(invalid["error_count"], 2)

    def test_legacy_history_import_deduplicates_and_uses_handicap_fallback(self):
        db = self.root / "legacy-history.db"
        rows = [
            {"场次": "周三001", "联赛": "测试联赛", "对阵": "甲 VS 乙", "比赛时间": "2026/5/28 3:00",
             "让球": "0/-1", "主胜": "2.0", "平局": "3.0", "主负": "4.0", "让球主胜": "2.8",
             "让球平局": "3.4", "让球主负": "2.2", "预测比分_Kimi": "1:0/1:1", "实际比分": "1:0",
             "结果类型": "🟢不爆冷", "AI比分预测结果": "1️⃣"},
            {"场次": "周三999", "联赛": "测试联赛", "对阵": "甲 VS 乙", "比赛时间": "05-28 03:00",
             "让球": "0/-1", "主胜": "2.0", "平局": "3.0", "主负": "4.0", "让球主胜": "2.8",
             "让球平局": "3.4", "让球主负": "2.2", "预测比分_Kimi": "1:1/0:0", "实际比分": "1:1",
             "结果类型": "🟡小爆冷", "AI比分预测结果": "1️⃣"},
            {"场次": "周三002", "联赛": "测试联赛", "对阵": "丙 VS 丁", "比赛时间": "05-28 04:00",
             "让球": "0/+1", "主胜": "--", "平局": "--", "主负": "--", "让球主胜": "1.5",
             "让球平局": "3.0", "让球主负": "4.0", "预测比分_Kimi": "1:1/0:0", "实际比分": "0:0",
             "结果类型": "🟢不爆冷", "AI比分预测结果": "2️⃣"},
        ]
        source = self.root / "history.csv"
        self._write_rows(source, rows, HISTORY_COLUMNS)
        manager = HistoryManager(db)
        imported = manager.import_file(source)
        self.assertEqual(imported["unique_matches"], 2)
        self.assertEqual(imported["results_imported"], 2)
        self.assertEqual(imported["validation_warnings"], [])
        with Database(db).connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM predictions").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM legacy_score_predictions").fetchone()[0], 2)
            types = {row["actual_score"]: row["result_type"] for row in conn.execute("SELECT actual_score,result_type FROM results")}
            self.assertEqual(types["0:0"], "🟢不爆冷")
        exported = manager.export(self.root / "history-out.csv")
        self.assertEqual(exported["rows"], 2)
        with open(exported["csv"], encoding="utf-8-sig", newline="") as handle:
            output = list(csv.DictReader(handle))
        self.assertEqual(output[0]["预测比分_Kimi"], "1:1/0:0")
        self.assertEqual(output[0]["实际比分"], "1:1")

    def test_league_full_names_and_abbreviations_share_history(self):
        from selections import BIG_UPSET, NOT_UPSET, normalize_league
        from strategy_engine import summarize_history

        self.assertEqual(normalize_league("意大利甲级联赛"), "意甲")
        records = [
            {"league": "意甲", "scores": [], "actual_score": "1:0", "result_type": NOT_UPSET},
            {"league": "意大利甲级联赛", "scores": [], "actual_score": "0:1", "result_type": BIG_UPSET},
        ]
        summary = summarize_history(records)
        self.assertEqual(summary["overall"]["samples"], 2)
        self.assertEqual(summary["by_league"]["意甲"]["samples"], 2)

    @staticmethod
    def _write_rows(path: Path, rows: list[dict], columns: list[str]):
        with open(path, "w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)


if __name__ == "__main__":
    unittest.main()
