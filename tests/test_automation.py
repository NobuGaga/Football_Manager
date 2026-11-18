from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from automation import DailyAutomation
from database import Database
from importers import FixtureImporter, ResultImporter
from predictor import FootballPredictor


class AutomationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    @staticmethod
    def _fixture(code: str, date: str, home: str, away: str) -> dict:
        return {
            "match_code": code, "business_date": date, "league": "测试联赛",
            "home_team": home, "away_team": away, "match_time": f"{date} 20:00",
            "timezone": "Asia/Shanghai", "odds_home": 2.0, "odds_draw": 3.3, "odds_away": 3.6,
        }

    def _write_result(self, name: str, code: str, date: str, home: str, away: str) -> Path:
        path = self.root / name
        path.write_text(json.dumps([{
            "match_code": code, "business_date": date, "home_team": home, "away_team": away,
            "actual_score": "2:1",
        }], ensure_ascii=False), encoding="utf-8")
        return path

    def test_require_results_rejects_partial_coverage_for_results_date(self):
        db = self.root / "football.db"
        FixtureImporter(db).import_rows([
            self._fixture("周日001", "2099-10-25", "主队A", "客队A"),
            self._fixture("周日002", "2099-10-25", "主队B", "客队B"),
        ])
        result_file = self._write_result(
            "partial.json", "周日001", "2099-10-25", "主队A", "客队A",
        )
        with self.assertRaisesRegex(RuntimeError, r"1/2.*周日002") as raised:
            DailyAutomation(ROOT, db, self.root / "reports", frontend_dir=self.root / "web").run(
                date="2099-10-26", results_date="2099-10-25",
                fixtures=str(ROOT / "examples" / "fixtures.csv"), results=str(result_file),
                require_results=True,
            )
        self.assertIn("2099-10-25", str(raised.exception))
        self.assertFalse((self.root / "reports" / "每日运行状态_2099-10-26.json").exists())

    def test_require_results_rejects_results_imported_for_another_date(self):
        db = self.root / "football.db"
        FixtureImporter(db).import_rows([
            self._fixture("周日001", "2099-10-25", "目标主队", "目标客队"),
            self._fixture("周六001", "2099-10-24", "旧主队", "旧客队"),
        ])
        result_file = self._write_result(
            "wrong-date.json", "周六001", "2099-10-24", "旧主队", "旧客队",
        )
        with self.assertRaisesRegex(RuntimeError, r"0/1.*周日001") as raised:
            DailyAutomation(ROOT, db, self.root / "reports", frontend_dir=self.root / "web").run(
                date="2099-10-26", results_date="2099-10-25",
                fixtures=str(ROOT / "examples" / "fixtures.csv"), results=str(result_file),
                require_results=True,
            )
        self.assertIn("2099-10-25", str(raised.exception))

    def test_require_results_accepts_complete_database_without_new_file(self):
        db = self.root / "football.db"
        previous = self._fixture("周日001", "2099-10-25", "主队A", "客队A")
        FixtureImporter(db).import_rows([previous])
        FootballPredictor(db).predict_all("2099-10-25", as_of="2099-10-24T00:00:00+00:00")
        ResultImporter(db).import_rows([{**previous, "actual_score": "2:1"}])
        today_file = self.root / "today.json"
        today_file.write_text(json.dumps([
            self._fixture("周一001", "2099-10-26", "今日主队", "今日客队"),
        ], ensure_ascii=False), encoding="utf-8")

        status = DailyAutomation(ROOT, db, self.root / "reports", frontend_dir=self.root / "web").run(
            date="2099-10-26", results_date="2099-10-25", fixtures=str(today_file),
            require_results=True,
        )
        self.assertEqual(status["results_imported"], 0)
        self.assertEqual(status["results_coverage"]["expected"], 1)
        self.assertEqual(status["results_coverage"]["covered"], 1)
        self.assertTrue(status["results_coverage"]["complete"])
        self.assertEqual(len(status["checked_checklists"]), 1)
        summary = (self.root / "reports" / "每日中文摘要_2099-10-26.md").read_text(encoding="utf-8")
        self.assertIn("昨日赛果覆盖：1/1 场（完整）", summary)

    def test_manual_fixtures_from_wrong_business_date_abort_before_reports(self):
        db = self.root / "football.db"
        wrong_file = self.root / "wrong-date-fixtures.json"
        wrong_file.write_text(json.dumps([
            self._fixture("周四001", "2099-10-23", "错日期主队", "错日期客队"),
        ], ensure_ascii=False), encoding="utf-8")
        reports = self.root / "reports"

        with self.assertRaisesRegex(
                RuntimeError, r"2099-10-25.*没有有效官方对阵.*2099-10-23") as raised:
            DailyAutomation(ROOT, db, reports, frontend_dir=self.root / "web").run(
                date="2099-10-25", results_date="2099-10-24", fixtures=str(wrong_file),
            )

        self.assertIn("已停止推断和推送", str(raised.exception))
        self.assertFalse((reports / "每日运行状态_2099-10-25.json").exists())
        self.assertFalse(list(reports.glob("足球经理的推断_2099-10-25.*")))
        self.assertFalse((self.root / "web" / "generated" / "daily-report.js").exists())
        with Database(db).connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM predictions").fetchone()[0], 0)

    def test_mixed_business_date_file_counts_only_target_active_fixtures(self):
        db = self.root / "football.db"
        cancelled = self._fixture("周一002", "2099-10-25", "取消主队", "取消客队")
        cancelled["match_status"] = "cancelled"
        fixture_file = self.root / "mixed-date-fixtures.json"
        fixture_file.write_text(json.dumps([
            self._fixture("周一001", "2099-10-25", "目标主队", "目标客队"),
            cancelled,
            self._fixture("周日001", "2099-10-24", "昨日主队", "昨日客队"),
        ], ensure_ascii=False), encoding="utf-8")

        status = DailyAutomation(ROOT, db, self.root / "reports", frontend_dir=self.root / "web").run(
            date="2099-10-25", results_date="2099-10-24", fixtures=str(fixture_file),
        )

        self.assertEqual(status["fixtures"], 1)
        self.assertEqual(status["new_predictions"], 1)
        self.assertTrue(any(
            "非目标营业日" in warning and "2099-10-24" in warning
            for warning in status["warnings"]
        ))
        report = json.loads((self.root / "reports" / "足球经理的推断_2099-10-25.json").read_text(encoding="utf-8"))
        self.assertEqual(report["match_count"], 1)
        self.assertEqual(report["matches"][0]["basic_info"]["home_team"], "目标主队")
        mini_data = Path(status["miniprogram_data"]).read_text(encoding="utf-8")
        self.assertIn('"matchCount": 1', mini_data)
        self.assertNotIn("昨日主队", mini_data)

    def test_local_daily_pipeline_generates_chinese_summary(self):
        db = self.root / "football.db"
        FixtureImporter(db).import_file(ROOT / "examples" / "fixtures.csv")
        status = DailyAutomation(ROOT, db, self.root / "reports", frontend_dir=self.root / "web").run(
            date="2099-10-25", results_date="2099-10-25",
            fixtures=str(ROOT / "examples" / "fixtures.csv"),
            results=str(ROOT / "examples" / "results.csv"),
            features=str(ROOT / "examples" / "features.csv"),
        )
        self.assertEqual(status["results_imported"], 1)
        self.assertEqual(status["new_predictions"], 1)
        self.assertEqual(status["features"]["manual_imported"], 1)
        self.assertEqual(status["features"]["official_imported"], 0)
        summary = (self.root / "reports" / "每日中文摘要_2099-10-25.md").read_text(encoding="utf-8")
        self.assertIn("昨日赛果回填：1 场", summary)
        self.assertIn("今日新增推断：1 条", summary)
        self.assertIn("赛前特征：官方新增 0 场；本地文件导入 1 场", summary)
        self.assertTrue((self.root / "reports" / "回测结果_2099-10-25.json").exists())
        mini_data = Path(status["miniprogram_data"]).read_text(encoding="utf-8")
        self.assertIn('"reportDate": "2099-10-25"', mini_data)
        self.assertIn('"matchCount": 1', mini_data)
        self.assertNotIn("score_probabilities", mini_data)

    def test_cross_day_business_date_drives_whole_daily_pipeline(self):
        db = self.root / "football.db"
        fixture_file = self.root / "cross_day_fixture.json"
        fixture_file.write_text(json.dumps([{
            "比赛编号": "周日001", "比赛日期": "2099-10-25", "联赛": "测试联赛",
            "主队": "主队A", "客队": "客队B", "开赛时间": "2099-10-26 02:45",
            "主胜": 2.0, "平局": 3.4, "客胜": 3.8,
        }], ensure_ascii=False), encoding="utf-8")
        FixtureImporter(db).import_file(fixture_file)
        result_file = self.root / "cross_day_result.json"
        result_file.write_text(json.dumps([{
            "比赛编号": "周日001", "比赛日期": "2099-10-25", "主队": "主队A", "客队": "客队B",
            "实际比分": "2:1",
        }], ensure_ascii=False), encoding="utf-8")

        class FakeFeatureFetcher:
            def __init__(self):
                self.dates = []

            def fetch_daily_features(self, date):
                self.dates.append(date)
                return {"imported": 1, "skipped": 0, "warnings": [], "errors": []}

        fetcher = FakeFeatureFetcher()
        status = DailyAutomation(ROOT, db, self.root / "reports", frontend_dir=self.root / "web",
                                 feature_fetcher=fetcher).run(
            date="2099-10-25", results_date="2099-10-24",
            fixtures=str(fixture_file), results=str(result_file), official_features=True,
        )
        self.assertEqual(status["results_imported"], 1)
        self.assertEqual(status["new_predictions"], 1)
        self.assertEqual(status["features"]["official_imported"], 1)
        self.assertEqual(fetcher.dates, ["2099-10-25"])
        report = json.loads((self.root / "reports" / "足球经理的推断_2099-10-25.json").read_text(encoding="utf-8"))
        self.assertEqual(report["match_count"], 1)
        basic = report["matches"][0]["basic_info"]
        self.assertEqual(basic["business_date"], "2099-10-25")
        self.assertEqual(basic["match_date"], "2099-10-26")

    def test_successful_run_keeps_data_products_local_without_commit_or_push(self):
        work = self.root / "work"
        remote = self.root / "remote.git"
        shutil.copytree(ROOT, work, ignore=shutil.ignore_patterns(
            ".git", "data", "reports", "__pycache__", "*.pyc",
            "node_modules", "dist", ".tools", ".chrome-inspect", ".chrome-validate",
        ))
        (work / "data").mkdir()
        self._git(work, "init", "-b", "main")
        self._git(work, "config", "user.name", "Test")
        self._git(work, "config", "user.email", "test@example.com")
        self._git(work, "add", ".")
        self._git(work, "commit", "-m", "initial")
        self._git(self.root, "init", "--bare", str(remote))
        self._git(work, "remote", "add", "origin", str(remote))
        self._git(work, "push", "-u", "origin", "main")
        FixtureImporter(work / "data" / "football.db").import_file(work / "examples" / "fixtures.csv")
        remote_before = subprocess.check_output(
            ["git", "--git-dir", str(remote), "rev-parse", "main"], text=True).strip()
        local_before = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=work, text=True).strip()

        status = DailyAutomation(work).run(
            date="2099-10-25", results_date="2099-10-25",
            fixtures=str(work / "examples" / "fixtures.csv"),
            results=str(work / "examples" / "results.csv"), pull=True, push=True,
        )
        self.assertTrue(status["local_products_only"])
        self.assertFalse(status["pushed"])
        self.assertIsNone(status["commit"])
        remote_after = subprocess.check_output(
            ["git", "--git-dir", str(remote), "rev-parse", "main"], text=True).strip()
        local_after = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=work, text=True).strip()
        self.assertEqual(remote_after, remote_before)
        self.assertEqual(local_after, local_before)
        tracked = subprocess.check_output(["git", "-c", "core.quotepath=false", "--git-dir", str(remote), "ls-tree", "-r", "--name-only", "main"], text=True)
        self.assertNotIn("reports/", tracked)
        self.assertNotIn("data/football.db", tracked)
        self.assertNotIn("web/generated/daily-report.js", tracked)
        self.assertTrue((work / "data" / "football.db").exists())
        self.assertTrue((work / "reports" / "每日中文摘要_2099-10-25.md").exists())
        self.assertTrue((work / "web" / "generated" / "daily-report.js").exists())
        summary = (work / "reports" / "每日中文摘要_2099-10-25.md").read_text(encoding="utf-8")
        self.assertIn("本地关键文件：仅保存本地，未进入 Git 跟踪或暂存", summary)

        self._git(work, "add", "-f", "data/football.db")
        with self.assertRaisesRegex(RuntimeError, "本地关键文件"):
            DailyAutomation(work).git.assert_local_products_untracked()

    @staticmethod
    def _git(cwd: Path, *args: str):
        subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True)


if __name__ == "__main__":
    unittest.main()
