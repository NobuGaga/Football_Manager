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
from importers import FixtureImporter


class AutomationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def test_local_daily_pipeline_generates_chinese_summary(self):
        db = self.root / "football.db"
        FixtureImporter(db).import_file(ROOT / "examples" / "fixtures.csv")
        status = DailyAutomation(ROOT, db, self.root / "reports", frontend_dir=self.root / "web" / "src").run(
            date="2099-10-25", results_date="2099-10-25",
            fixtures=str(ROOT / "examples" / "fixtures.csv"),
            results=str(ROOT / "examples" / "results.csv"),
        )
        self.assertEqual(status["results_imported"], 1)
        self.assertEqual(status["new_predictions"], 1)
        summary = (self.root / "reports" / "每日中文摘要_2099-10-25.md").read_text(encoding="utf-8")
        self.assertIn("昨日赛果回填：1 场", summary)
        self.assertIn("今日新增推断：1 条", summary)
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
        status = DailyAutomation(ROOT, db, self.root / "reports", frontend_dir=self.root / "web" / "src").run(
            date="2099-10-25", results_date="2099-10-24",
            fixtures=str(fixture_file), results=str(result_file),
        )
        self.assertEqual(status["results_imported"], 1)
        self.assertEqual(status["new_predictions"], 1)
        report = json.loads((self.root / "reports" / "足球经理的推断_2099-10-25.json").read_text(encoding="utf-8"))
        self.assertEqual(report["match_count"], 1)
        basic = report["matches"][0]["basic_info"]
        self.assertEqual(basic["business_date"], "2099-10-25")
        self.assertEqual(basic["match_date"], "2099-10-26")

    def test_successful_run_commits_and_pushes_only_data_products(self):
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
        self._git(work, "add", "-f", "data/football.db")
        self._git(work, "commit", "-m", "fixture")
        self._git(work, "push")

        status = DailyAutomation(work).run(
            date="2099-10-25", results_date="2099-10-25",
            fixtures=str(work / "examples" / "fixtures.csv"),
            results=str(work / "examples" / "results.csv"), pull=True, push=True,
        )
        self.assertTrue(status["pushed"])
        message = subprocess.check_output(["git", "--git-dir", str(remote), "log", "main", "-1", "--pretty=%s"], text=True).strip()
        self.assertEqual(message, "Daily football prediction: 2099-10-25")
        tracked = subprocess.check_output(["git", "-c", "core.quotepath=false", "--git-dir", str(remote), "ls-tree", "-r", "--name-only", "main"], text=True)
        self.assertIn("reports/每日中文摘要_2099-10-25.md", tracked)
        self.assertIn("data/football.db", tracked)
        self.assertIn("web/src/data/daily-report.js", tracked)

    @staticmethod
    def _git(cwd: Path, *args: str):
        subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True)


if __name__ == "__main__":
    unittest.main()
