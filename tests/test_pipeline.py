from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from backtest import Backtester
from database import Database
from importers import FeatureImporter, FixtureImporter, ResultImporter
from predictor import FootballPredictor
from reporter import ReportGenerator
from utils import normalize_score, parse_datetime


class PipelineTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "football.db"
        self.fixture_file = self.root / "fixtures.json"
        self.fixture_file.write_text(json.dumps([{
            "竞彩编号": "周日001", "联赛": "测试联赛", "主队": "主队A", "客队": "客队B",
            "开赛时间": "2099-10-25 18:00", "让球": "0", "主胜": 2.0, "平局": 3.4, "客胜": 3.8,
        }], ensure_ascii=False), encoding="utf-8")
        FixtureImporter(self.db).import_file(self.fixture_file)

    def tearDown(self):
        self.temp.cleanup()

    def test_snapshot_is_immutable_and_duplicate_is_not_inserted(self):
        result = FixtureImporter(self.db).import_file(self.fixture_file)
        self.assertEqual(result["unchanged"], 1)
        with Database(self.db).connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM fixtures").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM fixture_snapshots").fetchone()[0], 1)

    def test_cross_day_postponement_reuses_fixture_and_keeps_snapshots(self):
        row = json.loads(self.fixture_file.read_text(encoding="utf-8"))[0]
        row["比赛日期"] = "2099-10-26"
        row["开赛时间"] = "2099-10-26 02:00"
        row["比赛状态"] = "postponed"
        postponed = self.root / "postponed.json"
        postponed.write_text(json.dumps([row], ensure_ascii=False), encoding="utf-8")
        FixtureImporter(self.db).import_file(postponed)
        with Database(self.db).connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM fixtures").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT match_date FROM fixtures").fetchone()[0], "2099-10-26")
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM fixture_snapshots").fetchone()[0], 2)

    def test_prediction_probabilities_and_refresh_versions(self):
        predictor = FootballPredictor(self.db)
        predictions = predictor.predict_all(as_of="2099-10-24T08:00:00+00:00")
        self.assertEqual(len(predictions), 1)
        prediction = predictions[0]
        self.assertAlmostEqual(prediction["probability_home"] + prediction["probability_draw"] +
                               prediction["probability_away"], 1.0, places=7)
        self.assertEqual(len(prediction["top_scores"]), 5)
        self.assertEqual(predictor.predict_all(as_of="2099-10-24T08:00:00+00:00"), [])
        predictor.predict_all(refresh=True, as_of="2099-10-24T09:00:00+00:00")
        with Database(self.db).connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM predictions").fetchone()[0], 2)

    def test_future_features_are_excluded(self):
        feature_file = self.root / "features.json"
        feature_file.write_text(json.dumps([{
            "fixture_key": self._key(), "as_of": "2099-10-25T09:30:00+00:00",
            "expected_home_goals": 3.0, "expected_away_goals": 0.2,
        }]), encoding="utf-8")
        FeatureImporter(self.db).import_file(feature_file)
        prediction = FootballPredictor(self.db).predict_all(as_of="2099-10-25T09:00:00+00:00")[0]
        self.assertEqual(prediction["analysis"]["method"], "odds_only_fallback")

    def test_explicit_features_results_report_and_backtest(self):
        feature_file = self.root / "features.json"
        feature_file.write_text(json.dumps([{
            "fixture_key": self._key(), "as_of": "2099-10-24T07:00:00+00:00",
            "expected_home_goals": 1.8, "expected_away_goals": 0.9,
            "injuries": [], "lineups": [], "recent_form": {"home": "WWDL"},
        }], ensure_ascii=False), encoding="utf-8")
        FeatureImporter(self.db).import_file(feature_file)
        prediction = FootballPredictor(self.db).predict_all(as_of="2099-10-24T08:00:00+00:00")[0]
        self.assertEqual(prediction["analysis"]["method"], "explicit_expected_goals")
        paths = ReportGenerator(self.db, self.root / "reports").generate_daily_report("2099-10-25")
        for path in paths.values():
            self.assertTrue(Path(path).exists())
        report = json.loads(Path(paths["json"]).read_text(encoding="utf-8"))
        self.assertEqual(report["match_count"], 1)
        self.assertIn("post_match", report["matches"][0])
        result_file = self.root / "results.json"
        result_file.write_text(json.dumps([{
            "fixture_key": self._key(), "实际比分": "01:00", "复盘备注": "测试复盘",
        }]), encoding="utf-8")
        ResultImporter(self.db).import_file(result_file)
        metrics = Backtester(self.db).evaluate("poisson-dc-v1")
        self.assertEqual(metrics["eligible_predictions"], 1)
        self.assertIn("log_loss", metrics["metrics"]["测试联赛"])
        self.assertEqual(len(metrics["calibration"]), 10)

    def test_legacy_matches_are_migrated_without_inventing_probabilities(self):
        legacy = self.root / "legacy.db"
        conn = sqlite3.connect(legacy)
        conn.execute("CREATE TABLE matches(id INTEGER PRIMARY KEY,match_code TEXT,league TEXT,home_team TEXT,away_team TEXT,match_time DATETIME,handicap TEXT,odds_home REAL,odds_draw REAL,odds_away REAL,odds_handicap_home REAL,odds_handicap_draw REAL,odds_handicap_away REAL,prediction_1 TEXT,prediction_2 TEXT,prediction_3 TEXT,actual_result TEXT,match_status TEXT,is_upset INTEGER,hit_prediction INTEGER,created_at TEXT,updated_at TEXT)")
        conn.execute("INSERT INTO matches VALUES(1,'旧001','旧联赛','甲','乙','2026-01-01 10:00:00','0',2,3,4,0,0,0,'1:0','1:1','0:0','1:0','finished',0,1,'2026-01-01','2026-01-01')")
        conn.commit()
        conn.close()
        Database(legacy).initialize()
        with Database(legacy).connect() as migrated:
            self.assertEqual(migrated.execute("SELECT COUNT(*) FROM fixtures").fetchone()[0], 1)
            self.assertEqual(migrated.execute("SELECT COUNT(*) FROM results").fetchone()[0], 1)
            self.assertEqual(migrated.execute("SELECT COUNT(*) FROM predictions").fetchone()[0], 0)

    def _key(self):
        with Database(self.db).connect() as conn:
            return conn.execute("SELECT fixture_key FROM fixtures").fetchone()[0]


class ParsingTest(unittest.TestCase):
    def test_timezone_and_score(self):
        value, _ = parse_datetime("2099-10-25 18:00", tz_name="Asia/Shanghai")
        self.assertEqual(value, "2099-10-25T10:00:00+00:00")
        self.assertEqual(normalize_score("01:02"), "1:2")


if __name__ == "__main__":
    unittest.main()
