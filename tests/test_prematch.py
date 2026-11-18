from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from database import Database
from importers import FixtureImporter
from predictor import FootballPredictor
from sporttery_features import SportteryFeatureFetcher


def api_payload(value):
    return {"success": True, "errorCode": "0", "errorMessage": "处理成功", "value": value}


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, payloads):
        self.payloads = payloads
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        for key, result in self.payloads.items():
            if key in url:
                if isinstance(result, Exception):
                    raise result
                return FakeResponse(result)
        raise AssertionError(f"未模拟的请求：{url}")


class SportteryFeatureFetcherTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "football.db"
        FixtureImporter(self.db).import_rows([{
            "match_code": "周日001", "business_date": "2099-10-25", "league": "测试联赛",
            "home_team": "主队A", "away_team": "客队B", "match_time": "2099-10-25 20:00",
            "timezone": "Asia/Shanghai", "odds_home": 2.0, "odds_draw": 3.3, "odds_away": 3.6,
        }])

    def tearDown(self):
        self.temp.cleanup()

    def payloads(self):
        return {
            "getMatchCalculatorV1": api_payload({"matchInfoList": [{
                "businessDate": "2099-10-25", "subMatchList": [{
                    "matchId": 2042830, "matchNumStr": "周日001",
                    "homeTeamAllName": "主队A", "awayTeamAllName": "客队B",
                }],
            }]}),
            "getMatchFeatureV1": api_payload({
                "goalAvg": {"homeGoalAvgCnt": "1.9", "awayGoalAvgCnt": "1.5", "totalLegCnt": 10},
                "lossGoalAvg": {"homeLossGoalAvgCnt": "1.0", "awayLossGoalAvgCnt": "0.9"},
            }),
            "getInjurySuspensionV1": api_payload({
                "home": {"teamShortName": "主队A", "injuriesAndSuspensionsList": [{
                    "personName": "球员甲", "playerPositionDesc": "前锋", "injuryFlag": 1, "suspensionFlag": 0,
                }]},
                "away": {"teamShortName": "客队B", "injuriesAndSuspensionsList": []},
            }),
            "getMatchResultV1": api_payload({
                "home": {"matchList": [
                    {"matchDate": "2099-10-25", "homeTeamShortName": "主队A", "awayTeamShortName": "未来队",
                     "fullCourtGoal": "9:0"},
                    {"matchDate": "2099-10-20", "homeTeamShortName": "主队A", "awayTeamShortName": "旧队甲",
                     "fullCourtGoal": "2:1"},
                ]},
                "away": {"matchList": [
                    {"matchDate": "2099-10-19", "homeTeamShortName": "旧队乙", "awayTeamShortName": "客队B",
                     "fullCourtGoal": "0:1"},
                ]},
            }),
            "getResultHistoryV1": api_payload({"matchList": [{
                "matchDate": "2099-01-01", "homeTeamShortName": "主队A", "awayTeamShortName": "客队B",
                "fullCourtGoal": "1:1",
            }]}),
            "getFutureMatchesV1": api_payload({"home": {"matchList": []}, "away": {"matchList": []}}),
        }

    def test_resolves_id_imports_official_features_and_uses_recent_goal_model(self):
        session = FakeSession(self.payloads())
        result = SportteryFeatureFetcher(self.db, session=session).fetch_daily_features("2099-10-25")
        self.assertEqual(result["imported"], 1)
        self.assertEqual(result["errors"], [])
        with Database(self.db).connect() as conn:
            fixture = conn.execute("SELECT * FROM fixtures").fetchone()
            self.assertEqual(fixture["external_provider"], "sporttery")
            self.assertEqual(fixture["external_fixture_id"], "2042830")
            row = conn.execute("SELECT * FROM match_features").fetchone()
            self.assertEqual(row["source"], "sporttery_official_prematch")
            self.assertAlmostEqual(row["completeness"], 0.45)
            features = json.loads(row["feature_set_json"])
        self.assertEqual(features["home_recent_goals_avg"], 1.9)
        self.assertEqual(features["rest_days_home"], 5)
        self.assertEqual(features["rest_days_away"], 6)
        self.assertIn("球员甲", features["injuries"])
        self.assertIn("近1场1胜0平0负", features["recent_form_home"])
        self.assertNotIn("9", features["recent_form_home"])
        self.assertIn("0胜1平0负", features["h2h"])
        self.assertIn("official_raw", features)

        prediction = FootballPredictor(self.db).predict_all(
            "2099-10-25", as_of="2099-10-24T00:00:00+00:00",
        )[0]
        self.assertEqual(prediction["model_version"], "poisson-dc-v2")
        self.assertEqual(prediction["analysis"]["method"], "recent_goal_averages")
        self.assertAlmostEqual(prediction["expected_home_goals"], 1.4)
        self.assertAlmostEqual(prediction["expected_away_goals"], 1.25)
        self.assertEqual(prediction["data_completeness"], 0.45)
        self.assertEqual(len(list((self.root / "snapshots").glob("sporttery_features_*.json"))), 2)

    def test_partial_endpoint_failure_keeps_other_official_features(self):
        payloads = self.payloads()
        payloads["getMatchFeatureV1"] = requests.ConnectionError("temporary outage")
        payloads["getMatchResultV1"] = api_payload({})
        payloads["getResultHistoryV1"] = api_payload({})
        payloads["getFutureMatchesV1"] = api_payload({})
        result = SportteryFeatureFetcher(self.db, session=FakeSession(payloads)).fetch_daily_features("2099-10-25")
        self.assertEqual(result["imported"], 1)
        self.assertEqual(len(result["errors"]), 1)
        self.assertEqual(result["errors"][0]["endpoint"], "feature")
        with Database(self.db).connect() as conn:
            row = conn.execute("SELECT * FROM match_features").fetchone()
            features = json.loads(row["feature_set_json"])
            self.assertIn("球员甲", features["injuries"])
            self.assertNotIn("home_recent_goals_avg", features)


if __name__ == "__main__":
    unittest.main()
