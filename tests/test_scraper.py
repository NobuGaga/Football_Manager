from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from database import Database
from scraper import SportteryScraper


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return FakeResponse(self.payload)


def match(business_date="2099-10-25"):
    return {
        "matchNumStr": "周日001",
        "businessDate": business_date,
        "matchDate": "2099-10-26",
        "matchTime": "02:45:00",
        "leagueAllName": "测试联赛",
        "homeTeamAllName": "主队A",
        "awayTeamAllName": "客队B",
        "had": {"h": "2.00", "d": "3.40", "a": "3.80"},
        "hhad": {"goalLine": "+1", "h": "1.90", "d": "3.50", "a": "3.60"},
    }


def payload(groups=None):
    return {
        "success": True,
        "errorCode": "0",
        "errorMessage": "处理成功",
        "value": {"matchInfoList": groups if groups is not None else [
            {"businessDate": "2099-10-24", "subMatchList": [match("2099-10-24")]},
            {"businessDate": "2099-10-25", "subMatchList": [match()]},
        ]},
    }


class SportteryScraperTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.db = self.root / "football.db"
        Database(self.db).initialize()

    def tearDown(self):
        self.temp.cleanup()

    def test_fetches_requested_business_date_and_saves_raw_snapshot(self):
        session = FakeSession(payload())
        frame = SportteryScraper(self.db, session=session).fetch_daily_matches("2099-10-25")
        self.assertEqual(len(frame), 1)
        self.assertEqual(frame.iloc[0]["match_code"], "周日001")
        self.assertEqual(session.calls[0][1]["params"], {"channel": "c"})
        self.assertEqual(session.calls[0][1]["headers"]["Referer"], SportteryScraper.page_url)
        self.assertEqual(session.calls[0][1]["timeout"], 30)
        with Database(self.db).connect() as conn:
            row = conn.execute("SELECT * FROM fixtures").fetchone()
            self.assertEqual(row["start_time_utc"], "2099-10-25T18:45:00+00:00")
            self.assertEqual(row["timezone"], "Asia/Shanghai")
            self.assertEqual(row["source"], "sporttery_web")
            self.assertEqual(json.loads(row["odds_json"])["odds_handicap_home"], 1.9)
        snapshots = list((self.root / "snapshots").glob("sporttery_*.json"))
        self.assertEqual(len(snapshots), 1)
        self.assertEqual(json.loads(snapshots[0].read_text(encoding="utf-8"))["errorCode"], "0")

    def test_empty_requested_date_fails_without_importing(self):
        scraper = SportteryScraper(self.db, session=FakeSession(payload(groups=[])))
        with self.assertRaisesRegex(RuntimeError, "营业日期 2099-10-25"):
            scraper.fetch_daily_matches("2099-10-25")
        with Database(self.db).connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM fixtures").fetchone()[0], 0)
        self.assertEqual(len(list((self.root / "snapshots").glob("sporttery_*.json"))), 1)

    def test_invalid_match_fails_before_any_import(self):
        invalid = match()
        invalid["homeTeamAllName"] = ""
        invalid["homeTeamAbbName"] = ""
        data = payload(groups=[{"businessDate": "2099-10-25", "subMatchList": [match(), invalid]}])
        scraper = SportteryScraper(self.db, session=FakeSession(data))
        with self.assertRaisesRegex(RuntimeError, "未导入任何记录"):
            scraper.fetch_daily_matches("2099-10-25")
        with Database(self.db).connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM fixtures").fetchone()[0], 0)

    def test_api_business_error_is_not_treated_as_empty_fixtures(self):
        data = {"success": False, "errorCode": "500", "errorMessage": "测试失败"}
        scraper = SportteryScraper(self.db, session=FakeSession(data))
        with self.assertRaisesRegex(RuntimeError, "测试失败"):
            scraper.fetch_daily_matches("2099-10-25")
        with Database(self.db).connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM fixtures").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
