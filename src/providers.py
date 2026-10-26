"""API-Football 可选赛前数据适配器；官方体彩对阵始终是分析范围来源。"""
from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from database import Database
from utils import normalize_name, utc_now


class APIFootballClient:
    BASE_URL = "https://v3.football.api-sports.io"

    def __init__(self, api_key: str | None = None, cache_dir: str | Path = "data/cache"):
        self.api_key = api_key or os.getenv("API_FOOTBALL_KEY")
        if not self.api_key:
            raise RuntimeError("请通过环境变量 API_FOOTBALL_KEY 提供 API-Football 密钥")
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def get(self, endpoint: str, **params) -> list[dict]:
        import requests

        cache_key = hashlib.sha256(json.dumps([endpoint, params], sort_keys=True).encode()).hexdigest()
        cache_path = self.cache_dir / f"{cache_key}.json"
        if cache_path.exists():
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            fetched_at = datetime.fromisoformat(cached["fetched_at"])
            if (datetime.now(timezone.utc) - fetched_at).total_seconds() <= 15 * 60:
                return cached["response"]
        response = requests.get(f"{self.BASE_URL}/{endpoint.lstrip('/')}", params=params,
                                headers={"x-apisports-key": self.api_key}, timeout=30)
        response.raise_for_status()
        payload = response.json()
        if payload.get("errors"):
            raise RuntimeError(f"API-Football 返回错误：{payload['errors']}")
        cache_path.write_text(json.dumps({"fetched_at": utc_now(), "response": payload.get("response", [])},
                                        ensure_ascii=False), encoding="utf-8")
        return payload.get("response", [])


class APIFootballEnricher:
    def __init__(self, db_path: str | Path = "data/football.db", client: APIFootballClient | None = None):
        self.db = Database(db_path)
        self.db.initialize()
        self.client = client or APIFootballClient(cache_dir=self.db.path.parent / "cache")

    def enrich(self, date: str) -> dict:
        match_day = datetime.strptime(date, "%Y-%m-%d").date()
        external = []
        for offset in (-1, 0, 1):
            external.extend(self.client.get("fixtures", date=(match_day + timedelta(days=offset)).isoformat(), timezone="UTC"))
        enriched, queued, errors = 0, 0, []
        with self.db.connect() as conn:
            fixtures = conn.execute("SELECT * FROM fixtures WHERE match_date=?", (date,)).fetchall()
            for fixture in fixtures:
                try:
                    matched = self._match_fixture(conn, fixture, external)
                    if not matched:
                        self._queue(conn, fixture["fixture_key"], "外部比赛无法唯一自动匹配", external[:0])
                        queued += 1
                        continue
                    external_id = str(matched["fixture"]["id"])
                    conn.execute("""UPDATE fixtures SET external_provider='api-football',external_fixture_id=?,updated_at=?
                        WHERE fixture_key=?""", (external_id, utc_now(), fixture["fixture_key"]))
                    conn.execute("""UPDATE match_review_queue SET status='resolved',resolved_at=?
                        WHERE fixture_key=? AND reason='外部比赛无法唯一自动匹配' AND status='open'""",
                        (utc_now(), fixture["fixture_key"]))
                    feature_set = self._build_features(matched)
                    as_of = utc_now()
                    missing = ["home_xg", "home_xga", "away_xg", "away_xga", "expected_home_goals", "expected_away_goals"]
                    completeness = round((16 - len(missing)) / 16, 4)
                    conn.execute("""INSERT INTO match_features(
                        fixture_key,as_of,source,feature_set_json,completeness,missing_fields_json,created_at
                    ) VALUES(?,?,?,?,?,?,?)""", (
                        fixture["fixture_key"], as_of, "api-football", json.dumps(feature_set, ensure_ascii=False),
                        completeness, json.dumps(missing), utc_now(),
                    ))
                    enriched += 1
                except Exception as exc:
                    errors.append({"match_code": fixture["match_code"], "error": str(exc)})
        return {"enriched": enriched, "manual_review": queued, "errors": errors}

    def sync_results(self, date: str) -> dict:
        """回填已结束比赛；未完结和无法唯一匹配的记录不会被伪造成赛果。"""
        from importers import ResultImporter

        match_day = datetime.strptime(date, "%Y-%m-%d").date()
        external = []
        for offset in (-1, 0, 1):
            external.extend(self.client.get("fixtures", date=(match_day + timedelta(days=offset)).isoformat(), timezone="UTC"))
        rows, unmatched, pending, errors = [], [], 0, []
        with self.db.connect() as conn:
            fixtures = conn.execute("SELECT * FROM fixtures WHERE match_date=?", (date,)).fetchall()
            for fixture in fixtures:
                try:
                    matched = self._match_fixture(conn, fixture, external)
                    if not matched:
                        unmatched.append(fixture["match_code"])
                        self._queue(conn, fixture["fixture_key"], "外部比赛无法唯一自动匹配", [])
                        continue
                    status = matched["fixture"]["status"]["short"]
                    home = matched["goals"]["home"]
                    away = matched["goals"]["away"]
                    if status not in {"FT", "AET", "PEN"} or home is None or away is None:
                        pending += 1
                        continue
                    rows.append({"fixture_key": fixture["fixture_key"], "actual_score": f"{home}:{away}"})
                except (KeyError, TypeError, ValueError) as exc:
                    errors.append({"match_code": fixture["match_code"], "error": str(exc)})
        imported = ResultImporter(self.db.path).import_rows(rows, source="api-football")
        return {"date": date, "fixtures": len(fixtures), "imported": imported["imported"],
                "pending": pending, "unmatched": unmatched,
                "errors": errors + imported["errors"]}

    def _match_fixture(self, conn, fixture, candidates: list[dict]) -> dict | None:
        if fixture["external_provider"] == "api-football" and fixture["external_fixture_id"]:
            return next((item for item in candidates if str(item["fixture"]["id"]) == fixture["external_fixture_id"]), None)
        aliases = self._aliases(conn)
        possible = []
        for item in candidates:
            start = item["fixture"]["date"].replace("Z", "+00:00")
            delta = abs((datetime.fromisoformat(start) - datetime.fromisoformat(fixture["start_time_utc"])).total_seconds())
            if delta > 2 * 3600:
                continue
            home = normalize_name(item["teams"]["home"]["name"])
            away = normalize_name(item["teams"]["away"]["name"])
            expected_home = aliases.get((normalize_name(fixture["home_team"]), fixture["league"]),
                                        aliases.get((normalize_name(fixture["home_team"]), "*"), set()))
            expected_away = aliases.get((normalize_name(fixture["away_team"]), fixture["league"]),
                                        aliases.get((normalize_name(fixture["away_team"]), "*"), set()))
            if home in expected_home and away in expected_away:
                possible.append(item)
        return possible[0] if len(possible) == 1 else None

    @staticmethod
    def _aliases(conn) -> dict[tuple[str, str], set[str]]:
        result: dict[tuple[str, str], set[str]] = {}
        for row in conn.execute("SELECT * FROM team_aliases WHERE review_status='confirmed'"):
            values = {normalize_name(row["standard_name"]), normalize_name(row["english_name"])}
            values.discard("")
            result.setdefault((row["normalized_alias"], row["league"]), set()).update(values)
        return result

    def _build_features(self, matched: dict) -> dict:
        home_id = matched["teams"]["home"]["id"]
        away_id = matched["teams"]["away"]["id"]
        start = datetime.fromisoformat(matched["fixture"]["date"].replace("Z", "+00:00"))
        home_recent = self.client.get("fixtures", team=home_id, last=10)
        away_recent = self.client.get("fixtures", team=away_id, last=10)
        injuries = self.client.get("injuries", fixture=matched["fixture"]["id"])
        lineups = self.client.get("fixtures/lineups", fixture=matched["fixture"]["id"])
        h2h = self.client.get("fixtures", h2h=f"{home_id}-{away_id}", last=5)
        home_stats = self._recent_stats(home_recent, home_id, True)
        away_stats = self._recent_stats(away_recent, away_id, False)
        return {
            "home_attack": home_stats["attack"], "home_defense": home_stats["defense"],
            "away_attack": away_stats["attack"], "away_defense": away_stats["defense"],
            "recent_form": {"home": home_stats["form"], "away": away_stats["form"],
                            "home_sample": home_stats["sample"], "away_sample": away_stats["sample"]},
            "injuries": injuries, "lineups": lineups, "h2h": h2h,
            "rest_days_home": self._rest_days(home_recent, start),
            "rest_days_away": self._rest_days(away_recent, start),
            "external_fixture_id": matched["fixture"]["id"],
        }

    @staticmethod
    def _recent_stats(matches: list[dict], team_id: int, use_home: bool) -> dict:
        selected = [item for item in matches if item["teams"]["home" if use_home else "away"]["id"] == team_id]
        if len(selected) < 3:
            selected = matches
        scored, conceded, form = [], [], []
        for item in selected:
            is_home = item["teams"]["home"]["id"] == team_id
            goals_for = item["goals"]["home" if is_home else "away"]
            goals_against = item["goals"]["away" if is_home else "home"]
            if goals_for is None or goals_against is None:
                continue
            scored.append(float(goals_for))
            conceded.append(float(goals_against))
            form.append("W" if goals_for > goals_against else "D" if goals_for == goals_against else "L")
        count = max(1, len(scored))
        return {"attack": (sum(scored) / count) / 1.4 if scored else 1.0,
                "defense": (sum(conceded) / count) / 1.2 if conceded else 1.0,
                "form": "".join(form), "sample": len(scored)}

    @staticmethod
    def _rest_days(matches: list[dict], start: datetime) -> float | None:
        dates = [parsed for item in matches
                 if (parsed := datetime.fromisoformat(item["fixture"]["date"].replace("Z", "+00:00"))) < start]
        return round((start - max(dates)).total_seconds() / 86400, 2) if dates else None

    @staticmethod
    def _queue(conn, key: str, reason: str, candidates: list) -> None:
        if not conn.execute("SELECT 1 FROM match_review_queue WHERE fixture_key=? AND reason=? AND status='open'",
                            (key, reason)).fetchone():
            conn.execute("INSERT INTO match_review_queue(fixture_key,reason,candidates_json,created_at) VALUES(?,?,?,?)",
                         (key, reason, json.dumps(candidates, ensure_ascii=False), utc_now()))
