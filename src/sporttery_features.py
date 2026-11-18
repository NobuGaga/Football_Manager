"""竞彩网官方公开赛前数据：近期攻防、伤停、交锋与赛程。

只读取页面公开使用的接口，不处理登录、验证码或访问限制。场均进失球是历史
统计，不作为 xG；所有原始响应同时保存到本地快照，便于审计和复现。
"""
from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

from database import Database
from importers import FeatureImporter
from scraper import SportteryScraper
from utils import clean_text, normalize_name, normalize_score, optional_float, utc_now


class SportteryFeatureFetcher:
    base_url = "https://webapi.sporttery.cn/gateway/uniform/football"
    page_url = "https://m.sporttery.cn/zqlszl/bssj/"
    endpoints = {
        "feature": "getMatchFeatureV1.qry",
        "injuries": "getInjurySuspensionV1.qry",
        "results": "getMatchResultV1.qry",
        "h2h": "getResultHistoryV1.qry",
        "future": "getFutureMatchesV1.qry",
    }
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "User-Agent": SportteryScraper.headers["User-Agent"],
    }

    def __init__(self, db_path: str | Path = "data/football.db", timeout: float = 30, session=None):
        self.db_path = Path(db_path)
        self.db = Database(self.db_path)
        self.db.initialize()
        self.timeout = timeout
        self.session = session or requests.Session()

    def fetch_daily_features(self, date: str) -> dict:
        """抓取指定营业日的赛前特征；单场比赛失败不阻断其他比赛。"""
        with self.db.connect() as conn:
            fixtures = [dict(row) for row in conn.execute(
                """SELECT * FROM fixtures
                WHERE business_date=? AND match_status NOT IN ('cancelled','postponed','finished')
                ORDER BY start_time_utc,match_code""", (date,),
            ).fetchall()]
        match_ids, resolve_warnings = self._resolve_match_ids(fixtures, date)
        imported, skipped = 0, 0
        warnings = list(resolve_warnings)
        errors: list[dict] = []
        snapshots: list[str] = []
        for fixture in fixtures:
            label = f"{fixture['match_code']} {fixture['home_team']} VS {fixture['away_team']}"
            if fixture["start_time_utc"] <= utc_now():
                skipped += 1
                warnings.append(f"{label} 已开赛，未抓取赛后时点赛前特征")
                continue
            match_id = match_ids.get(fixture["fixture_key"])
            if not match_id:
                skipped += 1
                warnings.append(f"{label} 缺少可唯一确认的竞彩网比赛 ID，未抓取官方赛前特征")
                continue
            values, raw, fetch_errors = self._fetch_match(match_id)
            fetched_at = utc_now()
            features = self._build_features(fixture, match_id, values, fetched_at)
            snapshot = self._save_snapshot(date, match_id, {
                "fixture_key": fixture["fixture_key"],
                "sporttery_match_id": match_id,
                "fetched_at": fetched_at,
                "responses": raw,
                "errors": fetch_errors,
            })
            snapshots.append(str(snapshot))
            for error in fetch_errors:
                errors.append({"fixture_key": fixture["fixture_key"], "match": label, **error})
            if not any(value not in ({}, [], None) for key, value in features.items()
                       if key not in {"official_match_id", "official_source_url", "official_fetched_at"}):
                skipped += 1
                warnings.append(f"{label} 官方接口未返回可用赛前字段；原始快照：{snapshot}")
                continue
            features["official_raw"] = values
            result = FeatureImporter(self.db_path).import_rows(
                [{"fixture_key": fixture["fixture_key"], "as_of": fetched_at, **features}],
                source="sporttery_official_prematch",
            )
            if result["errors"]:
                errors.extend({"fixture_key": fixture["fixture_key"], "match": label, **error}
                              for error in result["errors"])
            else:
                imported += 1
        return {"imported": imported, "skipped": skipped, "warnings": warnings,
                "errors": errors, "snapshots": snapshots}

    def _resolve_match_ids(self, fixtures: list[dict], date: str) -> tuple[dict[str, str], list[str]]:
        ids, missing = {}, []
        for fixture in fixtures:
            provider = clean_text(fixture.get("external_provider")).lower()
            external_id = clean_text(fixture.get("external_fixture_id"))
            if external_id.isdigit() and provider in {"", "sporttery"}:
                ids[fixture["fixture_key"]] = external_id
            else:
                missing.append(fixture)
        if not missing:
            return ids, []
        try:
            payload = self._get(SportteryScraper.api_url, {"channel": "c"}, SportteryScraper.page_url)
            self._save_snapshot(date, "index", payload)
        except RuntimeError as exc:
            return ids, [f"竞彩网比赛 ID 索引获取失败：{exc}"]
        groups = (payload.get("value") or {}).get("matchInfoList") or []
        candidates = []
        for group in groups:
            if not isinstance(group, dict) or clean_text(group.get("businessDate")) != date:
                continue
            for item in group.get("subMatchList") or []:
                if isinstance(item, dict) and clean_text(item.get("matchId")).isdigit():
                    candidates.append(item)
        warnings = []
        with self.db.connect() as conn:
            for fixture in missing:
                matches = [item for item in candidates if (
                    clean_text(item.get("matchNumStr")) == fixture["match_code"]
                    and normalize_name(item.get("homeTeamAllName") or item.get("homeTeamAbbName"))
                    == normalize_name(fixture["home_team"])
                    and normalize_name(item.get("awayTeamAllName") or item.get("awayTeamAbbName"))
                    == normalize_name(fixture["away_team"])
                )]
                if len(matches) != 1:
                    warnings.append(
                        f"{fixture['match_code']} 官方比赛 ID 候选 {len(matches)} 个，不作猜测关联"
                    )
                    continue
                match_id = clean_text(matches[0].get("matchId"))
                try:
                    conn.execute(
                        "UPDATE fixtures SET external_provider='sporttery',external_fixture_id=? WHERE fixture_key=?",
                        (match_id, fixture["fixture_key"]),
                    )
                except sqlite3.IntegrityError:
                    warnings.append(f"{fixture['match_code']} 官方比赛 ID 已被其他对阵使用，需人工核查")
                    continue
                ids[fixture["fixture_key"]] = match_id
        return ids, warnings

    def _fetch_match(self, match_id: str) -> tuple[dict, dict, list[dict]]:
        values, raw, errors = {}, {}, []
        params_by_name = {
            "feature": {"sportteryMatchId": match_id, "termLimits": 10},
            "injuries": {"sportteryMatchId": match_id},
            "results": {"sportteryMatchId": match_id, "termLimits": 10,
                        "tournamentFlag": 0, "homeAwayFlag": 0},
            "h2h": {"sportteryMatchId": match_id, "termLimits": 10,
                    "tournamentFlag": 0, "homeAwayFlag": 0},
            "future": {"sportteryMatchId": match_id, "termLimits": 4},
        }
        referer = f"{self.page_url}?mid={match_id}"
        for name, endpoint in self.endpoints.items():
            url = f"{self.base_url}/{endpoint}"
            try:
                payload = self._get(url, params_by_name[name], referer)
                raw[name] = payload
                value = payload.get("value")
                values[name] = value if isinstance(value, dict) else {}
            except RuntimeError as exc:
                errors.append({"endpoint": name, "error": str(exc)})
        return values, raw, errors

    def _get(self, url: str, params: dict, referer: str) -> dict:
        try:
            response = self.session.get(url, params=params, headers={**self.headers, "Referer": referer},
                                        timeout=self.timeout)
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            raise RuntimeError(f"竞彩网公开接口请求失败：{exc}") from exc
        if not isinstance(payload, dict) or not payload.get("success") or str(payload.get("errorCode")) != "0":
            message = payload.get("errorMessage", "响应格式不正确") if isinstance(payload, dict) else "响应不是 JSON 对象"
            raise RuntimeError(f"竞彩网公开接口返回失败：{message}")
        return payload

    def _save_snapshot(self, date: str, match_id: str, payload: object) -> Path:
        snapshot_dir = self.db_path.parent / "snapshots"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        safe_id = re.sub(r"[^0-9A-Za-z_-]", "_", match_id)
        stamp = utc_now().replace(":", "").replace("+", "_")
        path = snapshot_dir / f"sporttery_features_{date}_{safe_id}_{stamp}.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return path

    def _build_features(self, fixture: dict, match_id: str, values: dict, fetched_at: str) -> dict:
        features = {
            "official_match_id": match_id,
            "official_source_url": f"{self.page_url}?mid={match_id}",
            "official_fetched_at": fetched_at,
        }
        feature = values.get("feature") or {}
        goal_avg = feature.get("goalAvg") or {}
        loss_avg = feature.get("lossGoalAvg") or {}
        for target, source, key in (
            ("home_recent_goals_avg", goal_avg, "homeGoalAvgCnt"),
            ("away_recent_goals_avg", goal_avg, "awayGoalAvgCnt"),
            ("home_recent_conceded_avg", loss_avg, "homeLossGoalAvgCnt"),
            ("away_recent_conceded_avg", loss_avg, "awayLossGoalAvgCnt"),
        ):
            value = optional_float(source.get(key))
            if value is not None:
                features[target] = value
        if optional_float(goal_avg.get("totalLegCnt")) is not None:
            features["recent_goal_sample_size"] = goal_avg.get("totalLegCnt")

        start_date = datetime.fromisoformat(fixture["start_time_utc"]).astimezone(
            ZoneInfo(fixture["timezone"])
        ).date()
        results = values.get("results") or {}
        for side, team_key in (("home", "home_team"), ("away", "away_team")):
            form, rest_days = self._recent_form(results.get(side) or {}, fixture[team_key], start_date)
            if form:
                features[f"recent_form_{side}"] = form
            if rest_days is not None:
                features[f"rest_days_{side}"] = rest_days
        if features.get("recent_form_home") or features.get("recent_form_away"):
            features["recent_form"] = "；".join(filter(None, (
                features.get("recent_form_home"), features.get("recent_form_away"),
            )))
        injuries = self._injury_text(values.get("injuries") or {})
        if injuries:
            features["injuries"] = injuries
        h2h = self._h2h_text(values.get("h2h") or {}, fixture["home_team"], start_date)
        if h2h:
            features["h2h"] = h2h
        return features

    @staticmethod
    def _match_view(item: dict, team_name: str) -> tuple[str, int, int] | None:
        try:
            home_goals, away_goals = map(int, normalize_score(item.get("fullCourtGoal")).split(":"))
        except (ValueError, TypeError):
            return None
        is_home = normalize_name(item.get("homeTeamShortName")) == normalize_name(team_name)
        is_away = normalize_name(item.get("awayTeamShortName")) == normalize_name(team_name)
        if is_home == is_away:
            return None
        if is_home:
            return ("胜" if home_goals > away_goals else "平" if home_goals == away_goals else "负",
                    home_goals, away_goals)
        return ("胜" if away_goals > home_goals else "平" if home_goals == away_goals else "负",
                away_goals, home_goals)

    @classmethod
    def _recent_form(cls, section: dict, team_name: str, start_date) -> tuple[str, int | None]:
        matches = [item for item in section.get("matchList") or []
                   if isinstance(item, dict) and cls._valid_previous_date(item.get("matchDate"), start_date)]
        matches.sort(key=lambda item: clean_text(item.get("matchDate"))[:10], reverse=True)
        rest_days = None
        if matches:
            latest = datetime.strptime(clean_text(matches[0].get("matchDate"))[:10], "%Y-%m-%d").date()
            rest_days = max(0, (start_date - latest).days)
        views = [view for item in matches[:5] if (view := cls._match_view(item, team_name))]
        if not views:
            return "", rest_days
        counts = {label: sum(view[0] == label for view in views) for label in ("胜", "平", "负")}
        return (f"{team_name}近{len(views)}场{counts['胜']}胜{counts['平']}平{counts['负']}负，"
                f"进{sum(view[1] for view in views)}球失{sum(view[2] for view in views)}球"), rest_days

    @classmethod
    def _h2h_text(cls, value: dict, home_team: str, start_date) -> str:
        matches = [item for item in value.get("matchList") or []
                   if isinstance(item, dict) and cls._valid_previous_date(item.get("matchDate"), start_date)]
        views = [view for item in matches if (view := cls._match_view(item, home_team))]
        if not views:
            return ""
        counts = {label: sum(view[0] == label for view in views) for label in ("胜", "平", "负")}
        return (f"双方近{len(views)}次交锋，{home_team}视角{counts['胜']}胜"
                f"{counts['平']}平{counts['负']}负")

    @staticmethod
    def _valid_previous_date(value: object, start_date) -> bool:
        text = clean_text(value)[:10]
        try:
            return datetime.strptime(text, "%Y-%m-%d").date() < start_date
        except ValueError:
            return False

    @staticmethod
    def _injury_text(value: dict) -> str:
        teams = []
        for side in ("home", "away"):
            section = value.get(side) or {}
            people = []
            for item in section.get("injuriesAndSuspensionsList") or []:
                if not isinstance(item, dict):
                    continue
                flags = []
                if item.get("injuryFlag"):
                    flags.append("伤病")
                if item.get("suspensionFlag"):
                    flags.append("停赛")
                name = clean_text(item.get("personName"))
                if name and flags:
                    position = clean_text(item.get("playerPositionDesc"))
                    people.append(f"{name}（{position}{'，' if position else ''}{'/'.join(flags)}）")
            if people:
                teams.append(f"{clean_text(section.get('teamShortName'), side)}：{'、'.join(people)}")
        return "；".join(teams)
