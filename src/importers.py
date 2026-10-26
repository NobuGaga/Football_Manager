"""官方对阵、名称映射、赛前特征和赛果导入。"""
from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from database import Database
from utils import (DEFAULT_TIMEZONE, clean_text, first, fixture_key, local_date,
                   normalize_name, normalize_score, optional_float, outcome,
                   parse_datetime, payload_hash, read_structured_file, utc_now)

FEATURE_FIELDS = (
    "home_attack", "home_defense", "away_attack", "away_defense",
    "home_xg", "home_xga", "away_xg", "away_xga",
    "expected_home_goals", "expected_away_goals", "injuries", "lineups",
    "rest_days_home", "rest_days_away", "recent_form", "h2h",
)


class BaseImporter:
    def __init__(self, db_path: str | Path = "data/football.db"):
        self.db = Database(db_path)
        self.db.initialize()

    @staticmethod
    def _teams(row: dict) -> tuple[str, str]:
        home = clean_text(first(row, "主队", "home_team", "home"))
        away = clean_text(first(row, "客队", "away_team", "away"))
        pairing = clean_text(first(row, "对阵", "fixture"))
        if pairing and not (home and away):
            for separator in (" VS ", " vs ", "VS", "vs", " - ", "–", "—"):
                if separator in pairing:
                    home, away = (part.strip() for part in pairing.split(separator, 1))
                    break
        if not home or not away:
            raise ValueError("主队和客队不能为空")
        return home, away

    def _locate_fixture(self, conn, row: dict) -> str:
        supplied = clean_text(first(row, "fixture_key", "比赛键"))
        if supplied:
            if conn.execute("SELECT 1 FROM fixtures WHERE fixture_key=?", (supplied,)).fetchone():
                return supplied
            raise ValueError(f"fixture_key 不存在：{supplied}")
        home, away = self._teams(row)
        code = clean_text(first(row, "竞彩编号", "场次", "match_code"))
        date = clean_text(first(row, "比赛日期", "match_date"))
        clauses = ["home_team=?", "away_team=?"]
        params: list = [home, away]
        if code:
            clauses.append("match_code=?")
            params.append(code)
        if date:
            clauses.append("match_date=?")
            params.append(date)
        matches = conn.execute(f"SELECT fixture_key FROM fixtures WHERE {' AND '.join(clauses)}", params).fetchall()
        if len(matches) != 1:
            raise ValueError(f"无法唯一关联对阵（候选 {len(matches)} 个）：{code} {home} VS {away}")
        return matches[0]["fixture_key"]


class FixtureImporter(BaseImporter):
    def import_file(self, path: str | Path, default_date: str | None = None,
                    source: str = "sporttery_manual", timezone_name: str = DEFAULT_TIMEZONE) -> dict:
        rows, raw = read_structured_file(path)
        snapshot_dir = self.db.path.parent / "snapshots"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        captured = utc_now()
        raw_path = snapshot_dir / f"{captured.replace(':', '').replace('+', '_')}_{payload_hash(raw)[:12]}_{Path(path).name}"
        shutil.copy2(path, raw_path)
        imported, duplicates, errors = 0, 0, []
        with self.db.connect() as conn:
            for number, row in enumerate(rows, 2):
                try:
                    key, changed = self._import_row(conn, row, default_date, source, timezone_name, str(raw_path), captured)
                    imported += 1
                    duplicates += 0 if changed else 1
                except (ValueError, TypeError) as exc:
                    errors.append({"row": number, "error": str(exc)})
        if imported == 0 and errors:
            raise ValueError(f"没有有效对阵：{errors[:3]}")
        return {"imported": imported, "unchanged": duplicates, "errors": errors, "raw_snapshot": str(raw_path)}

    def import_rows(self, rows: list[dict], default_date: str | None = None,
                    source: str = "sporttery_web", timezone_name: str = DEFAULT_TIMEZONE) -> dict:
        imported, errors = 0, []
        captured = utc_now()
        with self.db.connect() as conn:
            for number, row in enumerate(rows, 2):
                try:
                    self._import_row(conn, row, default_date, source, timezone_name, None, captured)
                    imported += 1
                except (ValueError, TypeError) as exc:
                    errors.append({"row": number, "error": str(exc)})
        return {"imported": imported, "errors": errors}

    def _import_row(self, conn, row: dict, default_date: str | None, source: str,
                    timezone_name: str, raw_path: str | None, captured: str) -> tuple[str, bool]:
        home, away = self._teams(row)
        code = clean_text(first(row, "竞彩编号", "场次", "match_code"), "未编号")
        league = clean_text(first(row, "联赛名称", "联赛", "league"), "未知联赛")
        tz = clean_text(first(row, "执行时区", "时区", "timezone"), timezone_name)
        date = clean_text(first(row, "比赛日期", "match_date"), default_date or "")
        start_value = first(row, "开赛时间", "比赛时间", "start_time", "match_time")
        start_utc, tz = parse_datetime(start_value, date or None, tz)
        match_date = local_date(start_utc, tz)
        handicap = clean_text(first(row, "让球信息", "让球", "handicap"), "0")
        odds = {
            "odds_home": optional_float(first(row, "主胜", "odds_home")),
            "odds_draw": optional_float(first(row, "平局", "odds_draw")),
            "odds_away": optional_float(first(row, "主负", "客胜", "odds_away")),
            "odds_handicap_home": optional_float(first(row, "让球主胜", "odds_handicap_home")),
            "odds_handicap_draw": optional_float(first(row, "让球平局", "odds_handicap_draw")),
            "odds_handicap_away": optional_float(first(row, "让球主负", "odds_handicap_away")),
        }
        external_provider = clean_text(first(row, "外部数据源", "external_provider")) or None
        external_id = clean_text(first(row, "外部比赛ID", "external_fixture_id")) or None
        existing = conn.execute("""SELECT fixture_key FROM fixtures
            WHERE match_code=? AND match_date=? AND home_team=? AND away_team=?""",
            (code, match_date, home, away)).fetchone()
        if not existing:
            # 跨日延期仍关联原比赛，但不把相隔一周以上的同编号对阵强行合并。
            candidates = conn.execute("""SELECT fixture_key,start_time_utc FROM fixtures
                WHERE match_code=? AND home_team=? AND away_team=?""", (code, home, away)).fetchall()
            current_dt = datetime.fromisoformat(start_utc)
            nearby = [row for row in candidates if abs((datetime.fromisoformat(row["start_time_utc"]) - current_dt).total_seconds()) <= 7 * 86400]
            if len(nearby) == 1:
                existing = nearby[0]
        key = existing["fixture_key"] if existing else fixture_key(code, start_utc, home, away)
        normalized = dict(row)
        normalized.update({"start_time_utc": start_utc, "timezone": tz, "odds": odds})
        content_hash = payload_hash(normalized)
        changed = conn.execute("SELECT 1 FROM fixture_snapshots WHERE fixture_key=? AND content_hash=?", (key, content_hash)).fetchone() is None
        now = utc_now()
        conn.execute("""INSERT INTO fixtures(
            fixture_key,match_code,match_date,league,home_team,away_team,start_time_utc,timezone,
            handicap,sale_status,match_status,odds_json,source,source_url,external_provider,external_fixture_id,created_at,updated_at
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(fixture_key) DO UPDATE SET
            league=excluded.league,match_date=excluded.match_date,start_time_utc=excluded.start_time_utc,timezone=excluded.timezone,
            handicap=excluded.handicap,sale_status=excluded.sale_status,match_status=excluded.match_status,
            odds_json=excluded.odds_json,source=excluded.source,source_url=COALESCE(excluded.source_url,fixtures.source_url),
            external_provider=COALESCE(excluded.external_provider,fixtures.external_provider),
            external_fixture_id=COALESCE(excluded.external_fixture_id,fixtures.external_fixture_id),updated_at=excluded.updated_at""", (
            key, code, match_date, league, home, away, start_utc, tz, handicap,
            clean_text(first(row, "销售状态", "sale_status")) or None,
            clean_text(first(row, "比赛状态", "match_status"), "pending"),
            json.dumps(odds, ensure_ascii=False), source,
            clean_text(first(row, "原始数据来源", "source_url")) or None,
            external_provider, external_id, now, now,
        ))
        if changed:
            cursor = conn.execute("""INSERT INTO fixture_snapshots(
                fixture_key,captured_at,source,source_url,content_hash,payload_json,raw_file
            ) VALUES(?,?,?,?,?,?,?)""", (
                key, captured, source, clean_text(first(row, "原始数据来源", "source_url")) or None,
                content_hash, json.dumps(normalized, ensure_ascii=False, default=str), raw_path,
            ))
            conn.execute("UPDATE fixtures SET latest_snapshot_id=? WHERE fixture_key=?", (cursor.lastrowid, key))
        for team in (home, away):
            alias = normalize_name(team)
            reason = f"球队名称未映射：{team}"
            if not conn.execute("SELECT 1 FROM team_aliases WHERE normalized_alias=?", (alias,)).fetchone() and not conn.execute(
                    "SELECT 1 FROM match_review_queue WHERE fixture_key=? AND reason=? AND status='open'", (key, reason)).fetchone():
                conn.execute("""INSERT INTO match_review_queue(fixture_key,reason,candidates_json,created_at)
                    VALUES(?,?,?,?)""", (key, reason, "[]", now))
        return key, changed


class AliasImporter(BaseImporter):
    def import_file(self, path: str | Path) -> dict:
        rows, _ = read_structured_file(path)
        count = 0
        with self.db.connect() as conn:
            for number, row in enumerate(rows, 2):
                alias = clean_text(first(row, "体彩名称", "别名", "alias"))
                standard = clean_text(first(row, "标准名称", "standard_name"))
                if not alias or not standard:
                    raise ValueError(f"第 {number} 行缺少 alias/standard_name")
                conn.execute("""INSERT INTO team_aliases(
                    alias,normalized_alias,league,standard_name,english_name,provider,external_id,review_status,updated_at
                ) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(normalized_alias,league,provider) DO UPDATE SET
                    alias=excluded.alias,standard_name=excluded.standard_name,english_name=excluded.english_name,
                    external_id=excluded.external_id,review_status=excluded.review_status,updated_at=excluded.updated_at""", (
                    alias, normalize_name(alias), clean_text(first(row, "联赛", "league"), "*"), standard,
                    clean_text(first(row, "英文名称", "english_name")) or None,
                    clean_text(first(row, "数据源", "provider")),
                    clean_text(first(row, "外部数据源ID", "external_id")) or None,
                    clean_text(first(row, "确认状态", "review_status"), "confirmed"), utc_now(),
                ))
                conn.execute("DELETE FROM match_review_queue WHERE reason=? AND status='open'", (f"球队名称未映射：{alias}",))
                count += 1
        return {"imported": count}


class FeatureImporter(BaseImporter):
    def import_file(self, path: str | Path, source: str = "manual_features") -> dict:
        rows, _ = read_structured_file(path)
        count, errors = 0, []
        with self.db.connect() as conn:
            for number, row in enumerate(rows, 2):
                try:
                    key = self._locate_fixture(conn, row)
                    as_of_text = clean_text(first(row, "数据时间", "as_of"), utc_now())
                    as_of, _ = parse_datetime(as_of_text, tz_name="UTC") if "T" not in as_of_text else (datetime.fromisoformat(as_of_text.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat(timespec="seconds"), "UTC")
                    features = clean_text(first(row, "features_json"))
                    feature_set = json.loads(features) if features else dict(row)
                    feature_set = {k: v for k, v in feature_set.items() if k not in {"fixture_key", "比赛键", "as_of", "数据时间"}}
                    missing = [field for field in FEATURE_FIELDS if field not in feature_set or not clean_text(feature_set[field])]
                    completeness = round((len(FEATURE_FIELDS) - len(missing)) / len(FEATURE_FIELDS), 4)
                    conn.execute("""INSERT INTO match_features(
                        fixture_key,as_of,source,feature_set_json,completeness,missing_fields_json,created_at
                    ) VALUES(?,?,?,?,?,?,?) ON CONFLICT(fixture_key,as_of,source) DO UPDATE SET
                        feature_set_json=excluded.feature_set_json,completeness=excluded.completeness,
                        missing_fields_json=excluded.missing_fields_json,created_at=excluded.created_at""", (
                        key, as_of, source, json.dumps(feature_set, ensure_ascii=False, default=str),
                        completeness, json.dumps(missing), utc_now(),
                    ))
                    count += 1
                except (ValueError, TypeError, json.JSONDecodeError) as exc:
                    errors.append({"row": number, "error": str(exc)})
        return {"imported": count, "errors": errors}


class ResultImporter(BaseImporter):
    def import_file(self, path: str | Path, source: str = "manual_results") -> dict:
        rows, _ = read_structured_file(path)
        return self.import_rows(rows, source)

    def import_rows(self, rows: list[dict], source: str = "manual_results") -> dict:
        count, errors = 0, []
        with self.db.connect() as conn:
            for number, row in enumerate(rows, 2):
                try:
                    key = self._locate_fixture(conn, row)
                    score = normalize_score(first(row, "实际比分", "actual_score", "actual_result"))
                    home, away = map(int, score.split(":"))
                    conn.execute("""INSERT INTO results(
                        fixture_key,home_goals,away_goals,outcome,actual_score,recorded_at,source,review_note
                    ) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(fixture_key) DO UPDATE SET
                        home_goals=excluded.home_goals,away_goals=excluded.away_goals,outcome=excluded.outcome,
                        actual_score=excluded.actual_score,recorded_at=excluded.recorded_at,
                        source=excluded.source,review_note=excluded.review_note""", (
                        key, home, away, outcome(home, away), score, utc_now(), source,
                        clean_text(first(row, "复盘备注", "review_note")) or None,
                    ))
                    conn.execute("UPDATE fixtures SET match_status='finished',updated_at=? WHERE fixture_key=?", (utc_now(), key))
                    count += 1
                except (ValueError, TypeError) as exc:
                    errors.append({"row": number, "error": str(exc)})
        return {"imported": count, "errors": errors}
