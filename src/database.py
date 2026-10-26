"""SQLite 数据层：每日快照、版本化预测和旧库迁移。"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from utils import fixture_key, local_date, normalize_score, outcome, utc_now

SCHEMA_VERSION = "2"

SCHEMA = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS fixtures (
    fixture_key TEXT PRIMARY KEY,
    match_code TEXT NOT NULL,
    match_date TEXT NOT NULL,
    league TEXT NOT NULL,
    home_team TEXT NOT NULL,
    away_team TEXT NOT NULL,
    start_time_utc TEXT NOT NULL,
    timezone TEXT NOT NULL,
    handicap TEXT,
    sale_status TEXT,
    match_status TEXT NOT NULL DEFAULT 'pending',
    odds_json TEXT NOT NULL DEFAULT '{}',
    source TEXT NOT NULL,
    source_url TEXT,
    external_provider TEXT,
    external_fixture_id TEXT,
    latest_snapshot_id INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(external_provider, external_fixture_id)
);
CREATE TABLE IF NOT EXISTS fixture_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fixture_key TEXT NOT NULL,
    captured_at TEXT NOT NULL,
    source TEXT NOT NULL,
    source_url TEXT,
    content_hash TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    raw_file TEXT,
    FOREIGN KEY(fixture_key) REFERENCES fixtures(fixture_key),
    UNIQUE(fixture_key, content_hash)
);
CREATE TABLE IF NOT EXISTS team_aliases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    alias TEXT NOT NULL,
    normalized_alias TEXT NOT NULL,
    league TEXT NOT NULL DEFAULT '*',
    standard_name TEXT NOT NULL,
    english_name TEXT,
    provider TEXT,
    external_id TEXT,
    review_status TEXT NOT NULL DEFAULT 'confirmed',
    updated_at TEXT NOT NULL,
    UNIQUE(normalized_alias, league, provider)
);
CREATE TABLE IF NOT EXISTS match_features (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fixture_key TEXT NOT NULL,
    as_of TEXT NOT NULL,
    source TEXT NOT NULL,
    feature_set_json TEXT NOT NULL,
    completeness REAL NOT NULL DEFAULT 0,
    missing_fields_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL,
    UNIQUE(fixture_key, as_of, source),
    FOREIGN KEY(fixture_key) REFERENCES fixtures(fixture_key)
);
CREATE TABLE IF NOT EXISTS model_versions (
    version TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    parameters_json TEXT NOT NULL,
    trained_from TEXT,
    trained_to TEXT,
    created_at TEXT NOT NULL,
    backtest_metrics_json TEXT
);
CREATE TABLE IF NOT EXISTS predictions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fixture_key TEXT NOT NULL,
    model_version TEXT NOT NULL,
    predicted_at TEXT NOT NULL,
    input_as_of TEXT NOT NULL,
    probability_home REAL NOT NULL,
    probability_draw REAL NOT NULL,
    probability_away REAL NOT NULL,
    expected_home_goals REAL NOT NULL,
    expected_away_goals REAL NOT NULL,
    score_probabilities_json TEXT NOT NULL,
    top_scores_json TEXT NOT NULL,
    data_completeness REAL NOT NULL,
    risk_flags_json TEXT NOT NULL,
    analysis_json TEXT NOT NULL,
    FOREIGN KEY(fixture_key) REFERENCES fixtures(fixture_key),
    FOREIGN KEY(model_version) REFERENCES model_versions(version)
);
CREATE TABLE IF NOT EXISTS results (
    fixture_key TEXT PRIMARY KEY,
    home_goals INTEGER NOT NULL,
    away_goals INTEGER NOT NULL,
    outcome TEXT NOT NULL,
    actual_score TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    source TEXT NOT NULL,
    review_note TEXT,
    FOREIGN KEY(fixture_key) REFERENCES fixtures(fixture_key)
);
CREATE TABLE IF NOT EXISTS match_review_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fixture_key TEXT NOT NULL,
    reason TEXT NOT NULL,
    candidates_json TEXT NOT NULL DEFAULT '[]',
    status TEXT NOT NULL DEFAULT 'open',
    created_at TEXT NOT NULL,
    resolved_at TEXT,
    FOREIGN KEY(fixture_key) REFERENCES fixtures(fixture_key)
);
CREATE INDEX IF NOT EXISTS idx_fixtures_date ON fixtures(match_date, start_time_utc);
CREATE INDEX IF NOT EXISTS idx_fixtures_status ON fixtures(match_status);
CREATE INDEX IF NOT EXISTS idx_snapshots_fixture ON fixture_snapshots(fixture_key, captured_at);
CREATE INDEX IF NOT EXISTS idx_features_cutoff ON match_features(fixture_key, as_of);
CREATE INDEX IF NOT EXISTS idx_predictions_fixture ON predictions(fixture_key, predicted_at);
"""


class Database:
    def __init__(self, path: str | Path = "data/football.db"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def initialize(self) -> None:
        with self.connect() as conn:
            conn.executescript(SCHEMA)
            conn.execute("INSERT OR REPLACE INTO metadata(key,value) VALUES('schema_version',?)", (SCHEMA_VERSION,))
            self._migrate_legacy_matches(conn)

    def _migrate_legacy_matches(self, conn: sqlite3.Connection) -> None:
        if conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='matches'").fetchone() is None:
            return
        rows = conn.execute("SELECT * FROM matches").fetchall()
        now = utc_now()
        for row in rows:
            start = str(row["match_time"] or "")
            try:
                from datetime import datetime, timezone
                from zoneinfo import ZoneInfo
                dt = datetime.fromisoformat(start)
                legacy_tz = "Asia/Shanghai"
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=ZoneInfo(legacy_tz))
                else:
                    legacy_tz = "UTC"
                start = dt.astimezone(timezone.utc).isoformat(timespec="seconds")
            except ValueError:
                continue
            key = fixture_key(row["match_code"] or f"legacy-{row['id']}", start, row["home_team"], row["away_team"])
            if conn.execute("SELECT 1 FROM fixtures WHERE fixture_key=?", (key,)).fetchone():
                continue
            odds = {k: row[k] for k in ("odds_home", "odds_draw", "odds_away", "odds_handicap_home", "odds_handicap_draw", "odds_handicap_away")}
            conn.execute("""INSERT INTO fixtures(
                fixture_key,match_code,match_date,league,home_team,away_team,start_time_utc,timezone,
                handicap,sale_status,match_status,odds_json,source,created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                key, row["match_code"] or f"legacy-{row['id']}", local_date(start), row["league"] or "未知联赛",
                row["home_team"], row["away_team"], start, legacy_tz, row["handicap"], None,
                row["match_status"] or "pending", json.dumps(odds), "legacy_matches", now, now,
            ))
            if row["actual_result"]:
                try:
                    score = normalize_score(row["actual_result"])
                    home, away = map(int, score.split(":"))
                    conn.execute("INSERT OR IGNORE INTO results VALUES(?,?,?,?,?,?,?,NULL)", (
                        key, home, away, outcome(home, away), score, now, "legacy_matches",
                    ))
                except ValueError:
                    pass
            scores = [row["prediction_1"], row["prediction_2"], row["prediction_3"]]
            if any(scores):
                conn.execute("INSERT OR IGNORE INTO model_versions VALUES(?,?,?,?,?,?,NULL)", (
                    "legacy-v0", "旧版比分迁移", "{}", None, None, now,
                ))
                # 旧记录没有概率和预期进球，保留原表供审计，不伪造概率写入新预测表。

    def upsert_model(self, version: str, name: str, parameters: dict) -> None:
        with self.connect() as conn:
            conn.execute("""INSERT INTO model_versions(version,name,parameters_json,created_at)
                VALUES(?,?,?,?) ON CONFLICT(version) DO UPDATE SET
                name=excluded.name,parameters_json=excluded.parameters_json""",
                (version, name, json.dumps(parameters, ensure_ascii=False), utc_now()))
