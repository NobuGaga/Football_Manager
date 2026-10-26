"""赛果关联与逐条预测核对，不再依赖旧表 hit_prediction 的 NULL 条件。"""
from __future__ import annotations

import json
from pathlib import Path

from database import Database


class ResultChecker:
    def __init__(self, db_path: str | Path = "data/football.db"):
        self.db = Database(db_path)
        self.db.initialize()

    def check_all(self) -> list[dict]:
        with self.db.connect() as conn:
            rows = conn.execute("""SELECT p.*,f.match_code,f.league,f.odds_json,
                r.actual_score,r.home_goals,r.away_goals,r.outcome
                FROM predictions p JOIN fixtures f ON f.fixture_key=p.fixture_key
                JOIN results r ON r.fixture_key=p.fixture_key
                ORDER BY f.start_time_utc,p.predicted_at""").fetchall()
            checked = []
            for row in rows:
                top_scores = json.loads(row["top_scores_json"])
                predicted = [item["score"] for item in top_scores[:3]]
                actual = row["actual_score"]
                hit = predicted.index(actual) + 1 if actual in predicted else 0
                predicted_outcome = max((("H", row["probability_home"]), ("D", row["probability_draw"]),
                                         ("A", row["probability_away"])), key=lambda item: item[1])[0]
                item = {
                    "prediction_id": row["id"], "fixture_key": row["fixture_key"],
                    "match_code": row["match_code"], "actual_result": actual,
                    "hit_prediction": hit, "outcome_hit": int(predicted_outcome == row["outcome"]),
                }
                checked.append(item)
            return checked

    @staticmethod
    def _check_single(row) -> dict:
        actual = row["actual_result"]
        predictions = [row["prediction_1"], row["prediction_2"], row["prediction_3"]]
        return {"match_code": row["match_code"],
                "hit_prediction": predictions.index(actual) + 1 if actual in predictions else 0}
