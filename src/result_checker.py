"""赛果关联、AI 推断核对与用户实际选择命中判定。"""
from __future__ import annotations

import json
from pathlib import Path

from database import Database


class ResultChecker:
    def __init__(self, db_path: str | Path = "data/football.db"):
        self.db = Database(db_path)
        self.db.initialize()

    def check_all(self) -> list[dict]:
        from selections import outcome_selection_hit

        with self.db.connect() as conn:
            rows = conn.execute("""SELECT p.*,f.match_code,f.league,f.odds_json,f.handicap,
                r.actual_score,r.home_goals,r.away_goals,r.outcome,r.result_type
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
                bet = conn.execute("SELECT * FROM user_bets WHERE fixture_key=?", (row["fixture_key"],)).fetchone()
                item = {
                    "prediction_id": row["id"], "fixture_key": row["fixture_key"],
                    "match_code": row["match_code"], "actual_result": actual,
                    "result_type": row["result_type"],
                    "hit_prediction": hit, "outcome_hit": int(predicted_outcome == row["outcome"]),
                    "user_outcome_hit": None, "user_score_hit": None,
                }
                if bet:
                    item["user_outcome_hit"] = outcome_selection_hit(
                        bet["outcome_selection"], row["home_goals"], row["away_goals"], row["handicap"])
                    selected_scores = json.loads(bet["score_selections_json"] or "[]")
                    if selected_scores:
                        item["user_score_hit"] = actual in selected_scores
                checked.append(item)
            return checked

    @staticmethod
    def _check_single(row) -> dict:
        actual = row["actual_result"]
        predictions = [row["prediction_1"], row["prediction_2"], row["prediction_3"]]
        return {"match_code": row["match_code"],
                "hit_prediction": predictions.index(actual) + 1 if actual in predictions else 0}
