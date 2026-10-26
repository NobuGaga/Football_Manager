"""基于赛前已保存预测的时间安全回测与概率质量评价。"""
from __future__ import annotations

import json
import math
from collections import defaultdict
from pathlib import Path

from database import Database
from utils import utc_now


class Backtester:
    def __init__(self, db_path: str | Path = "data/football.db"):
        self.db = Database(db_path)
        self.db.initialize()

    def evaluate(self, model_version: str | None = None, league: str | None = None) -> dict:
        query = """SELECT p.*,f.league,f.start_time_utc,r.actual_score,r.home_goals,r.away_goals,r.outcome
            FROM predictions p JOIN fixtures f ON f.fixture_key=p.fixture_key
            JOIN results r ON r.fixture_key=p.fixture_key WHERE 1=1"""
        params: list = []
        if model_version:
            query += " AND p.model_version=?"
            params.append(model_version)
        if league:
            query += " AND f.league=?"
            params.append(league)
        query += " ORDER BY f.start_time_utc,p.predicted_at"
        with self.db.connect() as conn:
            rows = conn.execute(query, params).fetchall()
        groups: dict[str, list] = defaultdict(list)
        for row in rows:
            if row["input_as_of"] > row["predicted_at"] or row["predicted_at"] > row["start_time_utc"]:
                # 赛前信息边界无效的记录不进入指标，避免未来数据泄漏。
                continue
            groups["全部联赛"].append(row)
            groups[row["league"]].append(row)
        result = {
            "generated_at": utc_now(), "model_version": model_version or "all",
            "league": league or "all", "matches": len(rows),
            "eligible_predictions": sum(len(group) for key, group in groups.items() if key == "全部联赛"),
            "metrics": {name: self._metrics(group) for name, group in groups.items()},
            "calibration": self._calibration(groups.get("全部联赛", [])),
            "method": "仅评价已落库且 predicted_at 不晚于开赛、input_as_of 不晚于预测时间的记录；按比赛时间排序。",
        }
        if model_version:
            with self.db.connect() as conn:
                conn.execute("UPDATE model_versions SET backtest_metrics_json=? WHERE version=?",
                             (json.dumps(result, ensure_ascii=False), model_version))
        return result

    @staticmethod
    def _metrics(rows: list) -> dict:
        if not rows:
            return {"predictions": 0}
        log_loss = brier = outcome_hits = top1_hits = top3_hits = goal_error = 0.0
        for row in rows:
            probs = {"H": row["probability_home"], "D": row["probability_draw"], "A": row["probability_away"]}
            log_loss -= math.log(max(1e-15, probs[row["outcome"]]))
            brier += sum((probs[key] - float(key == row["outcome"])) ** 2 for key in ("H", "D", "A"))
            predicted_outcome = max(probs, key=probs.get)
            outcome_hits += predicted_outcome == row["outcome"]
            scores = [item["score"] for item in json.loads(row["top_scores_json"])]
            top1_hits += bool(scores and scores[0] == row["actual_score"])
            top3_hits += row["actual_score"] in scores[:3]
            goal_error += (abs(row["expected_home_goals"] - row["home_goals"]) +
                           abs(row["expected_away_goals"] - row["away_goals"])) / 2
        count = len(rows)
        return {
            "predictions": count, "log_loss": log_loss / count, "brier_score": brier / count,
            "outcome_accuracy": outcome_hits / count, "top1_score_accuracy": top1_hits / count,
            "top3_score_accuracy": top3_hits / count, "expected_goals_mae": goal_error / count,
        }

    @staticmethod
    def _calibration(rows: list) -> list[dict]:
        buckets = [{"range": f"{low / 10:.1f}-{(low + 1) / 10:.1f}", "low": low / 10,
                    "high": (low + 1) / 10, "predictions": []} for low in range(10)]
        for row in rows:
            for outcome, probability in (("H", row["probability_home"]), ("D", row["probability_draw"]),
                                         ("A", row["probability_away"])):
                index = min(9, int(probability * 10))
                buckets[index]["predictions"].append((probability, outcome == row["outcome"]))
        output = []
        for bucket in buckets:
            values = bucket.pop("predictions")
            bucket.pop("low")
            bucket.pop("high")
            bucket["count"] = len(values)
            bucket["mean_predicted_probability"] = sum(v[0] for v in values) / len(values) if values else None
            bucket["observed_frequency"] = sum(v[1] for v in values) / len(values) if values else None
            output.append(bucket)
        return output
