"""可复现的泊松 / Dixon–Coles 基线预测。预测记录只追加，不覆盖。"""
from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path

from database import Database
from utils import clean_text, optional_float, utc_now


class FootballPredictor:
    MODEL_VERSION = "poisson-dc-v1"

    def __init__(self, db_path: str | Path = "data/football.db", parameters: dict | None = None):
        self.db = Database(db_path)
        self.db.initialize()
        self.parameters = {
            "league_avg_home_goals": 1.40,
            "league_avg_away_goals": 1.10,
            "dixon_coles_rho": -0.05,
            "max_goals": 10,
            "min_expected_goals": 0.10,
            "max_expected_goals": 5.00,
            "odds_fallback_total_goals": 2.50,
        }
        if parameters:
            self.parameters.update(parameters)
        self.db.upsert_model(self.MODEL_VERSION, "Poisson + Dixon–Coles 低比分修正", self.parameters)

    def predict_all(self, date: str | None = None, refresh: bool = False,
                    as_of: str | None = None) -> list[dict]:
        cutoff = as_of or utc_now()
        parsed_cutoff = datetime.fromisoformat(cutoff.replace("Z", "+00:00"))
        if parsed_cutoff.tzinfo is None:
            raise ValueError("as_of 必须包含时区，例如 +08:00 或 Z")
        cutoff = parsed_cutoff.astimezone(timezone.utc).isoformat(timespec="seconds")
        query = "SELECT * FROM fixtures WHERE match_status NOT IN ('cancelled','postponed','finished')"
        params: list = []
        if date:
            query += " AND match_date=?"
            params.append(date)
        query += " ORDER BY start_time_utc,match_code"
        with self.db.connect() as conn:
            fixtures = conn.execute(query, params).fetchall()
            predictions = []
            for fixture in fixtures:
                if not refresh and conn.execute("SELECT 1 FROM predictions WHERE fixture_key=? AND model_version=? LIMIT 1",
                                                (fixture["fixture_key"], self.MODEL_VERSION)).fetchone():
                    continue
                if fixture["start_time_utc"] <= cutoff:
                    continue
                feature = conn.execute("""SELECT * FROM match_features
                    WHERE fixture_key=? AND as_of<=? AND as_of<=?
                    ORDER BY as_of DESC,id DESC LIMIT 1""",
                    (fixture["fixture_key"], cutoff, fixture["start_time_utc"])).fetchone()
                pred = self._predict_single(fixture, feature, cutoff)
                self._insert_prediction(conn, fixture["fixture_key"], pred)
                predictions.append({"fixture_key": fixture["fixture_key"], "match_code": fixture["match_code"], **pred})
        return predictions

    def _predict_single(self, match_data, feature_row=None, predicted_at: str | None = None) -> dict:
        features = json.loads(feature_row["feature_set_json"]) if feature_row else {}
        odds = json.loads(match_data["odds_json"] or "{}") if "odds_json" in match_data.keys() else {
            key: match_data[key] for key in ("odds_home", "odds_draw", "odds_away") if key in match_data.keys()
        }
        home_xg, away_xg, method = self._expected_goals(features, odds)
        probabilities = self._score_matrix(home_xg, away_xg)
        total = sum(probabilities.values())
        probabilities = {score: probability / total for score, probability in probabilities.items()}
        home = sum(p for score, p in probabilities.items() if self._goals(score)[0] > self._goals(score)[1])
        draw = sum(p for score, p in probabilities.items() if self._goals(score)[0] == self._goals(score)[1])
        away = 1.0 - home - draw
        top_scores = [{"score": score, "probability": round(prob, 8)}
                      for score, prob in sorted(probabilities.items(), key=lambda item: item[1], reverse=True)[:5]]
        missing = json.loads(feature_row["missing_fields_json"]) if feature_row else [
            "home_xg", "home_xga", "away_xg", "away_xga", "injuries", "lineups"
        ]
        completeness = float(feature_row["completeness"]) if feature_row else (0.20 if self._valid_odds(odds) else 0.10)
        risks = [f"缺失数据：{field}" for field in missing if field in {
            "home_xg", "home_xga", "away_xg", "away_xga", "injuries", "lineups", "recent_form"
        }]
        if method != "explicit_expected_goals":
            risks.append(f"预期进球估计方式：{method}")
        league = match_data["league"]
        if any(word in league for word in ("杯", "Cup", "资格赛")):
            risks.append("杯赛或资格赛可能存在轮换与战意不确定性")
        analysis = self._analysis(features, method, feature_row["source"] if feature_row else "league_baseline")
        return {
            "model_version": self.MODEL_VERSION,
            "predicted_at": predicted_at or utc_now(),
            "input_as_of": feature_row["as_of"] if feature_row else (predicted_at or utc_now()),
            "probability_home": home,
            "probability_draw": draw,
            "probability_away": away,
            "expected_home_goals": home_xg,
            "expected_away_goals": away_xg,
            "score_probabilities": probabilities,
            "top_scores": top_scores,
            "data_completeness": completeness,
            "risk_flags": risks,
            "analysis": analysis,
            "prediction_1": top_scores[0]["score"],
            "prediction_2": top_scores[1]["score"],
            "prediction_3": top_scores[2]["score"],
            "confidence": round(max(home, draw, away), 6),
            "is_upset": 0,
        }

    def _expected_goals(self, features: dict, odds: dict) -> tuple[float, float, str]:
        explicit_home = optional_float(features.get("expected_home_goals"))
        explicit_away = optional_float(features.get("expected_away_goals"))
        if explicit_home is not None and explicit_away is not None:
            return self._clip(explicit_home), self._clip(explicit_away), "explicit_expected_goals"
        home_xg = optional_float(features.get("home_xg"))
        home_xga = optional_float(features.get("home_xga"))
        away_xg = optional_float(features.get("away_xg"))
        away_xga = optional_float(features.get("away_xga"))
        if None not in (home_xg, home_xga, away_xg, away_xga):
            home = (home_xg + away_xga) / 2
            away = (away_xg + home_xga) / 2
            return self._clip(home), self._clip(away), "xg_xga_baseline"
        attack = [optional_float(features.get(key)) for key in ("home_attack", "home_defense", "away_attack", "away_defense")]
        if all(value is not None for value in attack):
            home = self.parameters["league_avg_home_goals"] * attack[0] * attack[3]
            away = self.parameters["league_avg_away_goals"] * attack[2] * attack[1]
            return self._clip(home), self._clip(away), "attack_defense_multipliers"
        if self._valid_odds(odds):
            inverse = [1 / float(odds[key]) for key in ("odds_home", "odds_draw", "odds_away")]
            denominator = sum(inverse)
            p_home, _, p_away = (value / denominator for value in inverse)
            total = self.parameters["odds_fallback_total_goals"]
            home = total / 2 + 1.2 * (p_home - p_away)
            return self._clip(home), self._clip(total - home), "odds_only_fallback"
        return (self.parameters["league_avg_home_goals"], self.parameters["league_avg_away_goals"], "league_baseline_only")

    def _score_matrix(self, home_xg: float, away_xg: float) -> dict[str, float]:
        max_goals = int(self.parameters["max_goals"])
        rho = float(self.parameters["dixon_coles_rho"])
        matrix = {}
        for home in range(max_goals + 1):
            for away in range(max_goals + 1):
                probability = self._poisson(home, home_xg) * self._poisson(away, away_xg)
                if home == 0 and away == 0:
                    probability *= max(0.0, 1 - home_xg * away_xg * rho)
                elif home == 0 and away == 1:
                    probability *= max(0.0, 1 + home_xg * rho)
                elif home == 1 and away == 0:
                    probability *= max(0.0, 1 + away_xg * rho)
                elif home == 1 and away == 1:
                    probability *= max(0.0, 1 - rho)
                matrix[f"{home}:{away}"] = probability
        return matrix

    @staticmethod
    def _analysis(features: dict, method: str, source: str) -> dict:
        evidence = [{"field": key, "value": features[key]} for key in (
            "recent_form", "home_xg", "home_xga", "away_xg", "away_xga", "injuries",
            "lineups", "rest_days_home", "rest_days_away", "h2h"
        ) if key in features and clean_text(features[key])]
        return {"method": method, "feature_source": source, "evidence": evidence}

    def _insert_prediction(self, conn, key: str, pred: dict) -> None:
        conn.execute("""INSERT INTO predictions(
            fixture_key,model_version,predicted_at,input_as_of,probability_home,probability_draw,probability_away,
            expected_home_goals,expected_away_goals,score_probabilities_json,top_scores_json,
            data_completeness,risk_flags_json,analysis_json
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
            key, pred["model_version"], pred["predicted_at"], pred["input_as_of"],
            pred["probability_home"], pred["probability_draw"], pred["probability_away"],
            pred["expected_home_goals"], pred["expected_away_goals"],
            json.dumps(pred["score_probabilities"]), json.dumps(pred["top_scores"], ensure_ascii=False),
            pred["data_completeness"], json.dumps(pred["risk_flags"], ensure_ascii=False),
            json.dumps(pred["analysis"], ensure_ascii=False),
        ))

    def _save_predictions(self, predictions: list[dict]) -> None:
        """旧调用兼容：新流程由 predict_all 逐条追加保存。"""
        if not predictions:
            return
        with self.db.connect() as conn:
            for pred in predictions:
                if "fixture_key" in pred and "probability_home" in pred:
                    self._insert_prediction(conn, pred["fixture_key"], pred)

    @staticmethod
    def _valid_odds(odds: dict) -> bool:
        return all(optional_float(odds.get(key)) is not None and float(odds[key]) > 1 for key in ("odds_home", "odds_draw", "odds_away"))

    def _clip(self, value: float) -> float:
        return max(self.parameters["min_expected_goals"], min(self.parameters["max_expected_goals"], float(value)))

    @staticmethod
    def _poisson(goals: int, expected: float) -> float:
        return math.exp(-expected) * expected ** goals / math.factorial(goals)

    @staticmethod
    def _goals(score: str) -> tuple[int, int]:
        home, away = score.split(":")
        return int(home), int(away)
