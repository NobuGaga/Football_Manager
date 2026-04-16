import sqlite3
import pandas as pd
import numpy as np
from scipy.stats import poisson
from datetime import datetime

class FootballPredictor:
    def __init__(self, db_path='data/football.db'):
        self.db_path = db_path
        self.baseline = {'avg_goals_home': 1.4, 'avg_goals_away': 1.1}
    
    def predict_all(self):
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("""
            SELECT * FROM matches 
            WHERE match_status = 'pending' 
            AND prediction_1 IS NULL
            ORDER BY match_time ASC
        """, conn)
        conn.close()
        
        if df.empty:
            print("⚠️ 没有需要预测的比赛")
            return []
        
        predictions = []
        for _, row in df.iterrows():
            pred = self._predict_single(row)
            predictions.append({'match_code': row['match_code'], **pred})
        
        self._save_predictions(predictions)
        print(f"🎯 完成 {len(predictions)} 场比赛预测")
        return predictions
    
    def _predict_single(self, match_data):
        odds_analysis = self._analyze_odds(match_data)
        poisson_preds = self._poisson_predict(match_data, odds_analysis)
        
        if odds_analysis['upset_type'] == 'big':
            adjusted = [poisson_preds[2], poisson_preds[1], poisson_preds[0]]
        elif odds_analysis['upset_type'] == 'small':
            adjusted = [poisson_preds[1], poisson_preds[0], poisson_preds[2]]
        else:
            adjusted = poisson_preds
        
        return {
            'prediction_1': adjusted[0],
            'prediction_2': adjusted[1],
            'prediction_3': adjusted[2],
            'is_upset': odds_analysis['upset_code'],
            'confidence': odds_analysis['confidence']
        }
    
    def _analyze_odds(self, row):
        odds = [row['odds_home'], row['odds_draw'], row['odds_away']]
        sorted_odds = sorted(odds)
        min_odds, max_odds = sorted_odds[0], sorted_odds[2]
        ratio = max_odds / min_odds if min_odds > 0 else 1
        
        if max_odds >= 4.5 or ratio >= 2.5:
            upset_type, upset_code = 'big', 2
        elif max_odds >= 3.0 or ratio >= 1.8:
            upset_type, upset_code = 'small', 1
        else:
            upset_type, upset_code = 'none', 0
        
        confidence = min(1.0, (max_odds - min_odds) / min_odds) if min_odds > 0 else 0
        
        return {'upset_type': upset_type, 'upset_code': upset_code, 'confidence': round(confidence, 2)}
    
    def _poisson_predict(self, row, odds_analysis):
        home_lambda = self.baseline['avg_goals_home']
        away_lambda = self.baseline['avg_goals_away']
        
        total_odds = row['odds_home'] + row['odds_draw'] + row['odds_away']
        if total_odds > 0:
            home_prob = (1/row['odds_home']) / (1/row['odds_home'] + 1/row['odds_draw'] + 1/row['odds_away'])
            away_prob = (1/row['odds_away']) / (1/row['odds_home'] + 1/row['odds_draw'] + 1/row['odds_away'])
            home_lambda *= (home_prob / 0.33)
            away_lambda *= (away_prob / 0.33)
        
        probs = {}
        for h in range(6):
            for a in range(6):
                prob = poisson.pmf(h, home_lambda) * poisson.pmf(a, away_lambda)
                probs[f"{h}:{a}"] = prob
        
        sorted_scores = sorted(probs.items(), key=lambda x: x[1], reverse=True)
        return [s[0] for s in sorted_scores[:3]]
    
    def _save_predictions(self, predictions):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for pred in predictions:
            cursor.execute("""
                UPDATE matches 
                SET prediction_1 = ?, prediction_2 = ?, prediction_3 = ?, is_upset = ?
                WHERE match_code = ?
            """, (pred['prediction_1'], pred['prediction_2'], pred['prediction_3'], 
                  pred['is_upset'], pred['match_code']))
        
        conn.commit()
        conn.close()
