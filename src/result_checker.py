import sqlite3
import pandas as pd

class ResultChecker:
    def __init__(self, db_path='data/football.db'):
        self.db_path = db_path
    
    def check_all(self):
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("""
            SELECT * FROM matches 
            WHERE actual_result IS NOT NULL 
            AND prediction_1 IS NOT NULL
            AND hit_prediction IS NULL
        """, conn)
        conn.close()
        
        if df.empty:
            print("⚠️ 没有需要核对的比赛")
            return []
        
        results = []
        for _, row in df.iterrows():
            results.append(self._check_single(row))
        
        self._save_check_results(results)
        return results
    
    def _check_single(self, row):
        actual = row['actual_result']
        preds = [row['prediction_1'], row['prediction_2'], row['prediction_3']]
        
        hit = 0
        for i, pred in enumerate(preds, 1):
            if pred == actual:
                hit = i
                break
        
        upset_actual = self._classify_upset_actual(row, actual)
        
        return {
            'match_code': row['match_code'],
            'hit_prediction': hit,
            'is_upset_actual': upset_actual
        }
    
    def _classify_upset_actual(self, row, actual_score):
        odds = [row['odds_home'], row['odds_draw'], row['odds_away']]
        
        try:
            if ':' in actual_score:
                home_goals, away_goals = map(int, actual_score.split(':'))
                if home_goals > away_goals:
                    result_idx = 0
                elif home_goals == away_goals:
                    result_idx = 1
                else:
                    result_idx = 2
                
                actual_odds = odds[result_idx]
                sorted_odds = sorted(odds)
                
                if actual_odds == sorted_odds[2]:
                    return 2
                elif actual_odds == sorted_odds[1]:
                    return 1
                else:
                    return 0
            return 0
        except:
            return 0
    
    def _save_check_results(self, results):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for res in results:
            cursor.execute("""
                UPDATE matches 
                SET hit_prediction = ?, is_upset = ?
                WHERE match_code = ?
            """, (res['hit_prediction'], res['is_upset_actual'], res['match_code']))
        
        conn.commit()
        conn.close()
        print(f"✅ 已更新 {len(results)} 场核对结果")
