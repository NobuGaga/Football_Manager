import sqlite3
import pandas as pd
import os
from datetime import datetime
from math import comb

class ReportGenerator:
    def __init__(self, db_path='data/football.db', output_dir='reports'):
        self.db_path = db_path
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
    
    def generate_all_reports(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        excel_path = os.path.join(self.output_dir, f"足球预测报表_{timestamp}.xlsx")
        
        reports = {
            '01_实际爆冷概率': self._upset_stats(),
            '02_AI预测命中率': self._hit_rate_stats(),
            '03_爆冷比分TOP5': self._upset_scores_top5(),
            '04_不爆冷比分TOP5': self._normal_scores_top5(),
            '05_平均赔率数据': self._avg_odds(),
            '06_混合过关EV': self._mix_parlay_ev()
        }
        
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            for sheet_name, df in reports.items():
                if not df.empty:
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print(f"📊 Excel报表已生成: {excel_path}")
        return excel_path
    
    def _upset_stats(self):
        conn = sqlite3.connect(self.db_path)
        stats = pd.read_sql_query("""
            SELECT 
                SUM(CASE WHEN is_upset = 2 THEN 1 ELSE 0 END) as big_upset,
                SUM(CASE WHEN is_upset = 1 THEN 1 ELSE 0 END) as small_upset,
                SUM(CASE WHEN is_upset = 0 THEN 1 ELSE 0 END) as no_upset,
                COUNT(*) as total
            FROM matches WHERE actual_result IS NOT NULL
        """, conn)
        conn.close()
        
        if stats.empty or stats.iloc[0]['total'] == 0:
            return pd.DataFrame({'提示': ['暂无已完成比赛数据']})
        
        row = stats.iloc[0]
        total = row['total']
        
        data = [
            {'统计项': '大爆冷（🔴）', '出现场次': int(row['big_upset']), '占比': f"{row['big_upset']/total*100:.1f}%", '历史概率': '13.3%', '差值': f"{(row['big_upset']/total - 0.133)*100:+.1f}%"},
            {'统计项': '小爆冷（🟡）', '出现场次': int(row['small_upset']), '占比': f"{row['small_upset']/total*100:.1f}%", '历史概率': '40.0%', '差值': f"{(row['small_upset']/total - 0.40)*100:+.1f}%"},
            {'统计项': '不爆冷（🟢）', '出现场次': int(row['no_upset']), '占比': f"{row['no_upset']/total*100:.1f}%", '历史概率': '46.7%', '差值': f"{(row['no_upset']/total - 0.467)*100:+.1f}%"}
        ]
        return pd.DataFrame(data)
    
    def _hit_rate_stats(self):
        conn = sqlite3.connect(self.db_path)
        stats = pd.read_sql_query("""
            SELECT 
                SUM(CASE WHEN hit_prediction = 1 THEN 1 ELSE 0 END) as hit_1,
                SUM(CASE WHEN hit_prediction = 2 THEN 1 ELSE 0 END) as hit_2,
                SUM(CASE WHEN hit_prediction = 3 THEN 1 ELSE 0 END) as hit_3,
                SUM(CASE WHEN hit_prediction > 0 THEN 1 ELSE 0 END) as total_hit,
                COUNT(*) as total
            FROM matches WHERE actual_result IS NOT NULL AND prediction_1 IS NOT NULL
        """, conn)
        conn.close()
        
        if stats.empty or stats.iloc[0]['total'] == 0:
            return pd.DataFrame({'提示': ['暂无核对数据']})
        
        row = stats.iloc[0]
        total = row['total']
        
        data = [
            {'预测命中': '1号预测命中（首选）', '命中场数': int(row['hit_1']), '命中率': f"{row['hit_1']/total*100:.1f}%"},
            {'预测命中': '2号预测命中（次选）', '命中场数': int(row['hit_2']), '命中率': f"{row['hit_2']/total*100:.1f}%"},
            {'预测命中': '3号预测命中（备选）', '命中场数': int(row['hit_3']), '命中率': f"{row['hit_3']/total*100:.1f}%"},
            {'预测命中': '总命中场数', '命中场数': int(row['total_hit']), '命中率': f"{row['total_hit']/total*100:.1f}%"}
        ]
        return pd.DataFrame(data)
    
    def _upset_scores_top5(self):
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("""
            SELECT actual_result, COUNT(*) as count
            FROM matches WHERE is_upset IN (1, 2) AND actual_result IS NOT NULL
            GROUP BY actual_result ORDER BY count DESC LIMIT 5
        """, conn)
        
        totals = pd.read_sql_query("SELECT COUNT(*) as cnt FROM matches WHERE is_upset IN (1,2) AND actual_result IS NOT NULL", conn)
        total_matches = pd.read_sql_query("SELECT COUNT(*) as cnt FROM matches WHERE actual_result IS NOT NULL", conn)
        conn.close()
        
        if df.empty:
            return pd.DataFrame({'提示': ['暂无爆冷比分数据']})
        
        upset_total = totals.iloc[0]['cnt'] if not totals.empty else 1
        all_matches = total_matches.iloc[0]['cnt'] if not total_matches.empty else 1
        
        data = []
        for idx, row in df.iterrows():
            data.append({
                '排名': idx + 1, '比分': row['actual_result'], '出现场数': int(row['count']),
                '占爆冷比例': f"{row['count']/upset_total*100:.1f}%", '占总场数比例': f"{row['count']/all_matches*100:.1f}%"
            })
        return pd.DataFrame(data)
    
    def _normal_scores_top5(self):
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("""
            SELECT actual_result, COUNT(*) as count
            FROM matches WHERE is_upset = 0 AND actual_result IS NOT NULL
            GROUP BY actual_result ORDER BY count DESC LIMIT 5
        """, conn)
        
        totals = pd.read_sql_query("SELECT COUNT(*) as cnt FROM matches WHERE is_upset = 0 AND actual_result IS NOT NULL", conn)
        total_matches = pd.read_sql_query("SELECT COUNT(*) as cnt FROM matches WHERE actual_result IS NOT NULL", conn)
        conn.close()
        
        if df.empty:
            return pd.DataFrame({'提示': ['暂无正常比分数据']})
        
        normal_total = totals.iloc[0]['cnt'] if not totals.empty else 1
        all_matches = total_matches.iloc[0]['cnt'] if not total_matches.empty else 1
        
        data = []
        for idx, row in df.iterrows():
            data.append({
                '排名': idx + 1, '比分': row['actual_result'], '出现场数': int(row['count']),
                '占不爆冷比例': f"{row['count']/normal_total*100:.1f}%", '占总场数比例': f"{row['count']/all_matches*100:.1f}%"
            })
        return pd.DataFrame(data)
    
    def _avg_odds(self):
        conn = sqlite3.connect(self.db_path)
        avg = pd.read_sql_query("""
            SELECT 
                AVG(CASE WHEN is_upset = 2 THEN MAX(odds_home, odds_draw, odds_away) END) as avg_big,
                AVG(CASE WHEN is_upset = 1 THEN (odds_home + odds_draw + odds_away - MIN(odds_home, odds_draw, odds_away) - MAX(odds_home, odds_draw, odds_away)) END) as avg_small,
                AVG(CASE WHEN is_upset = 0 THEN MIN(odds_home, odds_draw, odds_away) END) as avg_no
            FROM matches WHERE actual_result IS NOT NULL
        """, conn)
        conn.close()
        
        data = [
            {'类型': '大爆冷（🔴）', '平均赔率': f"{avg.iloc[0]['avg_big']:.4f}" if pd.notna(avg.iloc[0]['avg_big']) else '5.4383', '说明': '对应最大赔率', '基准赔率': '5.4383'},
            {'类型': '小爆冷（🟡）', '平均赔率': f"{avg.iloc[0]['avg_small']:.4f}" if pd.notna(avg.iloc[0]['avg_small']) else '3.7508', '说明': '对应第二大赔率', '基准赔率': '3.7508'},
            {'类型': '不爆冷（🟢）', '平均赔率': f"{avg.iloc[0]['avg_no']:.4f}" if pd.notna(avg.iloc[0]['avg_no']) else '1.6033', '说明': '对应最小赔率', '基准赔率': '1.6033'}
        ]
        return pd.DataFrame(data)
    
    def _mix_parlay_ev(self):
        conn = sqlite3.connect(self.db_path)
        probs = pd.read_sql_query("""
            SELECT 
                CAST(SUM(CASE WHEN is_upset = 2 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) as p_big,
                CAST(SUM(CASE WHEN is_upset = 1 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) as p_small
            FROM matches WHERE actual_result IS NOT NULL
        """, conn)
        conn.close()
        
        p_big = probs.iloc[0]['p_big'] if not probs.empty and pd.notna(probs.iloc[0]['p_big']) else 0.133
        p_small = probs.iloc[0]['p_small'] if not probs.empty and pd.notna(probs.iloc[0]['p_small']) else 0.40
        odds_big, odds_small = 5.4383, 3.7508
        
        modes = [('2,3关', 2), ('3,4关', 3), ('4,5关', 4), ('2,3,4关', 2)]
        data = []
        
        for mode_name, min_hit in modes:
            exp_big = (p_big ** min_hit) * (odds_big ** min_hit) * 2
            exp_small = (p_small ** min_hit) * (odds_small ** min_hit) * 2
            
            data.append({
                '模式': mode_name, '最低命中': min_hit, '大爆冷概率': f"{p_big*100:.1f}%",
                '大爆冷期望': f"{exp_big:.2f}", '大爆冷EV比': f"{exp_big/2:.2f}",
                '小爆冷概率': f"{p_small*100:.1f}%", '小爆冷期望': f"{exp_small:.2f}", '小爆冷EV比': f"{exp_small/2:.2f}"
            })
        return pd.DataFrame(data)
