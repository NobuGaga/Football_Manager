import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from datetime import datetime
import re

class SportteryScraper:
    def __init__(self, db_path='data/football.db'):
        self.db_path = db_path
        self.base_url = "https://m.sporttery.cn/mjc/jsq/zqhhgg/"
        self.result_url = "https://m.sporttery.cn/mjc/zqsj/?tab=result"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://m.sporttery.cn/'
        }
    
    def fetch_daily_matches(self):
        try:
            response = requests.get(self.base_url, headers=self.headers, timeout=15)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')
            
            matches = []
            match_items = soup.find_all('div', class_=re.compile('match-item|game-item|match-box'))
            
            for item in match_items:
                try:
                    match_data = self._parse_match_item(item)
                    if match_data:
                        matches.append(match_data)
                except Exception as e:
                    print(f"⚠️ 解析单场失败: {e}")
                    continue
            
            if not matches:
                print("⚠️ 未解析到数据，尝试备用解析方案...")
                return pd.DataFrame()
            
            df = pd.DataFrame(matches)
            self._save_to_db(df)
            return df
            
        except Exception as e:
            print(f"❌ 抓取失败: {e}")
            return pd.DataFrame()
    
    def _parse_match_item(self, item):
        try:
            match_code = item.find('span', class_=re.compile('match-code|match-num'))
            match_code = match_code.text.strip() if match_code else ''
            
            league = item.find('span', class_=re.compile('league-name|league'))
            league = league.text.strip() if league else '未知联赛'
            
            teams = item.find('div', class_=re.compile('teams|match-teams'))
            home_team, away_team = '主队', '客队'
            if teams:
                vs_text = teams.get_text(strip=True)
                if 'VS' in vs_text:
                    parts = vs_text.split('VS')
                    home_team = parts[0].strip()
                    away_team = parts[1].strip()
            
            time_elem = item.find('span', class_=re.compile('match-time|time'))
            match_time = self._parse_time(time_elem.text if time_elem else '')
            
            handicap_elem = item.find('span', class_=re.compile('handicap|let-ball'))
            handicap = handicap_elem.text.strip() if handicap_elem else '0'
            
            odds_elems = item.find_all('span', class_=re.compile('odds|spf-odds'))
            odds_home = float(odds_elems[0].text) if len(odds_elems) > 0 else 0.0
            odds_draw = float(odds_elems[1].text) if len(odds_elems) > 1 else 0.0
            odds_away = float(odds_elems[2].text) if len(odds_elems) > 2 else 0.0
            
            handicap_odds_elems = item.find_all('span', class_=re.compile('handicap-odds|rq-odds'))
            odds_handicap_home = float(handicap_odds_elems[0].text) if len(handicap_odds_elems) > 0 else 0.0
            odds_handicap_draw = float(handicap_odds_elems[1].text) if len(handicap_odds_elems) > 1 else 0.0
            odds_handicap_away = float(handicap_odds_elems[2].text) if len(handicap_odds_elems) > 2 else 0.0
            
            return {
                'match_code': match_code,
                'league': league,
                'home_team': home_team,
                'away_team': away_team,
                'match_time': match_time,
                'handicap': handicap,
                'odds_home': odds_home,
                'odds_draw': odds_draw,
                'odds_away': odds_away,
                'odds_handicap_home': odds_handicap_home,
                'odds_handicap_draw': odds_handicap_draw,
                'odds_handicap_away': odds_handicap_away
            }
        except Exception as e:
            raise Exception(f"解析错误: {e}")
    
    def _parse_time(self, time_str):
        try:
            current_year = datetime.now().year
            if '-' in time_str:
                if len(time_str.split('-')[0]) == 2:
                    dt = datetime.strptime(f"{current_year}-{time_str}", "%Y-%m-%d %H:%M")
                else:
                    dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M")
                return dt
            return datetime.now()
        except:
            return datetime.now()
    
    def _save_to_db(self, df):
        if df.empty:
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT OR REPLACE INTO matches 
                (match_code, league, home_team, away_team, match_time, handicap,
                 odds_home, odds_draw, odds_away, 
                 odds_handicap_home, odds_handicap_draw, odds_handicap_away,
                 created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                row['match_code'], row['league'], row['home_team'], row['away_team'],
                row['match_time'], row['handicap'],
                row['odds_home'], row['odds_draw'], row['odds_away'],
                row['odds_handicap_home'], row['odds_handicap_draw'], row['odds_handicap_away']
            ))
        
        conn.commit()
        conn.close()
        print(f"💾 已保存 {len(df)} 条比赛数据")
    
    def fetch_results(self):
        try:
            response = requests.get(self.result_url, headers=self.headers, timeout=15)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')
            
            results = []
            result_items = soup.find_all('div', class_=re.compile('result-item|finished'))
            
            for item in result_items:
                try:
                    code_elem = item.find('span', class_=re.compile('match-code|match-num'))
                    if not code_elem:
                        continue
                    match_code = code_elem.text.strip()
                    
                    score_elem = item.find('span', class_=re.compile('score|match-score'))
                    if score_elem:
                        actual_result = score_elem.text.strip()
                        self._update_result(match_code, actual_result)
                        results.append({'match_code': match_code, 'score': actual_result})
                except:
                    continue
            
            print(f"✅ 已更新 {len(results)} 场比赛结果")
            return results
            
        except Exception as e:
            print(f"❌ 抓取结果失败: {e}")
            return []
    
    def _update_result(self, match_code, actual_result):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE matches 
            SET actual_result = ?, match_status = 'finished', updated_at = CURRENT_TIMESTAMP
            WHERE match_code = ?
        """, (actual_result, match_code))
        conn.commit()
        conn.close()
