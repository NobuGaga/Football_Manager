import pandas as pd
import sqlite3
from datetime import datetime
import re
import time
import os

class SportteryScraper:
    def __init__(self, db_path='data/football.db'):
        self.db_path = db_path
        self.base_url = "https://m.sporttery.cn/mjc/jsq/zqhhgg/"
        self.result_url = "https://m.sporttery.cn/mjc/zqsj/?tab=result"
        
        # 使用项目目录下的 chromedriver
        self.chromedriver_path = os.path.join(os.path.dirname(__file__), '..', 'chromedriver')
    
    def fetch_daily_matches(self):
        """使用 Selenium 抓取数据"""
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.service import Service
            from bs4 import BeautifulSoup
            
            print("🌐 启动浏览器...")
            options = webdriver.ChromeOptions()
            options.add_argument('--headless')
            options.add_argument('--disable-gpu')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--window-size=1920,1080')
            options.add_argument('user-agent=Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15')
            
            # 使用本地 chromedriver
            service = Service(self.chromedriver_path)
            driver = webdriver.Chrome(service=service, options=options)
            
            print(f"🕷️ 正在访问 {self.base_url}")
            driver.get(self.base_url)
            
            print("⏳ 等待数据加载...")
            time.sleep(5)  # 给JS足够时间渲染
            
            html = driver.page_source
            driver.quit()
            
            # 解析HTML
            soup = BeautifulSoup(html, 'html.parser')
            
            # 调试：保存HTML查看结构
            with open('debug_page.html', 'w', encoding='utf-8') as f:
                f.write(html)
            print("📝 已保存页面结构到 debug_page.html")
            
            matches = []
            match_items = soup.find_all('div', class_=re.compile('match-item|game-item|match-box|bet-item|jc-item'))
            
            if not match_items:
                print(f"⚠️ 未找到比赛数据，尝试备用 class...")
                # 尝试其他可能的 class 名
                match_items = soup.find_all('div', class_=lambda x: x and any(keyword in ' '.join(x) if isinstance(x, list) else x for keyword in ['match', 'game', 'race', 'event']))
            
            print(f"🔍 找到 {len(match_items)} 个潜在比赛条目")
            
            for item in match_items:
                try:
                    match_data = self._parse_match_item(item)
                    if match_data:
                        matches.append(match_data)
                except Exception as e:
                    print(f"⚠️ 解析单场失败: {e}")
                    continue
            
            if not matches:
                print("⚠️ 没有解析到有效比赛数据")
                return pd.DataFrame()
            
            df = pd.DataFrame(matches)
            
            # 转换数据类型，确保能存入数据库
            df['match_time'] = pd.to_datetime(df['match_time'])
            df['odds_home'] = pd.to_numeric(df['odds_home'], errors='coerce').fillna(0)
            df['odds_draw'] = pd.to_numeric(df['odds_draw'], errors='coerce').fillna(0)
            df['odds_away'] = pd.to_numeric(df['odds_away'], errors='coerce').fillna(0)
            
            print(f"✅ 解析完成，准备保存 {len(df)} 场比赛")
            self._save_to_db(df)
            print(f"✅ 抓取完成，共 {len(df)} 场比赛")
            return df
            
        except ImportError:
            print("❌ 未安装 Selenium，请运行: pip3 install selenium")
            return pd.DataFrame()
        except Exception as e:
            print(f"❌ 抓取失败: {e}")
            import traceback
            traceback.print_exc()
            return pd.DataFrame()
    
    def _parse_match_item(self, item):
        """解析单场比赛HTML"""
        from bs4 import BeautifulSoup
        
        # 提取场次代码
        match_code = item.find('span', class_=re.compile('match-code|match-num|code|serial|no'))
        match_code = match_code.text.strip() if match_code else ''
        
        # 提取联赛
        league = item.find('span', class_=re.compile('league-name|league|competition|match-type'))
        league = league.text.strip() if league else '未知联赛'
        
        # 提取对阵
        teams = item.find('div', class_=re.compile('teams|match-teams|vs-box|duizhen|home-away'))
        home_team, away_team = '主队', '客队'
        if teams:
            vs_text = teams.get_text(strip=True)
            # 尝试多种分割方式
            for sep in [' VS ', ' vs ', 'VS', 'vs', ' - ', '–', ' VS']:
                if sep in vs_text:
                    parts = vs_text.split(sep)
                    if len(parts) == 2:
                        home_team = parts[0].strip()
                        away_team = parts[1].strip()
                        break
        
        # 提取时间
        time_elem = item.find('span', class_=re.compile('match-time|time|date|kssj'))
        match_time = self._parse_time(time_elem.text if time_elem else '')
        
        # 提取赔率（多种可能的class名）
        odds_elems = item.find_all('span', class_=re.compile('odds|spf-odds|rate|pei|pl'))
        odds = [0.0, 0.0, 0.0]
        for i, elem in enumerate(odds_elems[:3]):
            try:
                text = elem.text.strip()
                if text and text.replace('.', '').isdigit():
                    odds[i] = float(text)
            except:
                pass
        
        return {
            'match_code': match_code,
            'league': league,
            'home_team': home_team,
            'away_team': away_team,
            'match_time': match_time,
            'handicap': '0',
            'odds_home': odds[0],
            'odds_draw': odds[1],
            'odds_away': odds[2],
            'odds_handicap_home': 0.0,
            'odds_handicap_draw': 0.0,
            'odds_handicap_away': 0.0
        }
    
    def _parse_time(self, time_str):
        """解析时间字符串，返回 datetime 对象"""
        try:
            current_year = datetime.now().year
            if time_str and '-' in time_str:
                # 尝试多种格式
                for fmt in [f"{current_year}-%m-%d %H:%M", "%Y-%m-%d %H:%M", "%m-%d %H:%M"]:
                    try:
                        dt = datetime.strptime(time_str.strip(), fmt)
                        return dt
                    except:
                        continue
            # 如果解析失败，返回当前时间
            return datetime.now()
        except:
            return datetime.now()
    
    def _save_to_db(self, df):
        """保存到数据库，正确处理数据类型"""
        if df.empty:
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for _, row in df.iterrows():
            # 转换 datetime 为字符串格式存入 SQLite
            match_time_str = row['match_time'].strftime('%Y-%m-%d %H:%M:%S') if pd.notna(row['match_time']) else datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            cursor.execute("""
                INSERT OR REPLACE INTO matches 
                (match_code, league, home_team, away_team, match_time, handicap,
                 odds_home, odds_draw, odds_away, 
                 odds_handicap_home, odds_handicap_draw, odds_handicap_away,
                 created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                str(row['match_code']) if pd.notna(row['match_code']) else '',
                str(row['league']) if pd.notna(row['league']) else '',
                str(row['home_team']) if pd.notna(row['home_team']) else '',
                str(row['away_team']) if pd.notna(row['away_team']) else '',
                match_time_str,  # 转为字符串
                str(row['handicap']) if pd.notna(row['handicap']) else '0',
                float(row['odds_home']),
                float(row['odds_draw']),
                float(row['odds_away']),
                float(row['odds_handicap_home']),
                float(row['odds_handicap_draw']),
                float(row['odds_handicap_away'])
            ))
        
        conn.commit()
        conn.close()
        print(f"💾 已保存 {len(df)} 条比赛数据到数据库")
    
    def fetch_results(self):
        print("📝 结果抓取功能待实现")
        return []
