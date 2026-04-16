#!/usr/bin/env python3
"""
足球预测自动化系统主程序
Usage:
    python3 main.py init      # 初始化数据库
    python3 main.py scrape    # 抓取当日比赛
    python3 main.py predict   # 生成预测
    python3 main.py check     # 核对结果
    python3 main.py report    # 生成报表
    python3 main.py all       # 执行完整流程
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from scraper import SportteryScraper
from predictor import FootballPredictor
from result_checker import ResultChecker
from reporter import ReportGenerator

def init_db():
    """初始化数据库"""
    os.makedirs('data', exist_ok=True)
    import sqlite3
    conn = sqlite3.connect('data/football.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_code TEXT UNIQUE,
            league TEXT,
            home_team TEXT,
            away_team TEXT,
            match_time DATETIME,
            handicap TEXT,
            odds_home REAL,
            odds_draw REAL,
            odds_away REAL,
            odds_handicap_home REAL,
            odds_handicap_draw REAL,
            odds_handicap_away REAL,
            prediction_1 TEXT,
            prediction_2 TEXT,
            prediction_3 TEXT,
            actual_result TEXT,
            match_status TEXT DEFAULT 'pending',
            is_upset INTEGER DEFAULT 0,
            hit_prediction INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_match_time ON matches(match_time)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_match_code ON matches(match_code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_status ON matches(match_status)')
    
    conn.commit()
    conn.close()
    print("✅ 数据库初始化完成")

def scrape():
    print("🕷️ 正在抓取比赛数据...")
    scraper = SportteryScraper()
    df = scraper.fetch_daily_matches()
    print(f"✅ 抓取完成，共 {len(df)} 场比赛")

def predict():
    print("🤖 正在生成AI预测...")
    predictor = FootballPredictor()
    preds = predictor.predict_all()
    print(f"✅ 预测完成，共 {len(preds)} 场比赛")

def check():
    print("🔍 正在核对预测结果...")
    checker = ResultChecker()
    results = checker.check_all()
    print(f"✅ 核对完成，共 {len(results)} 场比赛")

def report():
    print("📊 正在生成统计报表...")
    reporter = ReportGenerator()
    path = reporter.generate_all_reports()
    print(f"✅ 报表已生成: {path}")

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == 'init':
        init_db()
    elif command == 'scrape':
        scrape()
    elif command == 'predict':
        predict()
    elif command == 'check':
        check()
    elif command == 'report':
        report()
    elif command == 'all':
        scrape()
        predict()
        check()
        report()
    else:
        print(f"❌ 未知命令: {command}")
        print(__doc__)
        sys.exit(1)

if __name__ == '__main__':
    main()
