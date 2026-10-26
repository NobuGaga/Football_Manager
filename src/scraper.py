"""体彩公开网页的可降级抓取入口。手动导入是稳定主路径。"""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from importers import FixtureImporter
from utils import clean_text, utc_now


class SportteryScraper:
    def __init__(self, db_path: str | Path = "data/football.db"):
        self.db_path = str(db_path)
        self.base_url = "https://m.sporttery.cn/mjc/jsq/zqhhgg/"
        self.result_url = "https://m.sporttery.cn/mjc/zqsj/?tab=result"

    def fetch_daily_matches(self, date: str | None = None) -> pd.DataFrame:
        try:
            from bs4 import BeautifulSoup
            from selenium import webdriver
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
        except ImportError as exc:
            raise RuntimeError("自动抓取需要 beautifulsoup4 和 selenium；也可直接使用 import-fixtures 手动导入") from exc
        options = webdriver.ChromeOptions()
        for argument in ("--headless", "--disable-gpu", "--no-sandbox", "--disable-dev-shm-usage", "--window-size=1920,1080"):
            options.add_argument(argument)
        driver = None
        try:
            # 不捆绑特定平台 chromedriver，由 Selenium Manager 匹配本机浏览器。
            driver = webdriver.Chrome(options=options)
            driver.get(self.base_url)
            WebDriverWait(driver, 15).until(lambda browser: browser.find_elements(
                By.CSS_SELECTOR, "div[class*='match'],div[class*='game'],div[class*='bet-item'],div[class*='jc-item']"))
            html = driver.page_source
        finally:
            if driver is not None:
                driver.quit()
        snapshot_dir = Path(self.db_path).parent / "snapshots"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        snapshot = snapshot_dir / f"sporttery_{utc_now().replace(':', '').replace('+', '_')}.html"
        snapshot.write_text(html, encoding="utf-8")
        soup = BeautifulSoup(html, "html.parser")
        items = soup.find_all("div", class_=re.compile("match-item|game-item|match-box|bet-item|jc-item"))
        matches = []
        errors = []
        for item in items:
            try:
                parsed = self._parse_match_item(item)
                if parsed:
                    matches.append(parsed)
            except ValueError as exc:
                errors.append(str(exc))
        if not matches:
            raise RuntimeError(f"网页未解析出有效对阵，原始快照：{snapshot}。请改用手动对阵导入。{errors[:3]}")
        result = FixtureImporter(self.db_path).import_rows(matches, default_date=date, source="sporttery_web")
        if result["errors"]:
            raise RuntimeError(f"部分网页对阵导入失败：{result['errors'][:3]}")
        return pd.DataFrame(matches)

    def _parse_match_item(self, item) -> dict | None:
        text = item.get_text(" ", strip=True)
        if not text:
            return None
        code_element = item.find("span", class_=re.compile("match-code|match-num|code|serial|no"))
        league_element = item.find("span", class_=re.compile("league-name|league|competition|match-type"))
        teams = item.find("div", class_=re.compile("teams|match-teams|vs-box|duizhen|home-away"))
        if not teams:
            return None
        pairing = teams.get_text(" ", strip=True)
        for separator in (" VS ", " vs ", "VS", "vs", " - ", "–", "—"):
            if separator in pairing:
                home, away = (part.strip() for part in pairing.split(separator, 1))
                break
        else:
            raise ValueError(f"对阵格式无法识别：{pairing[:80]}")
        time_element = item.find("span", class_=re.compile("match-time|time|date|kssj"))
        if not time_element or not clean_text(time_element.text):
            raise ValueError("缺少开赛时间，不使用当前时间代替")
        odds = []
        for element in item.find_all("span", class_=re.compile("odds|spf-odds|rate|pei|pl"))[:3]:
            try:
                odds.append(float(element.text.strip()))
            except ValueError:
                odds.append(None)
        odds += [None] * (3 - len(odds))
        return {
            "match_code": clean_text(code_element.text if code_element else "", "未编号"),
            "league": clean_text(league_element.text if league_element else "", "未知联赛"),
            "home_team": home, "away_team": away,
            "match_time": clean_text(time_element.text), "handicap": "0",
            "odds_home": odds[0], "odds_draw": odds[1], "odds_away": odds[2],
        }

    def fetch_results(self):
        raise NotImplementedError("赛果网页结构不稳定，请使用 import-results 导入官方赛果，避免错误关联")
