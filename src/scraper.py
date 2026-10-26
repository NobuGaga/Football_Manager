"""体彩官方公开接口抓取入口。手动导入仍是稳定降级路径。"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import requests

from importers import FixtureImporter
from utils import clean_text, utc_now


class SportteryScraper:
    api_url = "https://webapi.sporttery.cn/gateway/uniform/football/getMatchCalculatorV1.qry"
    page_url = "https://m.sporttery.cn/mjc/jsq/zqhhgg/"
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Referer": page_url,
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    }

    def __init__(self, db_path: str | Path = "data/football.db", timeout: float = 30, session=None):
        self.db_path = str(db_path)
        self.timeout = timeout
        self.session = session or requests.Session()

    def fetch_daily_matches(self, date: str | None = None) -> pd.DataFrame:
        """按体彩营业日期获取对阵；指定日期没有有效对阵时显式失败。"""
        try:
            response = self.session.get(
                self.api_url, params={"channel": "c"}, headers=self.headers, timeout=self.timeout,
            )
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as exc:
            raise RuntimeError(f"体彩官方接口请求失败：{exc}；请稍后重试或改用手动对阵导入") from exc

        snapshot = self._save_snapshot(payload)
        if not isinstance(payload, dict) or not payload.get("success") or str(payload.get("errorCode")) != "0":
            message = payload.get("errorMessage", "响应格式不正确") if isinstance(payload, dict) else "响应不是 JSON 对象"
            raise RuntimeError(f"体彩官方接口返回失败：{message}；原始快照：{snapshot}")
        value = payload.get("value")
        groups = value.get("matchInfoList") if isinstance(value, dict) else None
        if not isinstance(groups, list):
            raise RuntimeError(f"体彩官方接口缺少 matchInfoList；原始快照：{snapshot}")

        matches, errors, seen = [], [], set()
        for group in groups:
            if not isinstance(group, dict):
                errors.append("对阵分组格式不正确")
                continue
            business_date = clean_text(group.get("businessDate"))
            if date and business_date != date:
                continue
            items = group.get("subMatchList")
            if not isinstance(items, list):
                errors.append(f"营业日期 {business_date or '未知'} 缺少 subMatchList")
                continue
            for item in items:
                try:
                    parsed = self._parse_api_match(item, business_date)
                    identity = (parsed["match_code"], parsed["match_time"], parsed["home_team"], parsed["away_team"])
                    if identity in seen:
                        raise ValueError(f"官方对阵重复：{parsed['match_code']} {parsed['home_team']} VS {parsed['away_team']}")
                    seen.add(identity)
                    matches.append(parsed)
                except (TypeError, ValueError) as exc:
                    errors.append(str(exc))
        if errors:
            raise RuntimeError(f"官方对阵校验失败，未导入任何记录：{errors[:3]}；原始快照：{snapshot}")
        if not matches:
            scope = f"营业日期 {date} " if date else ""
            raise RuntimeError(f"体彩官方接口{scope}没有有效对阵；原始快照：{snapshot}。请核对日期或改用手动对阵导入")

        result = FixtureImporter(self.db_path).import_rows(matches, default_date=date, source="sporttery_web")
        if result["errors"]:
            raise RuntimeError(f"部分官方对阵导入失败：{result['errors'][:3]}；原始快照：{snapshot}")
        return pd.DataFrame(matches)

    def _save_snapshot(self, payload: object) -> Path:
        snapshot_dir = Path(self.db_path).parent / "snapshots"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        snapshot = snapshot_dir / f"sporttery_{utc_now().replace(':', '').replace('+', '_')}.json"
        snapshot.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return snapshot

    @staticmethod
    def _parse_api_match(item: dict, business_date: str) -> dict:
        if not isinstance(item, dict):
            raise ValueError("对阵记录格式不正确")
        code = clean_text(item.get("matchNumStr"))
        league = clean_text(item.get("leagueAllName")) or clean_text(item.get("leagueAbbName"))
        home = clean_text(item.get("homeTeamAllName")) or clean_text(item.get("homeTeamAbbName"))
        away = clean_text(item.get("awayTeamAllName")) or clean_text(item.get("awayTeamAbbName"))
        match_date = clean_text(item.get("matchDate"))
        match_time = clean_text(item.get("matchTime"))
        missing = [name for name, value in (
            ("竞彩编号", code), ("联赛", league), ("主队", home), ("客队", away),
            ("开赛日期", match_date), ("开赛时间", match_time),
        ) if not value]
        if missing:
            raise ValueError(f"官方对阵缺少必要字段 {missing}：{code or '未编号'}")
        had = item.get("had") or {}
        hhad = item.get("hhad") or {}
        if not isinstance(had, dict) or not isinstance(hhad, dict):
            raise ValueError(f"赔率格式不正确：{code}")
        return {
            "match_code": code,
            "match_date": business_date,
            "league": league,
            "home_team": home,
            "away_team": away,
            "match_time": f"{match_date} {match_time[:5]}",
            "timezone": "Asia/Shanghai",
            "handicap": clean_text(hhad.get("goalLine"), "0"),
            "match_status": "pending",
            "odds_home": had.get("h"),
            "odds_draw": had.get("d"),
            "odds_away": had.get("a"),
            "odds_handicap_home": hhad.get("h"),
            "odds_handicap_draw": hhad.get("d"),
            "odds_handicap_away": hhad.get("a"),
            "source_url": f"{SportteryScraper.api_url}?channel=c",
        }

    def fetch_results(self):
        raise NotImplementedError("赛果网页结构不稳定，请使用 import-results 导入官方赛果，避免错误关联")
