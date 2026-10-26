"""Kimi 可读取的每日报告与回测 Excel 报表。"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from backtest import Backtester
from database import Database
from utils import utc_now


class ReportGenerator:
    def __init__(self, db_path: str | Path = "data/football.db", output_dir: str | Path = "reports"):
        self.db = Database(db_path)
        self.db.initialize()
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def daily_items(self, date: str) -> list[dict]:
        with self.db.connect() as conn:
            rows = conn.execute("""SELECT f.*,p.id AS prediction_id,p.model_version,p.predicted_at,p.input_as_of,
                p.probability_home,p.probability_draw,p.probability_away,p.expected_home_goals,p.expected_away_goals,
                p.top_scores_json,p.score_probabilities_json,p.data_completeness,p.risk_flags_json,p.analysis_json,
                r.actual_score,r.outcome,r.review_note
                FROM fixtures f JOIN predictions p ON p.fixture_key=f.fixture_key
                LEFT JOIN results r ON r.fixture_key=f.fixture_key
                WHERE f.match_date=? ORDER BY f.start_time_utc,f.match_code,p.predicted_at""", (date,)).fetchall()
        latest: dict[str, dict] = {}
        for row in rows:
            item = dict(row)
            start = datetime.fromisoformat(item["start_time_utc"]).astimezone(ZoneInfo(item["timezone"]))
            top_scores = json.loads(item.pop("top_scores_json"))
            score_probabilities = json.loads(item.pop("score_probabilities_json"))
            analysis = json.loads(item.pop("analysis_json"))
            item.update({
                "score_probabilities": score_probabilities,
                "basic_info": {key: item[key] for key in ("match_code", "league", "home_team", "away_team", "match_date")},
                "start_time_local": start.isoformat(timespec="minutes"),
                "data_status": {"updated_at": item["updated_at"], "input_as_of": item["input_as_of"],
                                "completeness": item["data_completeness"], "feature_source": analysis.get("feature_source")},
                "probabilities": {"home_win": item["probability_home"], "draw": item["probability_draw"],
                                  "away_win": item["probability_away"]},
                "expected_goals": {"home": item["expected_home_goals"], "away": item["expected_away_goals"]},
                "top_scores": top_scores,
                "analysis": analysis,
                "risk_flags": json.loads(item.pop("risk_flags_json")),
                "model_info": {"version": item["model_version"], "predicted_at": item["predicted_at"]},
                "post_match": {"actual_score": item["actual_score"], "outcome": item["outcome"],
                               "review_note": item["review_note"]},
            })
            latest[item["fixture_key"]] = item
        return list(latest.values())

    def generate_daily_report(self, date: str) -> dict[str, str]:
        items = self.daily_items(date)
        stem = f"竞彩足球分析_{date}"
        json_path = self.output_dir / f"{stem}.json"
        csv_path = self.output_dir / f"{stem}.csv"
        md_path = self.output_dir / f"{stem}.md"
        xlsx_path = self.output_dir / f"{stem}.xlsx"
        document = {"report_date": date, "generated_at": utc_now(), "match_count": len(items),
                    "disclaimer": "概率估计仅供分析，不构成收益保证或确定性投注建议。", "matches": items}
        json_path.write_text(json.dumps(document, ensure_ascii=False, indent=2), encoding="utf-8")
        rows = [self._flat_item(item) for item in items]
        frame = pd.DataFrame(rows)
        frame.to_csv(csv_path, index=False, encoding="utf-8-sig")
        frame.to_excel(xlsx_path, index=False, sheet_name="每日预测")
        md_path.write_text(self._daily_markdown(document), encoding="utf-8")
        return {"json": str(json_path), "csv": str(csv_path), "markdown": str(md_path), "excel": str(xlsx_path)}

    def generate_all_reports(self) -> str:
        result = Backtester(self.db.path).evaluate()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        excel_path = self.output_dir / f"预测回测报表_{timestamp}.xlsx"
        metrics = pd.DataFrame([{"联赛": league, **values} for league, values in result["metrics"].items()])
        calibration = pd.DataFrame(result["calibration"])
        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            metrics.to_excel(writer, sheet_name="概率质量与命中指标", index=False)
            calibration.to_excel(writer, sheet_name="概率校准", index=False)
        json_path = excel_path.with_suffix(".json")
        json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        return str(excel_path)

    @staticmethod
    def _flat_item(item: dict) -> dict:
        basic = item["basic_info"]
        probs = item["probabilities"]
        expected = item["expected_goals"]
        return {
            "竞彩编号": basic["match_code"], "比赛日期": basic["match_date"], "联赛": basic["league"],
            "主队": basic["home_team"], "客队": basic["away_team"], "开赛时间": item["start_time_local"],
            "数据完整度": item["data_completeness"], "数据时间": item["input_as_of"],
            "主胜概率": probs["home_win"], "平局概率": probs["draw"], "客胜概率": probs["away_win"],
            "主队预期进球": expected["home"], "客队预期进球": expected["away"],
            "高概率比分": "/".join(f"{v['score']}({v['probability']:.2%})" for v in item["top_scores"]),
            "风险提示": "；".join(item["risk_flags"]), "模型版本": item["model_version"],
            "预测生成时间": item["predicted_at"], "实际比分": item["actual_score"] or "",
            "复盘备注": item["review_note"] or "",
        }

    @staticmethod
    def _daily_markdown(document: dict) -> str:
        lines = [f"# 竞彩足球批量分析（{document['report_date']}）", "",
                 f"生成时间：{document['generated_at']}  ", f"比赛数量：{document['match_count']}", "",
                 "> 比分与胜平负均为概率估计；数据不足时降低结论强度，不构成收益保证。", ""]
        if not document["matches"]:
            lines.append("当日暂无已保存的预测记录。")
        for item in document["matches"]:
            basic = item["basic_info"]
            probs = item["probabilities"]
            expected = item["expected_goals"]
            lines += [f"## {basic['match_code']}｜{basic['league']}｜{basic['home_team']} VS {basic['away_team']}", "",
                      f"- **开赛时间**：{item['start_time_local']}（{item['timezone']}）",
                      f"- **数据状态**：完整度 {item['data_completeness']:.0%}；输入截至 {item['input_as_of']}；来源 {item['data_status']['feature_source']}",
                      f"- **胜平负概率**：主胜 {probs['home_win']:.1%}／平局 {probs['draw']:.1%}／客胜 {probs['away_win']:.1%}",
                      f"- **预期进球**：主队 {expected['home']:.2f}／客队 {expected['away']:.2f}",
                      "- **高概率比分**：" + "；".join(f"{v['score']}（{v['probability']:.1%}）" for v in item["top_scores"]),
                      f"- **分析依据**：{item['analysis']['method']}；赛前特征 {len(item['analysis']['evidence'])} 项",
                      "- **风险提示**：" + ("；".join(item["risk_flags"]) if item["risk_flags"] else "仍需关注临场阵容、轮换与赛程变化"),
                      f"- **模型信息**：{item['model_version']}；生成于 {item['predicted_at']}",
                      f"- **赛后字段**：实际比分 {item['actual_score'] or '待补录'}；复盘 {item['review_note'] or '待补录'}", ""]
        return "\n".join(lines)
