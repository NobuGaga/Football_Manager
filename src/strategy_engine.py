from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from betting_policy import DEFAULT_POLICY_PATH, load_betting_policy, update_policy_calibration
from database import Database
from history import HistoryManager
from reporter import ReportGenerator
from selections import BIG_UPSET, NOT_UPSET, SMALL_UPSET, ai_hit_index
from utils import optional_float, payload_hash, utc_now

OUTCOME_LABELS = {"H": "主胜", "D": "平局", "A": "主负"}
STRATEGY_COLUMNS = [
    "比赛编号", "联赛", "对阵", "开赛时间", "数据完整度", "双选比分", "双选合计概率",
    "正路胜平负", "偏态胜平负", "偏态结果类型", "偏态样本", "偏态历史比例",
    "行动等级", "策略版本", "校准修订", "策略依据", "风险提示",
]


def summarize_history(records: list[dict]) -> dict:
    def empty_group() -> dict:
        return {"samples": 0, "with_scores": 0, NOT_UPSET: 0, SMALL_UPSET: 0, BIG_UPSET: 0,
                "top1_hits": 0, "top2_position_hits": 0, "top3_position_hits": 0,
                "dual_hits": 0, "top3_hits": 0}

    from selections import normalize_league

    groups = defaultdict(empty_group)
    pairs = defaultdict(lambda: {"samples": 0, "dual_hits": 0})
    for record in records:
        league = normalize_league(record["league"])
        for name, group in (("全部", groups["全部"]), (league, groups[league])):
            group["samples"] += 1
            if record["result_type"] in (NOT_UPSET, SMALL_UPSET, BIG_UPSET):
                group[record["result_type"]] += 1
            scores = record["scores"][:3]
            if not scores:
                continue
            group["with_scores"] += 1
            hit = ai_hit_index(scores, record["actual_score"])
            group["top1_hits"] += hit == 1
            group["top2_position_hits"] += hit == 2
            group["top3_position_hits"] += hit == 3
            group["dual_hits"] += bool(hit and hit <= 2)
            group["top3_hits"] += bool(hit)
            if name != "全部" and len(scores) >= 2:
                pair = "/".join(scores[:2])
                pairs[(name, pair)]["samples"] += 1
                pairs[(name, pair)]["dual_hits"] += bool(hit and hit <= 2)

    def finalize(group: dict) -> dict:
        result = dict(group)
        result["upset_rate"] = ((group[SMALL_UPSET] + group[BIG_UPSET]) / group["samples"]
                                if group["samples"] else None)
        for key in ("top1_hits", "top2_position_hits", "top3_position_hits", "dual_hits", "top3_hits"):
            result[key.replace("_hits", "_rate")] = (group[key] / group["with_scores"]
                                                       if group["with_scores"] else None)
        return result

    return {
        "overall": finalize(groups["全部"]),
        "by_league": {name: finalize(group) for name, group in groups.items() if name != "全部"},
        "by_league_score_pair": [
            {"league": league, "score_pair": pair, **values,
             "dual_hit_rate": values["dual_hits"] / values["samples"]}
            for (league, pair), values in sorted(pairs.items())
        ],
    }


class StrategyEngine:
    def __init__(self, db_path: str | Path = "data/football.db", output_dir: str | Path = "reports",
                 policy_path: str | Path = DEFAULT_POLICY_PATH, ai_model: str = "kimi"):
        self.db = Database(db_path)
        self.db.initialize()
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.policy_path = Path(policy_path)
        self.ai_model = ai_model

    def generate(self, date: str) -> dict:
        from selections import normalize_league

        policy = load_betting_policy(self.policy_path)
        config = policy["config"]
        records = HistoryManager(self.db.path).analysis_records()
        history_hash = payload_hash(records)
        history = summarize_history(records)
        items = [item for item in ReportGenerator(self.db.path).daily_items(date)
                 if item["ai_model"] == self.ai_model]
        decisions = [self._decide(item, config, history["by_league"].get(normalize_league(item["league"])))
                     for item in items]
        self._save(decisions, policy["sha256"], history_hash)
        document = {
            "report_date": date, "generated_at": utc_now(), "match_count": len(decisions),
            "strategy_version": config["strategy_version"],
            "calibration_revision": config.get("calibration", {}).get("revision", 0),
            "policy_sha256": policy["sha256"], "history_sha256": history_hash,
            "disclaimer": "双选与方向均为概率分析，不代表已实际下注，不构成收益保证。",
            "matches": decisions,
        }
        stem = f"双选与方向策略_{date}"
        paths = {key: self.output_dir / f"{stem}.{suffix}" for key, suffix in
                 (("json", "json"), ("csv", "csv"), ("markdown", "md"))}
        paths["json"].write_text(json.dumps(document, ensure_ascii=False, indent=2), encoding="utf-8")
        pd.DataFrame([self._flat(item) for item in decisions], columns=STRATEGY_COLUMNS).to_csv(
            paths["csv"], index=False, encoding="utf-8-sig")
        paths["markdown"].write_text(self._markdown(document), encoding="utf-8")
        return {**{key: str(value) for key, value in paths.items()}, "matches": len(decisions),
                "strategy_version": config["strategy_version"], "policy_sha256": policy["sha256"],
                "history_sha256": history_hash}

    def _decide(self, item: dict, config: dict, league_history: dict | None) -> dict:
        odds = json.loads(item["odds_json"] or "{}")
        normal_values = [optional_float(odds.get(key)) for key in ("odds_home", "odds_draw", "odds_away")]
        probabilities = {"H": item["probability_home"], "D": item["probability_draw"], "A": item["probability_away"]}
        reasons = ["双选取已保存比分分布的前两个比分"]
        risks = list(item["risk_flags"])
        normal_outcome = ""
        normal_type = ""
        skew_outcome = ""
        skew_type = ""
        skew_sample = league_history["samples"] if league_history else 0
        skew_rate = None
        if all(value is not None and value > 1 for value in normal_values):
            lowest = min(normal_values)
            normal_candidates = [key for index, key in enumerate(("H", "D", "A")) if normal_values[index] == lowest]
            normal_outcome = max(normal_candidates, key=lambda key: probabilities[key])
            normal_type = NOT_UPSET
            reasons.append("正路方向取正常胜平负赔率最低项")
            if len(normal_candidates) > 1:
                reasons.append("最低赔率并列，取其中模型概率较高的方向")
            eligible = bool(league_history and league_history["samples"] >= int(config["min_history_samples"])
                            and league_history["upset_rate"] is not None
                            and league_history["upset_rate"] >= float(config["skew_min_upset_rate"]))
            if eligible:
                target_type = (SMALL_UPSET if league_history[SMALL_UPSET] >= league_history[BIG_UPSET]
                               else BIG_UPSET)
                candidates = []
                for index, key in enumerate(("H", "D", "A")):
                    value = normal_values[index]
                    if target_type == BIG_UPSET and value == max(normal_values) and value > min(normal_values):
                        candidates.append(key)
                    elif target_type == SMALL_UPSET and min(normal_values) < value < max(normal_values):
                        candidates.append(key)
                if candidates:
                    skew_outcome = max(candidates, key=lambda key: probabilities[key])
                    skew_type = target_type
                    skew_rate = (league_history[SMALL_UPSET] if target_type == SMALL_UPSET
                                 else league_history[BIG_UPSET]) / skew_sample
                    reasons.append(f"偏态使用该联赛 {skew_sample} 场历史中的{target_type}倾向")
                else:
                    risks.append("赔率并列导致偏态结果类型无法唯一映射，偏态观望")
            else:
                reasons.append("联赛偏态样本或偏态比例未达到阈值，偏态观望")
        else:
            risks.append("正常胜平负赔率不完整，正路与偏态方向均观望")
            reasons.append("无完整赔率时不推断爆冷类型")
        scores = item["top_scores"][:2]
        completeness = float(item["data_completeness"])
        if completeness < float(config["min_completeness"]):
            action = "观望"
            risks.append("数据完整度低于可执行策略阈值，仅保留参考")
        else:
            action = "正路与偏态参考" if skew_outcome else "正路参考"
        return {
            "fixture_key": item["fixture_key"], "prediction_id": item["prediction_id"],
            "match_code": item["match_code"], "league": item["league"],
            "home_team": item["home_team"], "away_team": item["away_team"],
            "start_time_local": item["start_time_local"], "data_completeness": completeness,
            "dual_scores": [score["score"] for score in scores],
            "dual_score_probability": sum(score["probability"] for score in scores),
            "normal_outcome": OUTCOME_LABELS.get(normal_outcome, ""), "normal_result_type": normal_type,
            "skew_outcome": OUTCOME_LABELS.get(skew_outcome, ""), "skew_result_type": skew_type,
            "skew_samples": skew_sample, "skew_historical_rate": skew_rate,
            "action": action, "strategy_version": config["strategy_version"],
            "calibration_revision": config.get("calibration", {}).get("revision", 0),
            "reasons": reasons, "risk_flags": list(dict.fromkeys(risks)),
        }

    def _save(self, decisions: list[dict], policy_hash: str, history_hash: str) -> None:
        with self.db.connect() as conn:
            for item in decisions:
                conn.execute("""INSERT OR IGNORE INTO strategy_decisions(
                    fixture_key,prediction_id,strategy_version,policy_sha256,history_sha256,decision_json,generated_at
                ) VALUES(?,?,?,?,?,?,?)""", (
                    item["fixture_key"], item["prediction_id"], item["strategy_version"], policy_hash,
                    history_hash, json.dumps(item, ensure_ascii=False), utc_now(),
                ))

    @staticmethod
    def _flat(item: dict) -> dict:
        return {
            "比赛编号": item["match_code"], "联赛": item["league"],
            "对阵": f"{item['home_team']} VS {item['away_team']}", "开赛时间": item["start_time_local"],
            "数据完整度": item["data_completeness"], "双选比分": "/".join(item["dual_scores"]),
            "双选合计概率": round(item["dual_score_probability"], 8),
            "正路胜平负": item["normal_outcome"], "偏态胜平负": item["skew_outcome"],
            "偏态结果类型": item["skew_result_type"], "偏态样本": item["skew_samples"],
            "偏态历史比例": item["skew_historical_rate"] if item["skew_historical_rate"] is not None else "",
            "行动等级": item["action"], "策略版本": item["strategy_version"],
            "校准修订": item["calibration_revision"], "策略依据": "；".join(item["reasons"]),
            "风险提示": "；".join(item["risk_flags"]),
        }

    @staticmethod
    def _markdown(document: dict) -> str:
        lines = [f"# 双选比分与正路／偏态策略（{document['report_date']}）", "",
                 f"策略版本：{document['strategy_version']}；校准修订：{document['calibration_revision']}", "",
                 f"> {document['disclaimer']}", "",
                 "| 比赛编号 | 对阵 | 双选比分 | 正路胜平负 | 偏态胜平负 | 偏态类型 | 数据完整度 | 行动 |",
                 "|---|---|---|---|---|---|---:|---|"]
        for item in document["matches"]:
            lines.append("| {match_code} | {home_team} VS {away_team} | {dual} | {normal} | {skew} | {skew_type} | {completeness:.0%} | {action} |".format(
                match_code=item["match_code"], home_team=item["home_team"], away_team=item["away_team"],
                dual="/".join(item["dual_scores"]), normal=item["normal_outcome"] or "观望",
                skew=item["skew_outcome"] or "观望", skew_type=item["skew_result_type"] or "—",
                completeness=item["data_completeness"], action=item["action"]))
        return "\n".join(lines) + "\n"


class StrategyValidator:
    def __init__(self, db_path: str | Path = "data/football.db", output_dir: str | Path = "reports",
                 policy_path: str | Path = DEFAULT_POLICY_PATH):
        self.db = Database(db_path)
        self.db.initialize()
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.policy_path = Path(policy_path)

    def generate(self, date: str | None = None, update_policy: bool = False) -> dict:
        date = date or datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat()
        policy = load_betting_policy(self.policy_path)
        records = HistoryManager(self.db.path).analysis_records()
        history_hash = payload_hash(records)
        summary = summarize_history(records)
        midpoint = len(records) // 2
        first_summary = summarize_history(records[:midpoint])
        second_summary = summarize_history(records[midpoint:])
        time_split = {"前半段": first_summary["overall"], "后半段": second_summary["overall"]}
        calibration = policy["config"].get("calibration", {})
        checks = [
            {"item": "历史样本量", "declared": calibration.get("samples", 0),
             "actual": summary["overall"]["samples"],
             "status": "通过" if calibration.get("samples", 0) == summary["overall"]["samples"] else "需校准"},
            {"item": "历史内容哈希", "declared": calibration.get("history_sha256", ""),
             "actual": history_hash,
             "status": "通过" if calibration.get("history_sha256", "") == history_hash else "需校准"},
        ]
        candidates = []
        for name, values in summary["by_league"].items():
            if (values["samples"] < int(policy["config"]["min_history_samples"])
                    or values["upset_rate"] < float(policy["config"]["skew_min_upset_rate"])):
                continue
            first = first_summary["by_league"].get(name)
            second = second_summary["by_league"].get(name)
            delta = (abs(first["upset_rate"] - second["upset_rate"])
                     if first and second and first["upset_rate"] is not None and second["upset_rate"] is not None else None)
            candidates.append({"league": name, "samples": values["samples"], "upset_rate": values["upset_rate"],
                               "small_upset_rate": values[SMALL_UPSET] / values["samples"],
                               "big_upset_rate": values[BIG_UPSET] / values["samples"],
                               "period_upset_rate_delta": delta})
        pair_candidates = sorted((item for item in summary["by_league_score_pair"] if item["samples"] >= 5),
                                 key=lambda item: (item["dual_hit_rate"], item["samples"]), reverse=True)
        result = {"date": date, "generated_at": utc_now(), "summary": summary, "time_split": time_split,
                  "checks": checks, "score_pair_candidates": pair_candidates,
                  "skew_candidates": sorted(candidates, key=lambda item: item["upset_rate"], reverse=True),
                  "history_sha256": history_hash, "policy_updated": False}
        stem = f"策略校验_{date}"
        json_path = self.output_dir / f"{stem}.json"
        md_path = self.output_dir / f"{stem}.md"
        json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        md_path.write_text(self._markdown(result), encoding="utf-8")
        if update_policy:
            result["policy_updated"] = update_policy_calibration(
                self.policy_path, summary["overall"]["samples"], history_hash, date)
            json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        return {"json": str(json_path), "markdown": str(md_path),
                "samples": summary["overall"]["samples"], "policy_updated": result["policy_updated"]}

    @staticmethod
    def _markdown(result: dict) -> str:
        overall = result["summary"]["overall"]
        lines = [f"# 策略校验报告（{result['date']}）", "",
                 f"- 历史赛果：{overall['samples']} 场；含比分预测：{overall['with_scores']} 场",
                 f"- 首选／双选／前三命中率：{overall['top1_rate'] or 0:.1%}／{overall['dual_rate'] or 0:.1%}／{overall['top3_rate'] or 0:.1%}", "",
                 "## 时间分段稳定性", "", "| 区间 | 样本 | 偏态率 | 首选命中率 | 双选命中率 | 前三命中率 |",
                 "|---|---:|---:|---:|---:|---:|"]
        for name, values in result["time_split"].items():
            lines.append(f"| {name} | {values['samples']} | {values['upset_rate'] or 0:.1%} | "
                         f"{values['top1_rate'] or 0:.1%} | {values['dual_rate'] or 0:.1%} | {values['top3_rate'] or 0:.1%} |")
        lines += ["", "## 声明值校验", "", "| 校验项 | 声明值 | 实际值 | 结果 |", "|---|---|---|---|"]
        for check in result["checks"]:
            lines.append(f"| {check['item']} | {check['declared']} | {check['actual']} | {check['status']} |")
        lines += ["", "## 联赛校验", "", "| 联赛 | 样本 | 不爆冷 | 小爆冷 | 大爆冷 | 偏态率 | 双选命中率 |",
                  "|---|---:|---:|---:|---:|---:|---:|"]
        for league, values in sorted(result["summary"]["by_league"].items()):
            lines.append(f"| {league} | {values['samples']} | {values[NOT_UPSET]} | {values[SMALL_UPSET]} | "
                         f"{values[BIG_UPSET]} | {values['upset_rate'] or 0:.1%} | {values['dual_rate'] or 0:.1%} |")
        lines += ["", "## 双选比分组合候选（至少 5 场）", "",
                  "| 联赛 | 比分组合 | 样本 | 双选命中 | 双选命中率 |", "|---|---|---:|---:|---:|"]
        for item in result["score_pair_candidates"][:15]:
            lines.append(f"| {item['league']} | {item['score_pair']} | {item['samples']} | "
                         f"{item['dual_hits']} | {item['dual_hit_rate']:.1%} |")
        lines += ["", "## 可执行偏态候选", ""]
        if result["skew_candidates"]:
            for item in result["skew_candidates"]:
                delta = (f"{item['period_upset_rate_delta']:.1%}"
                         if item["period_upset_rate_delta"] is not None else "—")
                lines.append(f"- {item['league']}：{item['samples']} 场，偏态率 {item['upset_rate']:.1%}；"
                             f"小爆冷 {item['small_upset_rate']:.1%}，大爆冷 {item['big_upset_rate']:.1%}；"
                             f"前后段偏态率差 {delta}。")
        else:
            lines.append("- 没有联赛同时达到样本量和偏态率阈值。")
        lines += ["", "> 候选只表示进入下一轮观察，不自动升级为收益承诺或确定投注建议。", ""]
        return "\n".join(lines)
