from __future__ import annotations

import json
from pathlib import Path


def export_miniprogram_report(report_path: str | Path, output_path: str | Path) -> Path:
    report_path = Path(report_path)
    output_path = Path(output_path)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    matches = report.get("matches", [])
    if report.get("match_count") != len(matches):
        raise ValueError("每日报告比赛数量与明细不一致，停止生成小程序数据")

    compact_matches = []
    for match in matches:
        basic = match.get("basic_info", {})
        data_status = match.get("data_status", {})
        compact_matches.append({
            "fixtureKey": match["fixture_key"],
            "matchCode": match.get("match_code") or basic.get("match_code"),
            "league": match.get("league") or basic.get("league"),
            "homeTeam": match.get("home_team") or basic.get("home_team"),
            "awayTeam": match.get("away_team") or basic.get("away_team"),
            "startTimeLocal": match.get("start_time_local"),
            "probabilities": match["probabilities"],
            "expectedGoals": match.get("expected_goals", {}),
            "topScores": match.get("top_scores", [])[:2],
            "dataCompleteness": match.get("data_completeness", 0),
            "featureSource": data_status.get("feature_source"),
            "method": match.get("analysis", {}).get("method"),
            "riskFlags": match.get("risk_flags", []),
            "modelVersion": match.get("model_version"),
            "predictedAt": match.get("predicted_at"),
            "actualScore": match.get("actual_score"),
        })

    payload = {
        "reportDate": report["report_date"],
        "generatedAt": report["generated_at"],
        "matchCount": len(compact_matches),
        "disclaimer": report["disclaimer"],
        "matches": compact_matches,
    }
    content = (
        "const dailyReport = "
        + json.dumps(payload, ensure_ascii=False, indent=2)
        + "\n\nmodule.exports = { dailyReport };\n"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not output_path.exists() or output_path.read_text(encoding="utf-8") != content:
        output_path.write_text(content, encoding="utf-8")
    return output_path
