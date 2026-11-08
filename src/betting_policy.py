from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY_PATH = PROJECT_ROOT / "rules" / "football_manager_policy.md"
DEFAULT_CONFIG = {
    "strategy_version": "selection-v1",
    "dual_score_count": 2,
    "min_completeness": 0.35,
    "min_history_samples": 30,
    "skew_min_upset_rate": 0.50,
    "calibration": {"revision": 0, "samples": 0, "history_sha256": "", "updated_at": ""},
}


def _read_config(content: str) -> dict:
    for match in re.finditer(r"```json\s*(\{.*?\})\s*```", content, re.DOTALL):
        config = json.loads(match.group(1))
        if "strategy_version" in config and "dual_score_count" in config:
            return config
    return json.loads(json.dumps(DEFAULT_CONFIG))


def load_betting_policy(path: str | Path = DEFAULT_POLICY_PATH) -> dict:
    policy_path = Path(path)
    content = policy_path.read_text(encoding="utf-8")
    config = _read_config(content)
    if config["dual_score_count"] != 2:
        raise ValueError("双选比分策略的 dual_score_count 必须为 2")
    for key in ("min_completeness", "skew_min_upset_rate"):
        value = float(config[key])
        if not 0 <= value <= 1:
            raise ValueError(f"{key} 必须在 0 到 1 之间")
    if int(config["min_history_samples"]) < 1:
        raise ValueError("min_history_samples 必须大于 0")
    return {
        "config": config,
        "path": policy_path,
        "content": content,
        "sha256": hashlib.sha256(content.encode("utf-8")).hexdigest(),
    }


def update_policy_calibration(path: str | Path, samples: int, history_sha256: str, date: str) -> bool:
    policy_path = Path(path)
    policy = load_betting_policy(policy_path)
    config = policy["config"]
    calibration = config.setdefault("calibration", {})
    if calibration.get("history_sha256") == history_sha256 and calibration.get("samples") == samples:
        return False
    calibration.update({
        "revision": int(calibration.get("revision", 0)) + 1,
        "samples": samples,
        "history_sha256": history_sha256,
        "updated_at": date,
    })
    content = policy["content"]
    replaced = False
    matches = list(re.finditer(r"```json\s*(\{.*?\})\s*```", content, re.DOTALL))
    for match in matches:
        candidate = json.loads(match.group(1))
        if "strategy_version" in candidate and "dual_score_count" in candidate:
            block = json.dumps(config, ensure_ascii=False, indent=2)
            content = content[:match.start()] + f"```json\n{block}\n```" + content[match.end():]
            replaced = True
            break
    if not replaced:
        content += "\n\n## 九、可执行策略配置\n\n```json\n" + json.dumps(config, ensure_ascii=False, indent=2) + "\n```\n"
    content = re.sub(r"更新日期：\d{4}-\d{2}-\d{2}", f"更新日期：{date}", content, count=1)
    log_line = f"- {date}：校准样本 {samples} 场；历史哈希 `{history_sha256[:12]}`；校准修订 {calibration['revision']}。"
    start = "<!-- STRATEGY_CALIBRATION_START -->"
    end = "<!-- STRATEGY_CALIBRATION_END -->"
    if start in content and end in content:
        before, rest = content.split(start, 1)
        old_log, after = rest.split(end, 1)
        lines = [line for line in old_log.strip().splitlines() if line.strip()]
        lines.append(log_line)
        content = before + start + "\n" + "\n".join(lines) + "\n" + end + after
    else:
        content += f"\n\n## 十、策略校准日志\n\n{start}\n{log_line}\n{end}\n"
    policy_path.write_text(content, encoding="utf-8")
    return True
