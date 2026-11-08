from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
STRATEGY_DIR = PROJECT_ROOT / "rules" / "strategies"
DEFAULT_AI_MODEL = "kimi"


def load_ai_strategy(ai_model: str = DEFAULT_AI_MODEL) -> dict:
    normalized = ai_model.strip().lower()
    if not re.fullmatch(r"[a-z0-9-]+", normalized):
        raise ValueError("AI 模型分类只能包含小写字母、数字和连字符")
    path = STRATEGY_DIR / f"{normalized}.md"
    if not path.is_file():
        available = ", ".join(item["ai_model"] for item in available_ai_strategies())
        raise ValueError(f"未找到 AI 模型策略：{normalized}；可用分类：{available}")
    content = path.read_text(encoding="utf-8")
    match = re.search(r"```json\s*(\{.*?\})\s*```", content, re.DOTALL)
    if not match:
        raise ValueError(f"策略文档缺少 JSON 分类配置：{path}")
    config = json.loads(match.group(1))
    required = {"ai_model", "model_name", "strategy_id", "strategy_category"}
    if not required.issubset(config) or config["ai_model"] != normalized:
        raise ValueError(f"策略文档分类配置不完整或与文件名不一致：{path}")
    return {
        **config,
        "document_path": path.relative_to(PROJECT_ROOT).as_posix(),
        "document_sha256": hashlib.sha256(content.encode("utf-8")).hexdigest(),
        "content": content,
    }


def available_ai_strategies() -> list[dict]:
    strategies = []
    for path in sorted(STRATEGY_DIR.glob("*.md")):
        strategy = load_ai_strategy(path.stem)
        strategy.pop("content")
        strategies.append(strategy)
    return strategies
