"""通用解析、标识与时间工具。所有持久化时间均使用 ISO 8601。"""
from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

UTC = timezone.utc
DEFAULT_TIMEZONE = "Asia/Shanghai"


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def normalize_name(value: object) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).strip().lower()
    return re.sub(r"[\s\-_·•（）()\[\]【】]", "", text)


def clean_text(value: object, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return default if text.lower() in {"", "nan", "none", "null", "--"} else text


def optional_float(value: object) -> float | None:
    text = clean_text(value)
    if not text:
        return None
    try:
        number = float(text)
        return number if number == number else None
    except (TypeError, ValueError):
        return None


def parse_datetime(value: object, default_date: str | None = None,
                   tz_name: str = DEFAULT_TIMEZONE) -> tuple[str, str]:
    """返回（UTC 时间、原始执行时区）。缺少日期时仅使用显式 default_date。"""
    text = clean_text(value)
    if not text:
        raise ValueError("开赛时间不能为空")
    text = text.replace("年", "-").replace("月", "-").replace("日", " ")
    text = text.replace("/", "-").replace("T", " ").strip()
    text = re.sub(r"\s+", " ", text)
    if default_date and re.fullmatch(r"\d{1,2}-\d{1,2}\s+\d{1,2}:\d{2}(?::\d{2})?", text):
        text = f"{default_date} {text}"
    formats = (
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
        "%m-%d %H:%M:%S", "%m-%d %H:%M",
    )
    parsed = None
    for fmt in formats:
        try:
            parsed = datetime.strptime(text, fmt)
            break
        except ValueError:
            continue
    if parsed is None:
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"无法识别开赛时间：{value}") from exc
    try:
        tz = ZoneInfo(clean_text(tz_name, DEFAULT_TIMEZONE))
    except Exception as exc:
        raise ValueError(f"无效时区：{tz_name}") from exc
    if parsed.tzinfo is None:
        if parsed.year == 1900:
            if not default_date:
                raise ValueError(f"开赛时间缺少日期，请提供 --date：{value}")
            parsed = datetime.combine(datetime.strptime(default_date, "%Y-%m-%d").date(), parsed.time())
        parsed = parsed.replace(tzinfo=tz)
    return parsed.astimezone(UTC).isoformat(timespec="seconds"), str(tz)


def local_date(utc_iso: str, tz_name: str = DEFAULT_TIMEZONE) -> str:
    dt = datetime.fromisoformat(utc_iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(ZoneInfo(tz_name)).date().isoformat()


def fixture_key(match_code: str, start_time_utc: str, home_team: str, away_team: str) -> str:
    raw = "|".join((match_code, start_time_utc, normalize_name(home_team), normalize_name(away_team)))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def payload_hash(payload: object) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def normalize_score(value: object) -> str:
    text = clean_text(value).replace("：", ":").replace("-", ":")
    match = re.fullmatch(r"(\d+):(\d+)", text)
    if not match:
        raise ValueError(f"无效比分：{value}")
    return f"{int(match.group(1))}:{int(match.group(2))}"


def outcome(home_goals: int, away_goals: int) -> str:
    return "H" if home_goals > away_goals else "D" if home_goals == away_goals else "A"


def read_structured_file(path: str | Path) -> tuple[list[dict], object]:
    """读取 CSV/JSON；JSON 同时返回原始对象用于不可变快照。"""
    import pandas as pd

    source = Path(path)
    if not source.exists():
        raise FileNotFoundError(source)
    if source.suffix.lower() == ".json":
        raw = json.loads(source.read_text(encoding="utf-8"))
        rows = raw.get("matches", raw) if isinstance(raw, dict) else raw
        if not isinstance(rows, list):
            raise ValueError("JSON 顶层必须是数组，或包含 matches 数组")
        return [dict(row) for row in rows], raw
    if source.suffix.lower() in {".csv", ".txt"}:
        frame = pd.read_csv(source, dtype=str, keep_default_na=False)
        return frame.to_dict("records"), frame.to_dict("records")
    if source.suffix.lower() in {".xlsx", ".xls"}:
        frame = pd.read_excel(source, dtype=str).fillna("")
        return frame.to_dict("records"), frame.to_dict("records")
    raise ValueError("仅支持 CSV、JSON、XLSX 或 XLS 文件")


def first(row: dict, *names: str, default: object = "") -> object:
    for name in names:
        if name in row and clean_text(row[name]):
            return row[name]
    return default
