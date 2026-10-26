#!/usr/bin/env bash
# Kimi Claw cron 入口。凭证只从环境读取，不写入仓库或日志。
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

args=(daily)
if [[ "${DAILY_NO_GIT:-0}" != "1" ]]; then
  args+=(--pull --push)
fi
if [[ -n "${DAILY_DATE:-}" ]]; then
  args+=(--date "$DAILY_DATE")
fi
if [[ -n "${DAILY_RESULTS_DATE:-}" ]]; then
  args+=(--results-date "$DAILY_RESULTS_DATE")
fi
if [[ -n "${DAILY_FIXTURES_FILE:-}" ]]; then
  args+=(--fixtures "$DAILY_FIXTURES_FILE")
fi
if [[ -n "${DAILY_RESULTS_FILE:-}" ]]; then
  args+=(--results "$DAILY_RESULTS_FILE")
fi
if [[ "${DAILY_REFRESH:-0}" == "1" ]]; then
  args+=(--refresh)
fi
if [[ "${DAILY_REQUIRE_RESULTS:-0}" == "1" ]]; then
  args+=(--require-results)
fi
if [[ -n "${API_FOOTBALL_KEY:-}" ]]; then
  args+=(--sync-results --enrich)
else
  echo "提示：未配置 API_FOOTBALL_KEY；将跳过 API 赛果同步和赛前特征补充。" >&2
fi

python3 main.py "${args[@]}"
