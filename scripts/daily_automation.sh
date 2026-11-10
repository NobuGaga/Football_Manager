#!/usr/bin/env bash
# Kimi Claw cron 入口。凭证只从环境读取，不写入仓库或日志。
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

args=(daily)
if [[ "${DAILY_NO_GIT:-0}" != "1" ]]; then
  args+=(--pull)
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
if [[ -n "${AI_MODEL:-}" ]]; then
  args+=(--ai-model "$AI_MODEL")
fi

python3 main.py "${args[@]}"

if [[ "${DAILY_NO_GIT:-0}" == "1" ]]; then
  echo "手动验收：每日报告已打包到小程序，本次不上传体验版。"
elif [[ "${DAILY_MINIPROGRAM_UPLOAD:-1}" == "1" ]]; then
  bash scripts/wechat_daily_upload.sh
else
  echo "提示：DAILY_MINIPROGRAM_UPLOAD=0，已跳过每日小程序体验版上传。"
fi
