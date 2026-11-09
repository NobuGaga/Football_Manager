#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NODE_BIN="${NODE_BIN:-node}"
NPM_BIN="${NPM_BIN:-npm}"

if [[ -z "${WECHAT_APPID:-}" || -z "${WECHAT_PRIVATE_KEY:-}" ]]; then
  echo "错误：每日小游戏上传需要 WECHAT_APPID 和 WECHAT_PRIVATE_KEY；可设置 DAILY_MINIPROGRAM_UPLOAD=0 显式关闭。" >&2
  exit 1
fi
if ! command -v "$NODE_BIN" >/dev/null 2>&1; then
  echo "错误：未找到 Node.js（$NODE_BIN），无法测试、构建并上传每日小游戏。" >&2
  exit 1
fi
NODE_PATH="$(command -v "$NODE_BIN")"
export PATH="$(dirname "$NODE_PATH"):$PATH"
if ! command -v "$NPM_BIN" >/dev/null 2>&1; then
  echo "错误：未找到 npm（$NPM_BIN），无法构建 Canvas 微信小游戏。" >&2
  exit 1
fi

cd "$REPO_ROOT/web"
"$NPM_BIN" test
"$NPM_BIN" run build:weapp

cd "$REPO_ROOT/miniprogram"
"$NODE_BIN" scripts/test-ci.cjs
"$NODE_BIN" scripts/upload-ci.cjs
