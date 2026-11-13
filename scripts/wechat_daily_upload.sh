#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NODE_BIN="${NODE_BIN:-node}"
NPM_BIN="${NPM_BIN:-npm}"

UPLOAD_SOURCES_STATUS="$(git -C "$REPO_ROOT" status --short -- web deploy/wechat scripts)"
if [[ -n "$UPLOAD_SOURCES_STATUS" ]]; then
  echo '错误：web/、deploy/wechat/ 或 scripts/ 存在未提交改动，停止上传混合版本。' >&2
  exit 1
fi

if ! command -v "$NODE_BIN" >/dev/null 2>&1; then
  echo "错误：未找到 Node.js（${NODE_BIN}），无法测试、构建并上传每日微信小游戏。" >&2
  exit 1
fi
NODE_PATH="$(command -v "$NODE_BIN")"
export PATH="$(dirname "$NODE_PATH"):$PATH"
if ! command -v "$NPM_BIN" >/dev/null 2>&1; then
  echo "错误：未找到 npm（${NPM_BIN}），无法构建 Canvas 微信小游戏。" >&2
  exit 1
fi

cd "$REPO_ROOT/web"
"$NPM_BIN" test
"$NPM_BIN" run build:minigame

cd "$REPO_ROOT/deploy/wechat"
"$NPM_BIN" test

source "$REPO_ROOT/scripts/wechat_credentials.sh"
if ! load_wechat_credentials "$REPO_ROOT"; then
  echo '错误：无法加载微信上传凭证；可设置 DAILY_MINIPROGRAM_UPLOAD=0 显式关闭每日上传。' >&2
  exit 1
fi
"$NODE_BIN" scripts/upload-ci.cjs
