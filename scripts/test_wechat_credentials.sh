#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$REPO_ROOT/scripts/wechat_credentials.sh"
TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TMP_ROOT"' EXIT

unset WECHAT_APPID WECHAT_PRIVATE_KEY WECHAT_KEY_DIR
export WECHAT_APPID='wx0000000000000001'
export WECHAT_PRIVATE_KEY='from-environment'
load_wechat_credentials "$REPO_ROOT"
[[ "$WECHAT_PRIVATE_KEY" == 'from-environment' ]]

mkdir -p "$TMP_ROOT/infer-project" "$TMP_ROOT/single"
printf 'from-file\n' > "$TMP_ROOT/single/private.wx0000000000000002.key"
unset WECHAT_APPID WECHAT_PRIVATE_KEY
export WECHAT_KEY_DIR="$TMP_ROOT/single"
load_wechat_credentials "$TMP_ROOT/infer-project"
[[ "$WECHAT_APPID" == 'wx0000000000000002' ]]
[[ "$WECHAT_PRIVATE_KEY" == 'from-file' ]]

mkdir -p "$TMP_ROOT/multiple"
printf 'selected\n' > "$TMP_ROOT/multiple/private.wx0000000000000003.key"
printf 'other\n' > "$TMP_ROOT/multiple/private.wx0000000000000004.key"
unset WECHAT_PRIVATE_KEY
export WECHAT_APPID='wx0000000000000003'
export WECHAT_KEY_DIR="$TMP_ROOT/multiple"
load_wechat_credentials "$REPO_ROOT"
[[ "$WECHAT_PRIVATE_KEY" == 'selected' ]]

mkdir -p "$TMP_ROOT/generic"
printf 'generic\n' > "$TMP_ROOT/generic/upload.key"
unset WECHAT_PRIVATE_KEY
export WECHAT_APPID='wx0000000000000005'
export WECHAT_KEY_DIR="$TMP_ROOT/generic"
load_wechat_credentials "$REPO_ROOT"
[[ "$WECHAT_PRIVATE_KEY" == 'generic' ]]

mkdir -p "$TMP_ROOT/project-appid/web" "$TMP_ROOT/project-key"
printf '{"appid":"wx0000000000000006"}\n' > "$TMP_ROOT/project-appid/web/project.private.config.json"
printf 'project-key\n' > "$TMP_ROOT/project-key/private.wx0000000000000006.key"
printf 'other\n' > "$TMP_ROOT/project-key/private.wx0000000000000007.key"
unset WECHAT_APPID WECHAT_PRIVATE_KEY
export WECHAT_KEY_DIR="$TMP_ROOT/project-key"
load_wechat_credentials "$TMP_ROOT/project-appid"
[[ "$WECHAT_APPID" == 'wx0000000000000006' ]]
[[ "$WECHAT_PRIVATE_KEY" == 'project-key' ]]

mkdir -p "$TMP_ROOT/mismatch-project/web" "$TMP_ROOT/mismatch-key"
printf '{"appid":"wx0000000000000008"}\n' > "$TMP_ROOT/mismatch-project/web/project.private.config.json"
printf 'old-game-key\n' > "$TMP_ROOT/mismatch-key/private.wx0000000000000009.key"
unset WECHAT_APPID WECHAT_PRIVATE_KEY
export WECHAT_KEY_DIR="$TMP_ROOT/mismatch-key"
if load_wechat_credentials "$TMP_ROOT/mismatch-project" >/dev/null 2>&1; then
  echo '项目 AppID 与唯一密钥文件名不一致时应拒绝加载。' >&2
  exit 1
fi

unset WECHAT_APPID WECHAT_PRIVATE_KEY
export WECHAT_KEY_DIR="$TMP_ROOT/multiple"
if load_wechat_credentials "$TMP_ROOT/infer-project" >/dev/null 2>&1; then
  echo '多个密钥且缺少 AppID 时应拒绝加载。' >&2
  exit 1
fi

mkdir -p "$TMP_ROOT/empty"
unset WECHAT_APPID WECHAT_PRIVATE_KEY
export WECHAT_KEY_DIR="$TMP_ROOT/empty"
if load_wechat_credentials "$TMP_ROOT/infer-project" >/dev/null 2>&1; then
  echo '缺少凭证时应拒绝上传。' >&2
  exit 1
fi

echo 'WECHAT_CREDENTIALS_OK environment_fallback appid_inference unique_selection project_appid_precedence mismatch_rejection ambiguous_rejection missing_rejection'
