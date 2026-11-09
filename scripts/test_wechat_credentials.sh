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

mkdir -p "$TMP_ROOT/single"
printf 'from-file\n' > "$TMP_ROOT/single/private.wx0000000000000002.key"
unset WECHAT_APPID WECHAT_PRIVATE_KEY
export WECHAT_KEY_DIR="$TMP_ROOT/single"
load_wechat_credentials "$REPO_ROOT"
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

unset WECHAT_APPID WECHAT_PRIVATE_KEY
export WECHAT_KEY_DIR="$TMP_ROOT/multiple"
if load_wechat_credentials "$REPO_ROOT" >/dev/null 2>&1; then
  echo '多个密钥且缺少 AppID 时应拒绝加载。' >&2
  exit 1
fi

mkdir -p "$TMP_ROOT/empty"
unset WECHAT_APPID WECHAT_PRIVATE_KEY
export WECHAT_KEY_DIR="$TMP_ROOT/empty"
if load_wechat_credentials "$REPO_ROOT" >/dev/null 2>&1; then
  echo '缺少凭证时应拒绝上传。' >&2
  exit 1
fi

echo 'WECHAT_CREDENTIALS_OK environment_fallback appid_inference unique_selection ambiguous_rejection missing_rejection'
