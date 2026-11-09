#!/usr/bin/env bash
# 只在本进程内加载凭证；不得打印 WECHAT_PRIVATE_KEY。

load_wechat_credentials() {
  local repo_root="$1"
  local key_dir="${WECHAT_KEY_DIR:-${repo_root}/wechat_key}"
  local key_name
  local key_file
  local -a all_keys=()
  local -a matching_keys=()

  if [[ -n "${WECHAT_APPID:-}" && ! "${WECHAT_APPID}" =~ ^wx[[:xdigit:]]{16}$ ]]; then
    echo '错误：WECHAT_APPID 格式无效。' >&2
    return 1
  fi

  if [[ -z "${WECHAT_APPID:-}" || -z "${WECHAT_PRIVATE_KEY:-}" ]]; then
    shopt -s nullglob
    all_keys=("${key_dir}"/*.key)

    if [[ -z "${WECHAT_APPID:-}" ]]; then
      if (( ${#all_keys[@]} != 1 )); then
        echo '错误：请配置 WECHAT_APPID，或只保留一个可供识别的本地 .key 文件。' >&2
        return 1
      fi
      key_name="${all_keys[0]##*/}"
      if [[ ! "$key_name" =~ wx[[:xdigit:]]{16} ]]; then
        echo '错误：本地密钥文件名无法识别 AppID，请显式配置 WECHAT_APPID。' >&2
        return 1
      fi
      WECHAT_APPID="${BASH_REMATCH[0]}"
    fi

    if [[ -z "${WECHAT_PRIVATE_KEY:-}" ]]; then
      matching_keys=("${key_dir}"/*"${WECHAT_APPID}"*.key)
      if (( ${#matching_keys[@]} == 1 )); then
        key_file="${matching_keys[0]}"
      elif (( ${#all_keys[@]} == 1 )); then
        key_file="${all_keys[0]}"
      else
        echo '错误：无法唯一确定微信代码上传密钥，请配置 WECHAT_PRIVATE_KEY 或整理本地 .key 文件。' >&2
        return 1
      fi
      if [[ ! -r "$key_file" ]]; then
        echo '错误：本地微信代码上传密钥不可读取。' >&2
        return 1
      fi
      WECHAT_PRIVATE_KEY="$(< "$key_file")"
    fi
  fi

  if [[ ! "${WECHAT_APPID:-}" =~ ^wx[[:xdigit:]]{16}$ || -z "${WECHAT_PRIVATE_KEY:-}" ]]; then
    echo '错误：微信上传需要有效的 WECHAT_APPID 和 WECHAT_PRIVATE_KEY。' >&2
    return 1
  fi
  export WECHAT_APPID WECHAT_PRIVATE_KEY
}
