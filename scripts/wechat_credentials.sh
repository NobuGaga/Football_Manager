#!/usr/bin/env bash
# 只在本进程内加载凭证；不得打印 WECHAT_PRIVATE_KEY。

load_wechat_credentials() {
  local repo_root="$1"
  local key_dir="${WECHAT_KEY_DIR:-${repo_root}/wechat_key}"
  local node_bin="${NODE_BIN:-node}"
  local project_appid
  local key_name
  local key_file
  local -a all_keys=()
  local -a matching_keys=()

  if [[ -n "${WECHAT_APPID:-}" && ! "${WECHAT_APPID}" =~ ^wx[[:xdigit:]]{16}$ ]]; then
    echo '错误：WECHAT_APPID 格式无效。' >&2
    return 1
  fi

  # 环境变量优先，其次使用当前微信小游戏项目的本机私有 AppID。
  # 公共配置中的 touristappid 只是占位，不作为上传身份。
  if [[ -z "${WECHAT_APPID:-}" ]]; then
    if ! project_appid="$("$node_bin" -e '
      const fs = require("node:fs");
      const path = require("node:path");
      const root = process.argv[1];
      for (const file of ["web/project.private.config.json", "web/project.config.json"]) {
        const filePath = path.join(root, file);
        if (!fs.existsSync(filePath)) continue;
        const appid = JSON.parse(fs.readFileSync(filePath, "utf8")).appid || "";
        if (/^wx[0-9a-fA-F]{16}$/.test(appid)) {
          process.stdout.write(appid);
          break;
        }
      }
    ' "$repo_root" 2>/dev/null)"; then
      echo '错误：无法读取当前微信小游戏项目的 AppID 配置。' >&2
      return 1
    fi
    WECHAT_APPID="$project_appid"
  fi

  if [[ -z "${WECHAT_APPID:-}" || -z "${WECHAT_PRIVATE_KEY:-}" ]]; then
    shopt -s nullglob
    all_keys=("${key_dir}"/*.key)

    # 仅在项目没有配置 AppID 时，保留从唯一密钥文件名推断的兼容路径。
    if [[ -z "${WECHAT_APPID:-}" ]]; then
      if (( ${#all_keys[@]} != 1 )); then
        echo '错误：请配置 WECHAT_APPID，或在项目私有配置中填写微信小游戏 AppID。' >&2
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
      elif (( ${#matching_keys[@]} > 1 )); then
        echo '错误：当前微信小游戏 AppID 对应多个代码上传密钥，请只保留一个有效文件。' >&2
        return 1
      elif (( ${#all_keys[@]} == 1 )); then
        key_name="${all_keys[0]##*/}"
        if [[ "$key_name" =~ wx[[:xdigit:]]{16} ]]; then
          echo '错误：本地密钥文件名属于其他 AppID；请使用当前微信小游戏的上传密钥，不要沿用其他账号或小程序密钥。' >&2
          return 1
        fi
        key_file="${all_keys[0]}"
      else
        echo '错误：无法唯一确定当前微信小游戏的代码上传密钥，请配置 WECHAT_PRIVATE_KEY 或整理本地 .key 文件。' >&2
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
