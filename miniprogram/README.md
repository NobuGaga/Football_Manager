# 足球经理的推断 · 微信小程序

这是“足球经理的推断”的原生微信小程序。五个标签页展示工程概览、数据流水线、模型与回测、每日自动化和工程结构；概览中的“查看每日推断”进入每日报告页面，展示对阵、三向概率、比分参考、数据完整度和风险。

工程信息为本地静态内容；每日报告由 Kimi Claw 从当日 JSON 转换为 `data/daily-report.js` 后随体验版打包。小程序不实时连接数据库、不执行仓库命令，也不包含凭证。

## 当前方式：本人测试号预览

当前使用已登录微信账号下的普通小程序测试号。2026-11-07 的历史版本已通过官方编译并生成预览二维码；新增每日页面后仍需重新编译、上传并完成真机验收。

操作说明见 [`自用预览说明.txt`](../delivery/wechat-miniprogram/publishing/自用预览说明.txt)。公共配置中的 `appid` 保留为 `touristappid` 占位，本机实际 AppID 写入已被 `.gitignore` 排除的 `project.private.config.json`，不要提交个人配置。

## 本地导入与预览

1. 下载并安装当前 Stable 版微信开发者工具：<https://developers.weixin.qq.com/miniprogram/dev/devtools/download.html>。
2. 打开“导入项目”，选择本 `miniprogram/` 目录，即包含 `project.config.json` 的这一层，不要选择仓库根目录。
3. 账号类型选择“小程序”，不要选择“小游戏”。
4. 编译并检查五个标签页，以及概览中的“每日推断”入口、比赛列表、概率和风险展示。
5. 真机使用时，在开发者工具中登录当前测试号所属的微信账号，点击“预览”并扫码。

## Kimi Claw 每日打包更新

正式每日任务：

```bash
bash scripts/daily_automation.sh
```

执行顺序为：生成每日报告 → 导出紧凑的小程序数据 → 将数据库、报告和 `miniprogram/data/daily-report.js` 提交并推送 → 运行小程序测试 → 上传体验版。上传备注包含报告日期，默认使用固定 `CI 机器人 1`。

`AI_MODEL` 选择 `rules/strategies/` 中的策略分类，默认 `kimi`。每日页面显示每条记录的 AI 模型和分类；同一场比赛的多个 AI 策略记录会分别保留。项目不调用外部足球数据 API。

- `DAILY_NO_GIT=1`：手动验收，只生成报告和小程序数据，不提交、不推送、不上传。
- `DAILY_MINIPROGRAM_UPLOAD=0`：正式任务保留报告与 Git 推送，但显式关闭小程序上传。
- `NODE_BIN`：可选，指定 Node.js 可执行文件，默认是 `node`。

也可只修复当日小程序数据并重新上传：

```bash
python3 main.py export-miniprogram --date YYYY-MM-DD
bash scripts/wechat_daily_upload.sh
```

### 一次性配置

1. 安装 Node.js 22 或兼容版本，并在 `miniprogram/` 执行 `npm ci`。
2. 在微信公众平台进入“开发管理 → 开发设置 → 小程序代码上传”，由管理员生成代码上传密钥，并按平台要求配置出口 IP 白名单。
3. 确保自己的微信账号具有开发、预览或体验权限。
4. 在 Kimi Claw 的安全运行环境中配置 `WECHAT_APPID` 和多行私钥 `WECHAT_PRIVATE_KEY`，不要写入仓库、脚本、命令参数或日志。
5. 如果启用了上传 IP 白名单，将 Claw 实际运行环境的固定出口 IP 加入白名单；不要为图方便关闭白名单。

版本号默认为 `1.0.<UTC 时间戳>`，也可通过 `WECHAT_VERSION=x.y.z` 显式指定；`WECHAT_ROBOT` 可指定 1–30 的机器人编号，但为持续更新体验版不应随意更换。首次使用时，需在微信公众平台将 `CI 机器人 1` 的开发版本设为体验版；之后同机器人上传会持续更新体验版。

### 生成自用预览二维码

```bash
cd miniprogram
WECHAT_PREVIEW_OUTPUT="$PWD/../delivery/wechat-miniprogram/preview-ci.jpg" npm run preview
```

二维码保存到上述本地路径，由有权限的微信账号扫码。该模式不上传开发版本，也不更新体验版；预览图片不要提交到仓库。

私钥只在运行环境的临时目录中生成，脚本结束后立即删除。密钥疑似泄露时，应立即在微信公众平台重置。

`npm test` 覆盖五个标签页、每日页面数据加载、主要切换、复制调用、机器人参数和版本号生成；它不能替代微信开发者工具的视觉检查和真机验收。没有真实环境凭证时不要运行上传或预览命令。

## 自用与发布边界

“生成预览二维码”“上传开发版本”“设置体验版”“提交审核”“正式发布”是不同阶段。本工程面向自己的微信账号，Kimi Claw 可执行每日数据打包、预览、上传和已绑定机器人的体验版持续更新；首次体验版绑定需要人工完成，不绕过账号授权，也不自动执行公开审核或正式发布。
