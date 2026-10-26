# Football Manager：Kimi Claw 工作上下文与每日 SOP

本文件供人和 AI 在每次仓库任务前阅读。工程细节见 `docs/项目工程.md`，命令说明见 `README.md`。

## 1. 目标与边界

每日按 Asia/Shanghai 日期执行：拉取仓库 → 回填昨日赛果 → 核对历史预测 → 获取今日官方对阵 → 补充赛前数据 → 生成今日预测和报告 → 更新概率质量统计 → 只提交数据产物并推送 → 输出中文摘要。

约束：

- 体彩官方对阵决定分析范围，API-Football 只补充数据。
- 不绕过验证码、登录、反爬或访问限制；网页失败时显式中止，改用官方对阵文件。
- 预测是概率分析，不使用“包中”“稳赢”等措辞，不承诺收益。
- 不覆盖历史预测；临场更新必须追加版本。
- 不使用赛后信息生成赛前预测，不错配比赛，不把未完结比赛写成赛果。
- 聊天渠道只发送中文摘要和报告路径，不发送数据库、凭证或大段原始数据。

## 2. 关键文件

| 文件 | 用途 |
|---|---|
| `main.py` | 全部命令入口，`daily` 是每日总流程 |
| `src/automation.py` | Git 检查、流程编排、锁、摘要与安全提交 |
| `src/providers.py` | API-Football 赛前特征和已完结赛果同步 |
| `data/football.db` | 对阵、快照、特征、预测、赛果和模型版本 |
| `reports/` | 每日 JSON／CSV／Excel／Markdown、回测和中文摘要 |
| `scripts/daily_automation.sh` | Claw／cron 推荐入口 |
| `docs/项目工程.md` | 架构、字段、模型和回测口径 |

`reports/` 是需要写回仓库的数据产物。`data/cache/`、`data/snapshots/`、`.env` 和锁文件不提交。

## 3. 一次性接入（先配置，不挂 cron）

1. 在 GitHub 创建 fine-grained personal access token：只授权本仓库，Contents 读写，设置有效期并定期轮换。
2. 在 Claw 的安全终端配置 Git 凭证并 clone。不要把 token 写入 `MEMORY.md`、本文件、仓库、远程 URL、脚本或执行日志；优先使用环境凭证或凭证助手。
3. 配置 Git 用户名和邮箱，验证 `git pull` 与一次人工测试提交／推送。
4. 安装依赖并初始化：

   ```bash
   python3 -m pip install -r requirements.txt
   python3 main.py init
   ```

5. 如需 API-Football，在运行环境中配置 `API_FOOTBALL_KEY`。不要把密钥写入仓库；维护球队映射，否则外部比赛会进入人工确认队列。
6. 如需微信、飞书或企业微信通知，由用户在 Kimi Claw「设置 → 聊天频道」配置，工程不保存渠道凭证。

可写入长期记忆的内容仅包括：仓库用途、非敏感 SOP、已确认的数据格式和故障处理偏好。不得记录 token、API key、渠道 secret 或含凭证的 URL。

## 4. 手动验收（通过后再定时）

有昨日对阵和赛果文件时：

```bash
DAILY_NO_GIT=1 \
DAILY_FIXTURES_FILE=/绝对路径/今日对阵.csv \
DAILY_RESULTS_FILE=/绝对路径/昨日赛果.csv \
DAILY_DATE=YYYY-MM-DD \
DAILY_RESULTS_DATE=YYYY-MM-DD \
bash scripts/daily_automation.sh
```

没有文件时，可仅设置 `DAILY_NO_GIT=1` 测试网页对阵入口。`DAILY_NO_GIT=1` 不拉取、不提交、不推送。赛果文件要求昨日对阵已存在于数据库，否则系统会停止而不是猜测关联。

验收项目：

- 中文摘要中的对阵、赛果、预测数量与实际一致；
- `reports/竞彩足球分析_YYYY-MM-DD.*` 四种格式均可读取；
- 回测只包含有赛果且时间边界合法的预测；
- 风险与待确认事项没有被隐藏；
- 工作区没有凭证、缓存或无关源码被加入提交。

手动推送验收：确认工作区干净后执行 `bash scripts/daily_automation.sh`，到 GitHub 检查提交只包含 `data/football.db` 和 `reports/`。

## 5. 正式每日命令

```bash
bash scripts/daily_automation.sh
```

默认执行 `git pull --rebase`，成功后只暂存 `data/football.db` 和 `reports/`，再 commit／push。工作区不干净、已有任务运行、对阵或赛果关联失败时均中止，不强行覆盖或推送。

可选环境变量：

| 变量 | 作用 |
|---|---|
| `DAILY_DATE` / `DAILY_RESULTS_DATE` | 指定对阵和赛果日期 |
| `DAILY_FIXTURES_FILE` / `DAILY_RESULTS_FILE` | 使用官方文件代替自动入口 |
| `DAILY_REFRESH=1` | 追加临场预测版本 |
| `DAILY_REQUIRE_RESULTS=1` | 昨日有比赛但未可靠回填时中止 |
| `DAILY_NO_GIT=1` | 手动测试，禁用 Git 拉取与推送 |
| `API_FOOTBALL_KEY` | 启用赛前特征和赛果 API 同步 |

## 6. cron 配置（手动验收之后）

避开整点，任务指令写清执行入口、输出和约束：

```text
/cron add "13 9 * * *" 在 Football_Manager 仓库根目录执行 bash scripts/daily_automation.sh；完成后输出中文摘要，包含对阵数、赛果回填数、新增预测数、命中率与 Log Loss、报告路径、Git 提交和待处理风险；失败时明确报告失败步骤，不得推送半成品，不得输出任何凭证
```

维护：`/cron` 查看，`/cron log <任务ID>` 查日志，`/cron enable/disable <任务ID>` 启用或暂停。故障依次检查 `/status` → `/logs` → `/cron log <任务ID>`。

## 7. 失败处理

- **Git dirty**：先人工审查修改，不自动 stash、删除或覆盖。
- **pull／push 冲突**：保留现场并人工处理，不强制 push。
- **网页无对阵**：使用 `DAILY_FIXTURES_FILE` 重跑，不生成空预测。
- **赛果未完结**：记录 pending，下一天继续；不填 0:0。
- **外部匹配不唯一**：运行 `python3 main.py review-queue`，完善映射后重跑。
- **API 特征部分失败**：摘要中保留警告，模型只使用已有赛前数据。
- **重复执行**：文件锁阻止并发；同日非 refresh 不重复生成相同模型预测。

官方用法参考：<https://www.kimi.com/help/kimi-claw/overview>、<https://www.kimi.com/help/kimi-claw/concepts>、<https://www.kimi.com/help/kimi-claw/usage-tips>。
