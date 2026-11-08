# Football Manager：项目上下文入口与每日 SOP

本文件是项目级上下文入口，放在仓库根目录供 Codex 默认读取，也供人和其他 AI 在每次仓库任务前阅读。原有工作边界与 Kimi Claw 每日 SOP 统一保留于此。

## 文档路由

开始任务时，先阅读本文件和 [README.md](README.md)，了解项目用途、运行入口和命令约定，再按下表读取与当前任务有关的文档。路由链接不会自动展开，必须主动打开对应文件；同一任务中已读且未变化的内容无需重复读取。

| 任务范围 | 必读文档 | 读取重点 |
|---|---|---|
| 项目介绍、环境安装、命令使用 | [README.md](README.md) | 依赖、输入方式、命令参数与运行示例 |
| 对阵导入、赛前特征、推断、报告、赛果核对或回测 | [数据与推断规则](rules/football_manager_rules.md)、[概率分析与复盘策略](rules/football_manager_policy.md) | 来源优先级、时间边界、版本管理、数据完整度、概率评估与报告表达 |
| 代码修改、架构、数据库、模型公式、接口或测试 | [项目工程文档](docs/项目工程.md)，再读取对应源码与测试 | 数据流程、字段约定、模型口径、迁移策略和验证范围；涉及业务行为时同时读取上面的两份现行规则 |
| Kimi Claw 接入、每日运行、cron 配置或故障处理 | 本文件第 3–8 节、[README.md](README.md)、[项目工程文档](docs/项目工程.md)第 9、11 节 | 接入、手动验收、执行顺序、环境变量、提交范围与故障处理；运行推断流程前同时读取两份现行规则 |
| 历史版本对比或审计 | [旧版规则 V2](docs/legacy/football_manager_rules_v2.md)、[旧版策略 V6](docs/legacy/football_manager_policy_v6.md)，以及对应现行规则 | 仅在任务涉及历史内容时读取，旧版不作为现行模型参数或执行依据 |

`rules/` 中的两份文档是现行业务规则，`docs/项目工程.md` 说明工程实现，`README.md` 说明命令用法。发现文档与代码不一致时，核实并说明差异。下文每日 SOP 在用户要求每日运行、部署或排障时使用；读取上下文不代表执行每日命令。

## 1. 目标与边界

每日按 Asia/Shanghai 日期由 Kimi Claw 执行：拉取仓库 → 回填昨日赛果 → 核对历史推断 → 获取今日官方对阵 → 补充赛前数据 → 生成今日推断和报告 → 打包小程序每日数据 → 更新概率质量统计 → 提交数据产物并推送 → 测试并上传小程序体验版 → 读取当日报告生成中文预测策略 → 推送到聊天频道。

运行职责统一如下：

- GitHub 只保存当前项目的源码、文档和数据产物版本，不运行 Actions、定时任务、策略生成、消息推送或小程序上传。
- Kimi Claw 负责每日定时运行、小程序每日数据打包与体验版上传、预测策略整理和聊天频道推送。
- Python 命令负责可复现的数据处理、概率模型、报告生成和小程序数据导出；Claw 不绕过命令直接修改数据库或伪造推断。

约束：

- 体彩官方对阵决定分析范围；本地赛前文件和项目策略文档提供输入，不调用外部足球数据 API。
- 不绕过验证码、登录、反爬或访问限制；官方公开接口失败时显式中止，改用官方对阵文件。
- 推断是概率分析，不使用“包中”“稳赢”等措辞，不承诺收益。
- 不覆盖历史推断；临场更新必须追加版本。
- 不使用赛后信息生成赛前推断，不错配比赛，不把未完结比赛写成赛果。
- 预测策略只能依据当日已落库报告和合法赛前信息，保留概率、数据完整度与风险，不额外虚构推荐。
- 每日自动化聊天渠道只发送中文预测策略摘要和报告路径；“实现需求”任务按第 8 节发送执行结果摘要。均不发送数据库、凭证或大段原始数据。

## 2. 关键文件

| 文件 | 用途 |
|---|---|
| `main.py` | 全部命令入口，`daily` 是每日总流程 |
| `src/automation.py` | Git 检查、流程编排、锁、摘要与安全提交 |
| `src/miniprogram_export.py` | 将当日 JSON 报告转换为紧凑的小程序数据 |
| `src/ai_strategies.py` / `rules/strategies/` | AI 模型策略文档加载、校验与分类 |
| `data/football.db` | 对阵、快照、特征、推断、赛果和模型版本 |
| `reports/` | 每日 JSON／CSV／Excel／Markdown、回测和中文摘要 |
| `miniprogram/data/daily-report.js` | 每日推断页面的打包报告快照 |
| `scripts/daily_automation.sh` | Claw／cron 推荐入口 |
| `scripts/wechat_daily_upload.sh` | 小程序测试与每日体验版上传入口 |
| `rules/football_manager_policy.md` | Claw 预测策略整理与推送口径 |
| `docs/项目工程.md` | 架构、字段、模型和回测口径 |

`reports/` 和 `miniprogram/data/daily-report.js` 是需要写回仓库的数据产物。`data/cache/`、`data/snapshots/`、`.env` 和锁文件不提交。

## 3. 一次性接入（先配置，不挂 cron）

1. 在 GitHub 创建 fine-grained personal access token：只授权本仓库，Contents 读写，设置有效期并定期轮换。
2. 在 Claw 的安全终端配置 Git 凭证并 clone。不要把 token 写入 `MEMORY.md`、本文件、仓库、远程 URL、脚本或执行日志；优先使用环境凭证或凭证助手。
3. 配置 Git 用户名和邮箱，验证 `git pull` 与一次人工测试提交／推送。
4. 安装依赖并初始化：

   ```bash
   python3 -m pip install -r requirements.txt
   python3 main.py init
   cd miniprogram && npm ci && cd ..
   ```

5. 正式每日任务默认上传小程序，需安装 Node.js，并在 Claw 安全环境配置 `WECHAT_APPID` 和多行私钥 `WECHAT_PRIVATE_KEY`。凭证不写仓库或日志；如启用上传 IP 白名单，加入 Claw 固定出口 IP。
6. 通过 `AI_MODEL` 选择 `rules/strategies/` 中的 AI 分类，默认 `kimi`；可用 `python3 main.py ai-strategies` 查看。无需任何足球数据 API 密钥。
7. 如需微信、飞书或企业微信通知，由用户在 Kimi Claw「设置 → 聊天频道」配置，工程不保存渠道凭证。

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

没有文件时，可仅设置 `DAILY_NO_GIT=1` 测试官方公开接口对阵入口。`DAILY_NO_GIT=1` 不拉取、不提交、不推送、不上传小程序。赛果文件要求昨日对阵已存在于数据库，否则系统会停止而不是猜测关联。

验收项目：

- 中文摘要中的对阵、赛果、推断数量与实际一致；
- `reports/足球经理的推断_YYYY-MM-DD.*` 四种格式均可读取；
- `miniprogram/data/daily-report.js` 与当日 JSON 报告日期、比赛数和概率一致；
- 小程序概览可进入“每日推断”，比赛、比分、完整度和风险显示正常；
- Claw 按 `rules/football_manager_policy.md` 生成的预测策略覆盖当日全部有效对阵，概率、比分、完整度和风险与 JSON 报告一致；
- 回测只包含有赛果且时间边界合法的推断；
- 风险与待确认事项没有被隐藏；
- 工作区没有凭证、缓存或无关源码被加入提交。

手动推送验收：确认工作区干净后执行 `bash scripts/daily_automation.sh`，到 GitHub 检查提交只包含 `data/football.db`、`reports/` 和 `miniprogram/data/daily-report.js`。确认 Git 推送和小程序上传成功后，检查体验版每日页面，再由 Claw 生成并向目标频道发送中文预测策略。

## 5. 正式每日命令

```bash
bash scripts/daily_automation.sh
```

默认执行 `git pull --rebase`，成功后暂存 `data/football.db`、`reports/` 和 `miniprogram/data/daily-report.js`，再 commit／push。随后 `scripts/wechat_daily_upload.sh` 运行小程序测试并上传体验版。工作区不干净、已有任务运行、对阵或赛果关联失败时均中止，不强行覆盖或推送。预测策略整理和聊天频道推送由 Claw 在每日命令及上传成功后完成。

可选环境变量：

| 变量 | 作用 |
|---|---|
| `DAILY_DATE` / `DAILY_RESULTS_DATE` | 指定对阵和赛果所属的体彩营业日 |
| `DAILY_FIXTURES_FILE` / `DAILY_RESULTS_FILE` | 使用官方文件代替自动入口 |
| `DAILY_REFRESH=1` | 追加临场推断版本 |
| `DAILY_REQUIRE_RESULTS=1` | 昨日有比赛但未可靠回填时中止 |
| `DAILY_NO_GIT=1` | 手动测试，禁用 Git 拉取、推送和小程序上传 |
| `DAILY_MINIPROGRAM_UPLOAD=0` | 显式关闭正式任务的小程序上传 |
| `NODE_BIN` | 指定 Node.js 可执行文件，默认 `node` |
| `WECHAT_APPID` / `WECHAT_PRIVATE_KEY` | 每日体验版上传凭证 |
| `AI_MODEL` | 选择项目 AI 策略分类，默认 `kimi` |

## 6. cron 配置（手动验收之后）

避开整点，任务指令写清执行入口、策略来源、输出和约束：

```text
/cron add "13 9 * * *" 在 Football_Manager 仓库根目录执行 bash scripts/daily_automation.sh；仅在命令成功且 Git 推送完成后，读取当日 reports/足球经理的推断_YYYY-MM-DD.json、每日运行状态_YYYY-MM-DD.json、每日中文摘要_YYYY-MM-DD.md 和回测结果_YYYY-MM-DD.json，严格按 rules/football_manager_policy.md 生成并推送中文预测策略；逐场保留比赛编号、对阵、开赛时间、三向概率、最高概率倾向、比分参考、数据完整度和风险，同时发送运行统计、回测指标、报告路径、Git 提交和待处理风险；失败时明确报告失败步骤，不生成或推送旧日策略，不推送半成品，不输出任何凭证
```

维护：`/cron` 查看，`/cron log <任务ID>` 查日志，`/cron enable/disable <任务ID>` 启用或暂停。故障依次检查 `/status` → `/logs` → `/cron log <任务ID>`。

## 7. 失败处理

- **Git dirty**：先人工审查修改，不自动 stash、删除或覆盖。
- **pull／push 冲突**：保留现场并人工处理，不强制 push。
- **官方公开接口无对阵**：使用 `DAILY_FIXTURES_FILE` 重跑，不生成空推断。
- **赛果未完结**：记录 pending，下一天继续；不填 0:0。
- **外部匹配不唯一**：运行 `python3 main.py review-queue`，完善映射后重跑。
- **本地特征不足**：摘要和报告中保留缺失项，模型只使用已有赛前数据与所选策略文档。
- **小程序上传失败**：保留上传错误和已推送的 Git 提交信息，不宣称体验版已更新；修复凭证、Node.js 或白名单后运行 `bash scripts/wechat_daily_upload.sh` 重试。
- **重复执行**：文件锁阻止并发；同日非 refresh 不重复生成相同模型推断。

官方用法参考：<https://www.kimi.com/help/kimi-claw/overview>、<https://www.kimi.com/help/kimi-claw/concepts>、<https://www.kimi.com/help/kimi-claw/usage-tips>。

## 8. 微信“实现需求”入口

使用 Kimi Claw 原生微信渠道接收指令并回复执行结果，不修改 `.agents/skills/implement-requirement/SKILL.md`，不另建微信消息脚本。

1. 由用户在 Kimi Claw「设置 → 聊天频道 → 微信」扫码连接。渠道凭证只保存在 Kimi Claw 配置中，不写入仓库或长期记忆。
2. Claw 在仓库根目录收到以“实现需求”开头的消息时，主动读取并严格遵循 `.agents/skills/implement-requirement/SKILL.md`，将首个“实现需求”之后的内容作为具体需求。`/skills` 未列出该技能时仍按此路由读取文件，不把未注册当作跳过执行的理由。
3. 只有“实现需求”前缀而没有具体需求时，按 Skill 约定请求补充，不修改文件、不提交、不发布。不要使用其他会话或旧消息中的需求代替。
4. 执行完成后，无论成功、部分完成或失败，都把 Skill 第 5 节要求的最终回复发送到发起指令的原微信会话；长时间任务可以先回复“已收到，开始执行”，但开始执行不代表完成。
5. 微信回复只包含中文实现摘要、验证结果、GitHub commit／分支／推送状态、小程序版本／上传／体验版状态和待处理事项。不发送 Git 或微信凭证、私钥、原始日志、数据库、大段代码或大段原始数据。
6. 执行前确认 Claw 环境已有仓库和 GitHub 推送凭证，以及 Node.js／npm、`WECHAT_APPID`、`WECHAT_PRIVATE_KEY`。缺失时仍按 Skill 停止受阻步骤并如实回复，不用预览二维码代替体验版，不虚报成功。
7. 首次接入后使用一个低风险真实需求验收：微信指令只触发一次，Claw 执行同一 Skill，GitHub 与小程序状态真实可查，原微信会话收到最终成功或失败结果。读取本节不代表自动执行验收。
