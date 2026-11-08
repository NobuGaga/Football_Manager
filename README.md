# Football Manager：足球经理的推断 · 多联赛概率分析

以中国体育彩票官方足球对阵确定分析范围，使用 SQLite 保存每日快照、赛前特征、版本化推断和赛果，并输出供 Kimi 批量读取的 JSON、CSV、Excel 与 Markdown 报告。

> 推断是概率估计，不构成收益保证或确定性投注建议。信息不足时报告会标记数据缺失和风险。

## 工程说明

项目默认上下文入口与文档读取路由见 [AGENTS.md](AGENTS.md)，Codex 会默认读取仓库根目录的该文件。完整架构、数据表、模型口径、迁移策略与方案覆盖情况见 [项目工程文档](docs/项目工程.md)。输入样例位于 `examples/`。Kimi Claw 接入和每日标准流程也见 `AGENTS.md`。

GitHub 仅用于保存当前项目的源码、文档和数据产物版本，不承担定时计算、预测策略生成、消息推送或小程序上传。上述运行任务统一由 Kimi Claw 执行，仓库不保留 GitHub Actions 工作流。

对外名称为“足球经理的推断”，正文简称“推断”；命令继续使用 `predict`。新每日报告命名为 `reports/足球经理的推断_YYYY-MM-DD.*`，中文比赛标识使用“比赛编号”，导入兼容旧列名。历史报告与原始快照保留原貌。

## Kimi Claw 每日自动化与策略推送

```bash
# 示例赛果需先有关联对阵；真实每日任务使用库中已有昨日对阵
python3 main.py import-fixtures examples/fixtures.csv

# 手动验收：不拉取、不提交、不推送
DAILY_NO_GIT=1 DAILY_DATE=2099-10-25 DAILY_RESULTS_DATE=2099-10-25 \
DAILY_FIXTURES_FILE="$PWD/examples/fixtures.csv" \
DAILY_RESULTS_FILE="$PWD/examples/results.csv" \
bash scripts/daily_automation.sh

# 正式任务：拉取、回填、推断、回测、提交并推送
bash scripts/daily_automation.sh
```

正式入口要求 Git 工作区干净，成功后只提交 `data/football.db` 和 `reports/`。配置 `API_FOOTBALL_KEY` 后会自动补充赛前特征并同步昨日已完结赛果；未配置时会在中文摘要中明确标记跳过。

命令成功并完成 Git 推送后，Kimi Claw 读取当日 JSON 报告、运行状态和回测结果，按现行概率策略生成中文预测策略并推送到已配置频道。策略必须保留每场三向概率、数据完整度和风险，不得改写成确定性或收益承诺。cron 指令、策略表达、频道配置和故障处理见 `AGENTS.md` 与 `rules/football_manager_policy.md`，先手动验收再设置定时。

## 环境与安装

- Python 3.10+
- SQLite（Python 自带）
- 自动对阵抓取使用体彩官方公开接口，不需要本机 Chrome 或 Selenium

```bash
python3 -m pip install -r requirements.txt
python3 main.py init
```

初始化会创建新数据表，并将旧版 `matches` 数据迁移到 `fixtures`／`results`。原表保留用于审计；旧推断没有原始概率，因此不会伪造概率写入 `predictions`。

## 每日手动对阵流程（推荐）

```bash
# 1. 导入官方对阵；--date 和比赛日期均指体彩营业日，凌晨开赛不改变营业日
python3 main.py import-fixtures examples/fixtures.csv --date 2099-10-25

# 2. 可选：维护球队标准名、英文名和外部 ID
python3 main.py import-aliases examples/aliases.csv

# 3. 可选：导入赛前特征；as_of 表示这些信息何时已经可用
python3 main.py import-features examples/features.csv

# 4. 生成胜平负概率、预期进球和前 5 个比分
python3 main.py predict --date 2099-10-25

# 5. 输出每日报告，默认写入 reports/
python3 main.py report --date 2099-10-25

# 也可一步执行导入、推断与报告
python3 main.py all --date 2099-10-25 --fixtures examples/fixtures.csv
```

临场信息更新后重新导入特征，再执行 `predict --date YYYY-MM-DD --refresh`。系统会追加新版本，不覆盖赛前初版。

## 赛后复盘

```bash
python3 main.py import-results examples/results.csv
python3 main.py check
python3 main.py backtest --output reports/回测结果.json
python3 main.py report  # 生成回测 Excel
```

回测包含 Log Loss、Brier 分数、胜平负准确率、首选／前三比分命中率、预期进球 MAE 和概率校准。只有满足 `input_as_of ≤ predicted_at ≤ 开赛时间` 的记录才进入指标。

## 可选数据源与网页入口

### API-Football

```bash
export API_FOOTBALL_KEY="你的密钥"
python3 main.py enrich --date YYYY-MM-DD
python3 main.py predict --date YYYY-MM-DD --refresh
```

该适配器通过球队映射、对阵双方和开赛时间进行唯一匹配；无法确认时进入人工队列。响应缓存在 `data/cache/`，可减少重复请求。高级字段是否可用取决于账户套餐，未获取到 xG 时不会伪造。

### 体彩官方公开接口

```bash
python3 main.py scrape --date YYYY-MM-DD
```

抓取直接读取体彩页面使用的官方公开接口，按体彩营业日期筛选，不依赖 Chrome 或页面 CSS 结构。请求设置 30 秒超时；原始 JSON 保存到 `data/snapshots/`。业务状态异常、指定日期无对阵、必要字段缺失或记录重复时命令显式退出，不会用空数据继续推断。请遵守网站条款和访问限制，不绕过验证码、登录或反爬机制；不稳定时改用手动导入。

## 其他命令

```bash
python3 main.py review-queue       # 查看待确认名称或外部比赛
python3 main.py --db other.db init # 使用其他数据库
python3 -m unittest discover -s tests -v
```

所有命令的全局 `--db` 参数必须放在子命令之前。运行 `python3 main.py --help` 可查看完整参数。
