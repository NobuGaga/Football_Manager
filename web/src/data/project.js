const stats = [
  { value: '04', unit: '种', label: '每日报告格式', detail: 'JSON / CSV / Excel / Markdown' },
  { value: '08', unit: '张', label: '核心数据表', detail: '从对阵快照到模型版本' },
  { value: '05', unit: '个', label: '前列比分输出', detail: '来自完整比分概率矩阵' },
  { value: '04', unit: '类', label: 'AI 策略分类', detail: 'Kimi / ChatGPT / Claude / Gemini' },
];

const principles = [
  { number: '01', title: '官方对阵定义边界', description: '体彩官方对阵决定分析哪些比赛，外部数据源只补充赛前信息，不扩大分析范围。', mark: '界' },
  { number: '02', title: '推断版本只追加', description: '临场信息更新会生成新版本，赛前初版不被覆盖，任何一次判断都可回溯。', mark: '版' },
  { number: '03', title: '时间边界防泄漏', description: '赛前特征必须早于推断，推断必须早于开赛，不满足条件的记录不进入回测。', mark: '时' },
  { number: '04', title: '异常显式中止', description: '关联失败、数据缺失或工作区不干净时不强行推送，更不用空数据生成推断。', mark: '止' },
];

const flowSteps = [
  {
    short: '官方对阵', eyebrow: 'INPUT', title: '先确定“分析哪些比赛”',
    description: '手动 CSV／JSON／Excel 导入是稳定主路径，体彩公开接口提供可降级的自动入口。',
    detail: '系统校验编号、营业日、联赛、双方和开赛时间；接口异常、必要字段缺失或记录重复时显式退出。',
    input: '官方对阵文件 / 公开接口', output: '标准化对阵记录', mark: '阵',
  },
  {
    short: '快照关联', eyebrow: 'SNAPSHOT', title: '每次变化都有原始证据',
    description: '使用 fixture_key 关联跨周编号，原始快照按内容哈希不可变追加。',
    detail: '对阵更新只改变当前视图，历史快照保持不动；UTC 开赛时间与原始执行时区同时保留。',
    input: '标准化对阵记录', output: 'fixtures + fixture_snapshots', mark: '照',
  },
  {
    short: '赛前特征', eyebrow: 'FEATURES', title: '只使用当时已经知道的信息',
    description: '预期进球、xG、攻防乘数、伤停与阵容等特征都带有 as_of 时间截面。',
    detail: '本地赛前文件保留 as_of 时间截面；项目 AI 策略文档决定分类与表达方式，不调用外部足球数据 API。',
    input: 'match_features / team_aliases', output: '带来源与完整度的赛前输入', mark: '征',
  },
  {
    short: '概率推断', eyebrow: 'MODEL', title: '从预期进球到完整比分分布',
    description: 'Poisson 与 Dixon–Coles 基线输出三向概率、双方预期进球和前 5 个比分。',
    detail: '模型参数写入 model_versions，推断记录追加保存；同日非 refresh 不重复生成同模型推断。',
    input: '赛前特征 + 模型参数', output: 'predictions（追加式）', mark: '率',
  },
  {
    short: '每日报告', eyebrow: 'REPORT', title: '同一批结果，四种阅读方式',
    description: '每日报告同步生成 JSON、CSV、Excel 和 Markdown，兼顾程序读取与人工复核。',
    detail: '每场包含概率、比分、数据完整度、分析依据、风险提示、模型版本和推断时间。',
    input: '当日推断与对阵', output: 'reports/足球经理的推断_日期.*', mark: '报',
  },
  {
    short: '赛果核对', eyebrow: 'RESULT', title: '未完结比赛绝不写成赛果',
    description: '只同步 FT／AET／PEN 且双方进球非空的比赛，逐条关联推断与结果。',
    detail: '延期、取消与未完结状态被明确区分；无法唯一匹配时进入待处理队列，而不是填入伪赛果。',
    input: '已完结赛果', output: 'results + 复盘字段', mark: '果',
  },
  {
    short: '滚动回测', eyebrow: 'EVALUATE', title: '用长期概率质量代替单次命中',
    description: '只评价已保存且时间边界合法的推断，分别统计总体与分联赛表现。',
    detail: '回测关注 Log Loss、Brier、准确率、比分命中、预期进球误差和概率校准，不用未来赛果重写赛前判断。',
    input: '合法推断 + 实际赛果', output: '回测指标与概率校准', mark: '测',
  },
];

const tables = ['fixtures', 'fixture_snapshots', 'team_aliases', 'match_features', 'predictions', 'results', 'model_versions', 'match_review_queue'];

const priorities = [
  { id: '01', title: '明确预期进球', detail: '直接输入双方 expected goals，优先级最高。' },
  { id: '02', title: 'xG / xGA 组合', detail: '结合本队进攻与对手防守表现估计。' },
  { id: '03', title: '联赛均值 × 攻防乘数', detail: '使用相对联赛均值的攻防强度。' },
  { id: '04', title: '完整赔率降级估计', detail: '去除水位后估计，并写入风险标记。' },
  { id: '05', title: '联赛基线均值', detail: '信息不足时低完整度兜底，不输出强结论。' },
];

const metrics = [
  { name: 'Log Loss', label: '概率质量' },
  { name: 'Brier 分数', label: '三分类误差' },
  { name: '胜平负准确率', label: '方向判断' },
  { name: '比分命中率', label: '首选 / 前三' },
  { name: '预期进球 MAE', label: '进球误差' },
  { name: '概率校准', label: '分箱频率' },
];

const commandModes = [
  {
    id: 'daily', label: '每日任务', badge: '推荐入口', title: '一键完成回填、推断、回测与提交',
    description: '开始前检查非并发锁与 Git 工作区，成功后只暂存数据库、报告和跨端报告快照。',
    command: 'bash scripts/daily_automation.sh',
  },
  {
    id: 'manual', label: '手动验收', badge: '禁用 Git', title: '先用官方文件验证完整流程',
    description: '不拉取、不提交、不推送；对阵与赛果日期需要按实际营业日补充。',
    command: 'DAILY_NO_GIT=1 \\\nDAILY_FIXTURES_FILE=/绝对路径/今日对阵.csv \\\nDAILY_RESULTS_FILE=/绝对路径/昨日赛果.csv \\\nDAILY_DATE=YYYY-MM-DD \\\nDAILY_RESULTS_DATE=YYYY-MM-DD \\\nbash scripts/daily_automation.sh',
  },
  {
    id: 'review', label: '赛后复盘', badge: '滚动评价', title: '赛果落库后再评价已保存推断',
    description: '回测只使用满足时间边界且已有真实赛果的记录。',
    command: 'python3 main.py import-results examples/results.csv\npython3 main.py check\npython3 main.py backtest --output reports/回测结果.json\npython3 main.py report',
  },
];

const automationSteps = [
  '非并发锁与 Git 工作区检查',
  '可选 git pull --rebase',
  '初始化 / 迁移数据库',
  '回填昨日已完结赛果',
  '核对历史推断',
  '导入今日官方对阵',
  '补充赛前特征',
  '生成推断与四格式报告',
  '回测并输出中文摘要',
  '导出 H5／微信小游戏共享报告快照',
  '提交并推送数据产物',
  '测试、构建并上传微信小游戏体验版',
  'Claw 读取当日报告与回测',
  '生成并推送中文预测策略',
];

const safetyRules = [
  { title: '最小提交范围', description: '只暂存数据库、reports/ 和跨端报告快照，不提交缓存与凭证。', mark: 'Git' },
  { title: '失败保留现场', description: '不自动 stash、不强制 push，关联冲突与未完结赛果进入待处理。', mark: '停' },
  { title: '重复执行受控', description: '文件锁阻止并发；临场更新追加版本，同日初版不重复生成。', mark: '锁' },
];

const projectFiles = [
  { path: 'main.py', type: '入口', description: 'argparse 全部命令入口，包含导入、推断、报告、回测和 daily 自动化。', tag: 'CLI' },
  { path: 'src/automation.py', type: '编排', description: '负责 Git 安全检查、每日流程编排、非并发锁、中文摘要与安全提交。', tag: 'AUTOMATION' },
  { path: 'src/database.py', type: '数据', description: '定义 SQLite schema、数据库连接，以及旧 matches 数据的无损迁移。', tag: 'SQLITE' },
  { path: 'src/importers.py', type: '数据', description: '处理对阵、球队映射、赛前特征与赛果导入，执行字段和时间校验。', tag: 'IMPORT' },
  { path: 'src/predictor.py', type: '模型', description: '实现 Poisson + Dixon–Coles 基线，生成完整比分概率与三向概率。', tag: 'MODEL' },
  { path: 'src/ai_strategies.py', type: '策略', description: '加载项目 AI 策略文档，校验分类并保存文档哈希。', tag: 'STRATEGY' },
  { path: 'src/scraper.py', type: '适配', description: '体彩官方公开接口入口，保存原始快照，失败时显式降级到手动导入。', tag: 'SOURCE' },
  { path: 'src/reporter.py', type: '输出', description: '生成每日 JSON／CSV／Excel／Markdown 报告和回测 Excel。', tag: 'REPORT' },
  { path: 'src/result_checker.py', type: '复盘', description: '逐条关联已保存推断与实际赛果，写入复盘字段。', tag: 'RESULT' },
  { path: 'src/backtest.py', type: '复盘', description: '计算 Log Loss、Brier、概率校准、比分命中和预期进球误差。', tag: 'BACKTEST' },
  { path: 'web/src/', type: '前端', description: 'Taro + React H5 页面，以及每日报告、工程内容和展示派生逻辑的共享数据源。', tag: 'H5 / DATA' },
  { path: 'web/minigame/', type: '前端', description: 'Canvas 2D 微信小游戏运行时，复用共享数据并提供六个信息页面和触控交互。', tag: 'MINIGAME' },
  { path: 'data/football.db', type: '产物', description: '保存对阵、不可变快照、赛前特征、追加式推断、赛果与模型版本。', tag: 'RUNTIME' },
  { path: 'reports/', type: '产物', description: '保存每日四格式报告、回测结果、运行状态与中文摘要，是需要写回仓库的数据产物。', tag: 'OUTPUT' },
];

const skillTestText = '测试实现需求 skill 试验';

module.exports = {
  skillTestText,
  stats,
  principles,
  flowSteps,
  tables,
  priorities,
  metrics,
  commandModes,
  automationSteps,
  safetyRules,
  projectFiles,
};
