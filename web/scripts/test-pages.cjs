const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const { dailyReport } = require('../generated/daily-report.js');
const project = require('../src/data/project.js');
const { boundedIndex, createMatrix, decorateMatches } = require('../src/utils/report.js');
const { writeClipboard } = require('../src/utils/clipboard-core.js');
const { createNavigator, PAGE_PATHS } = require('../src/utils/navigation-core.js');

assert.equal(project.stats.length, 4);
assert.equal(project.principles.length, 4);
assert.equal(project.flowSteps.length, 7);
assert.equal(project.tables.length, 8);
assert.equal(project.priorities.length, 5);
assert.equal(project.metrics.length, 6);
assert.equal(project.commandModes.length, 3);
assert.equal(project.safetyRules.length, 3);
assert.ok(project.projectFiles.some((item) => item.path === 'web/src/'));
assert.equal(Object.hasOwn(project, 'skillTestText'), false);
assert.doesNotMatch(JSON.stringify(project), /测试实现需求|提交并推送数据产物|只暂存数据库|需要写回仓库|安全提交/);
assert.ok(project.automationSteps.includes('核查本地关键文件未进入 Git'));
assert.equal(project.safetyRules[0].title, '关键文件仅本地保存');
assert.match(project.commandModes[0].description, /仅保存本地，不进入 Git/);
assert.match(project.projectFiles.find((item) => item.path === 'reports/').description, /仅保留在本地，不进入 Git/);
const overviewSource = fs.readFileSync(path.join(root, 'src', 'pages', 'overview', 'index.jsx'), 'utf8');
assert.doesNotMatch(overviewSource, /skillTestText|skill-test-banner|测试实现需求/);
const overviewStyles = fs.readFileSync(path.join(root, 'src', 'pages', 'overview', 'index.css'), 'utf8');
assert.doesNotMatch(overviewStyles, /\.skill-test-banner/);
const automationSource = fs.readFileSync(path.join(root, 'src', 'pages', 'automation', 'index.jsx'), 'utf8');
assert.match(automationSource, /关联失败时立即中止，不上传半成品/);
const automationStyles = fs.readFileSync(path.join(root, 'src', 'pages', 'automation', 'index.css'), 'utf8');
assert.match(automationStyles, /\.command-tab,\s*\.copy-button\s*\{[^}]*background: transparent/);
const structureSource = fs.readFileSync(path.join(root, 'src', 'pages', 'structure', 'index.jsx'), 'utf8');
const structureStyles = fs.readFileSync(path.join(root, 'src', 'pages', 'structure', 'index.css'), 'utf8');
assert.match(structureStyles, /\.file-row\s*\{[^}]*background: transparent/);
assert.match(structureSource, /仅保存本地，不进入 Git/);
assert.doesNotMatch(structureSource, /按安全范围写回/);
const dailySource = fs.readFileSync(path.join(root, 'src', 'pages', 'daily', 'index.jsx'), 'utf8');
assert.match(dailySource, /index === 0/);
assert.match(dailySource, /function FeaturedMatch/);
assert.match(dailySource, /function CompactMatch/);
assert.match(dailySource, /function MatchCard/);
assert.match(dailySource, /function MatchMain/);
assert.match(dailySource, /function TeamBadge/);
assert.match(dailySource, /function Scoreboard/);
assert.match(dailySource, /function FeaturedProbability/);
assert.match(dailySource, /function ProbabilityBar/);
assert.match(dailySource, /今日比赛中心/);
assert.match(dailySource, /tendencyLabel/);
assert.match(dailySource, /tendencyPercent/);
assert.equal((dailySource.match(/<MatchDetails item=\{item\} compact=\{compact\} \/>/g) || []).length, 1, '首场与紧凑卡必须共用完整详情，不得省略比分、完整度、方法和风险');
const dailyStyles = fs.readFileSync(path.join(root, 'src', 'pages', 'daily', 'index.css'), 'utf8');
assert.match(dailyStyles, /\.match-compact/);
assert.match(dailyStyles, /\.match-teams-scan/);
assert.match(dailyStyles, /\.match-tendency-scan/);
assert.match(dailyStyles, /\.team-badge/);
assert.match(dailyStyles, /\.featured-probability/);
assert.match(dailyStyles, /\.daily-notice/);
assert.match(dailyStyles, /\.probability-segment\.active/);
assert.equal(createMatrix().length, 49);
assert.equal(createMatrix()[24].hot, true);
assert.equal(boundedIndex(3, project.flowSteps.length), 3);
assert.equal(boundedIndex(99, project.flowSteps.length), project.flowSteps.length - 1);
assert.equal(boundedIndex(-1, project.flowSteps.length), 0);

const matches = decorateMatches(dailyReport);
assert.equal(matches.length, dailyReport.matches.length);
assert.equal(matches.length, dailyReport.predictionCount);
assert.equal(new Set(matches.map((item) => item.strategyKey)).size, matches.length);
assert.deepEqual(decorateMatches({ matches: [] }), []);
for (const [index, match] of matches.entries()) {
  const source = dailyReport.matches[index];
  assert.equal(match.aiModel, source.aiModel);
  assert.equal(match.strategyCategory, source.strategyCategory);
  assert.equal(match.homePercent, `${(source.probabilities.home_win * 100).toFixed(1)}%`);
  assert.equal(match.drawPercent, `${(source.probabilities.draw * 100).toFixed(1)}%`);
  assert.equal(match.awayPercent, `${(source.probabilities.away_win * 100).toFixed(1)}%`);
  assert.equal(match.topScores.length, source.topScores.length);
  assert.ok(match.tendency);
  assert.ok(match.completenessText.endsWith('%'));
  assert.ok(match.primaryRisk);
}

const navigationCalls = [];
const navigator = createNavigator({
  navigateTo: ({ url }) => navigationCalls.push(['navigateTo', url]),
  switchTab: ({ url }) => navigationCalls.push(['switchTab', url]),
});
navigator.goDaily();
navigator.goPipeline();
assert.deepEqual(navigationCalls, [
  ['navigateTo', PAGE_PATHS.daily],
  ['switchTab', PAGE_PATHS.pipeline],
]);

async function testClipboard() {
  const calls = [];
  const platform = {
    setClipboardData: async ({ data, options, success }) => {
      calls.push(['copy', data, options.hideDefaultMessage]);
      success({ errMsg: 'setClipboardData:ok' });
    },
  };
  assert.equal(await writeClipboard(platform, 'bash scripts/daily_automation.sh'), true);
  assert.deepEqual(calls, [['copy', 'bash scripts/daily_automation.sh', true]]);

  for (const setClipboardData of [
    async ({ fail }) => fail({ errMsg: 'setClipboardData:fail denied' }),
    async () => { throw new Error('denied'); },
  ]) {
    assert.equal(await writeClipboard({ setClipboardData }, 'secret-free text'), false);
  }

  const copySource = fs.readFileSync(path.join(root, 'src', 'utils', 'useCopy.js'), 'utf8');
  assert.match(copySource, /useEffect/);
  assert.match(copySource, /writeClipboard\(clipboardPlatform, request.text\)/);
  assert.match(copySource, /setCopyMessage\('正在复制…'\)/);
  assert.match(copySource, /setCopyMessage\(success \? request.title : '复制失败，请稍后重试'\)/);
  const platformSource = fs.readFileSync(path.join(root, 'src', 'utils', 'clipboard-platform.js'), 'utf8');
  assert.match(platformSource, /navigator.clipboard.writeText/);
  assert.match(platformSource, /process.env.TARO_ENV === 'h5' \? h5Clipboard : Taro/);

  const pageSource = fs.readFileSync(path.join(root, 'src', 'components', 'Page.jsx'), 'utf8');
  assert.match(pageSource, /copyMessage/);
  assert.match(pageSource, /onCopyMessageClear/);
  assert.match(pageSource, /useEffect/);
  assert.match(pageSource, /role='status'/);
}

const pages = ['overview', 'daily', 'pipeline', 'model', 'automation', 'structure'];
for (const page of pages) {
  const source = fs.readFileSync(path.join(root, 'src', 'pages', page, 'index.jsx'), 'utf8');
  assert.match(source, /@tarojs\/components/);
  assert.doesNotMatch(source, /\b(?:document|window|navigator|localStorage)\s*\./);
  assert.ok(fs.existsSync(path.join(root, 'src', 'pages', page, 'index.css')));
  assert.ok(fs.existsSync(path.join(root, 'src', 'pages', page, 'index.config.js')));
}
const appConfig = fs.readFileSync(path.join(root, 'src', 'app.config.js'), 'utf8');
for (const page of pages) assert.ok(appConfig.includes(`pages/${page}/index`));
assert.ok(appConfig.includes('tabBar'));
assert.ok(!appConfig.includes('web-view'));
const appStyles = fs.readFileSync(path.join(root, 'src', 'app.css'), 'utf8');
const h5Styles = fs.readFileSync(path.join(root, 'src', 'h5.css'), 'utf8');
assert.match(h5Styles, /taro_router > \.taro_page[\s\S]*transform: none !important/);
assert.match(h5Styles, /taro_page\.taro_page_shade[\s\S]*display: none !important/);
assert.match(h5Styles, /taro_navigation_page:not\(\.taro_page_show\):not\(\.taro_tabbar_page\)[\s\S]*display: none !important/);
assert.doesNotMatch(appStyles, /:not\(|\[src|nth-child|nth-last-child/);

assert.ok(project.projectFiles.some((item) => item.path === 'web/dist/weapp/'));
assert.ok(project.projectFiles.some((item) => item.path === 'web/minigame/'));
assert.match(overviewSource, /Taro React H5 · Canvas 2D 微信小游戏/);
assert.match(structureSource, /H5 \/ 微信小游戏/);
assert.doesNotMatch(JSON.stringify(project), /上传微信小程序|不上传小程序|H5／微信小程序/);
const packageJson = require('../package.json');
const { parseDevArgs } = require('./dev-h5.cjs');
assert.deepEqual(parseDevArgs(['--host', '127.0.0.1', '--port', '7100']), { host: '127.0.0.1', port: '7100' });
assert.deepEqual(parseDevArgs(['--host=0.0.0.0', '--port=10086']), { host: '0.0.0.0', port: '10086' });
assert.throws(() => parseDevArgs(['--port', '0']), /1–65535/);
assert.equal(packageJson.scripts.dev, 'node scripts/dev-h5.cjs');
assert.equal(packageJson.scripts['dev:h5'], 'node scripts/dev-h5.cjs');
assert.ok(packageJson.scripts['build:h5'].includes('taro build'));
assert.ok(packageJson.scripts['build:minigame'].includes('build-minigame.cjs'));
assert.equal(packageJson.scripts['build:weapp'], 'npm run build:minigame');
assert.equal(packageJson.scripts['dev:weapp'], 'npm run dev:minigame');
assert.equal(packageJson.dependencies['@tarojs/taro'], packageJson.dependencies['@tarojs/components']);
assert.equal(packageJson.dependencies['@tarojs/taro'], packageJson.devDependencies['@tarojs/cli']);

testClipboard().then(() => {
  console.log('CROSS_PLATFORM_LOGIC_OK overview daily pipeline model automation structure navigation clipboard');
}).catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
