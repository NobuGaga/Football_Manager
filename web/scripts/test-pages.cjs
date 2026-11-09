const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const { dailyReport } = require('../src/data/daily-report.js');
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
assert.match(appStyles, /taro_router > \.taro_page[\s\S]*transform: none !important/);
assert.match(appStyles, /taro_page\.taro_page_shade[\s\S]*display: none !important/);

assert.ok(project.projectFiles.some((item) => item.path === 'web/minigame/'));
const packageJson = require('../package.json');
assert.ok(packageJson.scripts['build:h5'].includes('taro build'));
assert.ok(packageJson.scripts['build:weapp'].includes('build-minigame.cjs'));
assert.equal(packageJson.dependencies['@tarojs/taro'], packageJson.dependencies['@tarojs/components']);
assert.equal(packageJson.dependencies['@tarojs/taro'], packageJson.devDependencies['@tarojs/cli']);

testClipboard().then(() => {
  console.log('CROSS_PLATFORM_LOGIC_OK overview daily pipeline model automation structure navigation clipboard');
}).catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
