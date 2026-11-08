const assert = require('node:assert/strict');
const path = require('node:path');

const calls = [];
global.wx = {
  switchTab: (options) => calls.push(['switchTab', options.url]),
  navigateTo: (options) => calls.push(['navigateTo', options.url]),
  setClipboardData: (options) => {
    calls.push(['copy', options.data]);
    options.success();
  },
  showToast: () => {},
};

function loadPage(relativePath) {
  let definition;
  global.Page = (value) => { definition = value; };
  const pagePath = path.resolve(__dirname, '..', relativePath);
  delete require.cache[pagePath];
  require(pagePath);
  const instance = {
    ...definition,
    data: JSON.parse(JSON.stringify(definition.data)),
    setData(update) {
      this.data = { ...this.data, ...update };
    },
  };
  return instance;
}

const overview = loadPage('pages/overview/overview.js');
overview.onLoad();
assert.equal(overview.data.matrix.length, 49);
overview.goDaily();
overview.goPipeline();
overview.copyDailyCommand();
assert.ok(calls.some(([type, value]) => type === 'navigateTo' && value === '/pages/daily/daily'));
assert.ok(calls.some(([type, value]) => type === 'switchTab' && value === '/pages/pipeline/pipeline'));
assert.ok(calls.some(([type, value]) => type === 'copy' && value === 'bash scripts/daily_automation.sh'));

const { dailyReport } = require('../data/daily-report.js');
const daily = loadPage('pages/daily/daily.js');
daily.onLoad();
assert.equal(daily.data.reportDate, dailyReport.reportDate);
assert.equal(daily.data.matchCount, dailyReport.matchCount);
assert.equal(daily.data.matches.length, dailyReport.matches.length);
assert.equal(daily.data.matches[0].topScores.length, 2);
assert.match(daily.data.matches[0].homePercent, /%$/);
assert.ok(daily.data.matches[0].tendency);

const pipeline = loadPage('pages/pipeline/pipeline.js');
pipeline.selectFlow({ currentTarget: { dataset: { index: 3 } } });
assert.equal(pipeline.data.currentFlow.short, '概率推断');
pipeline.nextFlow();
assert.equal(pipeline.data.currentFlow.short, '每日报告');

const model = loadPage('pages/model/model.js');
model.copyTimeBoundary();
assert.ok(calls.some(([type, value]) => type === 'copy' && value.includes('predicted_at <= start_time_utc')));

const automation = loadPage('pages/automation/automation.js');
automation.selectCommand({ currentTarget: { dataset: { mode: 'manual' } } });
assert.equal(automation.data.currentCommand.id, 'manual');
automation.copyCommand();
assert.ok(calls.some(([type, value]) => type === 'copy' && value.includes('DAILY_NO_GIT=1')));

const structure = loadPage('pages/structure/structure.js');
structure.selectFile({ currentTarget: { dataset: { index: 11 } } });
assert.equal(structure.data.currentFile.path, 'reports/');
structure.copyFilePath();
structure.copyTestCommand();
assert.ok(calls.some(([type, value]) => type === 'copy' && value === 'reports/'));
assert.ok(calls.some(([type, value]) => type === 'copy' && value.includes('unittest discover')));

console.log('PAGE_LOGIC_OK overview daily pipeline model automation structure');
