const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { execFileSync } = require('node:child_process');

const root = path.resolve(__dirname, '..');
execFileSync(process.execPath, [path.join(root, 'scripts', 'build-minigame.cjs')], { stdio: 'inherit' });

const { MiniGameApp, TABS } = require('../dist/weapp/src/app.js');
const { Painter } = require('../dist/weapp/src/draw.js');
const { dailyReport } = require('../src/data/daily-report.js');
const project = require('../src/data/project.js');

function createContext() {
  const texts = [];
  const textBounds = [];
  const gradient = { addColorStop() {} };
  return {
    texts,
    textBounds,
    font: 'normal 14px sans-serif',
    fillStyle: '',
    strokeStyle: '',
    lineWidth: 1,
    textAlign: 'left',
    textBaseline: 'top',
    save() {}, restore() {}, beginPath() {}, closePath() {},
    moveTo() {}, lineTo() {}, quadraticCurveTo() {}, arc() {},
    fill() {}, stroke() {}, fillRect() {}, clearRect() {}, rect() {}, clip() {}, scale() {},
    createLinearGradient: () => gradient,
    measureText(text) {
      const size = Number.parseInt(this.font.match(/(\d+)px/)?.[1] || '14', 10);
      const width = Array.from(String(text)).reduce((sum, character) => sum + (character.charCodeAt(0) > 255 ? size : size * 0.56), 0);
      return { width };
    },
    fillText(text, x, y) {
      const value = String(text);
      const width = this.measureText(value).width;
      const left = this.textAlign === 'center' ? x - width / 2 : this.textAlign === 'right' ? x - width : x;
      texts.push(value);
      textBounds.push({ text: value, left, right: left + width, top: y });
    },
  };
}

function createRuntime({ clipboardFails = false } = {}) {
  const ctx = createContext();
  const canvas = { width: 0, height: 0, getContext: () => ctx };
  const listeners = {};
  const clipboard = [];
  const platform = {
    createCanvas: () => canvas,
    getWindowInfo: () => ({
      windowWidth: 390,
      windowHeight: 844,
      pixelRatio: 2,
      safeArea: { top: 47, bottom: 810 },
    }),
    onTouchStart: (handler) => { listeners.touchStart = handler; },
    onTouchMove: (handler) => { listeners.touchMove = handler; },
    onTouchEnd: (handler) => { listeners.touchEnd = handler; },
    onTouchCancel: (handler) => { listeners.touchCancel = handler; },
    onWindowResize: (handler) => { listeners.resize = handler; },
    onShow: (handler) => { listeners.show = handler; },
    setClipboardData({ data, success, fail }) {
      clipboard.push(data);
      if (clipboardFails) fail({ errMsg: 'setClipboardData:fail denied' });
      else success({ errMsg: 'setClipboardData:ok' });
    },
  };
  return { platform, canvas, ctx, listeners, clipboard };
}

function tap(runtime, zone) {
  const point = { clientX: zone.x + zone.width / 2, clientY: zone.y + zone.height / 2 };
  runtime.listeners.touchStart({ touches: [point], changedTouches: [point] });
  runtime.listeners.touchEnd({ touches: [], changedTouches: [point] });
}

function assertTextWithinCanvas(runtime, width, label) {
  for (const bound of runtime.ctx.textBounds) {
    assert.ok(bound.left >= -0.5, `${label} 文字左溢出：${bound.text} / ${bound.left}`);
    assert.ok(bound.right <= width + 0.5, `${label} 文字右溢出：${bound.text} / ${bound.right}`);
  }
}

(async () => {
  const runtime = createRuntime();
  const app = new MiniGameApp(runtime.platform).start();
  assert.deepEqual(TABS.map((item) => item.id), ['overview', 'pipeline', 'model', 'automation', 'structure']);
  assert.equal(runtime.canvas.width, 780);
  assert.equal(runtime.canvas.height, 1688);
  assert.equal(app.page, 'overview');
  assert.equal(app.matches.length, dailyReport.matches.length);
  assert.ok(runtime.ctx.texts.includes(project.skillTestText), '小游戏首页应绘制技能试验文本');
  assert.ok(app.maxScroll > 0);
  assertTextWithinCanvas(runtime, app.width, '概览首屏');

  const dailyZone = app.zones.find((zone) => zone.action.type === 'page' && zone.action.page === 'daily');
  assert.ok(dailyZone, '概览应提供每日推断入口');
  tap(runtime, dailyZone);
  assert.equal(app.page, 'daily');
  assert.ok(app.maxScroll > 1000, '长报告应支持纵向滚动');
  assertTextWithinCanvas(runtime, app.width, '每日首屏');
  const compactMatch = app.matches[1];
  for (const expected of [
    compactMatch.homeTeam,
    compactMatch.awayTeam,
    compactMatch.homePercent,
    compactMatch.drawPercent,
    compactMatch.awayPercent,
    compactMatch.topScores[0].score,
    compactMatch.completenessText,
    compactMatch.methodText,
    compactMatch.primaryRisk,
  ]) {
    assert.ok(runtime.ctx.texts.some((text) => text.includes(expected)), `紧凑卡应实际绘制 ${expected}`);
  }
  app.scrollBy(160);
  assert.equal(app.scroll, 160);
  app.scrollBy(Number.POSITIVE_INFINITY);
  assert.equal(app.scroll, app.maxScroll);
  assertTextWithinCanvas(runtime, app.width, '每日底部');
  app.scrollBy(Number.NEGATIVE_INFINITY);
  assert.equal(app.scroll, 0);

  for (const page of ['overview', 'pipeline', 'model', 'automation', 'structure', 'daily']) {
    app.setPage(page);
    assert.equal(app.page, page);
    assert.ok(runtime.ctx.texts.length > 0, `${page} 应实际绘制文本`);
  }

  app.setPage('pipeline');
  for (let index = 0; index < project.flowSteps.length + 2; index += 1) app.handleAction({ type: 'flow-next' });
  assert.equal(app.flowIndex, project.flowSteps.length - 1);
  app.handleAction({ type: 'flow', index: -1 });
  assert.equal(app.flowIndex, 0);
  app.handleAction({ type: 'flow', index: 99 });
  assert.equal(app.flowIndex, project.flowSteps.length - 1);

  app.setPage('automation');
  app.handleAction({ type: 'mode', id: 'review' });
  assert.equal(app.commandMode, 'review');
  const reviewCommand = project.commandModes.find((item) => item.id === 'review').command;
  assert.equal(await app.handleAction({ type: 'copy', text: reviewCommand, success: '运行命令已复制' }), true);
  assert.equal(runtime.clipboard.at(-1), reviewCommand);
  assert.equal(app.message, '运行命令已复制');

  app.setPage('structure');
  app.handleAction({ type: 'file', index: 99 });
  assert.equal(app.selectedFile, project.projectFiles.length - 1);
  app.handleAction({ type: 'file', index: -1 });
  assert.equal(app.selectedFile, 0);

  const failedRuntime = createRuntime({ clipboardFails: true });
  const failedApp = new MiniGameApp(failedRuntime.platform).start();
  assert.equal(await failedApp.copy('safe text', '不应显示'), false);
  assert.equal(failedApp.message, '复制失败，请稍后重试');

  const emptyRuntime = createRuntime();
  const emptyReport = {
    ...dailyReport,
    matchCount: 0,
    predictionCount: 0,
    matches: [],
  };
  const emptyApp = new MiniGameApp(emptyRuntime.platform, null, { dailyReport: emptyReport });
  emptyApp.setPage('daily');
  assert.ok(emptyRuntime.ctx.texts.some((text) => text.includes('暂无可展示的每日推断')));

  const painter = new Painter(emptyRuntime.ctx, () => 844);
  const lines = painter.wrap('bash scripts/daily_automation.sh --with-a-very-long-option', 120, { size: 12, mono: true });
  assert.ok(lines.length > 1);
  for (const line of lines) assert.ok(emptyRuntime.ctx.measureText(line).width <= 120);

  const gameConfig = JSON.parse(fs.readFileSync(path.join(root, 'dist/weapp/game.json'), 'utf8'));
  assert.equal(gameConfig.deviceOrientation, 'portrait');
  const projectConfig = JSON.parse(fs.readFileSync(path.join(root, 'project.config.json'), 'utf8'));
  assert.equal(projectConfig.compileType, 'minigame');
  assert.equal(projectConfig.miniprogramRoot, './dist/weapp/');
  assert.ok(fs.statSync(path.join(root, 'dist/weapp/game.js')).size > 0);
  assert.ok(fs.existsSync(path.join(root, 'dist/weapp/data/daily-report.js')));
  assert.ok(fs.existsSync(path.join(root, 'dist/weapp/data/project.js')));
  assert.ok(fs.existsSync(path.join(root, 'minigame/src/pages/daily.js')));

  const sourceFiles = [
    'minigame/game.js',
    'minigame/src/app.js',
    ...fs.readdirSync(path.join(root, 'minigame/src/pages')).map((name) => `minigame/src/pages/${name}`),
  ];
  for (const file of sourceFiles) {
    const source = fs.readFileSync(path.join(root, file), 'utf8');
    assert.doesNotMatch(source, /\b(?:document|window|navigator|localStorage)\s*\./);
    assert.doesNotMatch(source, /wx\.request\s*\(/);
  }

  const packageJson = require('../package.json');
  assert.ok(packageJson.scripts['build:weapp'].includes('build-minigame.cjs'));
  assert.ok(packageJson.scripts['dev:weapp'].includes('--watch'));
  assert.ok(packageJson.scripts['build:h5'].includes('taro build'));

  console.log('MINIGAME_LOGIC_OK canvas navigation scrolling selection clipboard empty-state shared-data build');
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
