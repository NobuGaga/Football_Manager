const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { execFileSync } = require('node:child_process');

const root = path.resolve(__dirname, '..');
execFileSync(process.execPath, [path.join(root, 'scripts', 'build-minigame.cjs')], { stdio: 'inherit' });

const { MiniGameApp, TABS } = require('../dist/weapp/src/app.js');
const { Painter } = require('../dist/weapp/src/draw.js');
const { dailyReport } = require('../generated/daily-report.js');
const project = require('../src/data/project.js');

function textWidth(ctx, text) {
  const size = Number.parseInt(ctx.font.match(/(\d+)px/)?.[1] || '14', 10);
  return Array.from(String(text)).reduce((sum, character) => sum + (character.charCodeAt(0) > 255 ? size : size * 0.56), 0);
}

function createContext() {
  const texts = [];
  const textBounds = [];
  const gradient = { addColorStop() {} };
  return {
    texts,
    textBounds,
    measureTextCalls: 0,
    clearRectCalls: 0,
    font: 'normal 14px sans-serif',
    fillStyle: '',
    strokeStyle: '',
    lineWidth: 1,
    textAlign: 'left',
    textBaseline: 'top',
    save() {}, restore() {}, beginPath() {}, closePath() {},
    moveTo() {}, lineTo() {}, quadraticCurveTo() {}, arc() {},
    fill() {}, stroke() {}, fillRect() {}, rect() {}, clip() {}, scale() {},
    clearRect() { this.clearRectCalls++; },
    createLinearGradient: () => gradient,
    measureText(text) {
      this.measureTextCalls++;
      return { width: textWidth(this, text) };
    },
    fillText(text, x, y) {
      const value = String(text);
      const width = textWidth(this, value);
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
  const animationFrames = [];
  const windowInfo = {
    windowWidth: 390,
    windowHeight: 844,
    pixelRatio: 2,
    safeArea: { top: 47, bottom: 810 },
  };
  const platform = {
    createCanvas: () => canvas,
    getWindowInfo: () => windowInfo,
    requestAnimationFrame(callback) {
      animationFrames.push(callback);
      return animationFrames.length;
    },
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
  return { platform, canvas, ctx, listeners, clipboard, animationFrames, windowInfo };
}

function flushAnimationFrame(runtime) {
  assert.ok(runtime.animationFrames.length > 0, '应存在等待绘制的动画帧');
  const callbacks = runtime.animationFrames.splice(0);
  callbacks.forEach((callback) => callback());
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
  assert.ok(runtime.ctx.texts.includes('让每一次推断，'), '小游戏首页应保留 Web 品牌主视觉文案');
  assert.ok(runtime.ctx.texts.includes('都有迹可循。'), '小游戏首页应保留 Web 强调文案');
  assert.ok(!runtime.ctx.texts.some((text) => text.includes('测试实现需求')), '小游戏不得恢复已移除的试验文案');
  assert.ok(app.maxScroll > 0);
  assertTextWithinCanvas(runtime, app.width, '概览首屏');

  const dailyZone = app.zones.find((zone) => zone.action.type === 'page' && zone.action.page === 'daily');
  assert.ok(dailyZone, '概览应提供每日推断入口');
  tap(runtime, dailyZone);
  assert.equal(app.page, 'daily');
  assert.ok(app.maxScroll > 1000, '长报告应支持纵向滚动');
  assertTextWithinCanvas(runtime, app.width, '每日首屏');
  assert.ok(runtime.ctx.texts.includes('今日比赛中心'), '每日页应使用 C 方案标题');
  assert.ok(runtime.ctx.texts.includes('VS'), '首场应绘制 C 方案记分牌');
  for (const expected of [app.matches[0].homeTeam, app.matches[0].awayTeam, app.matches[0].tendencyPercent]) {
    assert.ok(runtime.ctx.texts.some((text) => text.includes(expected)), `首场记分牌应实际绘制 ${expected}`);
  }
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
  const warmedMeasureCalls = runtime.ctx.measureTextCalls;
  const warmedClearCalls = runtime.ctx.clearRectCalls;
  const startScroll = app.scroll;
  runtime.listeners.touchStart({ touches: [{ clientX: 100, clientY: 400 }] });
  for (let index = 1; index <= 20; index++) {
    runtime.listeners.touchMove({ touches: [{ clientX: 100, clientY: 400 - index * 4 }] });
  }
  runtime.listeners.touchEnd({ changedTouches: [{ clientX: 100, clientY: 320 }] });
  assert.ok(app.scroll > startScroll, '连续触控应更新滚动位置');
  assert.equal(runtime.animationFrames.length, 1, '连续触控应合并为一个待绘制帧');
  assert.equal(runtime.ctx.clearRectCalls, warmedClearCalls, '触控事件不应反复同步清屏');
  flushAnimationFrame(runtime);
  assert.equal(runtime.ctx.clearRectCalls, warmedClearCalls + 1, '合并后的帧应只绘制一次');
  assert.equal(runtime.ctx.measureTextCalls, warmedMeasureCalls, '布局缓存命中后不应重复测量文字');

  app.scrollBy(Number.NEGATIVE_INFINITY);
  flushAnimationFrame(runtime);
  app.scrollBy(160);
  assert.equal(app.scroll, 160);
  flushAnimationFrame(runtime);
  app.scrollBy(Number.POSITIVE_INFINITY);
  assert.equal(app.scroll, app.maxScroll);
  runtime.ctx.texts.length = 0;
  runtime.ctx.textBounds.length = 0;
  flushAnimationFrame(runtime);
  const lastMatch = app.matches.at(-1);
  assert.ok(runtime.ctx.texts.includes(lastMatch.homeTeam), '底部应实际绘制最后一场比赛');
  assertTextWithinCanvas(runtime, app.width, '每日底部');
  app.scrollBy(Number.NEGATIVE_INFINITY);
  assert.equal(app.scroll, 0);
  flushAnimationFrame(runtime);

  const measureBeforeResize = runtime.ctx.measureTextCalls;
  runtime.windowInfo.windowWidth = 360;
  runtime.ctx.textBounds.length = 0;
  app.refreshMetrics();
  app.render();
  assert.ok(runtime.ctx.measureTextCalls > measureBeforeResize, '宽度变化后应重新计算文字布局');
  assertTextWithinCanvas(runtime, app.width, '每日页宽度变化');

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
  assert.equal(projectConfig.compileType, 'game');
  assert.equal(projectConfig.miniprogramRoot, './dist/weapp/');
  assert.ok(fs.statSync(path.join(root, 'dist/weapp/game.js')).size > 0);
  assert.ok(fs.existsSync(path.join(root, 'minigame/src/pages/daily.js')));
  for (const file of ['app.js', 'app.json', 'app.wxss', 'sitemap.json']) {
    assert.ok(!fs.existsSync(path.join(root, 'dist/weapp', file)), `小游戏产物不得混入 ${file}`);
  }
  for (const [source, converted] of [
    ['generated/daily-report.js', 'data/daily-report.js'],
    ['src/data/project.js', 'data/project.js'],
    ['src/utils/report.js', 'utils/report.js'],
    ['src/utils/clipboard-core.js', 'utils/clipboard-core.js'],
  ]) {
    assert.equal(
      fs.readFileSync(path.join(root, 'dist/weapp', converted), 'utf8'),
      fs.readFileSync(path.join(root, source), 'utf8'),
      `转换产物必须与 Web 共享源一致：${source}`,
    );
  }

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
  const automationSource = fs.readFileSync(path.join(root, 'minigame/src/pages/automation.js'), 'utf8');
  const structureSource = fs.readFileSync(path.join(root, 'minigame/src/pages/structure.js'), 'utf8');
  assert.match(automationSource, /关联失败时立即中止，不上传半成品/);
  assert.match(structureSource, /仅保存本地，不进入 Git/);

  const packageJson = require('../package.json');
  assert.ok(packageJson.scripts['build:minigame'].includes('build-minigame.cjs'));
  assert.ok(packageJson.scripts['dev:minigame'].includes('--watch'));
  assert.equal(packageJson.scripts['build:weapp'], 'npm run build:minigame');
  assert.equal(packageJson.scripts['dev:weapp'], 'npm run dev:minigame');
  assert.ok(packageJson.scripts['build:h5'].includes('taro build'));

  console.log('MINIGAME_LOGIC_OK canvas navigation scrolling selection clipboard empty-state shared-data build');
})().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
