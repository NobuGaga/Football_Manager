const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { execFileSync } = require('node:child_process');

const root = path.resolve(__dirname, '..');
execFileSync(process.execPath, [path.join(root, 'scripts', 'build-minigame.cjs')], { stdio: 'inherit' });

const { MiniGameApp, TABS } = require('../dist/weapp/src/app.js');
const { Painter } = require('../dist/weapp/src/draw.js');
const { RenderProfiler } = require('../dist/weapp/src/profiler.js');
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
    gradientCalls: 0,
    backgroundFillCalls: 0,
    drawImageCalls: 0,
    translateX: 0,
    translateY: 0,
    font: 'normal 14px sans-serif',
    fillStyle: '',
    strokeStyle: '',
    lineWidth: 1,
    textAlign: 'left',
    textBaseline: 'top',
    save() {}, restore() {}, beginPath() {}, closePath() {},
    moveTo() {}, lineTo() {}, quadraticCurveTo() {}, arc() {},
    fill() {}, stroke() {}, rect() {}, clip() {}, scale() {},
    translate(x, y) { this.translateX += x; this.translateY += y; },
    fillRect() { if (this.fillStyle === gradient) this.backgroundFillCalls++; },
    drawImage(image, sx, sy, sw, sh, dx, dy, dw, dh) {
      this.drawImageCalls++;
      const source = image.ctx;
      if (!source) return;
      texts.push(...source.texts);
      for (const bound of source.textBounds) {
        textBounds.push({
          ...bound,
          left: dx + (bound.left - sx) * dw / sw,
          right: dx + (bound.right - sx) * dw / sw,
          top: dy + (bound.top - sy) * dh / sh,
          bottom: dy + (bound.bottom - sy) * dh / sh,
          baselineY: dy + (bound.baselineY - sy) * dh / sh,
        });
      }
    },
    clearRect() { this.clearRectCalls++; },
    createLinearGradient() { this.gradientCalls++; return gradient; },
    measureText(text) {
      this.measureTextCalls++;
      return { width: textWidth(this, text) };
    },
    fillText(text, x, y) {
      const value = String(text);
      const size = Number.parseInt(this.font.match(/(\d+)px/)?.[1] || '14', 10);
      const width = textWidth(this, value);
      const left = (this.textAlign === 'center' ? x - width / 2 : this.textAlign === 'right' ? x - width : x) + this.translateX;
      const baselineY = y + this.translateY;
      const vertical = this.textBaseline === 'middle'
        ? { top: baselineY - size * 0.4, bottom: baselineY + size * 0.4 }
        : { top: baselineY + size * 0.1, bottom: baselineY + size * 0.9 };
      texts.push(value);
      textBounds.push({ text: value, left, right: left + width, ...vertical, baselineY, baseline: this.textBaseline });
    },
  };
}

function createRuntime({ clipboardFails = false, pixelRatio = 2, offscreenFails = false } = {}) {
  const ctx = createContext();
  const canvas = { width: 0, height: 0, ctx, getContext: () => ctx };
  const listeners = {};
  const clipboard = [];
  const animationFrames = [];
  const offscreenCanvases = [];
  let mainCreated = false;
  const windowInfo = {
    windowWidth: 390,
    windowHeight: 844,
    pixelRatio,
    safeArea: { top: 47, bottom: 810 },
  };
  const platform = {
    createCanvas() {
      if (!mainCreated) {
        mainCreated = true;
        return canvas;
      }
      if (offscreenFails) return { getContext: () => null };
      const offscreenContext = createContext();
      const offscreen = { width: 0, height: 0, ctx: offscreenContext, getContext: () => offscreenContext };
      offscreenCanvases.push(offscreen);
      return offscreen;
    },
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
  return { platform, canvas, ctx, listeners, clipboard, animationFrames, windowInfo, offscreenCanvases };
}

function flushAnimationFrame(runtime, timestamp) {
  assert.ok(runtime.animationFrames.length > 0, '应存在等待绘制的动画帧');
  const callbacks = runtime.animationFrames.splice(0);
  callbacks.forEach((callback) => callback(timestamp));
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

function assertTextBlockCentered(bounds, y, height, label) {
  assert.ok(bounds.length > 0, `${label} 应实际绘制文字`);
  const top = Math.min(...bounds.map((item) => item.top));
  const bottom = Math.max(...bounds.map((item) => item.bottom));
  const actualCenter = (top + bottom) / 2;
  const expectedCenter = y + height / 2;
  assert.ok(
    Math.abs(actualCenter - expectedCenter) <= 0.5,
    `${label} 可见文字应垂直居中：实际 ${actualCenter}，预期 ${expectedCenter}`,
  );
  for (const bound of bounds) assert.equal(bound.baseline, 'middle', `${label} 应使用中线基线`);
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
  assert.ok(runtime.offscreenCanvases.length > 0, '每日可见卡片应使用离屏缓存');
  assert.ok(runtime.ctx.drawImageCalls > 0, '缓存卡片应实际贴回主画布');
  const cachedCanvasCount = runtime.offscreenCanvases.length;
  const cachedGradientCalls = runtime.ctx.gradientCalls;
  app.render();
  assert.equal(runtime.offscreenCanvases.length, cachedCanvasCount, '相同卡片重绘时应复用离屏缓存');
  assert.equal(runtime.ctx.gradientCalls, cachedGradientCalls, '相同页面与尺寸应复用背景渐变');

  const warmedMeasureCalls = runtime.ctx.measureTextCalls;
  const warmedGradientCalls = runtime.ctx.gradientCalls;
  const warmedBackgroundFills = runtime.ctx.backgroundFillCalls;
  const startScroll = app.scroll;
  runtime.listeners.touchStart({ timeStamp: 0, touches: [{ clientX: 100, clientY: 400 }] });
  for (let index = 1; index <= 20; index++) {
    runtime.listeners.touchMove({ timeStamp: index * 8, touches: [{ clientX: 100, clientY: 400 - index * 4 }] });
  }
  runtime.listeners.touchEnd({ timeStamp: 160, changedTouches: [{ clientX: 100, clientY: 320 }] });
  assert.ok(app.scroll > startScroll, '连续触控应更新滚动位置');
  assert.equal(runtime.animationFrames.length, 1, '连续触控应合并为一个待绘制帧');
  assert.equal(runtime.ctx.backgroundFillCalls, warmedBackgroundFills, '触控事件不应反复同步绘制背景');
  assert.equal(app.inertiaActive, true, '快速滑动松手后应进入惯性滚动');
  const releasedScroll = app.scroll;
  flushAnimationFrame(runtime, 16);
  assert.ok(app.scroll > releasedScroll, '惯性帧应在松手后继续滚动');
  assert.equal(runtime.ctx.backgroundFillCalls, warmedBackgroundFills + 1, '合并后的帧应只绘制一次背景');
  assert.equal(runtime.ctx.gradientCalls, warmedGradientCalls, '滚动帧不得重复创建背景渐变');
  assert.equal(runtime.ctx.clearRectCalls, 0, '不透明全屏背景绘制前不应再清屏');
  assert.equal(runtime.ctx.measureTextCalls, warmedMeasureCalls, '布局缓存命中后不应重复测量文字');
  assert.equal(runtime.animationFrames.length, 1, '惯性滚动每次只应保留下一帧');
  const firstVelocity = app.inertiaVelocity;
  flushAnimationFrame(runtime, 32);
  assert.ok(Math.abs(app.inertiaVelocity) < Math.abs(firstVelocity), '惯性速度应逐帧衰减');

  let frameTime = 32;
  while (runtime.animationFrames.length && frameTime < 2000) {
    frameTime += 16;
    flushAnimationFrame(runtime, frameTime);
  }
  assert.equal(app.inertiaActive, false, '惯性滚动应在低速时停止');
  assert.equal(runtime.animationFrames.length, 0, '惯性停止后不得保留永久动画循环');
  assert.ok(app.scroll >= 0 && app.scroll <= app.maxScroll, '惯性滚动不得越出内容边界');

  runtime.listeners.touchStart({ timeStamp: 2200, touches: [{ clientX: 100, clientY: 400 }] });
  for (let index = 1; index <= 20; index++) {
    runtime.listeners.touchMove({ timeStamp: 2200 + index * 8, touches: [{ clientX: 100, clientY: 400 - index * 4 }] });
  }
  runtime.listeners.touchEnd({ timeStamp: 2360, changedTouches: [{ clientX: 100, clientY: 320 }] });
  assert.equal(app.inertiaActive, true);
  runtime.listeners.touchStart({ timeStamp: 2400, touches: [{ clientX: 100, clientY: 240 }] });
  assert.equal(app.inertiaActive, false, '再次触摸应立即中断惯性滚动');
  runtime.listeners.touchCancel();
  flushAnimationFrame(runtime, 2416);
  assert.equal(runtime.animationFrames.length, 0, '中断后不得继续申请惯性帧');

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
  assert.ok(app.dailyCardCache.entries.has(app.matches.length - 1), '底部卡片应进入离屏缓存');
  assert.ok(app.dailyCardCache.entries.size <= 6, '卡片缓存不得超出 LRU 上限');
  assertTextWithinCanvas(runtime, app.width, '每日底部');
  app.scrollBy(Number.NEGATIVE_INFINITY);
  assert.equal(app.scroll, 0);
  flushAnimationFrame(runtime);

  const measureBeforeResize = runtime.ctx.measureTextCalls;
  const gradientsBeforeResize = runtime.ctx.gradientCalls;
  const canvasesBeforeResize = runtime.offscreenCanvases.length;
  runtime.windowInfo.windowWidth = 360;
  runtime.ctx.textBounds.length = 0;
  app.refreshMetrics();
  app.render();
  assert.ok(runtime.ctx.measureTextCalls > measureBeforeResize, '宽度变化后应重新计算文字布局');
  assert.ok(runtime.ctx.gradientCalls > gradientsBeforeResize, '尺寸变化后应重建背景渐变');
  assert.ok(runtime.offscreenCanvases.length > canvasesBeforeResize, '宽度变化后应重建卡片缓存');
  assertTextWithinCanvas(runtime, app.width, '每日页宽度变化');

  for (const page of ['overview', 'pipeline', 'model', 'automation', 'structure', 'daily']) {
    runtime.ctx.texts.length = 0;
    runtime.ctx.textBounds.length = 0;
    app.setPage(page);
    assert.equal(app.page, page);
    assert.ok(runtime.ctx.texts.length > 0, `${page} 应实际绘制文本`);
    assertTextWithinCanvas(runtime, app.width, `${page} 窄屏`);
  }

  app.setPage('pipeline');
  for (let index = 0; index < project.flowSteps.length + 2; index += 1) app.handleAction({ type: 'flow-next' });
  assert.equal(app.flowIndex, project.flowSteps.length - 1);
  app.handleAction({ type: 'flow', index: -1 });
  assert.equal(app.flowIndex, 0);
  app.handleAction({ type: 'flow', index: 99 });
  assert.equal(app.flowIndex, project.flowSteps.length - 1);

  app.setPage('automation');
  const expectedVersion = process.env.WECHAT_VERSION || '开发构建（未上传）';
  assert.ok(runtime.ctx.texts.includes(expectedVersion), '自动化页应显示当前构建版本号');
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

  const highDprRuntime = createRuntime({ pixelRatio: 3 });
  const highDprApp = new MiniGameApp(highDprRuntime.platform).start();
  assert.equal(highDprApp.dpr, 2, '高 DPR 设备应将渲染像素比限制为 2');
  assert.equal(highDprRuntime.canvas.width, 780);
  assert.equal(highDprRuntime.canvas.height, 1688);

  const fallbackRuntime = createRuntime({ offscreenFails: true });
  const fallbackApp = new MiniGameApp(fallbackRuntime.platform).start();
  fallbackApp.setPage('daily');
  assert.equal(fallbackApp.dailyCardCache.disabled, true, '离屏 Canvas 不可用时应停用缓存');
  assert.ok(fallbackRuntime.ctx.texts.includes(fallbackApp.matches[0].homeTeam), '回退路径仍应完整绘制比赛');

  let profileNow = 0;
  const profiler = new RenderProfiler(() => profileNow, true);
  profiler.render(10);
  profiler.render(20);
  profiler.render(30);
  profiler.frame(0);
  profiler.frame(16);
  profiler.frame(32);
  profiler.draw('fillText');
  profiler.draw('fillText');
  const profile = profiler.snapshot();
  assert.equal(profile.renders, 3);
  assert.equal(profile.renderAvgMs, 20);
  assert.equal(profile.renderP95Ms, 30);
  assert.equal(profile.renderMaxMs, 30);
  assert.equal(profile.fps, 62.5);
  assert.equal(profile.drawCalls.fillText, 2);
  profileNow = 1100;
  profiler.maybeLog(profileNow);

  const profiledRuntime = createRuntime();
  const profiledApp = new MiniGameApp(profiledRuntime.platform, null, { profile: true }).start();
  assert.ok(profiledApp.profiler.snapshot().drawCalls.fillText > 0, '性能工具应统计实际文字绘制');
  assert.ok(profiledApp.profiler.snapshot().drawCalls.fillRect > 0, '性能工具应统计实际背景绘制');
  profiledApp.setPage('daily');
  assert.ok(profiledApp.profiler.snapshot().drawCalls.drawImage > 0, '性能工具应统计实际缓存贴图');

  const painter = new Painter(emptyRuntime.ctx, () => 844);
  const lines = painter.wrap('bash scripts/daily_automation.sh --with-a-very-long-option', 120, { size: 12, mono: true });
  assert.ok(lines.length > 1);
  for (const line of lines) assert.ok(emptyRuntime.ctx.measureText(line).width <= 120);

  const alignRuntime = createRuntime();
  const alignApp = new MiniGameApp(alignRuntime.platform).start();

  alignRuntime.ctx.textBounds.length = 0;
  alignApp.button('单行按钮', 20, 100, 200, 46, { type: 'flow', index: 0 });
  let alignBounds = alignRuntime.ctx.textBounds;
  assert.equal(alignBounds.length, 1);
  assertTextBlockCentered(alignBounds, 100, 46, '通用单行按钮');
  assert.equal((alignBounds[0].left + alignBounds[0].right) / 2, 120);

  alignRuntime.ctx.textBounds.length = 0;
  alignApp.button('多行按钮居中测试', 20, 160, 100, 60, { type: 'flow', index: 0 });
  alignBounds = alignRuntime.ctx.textBounds;
  assert.equal(alignBounds.length, 2);
  assertTextBlockCentered(alignBounds, 160, 60, '通用多行按钮');
  for (const bound of alignBounds) {
    assert.equal((bound.left + bound.right) / 2, 70);
  }

  const { button: stadiumButton } = require('../dist/weapp/src/pages/stadium-theme.js');
  alignRuntime.ctx.textBounds.length = 0;
  stadiumButton(alignApp, '主题按钮', 20, 240, 200, 46, { type: 'flow', index: 0 });
  alignBounds = alignRuntime.ctx.textBounds;
  assert.equal(alignBounds.length, 1);
  assertTextBlockCentered(alignBounds, 240, 46, '球场主题按钮');
  assert.equal((alignBounds[0].left + alignBounds[0].right) / 2, 120);

  alignRuntime.ctx.textBounds.length = 0;
  alignApp.setPage('overview');
  const tabTop = alignApp.height - alignApp.getNavHeight();
  const tabWidth = alignApp.width / TABS.length;
  for (const [index, tab] of TABS.entries()) {
    const bound = alignRuntime.ctx.textBounds.find((item) => item.text === tab.label && item.baseline === 'middle');
    assert.ok(bound, `${tab.label} 页签应实际绘制`);
    assertTextBlockCentered([bound], tabTop, 62, `${tab.label} 页签`);
    assert.ok(Math.abs((bound.left + bound.right) / 2 - (index + 0.5) * tabWidth) < 1e-8);
  }

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
    'minigame/src/draw.js',
    'minigame/src/profiler.js',
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
