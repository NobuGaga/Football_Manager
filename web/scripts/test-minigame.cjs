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
    gradientCalls: 0,
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
    createLinearGradient() { this.gradientCalls++; return gradient; },
    measureText(text) {
      this.measureTextCalls++;
      return { width: textWidth(this, text) };
    },
    fillText(text, x, y) {
      const value = String(text);
      const size = Number.parseInt(this.font.match(/(\d+)px/)?.[1] || '14', 10);
      const width = textWidth(this, value);
      const left = this.textAlign === 'center' ? x - width / 2 : this.textAlign === 'right' ? x - width : x;
      const vertical = this.textBaseline === 'middle'
        ? { top: y - size * 0.4, bottom: y + size * 0.4 }
        : { top: y + size * 0.1, bottom: y + size * 0.9 };
      texts.push(value);
      textBounds.push({ text: value, left, right: left + width, ...vertical, baselineY: y, baseline: this.textBaseline });
    },
  };
}

function createRuntime({ clipboardFails = false, pixelRatio = 2 } = {}) {
  const ctx = createContext();
  const canvas = { width: 0, height: 0, getContext: () => ctx };
  const listeners = {};
  const clipboard = [];
  const exitCalls = [];
  const animationFrames = [];
  const windowInfo = {
    windowWidth: 390,
    windowHeight: 844,
    pixelRatio,
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
    exitMiniProgram: () => exitCalls.push(Date.now()),
    setClipboardData({ data, success, fail }) {
      clipboard.push(data);
      if (clipboardFails) fail({ errMsg: 'setClipboardData:fail denied' });
      else success({ errMsg: 'setClipboardData:ok' });
    },
  };
  return { platform, canvas, ctx, listeners, clipboard, exitCalls, animationFrames, windowInfo };
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

function swipe(runtime, { startX = 320, startY = 400, endX = 100, endY = 400 } = {}) {
  runtime.listeners.touchStart({ timeStamp: 10000, touches: [{ clientX: startX, clientY: startY }] });
  for (let index = 1; index <= 4; index += 1) {
    const ratio = index / 4;
    runtime.listeners.touchMove({
      timeStamp: 10000 + index * 40,
      touches: [{ clientX: startX + (endX - startX) * ratio, clientY: startY + (endY - startY) * ratio }],
    });
  }
  runtime.listeners.touchEnd({ timeStamp: 10160, changedTouches: [{ clientX: endX, clientY: endY }] });
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

function createLongReport(report, copies = 6) {
  assert.ok(report.matches.length > 0, '滚动测试需要至少一条基础比赛记录');
  const matches = Array.from({ length: report.matches.length * copies }, (_, index) => {
    const match = report.matches[index % report.matches.length];
    return {
      ...match,
      fixtureKey: `scroll-test-${index}`,
      homeTeam: `${match.homeTeam} 滚动${index + 1}`,
    };
  });
  return { ...report, matchCount: matches.length, predictionCount: matches.length, matches };
}

(async () => {
  const longReport = createLongReport(dailyReport);
  const runtime = createRuntime();
  const app = new MiniGameApp(runtime.platform, null, { dailyReport: longReport }).start();
  assert.deepEqual(TABS.map((item) => item.id), ['overview', 'pipeline', 'model', 'automation']);
  assert.equal(runtime.canvas.width, 780);
  assert.equal(runtime.canvas.height, 1688);
  assert.equal(app.page, 'overview');
  assert.equal(app.matches.length, longReport.matches.length);
  assert.equal(new Set(app.matches.map((match) => match.fixtureKey)).size, longReport.matches.length);
  assert.ok(runtime.ctx.texts.includes('让每一次推断，'), '小游戏首页应保留 Web 品牌主视觉文案');
  assert.ok(runtime.ctx.texts.includes('都有迹可循。'), '小游戏首页应保留 Web 强调文案');
  assert.ok(!runtime.ctx.texts.some((text) => text.includes('测试实现需求')), '小游戏不得恢复已移除的试验文案');
  assert.ok(!runtime.ctx.texts.includes('结构'), '小游戏底部不得再绘制结构页签');
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
  // 记分牌队名列宽有限，长队名会被 paragraph 折行绘制；断言需兼容折行（按绘制顺序拼接校验）。
  const drawnSequence = runtime.ctx.texts.join('');
  for (const expected of [app.matches[0].homeTeam, app.matches[0].awayTeam, app.matches[0].tendencyPercent]) {
    assert.ok(runtime.ctx.texts.some((text) => text.includes(expected)) || drawnSequence.includes(expected), `首场记分牌应实际绘制 ${expected}`);
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
  const warmedGradientCalls = runtime.ctx.gradientCalls;
  const startScroll = app.scroll;
  runtime.listeners.touchStart({ timeStamp: 0, touches: [{ clientX: 100, clientY: 400 }] });
  for (let index = 1; index <= 20; index++) {
    runtime.listeners.touchMove({ timeStamp: index * 8, touches: [{ clientX: 100, clientY: 400 - index * 4 }] });
  }
  runtime.listeners.touchEnd({ timeStamp: 160, changedTouches: [{ clientX: 100, clientY: 320 }] });
  assert.ok(app.scroll > startScroll, '连续触控应更新滚动位置');
  assert.equal(app.page, 'daily', '纵向滚动不得触发左滑返回');
  assert.equal(runtime.exitCalls.length, 0, '纵向滚动不得退出小游戏');
  assert.equal(runtime.animationFrames.length, 1, '连续触控应合并为一个待绘制帧');
  assert.equal(runtime.ctx.gradientCalls, warmedGradientCalls, '触控事件不应反复同步绘制背景');
  assert.equal(app.inertiaActive, true, '快速滑动松手后应进入惯性滚动');
  const releasedScroll = app.scroll;
  flushAnimationFrame(runtime, 16);
  assert.ok(app.scroll > releasedScroll, '惯性帧应在松手后继续滚动');
  assert.equal(runtime.ctx.gradientCalls, warmedGradientCalls + 1, '合并后的帧应只绘制一次背景');
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

  for (const page of ['overview', 'pipeline', 'model', 'automation', 'daily']) {
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
  assert.equal(app.page, 'automation', '已删除的结构页不得再被打开');

  swipe(runtime, { startX: 100, endX: 320 });
  assert.equal(runtime.exitCalls.length, 0, '右滑不得退出小游戏');
  swipe(runtime, { startX: 120, endX: 100 });
  assert.equal(runtime.exitCalls.length, 0, '短距离左滑不得退出小游戏');
  for (const tab of TABS) {
    app.setPage(tab.id);
    const exitCount = runtime.exitCalls.length;
    swipe(runtime, { endY: 420 });
    assert.equal(runtime.exitCalls.length, exitCount + 1, `${tab.label}主界面左滑应退出小游戏`);
    assert.equal(app.page, tab.id, '退出调用前不得误切换页面');
    assert.equal(app.scroll, 0, `${tab.label}横向左滑不得带动纵向滚动`);
  }
  app.setPage('daily');
  swipe(runtime);
  assert.equal(app.page, 'overview', '每日页左滑应返回概览');
  assert.equal(runtime.exitCalls.length, TABS.length, '每日页左滑不得调用退出 API');

  const failedRuntime = createRuntime({ clipboardFails: true });
  const failedApp = new MiniGameApp(failedRuntime.platform).start();
  assert.equal(await failedApp.copy('safe text', '不应显示'), false);
  assert.equal(failedApp.message, '复制失败，请稍后重试');

  const unsupportedRuntime = createRuntime();
  delete unsupportedRuntime.platform.exitMiniProgram;
  const unsupportedApp = new MiniGameApp(unsupportedRuntime.platform).start();
  swipe(unsupportedRuntime);
  assert.equal(unsupportedApp.page, 'overview');
  assert.equal(unsupportedApp.message, '当前环境不支持手势退出，请使用系统操作');

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

  const pacingRuntime = createRuntime();
  const pacingApp = new MiniGameApp(pacingRuntime.platform).start();
  const pacingDraws = pacingRuntime.ctx.gradientCalls;
  pacingApp.scrollBy(1);
  flushAnimationFrame(pacingRuntime, 0);
  pacingApp.scrollBy(1);
  flushAnimationFrame(pacingRuntime, 8);
  assert.equal(pacingRuntime.ctx.gradientCalls, pacingDraws + 2, '8ms 帧间隔也必须逐帧绘制，不得主动降帧');
  assert.equal(pacingRuntime.animationFrames.length, 0);

  const pixelRuntime = createRuntime();
  const pixelApp = new MiniGameApp(pixelRuntime.platform, null, { dailyReport: longReport }).start();
  for (const page of ['overview', 'pipeline', 'model', 'automation', 'daily']) {
    pixelApp.setPage(page);
    pixelApp.scrollBy(0.26);
    assert.ok(
      Number.isInteger(pixelApp.scroll * pixelApp.dpr),
      `${page} 滚动位置应对齐物理像素：${pixelApp.scroll}`,
    );
    if (page === 'daily') pixelRuntime.ctx.textBounds.length = 0;
    flushAnimationFrame(pixelRuntime, 0);
    if (page === 'daily') {
      const heading = pixelRuntime.ctx.textBounds.find((item) => item.text === '今日比赛中心');
      assert.ok(heading, '滚动后应绘制每日页主标题');
      assert.ok(Number.isInteger(heading.baselineY * pixelApp.dpr), '主要文字坐标应对齐物理像素');
    }
    pixelApp.scrollBy(Number.POSITIVE_INFINITY);
    flushAnimationFrame(pixelRuntime, 8);
    assert.equal(pixelApp.scroll, pixelApp.maxScroll, `${page} 应精确到达底部`);
    pixelApp.scrollBy(Number.NEGATIVE_INFINITY);
    flushAnimationFrame(pixelRuntime, 16);
    assert.equal(pixelApp.scroll, 0, `${page} 应精确到达顶部`);
  }

  const highDprRuntime = createRuntime({ pixelRatio: 3 });
  const highDprApp = new MiniGameApp(highDprRuntime.platform, null, { dailyReport: longReport }).start();
  assert.equal(highDprApp.dpr, 2, '高 DPR 设备的渲染倍率应限制为 2');
  assert.equal(highDprRuntime.canvas.width, 780);
  assert.equal(highDprRuntime.canvas.height, 1688);
  assert.equal(highDprApp.width, 390, '页面布局应继续使用 CSS 像素');
  assert.equal(highDprApp.height, 844);
  const highDprDailyZone = highDprApp.zones.find((zone) => zone.action.type === 'page' && zone.action.page === 'daily');
  assert.ok(highDprDailyZone, '高 DPR 设备应保留每日页入口');
  tap(highDprRuntime, highDprDailyZone);
  assert.equal(highDprApp.page, 'daily', '高 DPR 设备的 CSS 像素热区应可点击');
  assertTextWithinCanvas(highDprRuntime, highDprApp.width, '高 DPR 每日页');
  const highDprBackZone = highDprApp.zones.find((zone) => zone.action.type === 'page' && zone.action.page === 'overview');
  assert.ok(highDprBackZone, '高 DPR 设备应保留返回热区');
  tap(highDprRuntime, highDprBackZone);
  assert.equal(highDprApp.page, 'overview', '高 DPR 设备返回导航应保持正常');
  highDprRuntime.windowInfo.windowWidth = 360;
  highDprRuntime.windowInfo.windowHeight = 780;
  highDprRuntime.windowInfo.safeArea = { top: 47, bottom: 746 };
  highDprRuntime.ctx.textBounds.length = 0;
  highDprApp.refreshMetrics();
  highDprApp.render();
  assert.equal(highDprRuntime.canvas.width, 720, '窗口变化后应按 DPR=2 重算画布宽度');
  assert.equal(highDprRuntime.canvas.height, 1560, '窗口变化后应按 DPR=2 重算画布高度');
  assert.equal(highDprApp.safeBottom, 34, '底部安全区应继续使用 CSS 像素');
  assertTextWithinCanvas(highDprRuntime, highDprApp.width, '高 DPR 窗口变化');

  const tailRuntime = createRuntime();
  const tailApp = new MiniGameApp(tailRuntime.platform).start();
  tailApp.scroll = 0.5;
  tailApp.inertiaActive = true;
  tailApp.inertiaVelocity = 22;
  tailApp.inertiaFrameTime = 0;
  const tailDraws = tailRuntime.ctx.gradientCalls;
  tailApp.scheduleRender();
  flushAnimationFrame(tailRuntime, 8);
  assert.equal(tailApp.scroll, 0.5);
  assert.equal(tailApp.inertiaActive, true, '可见位置未变化时惯性仍应继续');
  assert.equal(tailRuntime.ctx.gradientCalls, tailDraws, '惯性位置未变化时不应完整重绘');
  assert.equal(tailRuntime.animationFrames.length, 1, '惯性继续时应只保留一个后续帧');
  flushAnimationFrame(tailRuntime, 16);
  assert.equal(tailRuntime.ctx.gradientCalls, tailDraws, '连续无效惯性帧均不应重绘');
  flushAnimationFrame(tailRuntime, 24);
  assert.equal(tailApp.inertiaActive, false, '低速惯性应正常结束');
  assert.equal(tailRuntime.ctx.gradientCalls, tailDraws + 1, '惯性结束时应执行最终绘制');
  assert.equal(tailRuntime.animationFrames.length, 0, '惯性结束后不得保留动画循环');

  tailApp.scroll = 0.5;
  tailApp.inertiaActive = true;
  tailApp.inertiaVelocity = -100;
  tailApp.inertiaFrameTime = 0;
  const boundaryDraws = tailRuntime.ctx.gradientCalls;
  tailApp.scheduleRender();
  flushAnimationFrame(tailRuntime, 32);
  assert.equal(tailApp.scroll, 0, '惯性越界应精确收敛到顶部');
  assert.equal(tailApp.inertiaActive, false);
  assert.equal(tailRuntime.ctx.gradientCalls, boundaryDraws + 1, '惯性到达边界时应执行最终绘制');
  assert.equal(tailRuntime.animationFrames.length, 0);

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
  assert.match(automationSource, /关联失败时立即中止，不上传半成品/);
  assert.equal(fs.existsSync(path.join(root, 'minigame/src/pages/structure.js')), false, '小游戏源码不得保留结构页面');
  assert.equal(fs.existsSync(path.join(root, 'dist/weapp/src/pages/structure.js')), false, '小游戏产物不得保留结构页面');

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
