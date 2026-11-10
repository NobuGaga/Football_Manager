import fs from 'node:fs';

const targets = await (await fetch('http://127.0.0.1:9222/json/list')).json();
const target = targets.find((item) => item.type === 'page' && item.url.startsWith('http://127.0.0.1:8765'));
if (!target) throw new Error('H5 target not found');
const ws = new WebSocket(target.webSocketDebuggerUrl);
await new Promise((resolve, reject) => {
  ws.addEventListener('open', resolve, { once: true });
  ws.addEventListener('error', reject, { once: true });
});
let nextId = 0;
const pending = new Map();
ws.addEventListener('message', (event) => {
  const message = JSON.parse(event.data);
  if (!message.id || !pending.has(message.id)) return;
  const { resolve, reject } = pending.get(message.id);
  pending.delete(message.id);
  if (message.error) reject(new Error(JSON.stringify(message.error)));
  else resolve(message.result);
});
function call(method, params = {}) {
  const id = ++nextId;
  ws.send(JSON.stringify({ id, method, params }));
  return new Promise((resolve, reject) => pending.set(id, { resolve, reject }));
}
async function evaluate(expression) {
  const result = await call('Runtime.evaluate', { expression, returnByValue: true, awaitPromise: true });
  if (result.exceptionDetails) throw new Error(JSON.stringify(result.exceptionDetails));
  return result.result.value;
}
const wait = (ms = 400) => new Promise((resolve) => setTimeout(resolve, ms));
await call('Page.enable');
await call('Runtime.enable');
await call('Emulation.setDeviceMetricsOverride', { width: 390, height: 844, deviceScaleFactor: 1, mobile: true });

await call('Page.navigate', { url: 'http://127.0.0.1:8765/#/pages/overview/index' });
await wait(700);
await evaluate(`document.querySelector('.hero-actions .primary-button').click()`);
await wait();
const navigation = await evaluate(`({ hash: location.hash, cards: document.querySelectorAll('.match-card').length })`);
if (navigation.hash !== '#/pages/daily/index' || navigation.cards !== 25) throw new Error(`每日入口导航失败 ${JSON.stringify(navigation)}`);

const detailCounts = await evaluate(`(() => ({
  cards: document.querySelectorAll('.match-card').length,
  bars: document.querySelectorAll('.match-card .probability-bar').length,
  scores: document.querySelectorAll('.match-card .score-row').length,
  data: document.querySelectorAll('.match-card .data-row').length,
  risks: document.querySelectorAll('.match-card .risk-text').length,
  emptyRisks: [...document.querySelectorAll('.match-card .risk-text')].filter((el) => !el.textContent.trim()).length,
}))()`);
for (const key of ['bars', 'scores', 'data', 'risks']) {
  if (detailCounts[key] !== 25 || detailCounts.emptyRisks !== 0) throw new Error(`每日详情缺失 ${JSON.stringify(detailCounts)}`);
}

const scroller = await evaluate(`(() => {
  const candidates = [...document.querySelectorAll('*')].filter((el) => el.clientHeight > 0 && el.scrollHeight > el.clientHeight + 100);
  const target = candidates.sort((a, b) => (b.scrollHeight - b.clientHeight) - (a.scrollHeight - a.clientHeight))[0];
  if (!target) return null;
  target.scrollTop = target.scrollHeight;
  return { tag: target.tagName, className: target.className, scrollHeight: target.scrollHeight, clientHeight: target.clientHeight };
})()`);
await wait();
const bottom = await evaluate(`(() => {
  const cards = [...document.querySelectorAll('.match-card')];
  const last = cards.at(-1);
  const rect = last.getBoundingClientRect();
  return { scrollY, lastTop: rect.top, lastBottom: rect.bottom, viewport: innerHeight, lastRisk: last.querySelector('.risk-text').textContent };
})()`);
if (!scroller || bottom.lastTop >= bottom.viewport || bottom.lastBottom > bottom.viewport + 2) {
  throw new Error(`长列表未滚动到底部 ${JSON.stringify({ scroller, bottom })}`);
}
const screenshot = await call('Page.captureScreenshot', { format: 'png', fromSurface: true, captureBeyondViewport: false });
fs.writeFileSync('delivery/ui-c-d-preview/h5-daily-bottom-mobile.png', Buffer.from(screenshot.data, 'base64'));

await call('Page.navigate', { url: 'http://127.0.0.1:8765/#/pages/overview/index' });
await wait(700);
await evaluate(`document.querySelectorAll('.hero-actions .secondary-button')[1].click()`);
await wait(700);
const copyFeedback = await evaluate(`document.querySelector('.copy-toast')?.textContent || ''`);
if (!copyFeedback.includes('每日命令已复制') && !copyFeedback.includes('复制失败，请稍后重试')) {
  throw new Error(`复制操作没有明确反馈：${copyFeedback}`);
}

await evaluate(`document.querySelectorAll('.hero-actions .secondary-button')[0].click()`);
await wait();
const pipelineHash = await evaluate(`location.hash`);
if (pipelineHash !== '#/pages/pipeline/index') throw new Error(`流水线导航失败 ${pipelineHash}`);
console.log(JSON.stringify({ navigation, detailCounts, bottom, copyFeedback, pipelineHash }, null, 2));
ws.close();
