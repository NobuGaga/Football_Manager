import fs from 'node:fs';

const [route = 'overview', widthText = '390', heightText = '844', output = 'capture.png'] = process.argv.slice(2);
const width = Number(widthText);
const height = Number(heightText);
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
await call('Page.enable');
await call('Runtime.enable');
await call('Emulation.setDeviceMetricsOverride', {
  width,
  height,
  deviceScaleFactor: 1,
  mobile: width < 600,
});
const url = `http://127.0.0.1:8765/#/pages/${route}/index`;
await call('Page.navigate', { url });
await new Promise((resolve) => setTimeout(resolve, 900));
const metrics = await call('Runtime.evaluate', {
  expression: `(() => ({
    url: location.href,
    innerWidth,
    innerHeight,
    clientWidth: document.documentElement.clientWidth,
    scrollWidth: document.documentElement.scrollWidth,
    bodyScrollWidth: document.body.scrollWidth,
    cards: document.querySelectorAll('.match-card').length,
    featured: document.querySelectorAll('.match-featured').length,
    compact: document.querySelectorAll('.match-compact').length,
    teams: document.querySelectorAll('.scoreboard-team').length,
    horizontalOverflow: document.documentElement.scrollWidth > document.documentElement.clientWidth,
  }))()`,
  returnByValue: true,
});
const screenshot = await call('Page.captureScreenshot', { format: 'png', fromSurface: true, captureBeyondViewport: false });
fs.writeFileSync(output, Buffer.from(screenshot.data, 'base64'));
console.log(JSON.stringify({ output, ...metrics.result.value }, null, 2));
ws.close();
