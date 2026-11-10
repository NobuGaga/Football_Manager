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
await call('Runtime.enable');
const expression = `(() => {
  const clientWidth = document.documentElement.clientWidth;
  const overflowing = [...document.querySelectorAll('body *')].map((el) => {
    const r = el.getBoundingClientRect();
    const style = getComputedStyle(el);
    return {
      tag: el.tagName,
      className: typeof el.className === 'string' ? el.className : '',
      left: Math.round(r.left * 10) / 10,
      right: Math.round(r.right * 10) / 10,
      width: Math.round(r.width * 10) / 10,
      display: style.display,
      whiteSpace: style.whiteSpace,
      text: (el.textContent || '').trim().slice(0, 45),
    };
  }).filter((item) => item.width > 0 && (item.right > clientWidth + 1 || item.left < -1)).slice(0, 45);
  return {
    url: location.href,
    readyState: document.readyState,
    innerWidth,
    clientWidth,
    documentScrollWidth: document.documentElement.scrollWidth,
    bodyScrollWidth: document.body.scrollWidth,
    overflowing,
  };
})()`;
const result = await call('Runtime.evaluate', { expression, returnByValue: true });
console.log(JSON.stringify(result.result.value, null, 2));
ws.close();
