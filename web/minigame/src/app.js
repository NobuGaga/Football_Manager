const projectData = require('../data/project.js');
const { dailyReport: reportData } = require('../data/daily-report.js');
const { boundedIndex, createMatrix, decorateMatches } = require('../utils/report.js');
const { writeClipboard } = require('../utils/clipboard-core.js');
const { Painter } = require('./draw.js');

const renderers = {
  overview: require('./pages/overview.js'),
  daily: require('./pages/daily.js'),
  pipeline: require('./pages/pipeline.js'),
  model: require('./pages/model.js'),
  automation: require('./pages/automation.js'),
  structure: require('./pages/structure.js'),
};

const TABS = [
  { id: 'overview', label: '概览' },
  { id: 'pipeline', label: '流水线' },
  { id: 'model', label: '模型' },
  { id: 'automation', label: '自动化' },
  { id: 'structure', label: '结构' },
];

const colors = {
  background: '#0b100d',
  card: '#121914',
  cardAlt: '#172118',
  border: '#283229',
  text: '#eef3e9',
  secondary: '#b2bdaf',
  muted: '#859385',
  accent: '#c8f55a',
  accentText: '#152008',
  risk: '#b7a77b',
};

const FLING_SAMPLE_MS = 100;
const FLING_MIN_SPEED = 40;
const FLING_MAX_SPEED = 2800;
const FLING_STOP_SPEED = 20;
const FLING_DECAY_MS = 220;

function pointFromEvent(event) {
  const point = (event.changedTouches && event.changedTouches[0]) || (event.touches && event.touches[0]);
  return point ? { x: point.clientX, y: point.clientY } : null;
}

class MiniGameApp {
  constructor(platform, canvas = null, data = {}) {
    if (!platform) throw new Error('缺少微信小游戏运行环境');
    this.platform = platform;
    this.canvas = canvas || platform.createCanvas();
    this.ctx = this.canvas.getContext('2d');
    this.project = data.project || projectData;
    this.dailyReport = data.dailyReport || reportData;
    this.matches = decorateMatches(this.dailyReport);
    this.matrix = createMatrix();
    this.colors = colors;
    this.padding = 16;
    this.page = 'overview';
    this.scroll = 0;
    this.maxScroll = 0;
    this.flowIndex = 0;
    this.commandMode = 'daily';
    this.selectedFile = 0;
    this.message = '';
    this.messageTimer = null;
    this.zones = [];
    this.touch = null;
    this.handlers = {};
    this.frameHandle = null;
    this.inertiaActive = false;
    this.inertiaVelocity = 0;
    this.inertiaFrameTime = null;
    this.painter = new Painter(this.ctx, () => this.height);
    this.refreshMetrics();
  }

  refreshMetrics() {
    const info = this.platform.getWindowInfo
      ? this.platform.getWindowInfo()
      : this.platform.getSystemInfoSync();
    this.width = info.windowWidth || 375;
    this.height = info.windowHeight || 667;
    this.dpr = info.pixelRatio || 1;
    const safeArea = info.safeArea;
    this.safeTop = Math.max(20, safeArea ? safeArea.top + 8 : 28);
    this.safeBottom = Math.max(12, safeArea ? this.height - safeArea.bottom : 12);
    this.canvas.width = Math.round(this.width * this.dpr);
    this.canvas.height = Math.round(this.height * this.dpr);
    this.ctx.scale(this.dpr, this.dpr);
  }

  start() {
    this.handlers.touchStart = (event) => this.onTouchStart(event);
    this.handlers.touchMove = (event) => this.onTouchMove(event);
    this.handlers.touchEnd = (event) => this.onTouchEnd(event);
    this.handlers.touchCancel = () => {
      this.touch = null;
      this.stopInertia();
    };
    this.handlers.resize = () => {
      this.stopInertia();
      this.refreshMetrics();
      this.render();
    };
    this.handlers.show = () => this.render();
    this.platform.onTouchStart(this.handlers.touchStart);
    this.platform.onTouchMove(this.handlers.touchMove);
    this.platform.onTouchEnd(this.handlers.touchEnd);
    if (this.platform.onTouchCancel) this.platform.onTouchCancel(this.handlers.touchCancel);
    if (this.platform.onWindowResize) this.platform.onWindowResize(this.handlers.resize);
    if (this.platform.onShow) this.platform.onShow(this.handlers.show);
    this.render();
    return this;
  }

  getNavHeight() {
    return this.page === 'daily' ? this.safeBottom : 62 + this.safeBottom;
  }

  write(text, x, y, width, options = {}) {
    return y + this.painter.paragraph(text, x, y, width, options);
  }

  card(y, height, fill = this.colors.card, stroke = this.colors.border) {
    this.painter.roundRect(this.padding, y, this.width - this.padding * 2, height, 16, fill, stroke);
  }

  section(y, kicker, title, description = '') {
    const x = this.padding;
    const w = this.width - this.padding * 2;
    y = this.write(kicker, x, y, w, { size: 10, weight: 'bold', color: this.colors.accent, mono: true, lineHeight: 15 }) + 8;
    y = this.write(title, x, y, w, { size: 24, weight: 'bold', lineHeight: 33 });
    if (description) y = this.write(description, x, y + 8, w, { size: 13, color: this.colors.secondary, lineHeight: 21 });
    return y + 20;
  }

  notice(y, text, mark = '!') {
    const x = this.padding;
    const w = this.width - this.padding * 2;
    const textH = this.painter.paragraphHeight(text, w - 66, { size: 12, color: this.colors.risk, lineHeight: 20 });
    const height = Math.max(58, textH + 28);
    this.painter.roundRect(x, y, w, height, 14, '#1b1c13', '#4a442c');
    this.painter.circle(x + 23, y + 23, 10, null, this.colors.risk);
    this.painter.text(mark, x + 23, y + 17, { size: 11, weight: 'bold', color: this.colors.risk, align: 'center' });
    this.painter.paragraph(text, x + 43, y + 14, w - 66, { size: 12, color: this.colors.risk, lineHeight: 20 });
    return y + height + 12;
  }

  simpleCard(y, { kicker = '', title, body, compact = false }) {
    const x = this.padding;
    const w = this.width - this.padding * 2;
    const inner = w - 32;
    const titleH = this.painter.paragraphHeight(title, inner, { size: compact ? 15 : 17, weight: 'bold', lineHeight: compact ? 23 : 26 });
    const bodyH = this.painter.paragraphHeight(body, inner, { size: 12, color: this.colors.secondary, lineHeight: 20 });
    const height = (compact ? 62 : 74) + titleH + bodyH;
    this.card(y, height);
    let cursor = y + 14;
    if (kicker) {
      this.painter.text(kicker, x + 16, cursor, { size: 10, weight: 'bold', color: this.colors.accent, mono: true });
      cursor += 24;
    }
    this.painter.paragraph(title, x + 16, cursor, inner, { size: compact ? 15 : 17, weight: 'bold', lineHeight: compact ? 23 : 26 });
    cursor += titleH + 8;
    this.painter.paragraph(body, x + 16, cursor, inner, { size: 12, color: this.colors.secondary, lineHeight: 20 });
    return y + height + 12;
  }

  button(label, x, y, width, height, action, options = {}) {
    const selected = options.selected;
    const primary = options.primary;
    const fill = primary || selected ? this.colors.accent : this.colors.cardAlt;
    const textColor = primary || selected ? this.colors.accentText : this.colors.text;
    const stroke = primary || selected ? this.colors.accent : this.colors.border;
    this.painter.roundRect(x, y, width, height, options.small ? 10 : 13, fill, stroke);
    const size = options.file ? 11 : options.small ? 11 : 13;
    const textWidth = width - (options.file ? 54 : 20);
    this.painter.paragraph(label, x + 10, y + Math.max(9, (height - size * 1.4) / 2 - 1), textWidth, {
      size,
      weight: selected || primary ? 'bold' : 'normal',
      color: textColor,
      align: options.file ? 'left' : 'center',
      mono: options.file,
      lineHeight: Math.round(size * 1.4),
    });
    if (y + height >= 0 && y <= this.height - this.getNavHeight()) {
      this.zones.push({ x, y, width, height, action });
    }
    return y + height;
  }

  setPage(page) {
    if (!renderers[page]) return;
    this.stopInertia();
    this.page = page;
    this.scroll = 0;
    this.maxScroll = 0;
    this.message = '';
    this.render();
  }

  handleAction(action) {
    if (!action) return;
    if (action.type === 'page') {
      this.setPage(action.page);
    } else if (action.type === 'flow') {
      this.flowIndex = boundedIndex(action.index, this.project.flowSteps.length);
      this.stopInertia();
      this.scroll = 0;
      this.render();
    } else if (action.type === 'flow-next') {
      this.flowIndex = boundedIndex(this.flowIndex + 1, this.project.flowSteps.length);
      this.stopInertia();
      this.scroll = 0;
      this.render();
    } else if (action.type === 'mode') {
      if (this.project.commandModes.some((item) => item.id === action.id)) {
        this.commandMode = action.id;
        this.stopInertia();
        this.scroll = 0;
        this.render();
      }
    } else if (action.type === 'file') {
      this.selectedFile = boundedIndex(action.index, this.project.projectFiles.length);
      this.render();
    } else if (action.type === 'copy') {
      return this.copy(action.text, action.success);
    }
    return undefined;
  }

  async copy(text, successMessage) {
    this.message = '正在复制…';
    this.render();
    const success = await writeClipboard(this.platform, text);
    this.message = success ? successMessage : '复制失败，请稍后重试';
    this.render();
    clearTimeout(this.messageTimer);
    this.messageTimer = setTimeout(() => {
      this.message = '';
      this.render();
    }, 2200);
    if (this.messageTimer && this.messageTimer.unref) this.messageTimer.unref();
    return success;
  }

  scheduleRender() {
    if (this.frameHandle !== null) return;
    const draw = (timestamp) => {
      this.frameHandle = null;
      this.advanceInertia(timestamp);
      this.render();
      if (this.inertiaActive) this.scheduleRender();
    };
    if (typeof this.platform.requestAnimationFrame === 'function') {
      this.frameHandle = this.platform.requestAnimationFrame(draw);
    } else if (this.canvas && typeof this.canvas.requestAnimationFrame === 'function') {
      this.frameHandle = this.canvas.requestAnimationFrame(draw);
    } else if (typeof requestAnimationFrame === 'function') {
      this.frameHandle = requestAnimationFrame(draw);
    } else {
      this.frameHandle = setTimeout(draw, 16);
      if (this.frameHandle && this.frameHandle.unref) this.frameHandle.unref();
    }
  }

  scrollBy(delta) {
    const next = Math.max(0, Math.min(this.maxScroll, this.scroll + delta));
    if (next !== this.scroll) {
      this.scroll = next;
      this.scheduleRender();
    }
  }

  eventTime(event) {
    return Number.isFinite(event.timeStamp) ? event.timeStamp : Date.now();
  }

  stopInertia() {
    this.inertiaActive = false;
    this.inertiaVelocity = 0;
    this.inertiaFrameTime = null;
  }

  startInertia(velocity) {
    if (!Number.isFinite(velocity)) {
      this.stopInertia();
      return;
    }
    const speed = Math.max(-FLING_MAX_SPEED, Math.min(FLING_MAX_SPEED, velocity));
    const atBoundary = (this.scroll <= 0 && speed < 0) || (this.scroll >= this.maxScroll && speed > 0);
    if (Math.abs(speed) < FLING_MIN_SPEED || this.maxScroll <= 0 || atBoundary) {
      this.stopInertia();
      return;
    }
    this.inertiaVelocity = speed;
    this.inertiaFrameTime = null;
    this.inertiaActive = true;
    this.scheduleRender();
  }

  advanceInertia(timestamp) {
    if (!this.inertiaActive) return;
    const now = Number.isFinite(timestamp) ? timestamp : Date.now();
    const elapsed = this.inertiaFrameTime === null
      ? 16
      : Math.max(0, Math.min(50, now - this.inertiaFrameTime));
    this.inertiaFrameTime = now;
    const unbounded = this.scroll + this.inertiaVelocity * elapsed / 1000;
    const next = Math.max(0, Math.min(this.maxScroll, unbounded));
    this.scroll = next;
    this.inertiaVelocity *= Math.exp(-elapsed / FLING_DECAY_MS);
    if (next !== unbounded || Math.abs(this.inertiaVelocity) < FLING_STOP_SPEED) this.stopInertia();
  }

  recordTouch(touch, point, time) {
    touch.samples.push({ y: point.y, time });
    while (touch.samples.length > 1 && time - touch.samples[0].time > FLING_SAMPLE_MS) {
      touch.samples.shift();
    }
  }

  flingVelocity(touch) {
    const first = touch.samples[0];
    const last = touch.samples[touch.samples.length - 1];
    const elapsed = last.time - first.time;
    return elapsed > 0 ? (first.y - last.y) * 1000 / elapsed : 0;
  }

  isContentPoint(point) {
    return point.y >= 0 && point.y < this.height - this.getNavHeight();
  }

  onTouchStart(event) {
    const point = pointFromEvent(event);
    if (!point) return;
    this.stopInertia();
    const time = this.eventTime(event);
    this.touch = {
      ...point,
      lastY: point.y,
      moved: false,
      allowScroll: this.isContentPoint(point),
      samples: [{ y: point.y, time }],
    };
  }

  onTouchMove(event) {
    const point = pointFromEvent(event);
    if (!point || !this.touch) return;
    if (Math.abs(point.y - this.touch.y) > 6 || Math.abs(point.x - this.touch.x) > 6) this.touch.moved = true;
    if (this.touch.moved && this.touch.allowScroll) {
      this.scrollBy(this.touch.lastY - point.y);
      this.recordTouch(this.touch, point, this.eventTime(event));
    }
    this.touch.lastY = point.y;
  }

  onTouchEnd(event) {
    const point = pointFromEvent(event);
    const touch = this.touch;
    this.touch = null;
    if (!point || !touch) return;
    if (touch.moved) {
      if (touch.allowScroll) {
        this.recordTouch(touch, point, this.eventTime(event));
        this.startInertia(this.flingVelocity(touch));
      }
      return;
    }
    const zone = [...this.zones].reverse().find((item) => (
      point.x >= item.x && point.x <= item.x + item.width
      && point.y >= item.y && point.y <= item.y + item.height
    ));
    if (zone) this.handleAction(zone.action);
  }

  render() {
    const ctx = this.ctx;
    const w = this.width;
    const h = this.height;
    const navHeight = this.getNavHeight();
    this.scroll = Math.max(0, Math.min(this.maxScroll, this.scroll));
    this.zones = [];
    const gradient = ctx.createLinearGradient(0, 0, w, h);
    const stadiumPage = this.page === 'overview' || this.page === 'daily';
    gradient.addColorStop(0, stadiumPage ? '#164d2c' : this.colors.background);
    gradient.addColorStop(1, stadiumPage ? '#081d13' : '#172318');
    ctx.fillStyle = gradient;
    ctx.fillRect(0, 0, w, h);

    ctx.save();
    ctx.beginPath();
    ctx.rect(0, 0, w, h - navHeight);
    ctx.clip();
    const renderer = renderers[this.page] || renderers.overview;
    const contentEnd = renderer(this, this.safeTop + 10 - this.scroll) + this.scroll;
    ctx.restore();

    this.maxScroll = Math.max(0, contentEnd + 24 - (h - navHeight));
    if (this.scroll > this.maxScroll && !this.correctingScroll) {
      this.scroll = this.maxScroll;
      this.correctingScroll = true;
      this.render();
      this.correctingScroll = false;
      return;
    }

    if (this.page !== 'daily') this.renderTabs();
    this.renderMessage();
  }

  renderTabs() {
    const p = this.painter;
    const navHeight = this.getNavHeight();
    const top = this.height - navHeight;
    const tabWidth = this.width / TABS.length;
    p.rect(0, top, this.width, navHeight, '#101712');
    p.line(0, top, this.width, top, this.colors.border);
    TABS.forEach((tab, index) => {
      const selected = this.page === tab.id;
      const x = index * tabWidth;
      if (selected) p.rect(x + 14, top, tabWidth - 28, 3, this.colors.accent);
      p.text(tab.label, x + tabWidth / 2, top + 22, {
        size: 12,
        weight: selected ? 'bold' : 'normal',
        color: selected ? this.colors.accent : this.colors.muted,
        align: 'center',
      });
      this.zones.push({ x, y: top, width: tabWidth, height: navHeight, action: { type: 'page', page: tab.id } });
    });
  }

  renderMessage() {
    if (!this.message) return;
    const p = this.painter;
    const w = Math.min(this.width - 48, 320);
    const x = (this.width - w) / 2;
    const textH = p.paragraphHeight(this.message, w - 28, { size: 12, align: 'center', lineHeight: 18 });
    const h = Math.max(44, textH + 24);
    const y = this.height - this.getNavHeight() - h - 16;
    p.roundRect(x, y, w, h, 14, '#25331c', this.colors.accent);
    p.paragraph(this.message, x + 14, y + 12, w - 28, { size: 12, color: this.colors.accent, align: 'center', lineHeight: 18 });
  }
}

module.exports = { MiniGameApp, TABS, colors };
