const SANS = 'sans-serif';
const MONO = 'monospace';

function fontString(size = 14, weight = 'normal', mono = false) {
  return `${weight} ${size}px ${mono ? MONO : SANS}`;
}

class Painter {
  constructor(ctx, viewportHeight) {
    this.ctx = ctx;
    this.viewportHeight = viewportHeight;
    this.wrapCache = new Map();
    this.wrapCacheLimit = 2048;
  }

  visible(y, height = 1) {
    const viewport = this.viewportHeight();
    return y + height >= -48 && y <= viewport + 48;
  }

  setFont(size, weight = 'normal', mono = false) {
    this.ctx.font = fontString(size, weight, mono);
  }

  wrap(text, maxWidth, options = {}) {
    const { size = 14, weight = 'normal', mono = false } = options;
    this.setFont(size, weight, mono);
    const value = String(text ?? '');
    const key = `${fontString(size, weight, mono)}|${maxWidth}|${value}`;
    const cached = this.wrapCache.get(key);
    if (cached) {
      this.wrapCache.delete(key);
      this.wrapCache.set(key, cached);
      return cached;
    }

    const lines = [];
    for (const rawLine of value.split('\n')) {
      if (!rawLine) {
        lines.push('');
        continue;
      }
      let line = '';
      for (const character of rawLine) {
        const candidate = line + character;
        if (line && this.ctx.measureText(candidate).width > maxWidth) {
          lines.push(line);
          line = character;
        } else {
          line = candidate;
        }
      }
      if (line) lines.push(line);
    }
    const result = lines.length ? lines : [''];
    this.wrapCache.set(key, result);
    if (this.wrapCache.size > this.wrapCacheLimit) {
      this.wrapCache.delete(this.wrapCache.keys().next().value);
    }
    return result;
  }

  paragraphHeight(text, maxWidth, options = {}) {
    const { size = 14, lineHeight = Math.round((options.size || 14) * 1.55) } = options;
    return this.wrap(text, maxWidth, options).length * lineHeight;
  }

  paragraph(text, x, y, maxWidth, options = {}) {
    const {
      size = 14,
      color = '#eef3e9',
      weight = 'normal',
      mono = false,
      align = 'left',
      lineHeight = Math.round(size * 1.55),
    } = options;
    const lines = this.wrap(text, maxWidth, { size, weight, mono });
    if (this.visible(y, lines.length * lineHeight)) {
      this.setFont(size, weight, mono);
      this.ctx.fillStyle = color;
      this.ctx.textAlign = align;
      this.ctx.textBaseline = 'top';
      const drawX = align === 'center' ? x + maxWidth / 2 : align === 'right' ? x + maxWidth : x;
      lines.forEach((line, index) => this.ctx.fillText(line, drawX, y + index * lineHeight));
    }
    return lines.length * lineHeight;
  }

  text(text, x, y, options = {}) {
    const {
      size = 14,
      color = '#eef3e9',
      weight = 'normal',
      mono = false,
      align = 'left',
    } = options;
    if (!this.visible(y, size + 4)) return;
    this.setFont(size, weight, mono);
    this.ctx.fillStyle = color;
    this.ctx.textAlign = align;
    this.ctx.textBaseline = 'top';
    this.ctx.fillText(String(text ?? ''), x, y);
  }

  rect(x, y, width, height, color) {
    if (!this.visible(y, height)) return;
    this.ctx.fillStyle = color;
    this.ctx.fillRect(x, y, width, height);
  }

  roundRect(x, y, width, height, radius, fill, stroke = null, lineWidth = 1) {
    if (!this.visible(y, height)) return;
    const r = Math.max(0, Math.min(radius, width / 2, height / 2));
    const ctx = this.ctx;
    ctx.beginPath();
    ctx.moveTo(x + r, y);
    ctx.lineTo(x + width - r, y);
    ctx.quadraticCurveTo(x + width, y, x + width, y + r);
    ctx.lineTo(x + width, y + height - r);
    ctx.quadraticCurveTo(x + width, y + height, x + width - r, y + height);
    ctx.lineTo(x + r, y + height);
    ctx.quadraticCurveTo(x, y + height, x, y + height - r);
    ctx.lineTo(x, y + r);
    ctx.quadraticCurveTo(x, y, x + r, y);
    ctx.closePath();
    if (fill) {
      ctx.fillStyle = fill;
      ctx.fill();
    }
    if (stroke) {
      ctx.strokeStyle = stroke;
      ctx.lineWidth = lineWidth;
      ctx.stroke();
    }
  }

  line(x1, y1, x2, y2, color, width = 1) {
    if (!this.visible(Math.min(y1, y2), Math.abs(y2 - y1) + 1)) return;
    const ctx = this.ctx;
    ctx.beginPath();
    ctx.moveTo(x1, y1);
    ctx.lineTo(x2, y2);
    ctx.strokeStyle = color;
    ctx.lineWidth = width;
    ctx.stroke();
  }

  circle(x, y, radius, fill, stroke = null) {
    if (!this.visible(y - radius, radius * 2)) return;
    const ctx = this.ctx;
    ctx.beginPath();
    ctx.arc(x, y, radius, 0, Math.PI * 2);
    if (fill) {
      ctx.fillStyle = fill;
      ctx.fill();
    }
    if (stroke) {
      ctx.strokeStyle = stroke;
      ctx.stroke();
    }
  }
}

module.exports = { Painter, fontString };
