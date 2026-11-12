const colors = {
  card: '#12351f',
  cardAlt: '#214b2a',
  border: '#3a6a40',
  text: '#f4faec',
  secondary: '#c2d3bc',
  muted: '#91ad8c',
  accent: '#b9f54a',
  accentText: '#10200b',
  risk: '#e8bd67',
  pitch: '#2b6740',
};

function card(app, y, height, radius = 18) {
  const x = app.padding;
  const w = app.width - app.padding * 2;
  app.painter.roundRect(x, y, w, height, radius, colors.card, colors.border);
}

function button(app, label, x, y, width, height, action, options = {}) {
  const p = app.painter;
  const primary = options.primary;
  const size = options.small ? 11 : 13;
  p.roundRect(x, y, width, height, options.small ? 10 : 14, primary ? colors.accent : colors.cardAlt, primary ? colors.accent : colors.border);
  p.paragraph(label, x + 10, y + Math.max(9, (height - size * 1.45) / 2 - 1), width - 20, {
    size,
    weight: primary ? 'bold' : 'normal',
    color: primary ? colors.accentText : colors.text,
    align: 'center',
    lineHeight: Math.round(size * 1.45),
  });
  if (y + height >= 0 && y <= app.height - app.getNavHeight()) {
    app.zones.push({ x, y, width, height, action });
  }
  return y + height;
}

function section(app, y, kicker, title, description = '') {
  const x = app.padding;
  const w = app.width - app.padding * 2;
  y = app.write(kicker, x, y, w, { size: 10, weight: 'bold', color: colors.accent, mono: true, lineHeight: 15 }) + 8;
  y = app.write(title, x, y, w, { size: 25, weight: 'bold', color: colors.text, lineHeight: 34 });
  if (description) y = app.write(description, x, y + 8, w, { size: 12, color: colors.secondary, lineHeight: 20 });
  return y + 20;
}

function notice(app, y, text) {
  const p = app.painter;
  const x = app.padding;
  const w = app.width - app.padding * 2;
  const textH = p.paragraphHeight(text, w - 64, { size: 11, lineHeight: 18 });
  const height = Math.max(58, textH + 26);
  p.roundRect(x, y, w, height, 14, '#2a3820', '#6a633a');
  p.circle(x + 23, y + 23, 10, colors.risk);
  p.text('i', x + 23, y + 17, { size: 11, weight: 'bold', color: '#2c210e', align: 'center' });
  p.paragraph(text, x + 43, y + 13, w - 64, { size: 11, color: '#e0d4b4', lineHeight: 18 });
  return y + height + 12;
}

function probabilityBar(app, match, x, y, width) {
  const p = app.painter;
  const segments = [
    { label: '主胜', value: match.probabilities.home_win, color: '#78936f' },
    { label: '平局', value: match.probabilities.draw, color: '#4f6c55' },
    { label: '客胜', value: match.probabilities.away_win, color: '#91a188' },
  ];
  const gap = 3;
  const usable = width - gap * 2;
  let cursor = x;
  for (const segment of segments) {
    const active = match.tendency.startsWith(segment.label);
    const segmentWidth = Math.max(3, usable * segment.value);
    p.roundRect(cursor, y, segmentWidth, 8, 4, active ? colors.accent : segment.color);
    cursor += segmentWidth + gap;
  }
}

function pitch(app, x, y, width, height) {
  const p = app.painter;
  p.roundRect(x, y, width, height, 12, null, colors.pitch);
  p.line(x + width / 2, y, x + width / 2, y + height, colors.pitch);
  p.circle(x + width / 2, y + height / 2, Math.min(28, height * 0.22), null, colors.pitch);
  p.rect(x, y + height * 0.28, 1, height * 0.44, colors.pitch);
  p.rect(x + width - 1, y + height * 0.28, 1, height * 0.44, colors.pitch);
}

module.exports = { colors, card, button, section, notice, probabilityBar, pitch };
