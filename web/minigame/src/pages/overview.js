const { colors: c, card, button, section, notice, pitch } = require('./stadium-theme.js');

function principleCard(app, item, y) {
  const p = app.painter;
  const x = app.padding;
  const w = app.width - app.padding * 2;
  const inner = w - 30;
  const titleH = p.paragraphHeight(item.title, inner - 44, { size: 15, weight: 'bold', lineHeight: 23 });
  const bodyH = p.paragraphHeight(item.description, inner, { size: 11, lineHeight: 18 });
  const height = 64 + titleH + bodyH;
  card(app, y, height, 16);
  p.text(item.number, x + 15, y + 14, { size: 10, color: c.accent, mono: true });
  p.circle(x + w - 35, y + 24, 15, c.cardAlt);
  p.text(item.mark, x + w - 35, y + 16, { size: 12, weight: 'bold', color: c.accent, align: 'center' });
  p.paragraph(item.title, x + 15, y + 42, inner - 44, { size: 15, weight: 'bold', color: c.text, lineHeight: 23 });
  p.paragraph(item.description, x + 15, y + 42 + titleH + 10, inner, { size: 11, color: c.secondary, lineHeight: 18 });
  return y + height + 12;
}

module.exports = function renderOverview(app, y) {
  const { painter: p, width, project } = app;
  const x = app.padding;
  const w = width - app.padding * 2;

  p.roundRect(x, y + 2, 42, 42, 12, c.accent);
  p.text('FM', x + 21, y + 14, { size: 16, weight: 'bold', color: c.accentText, align: 'center', mono: true });
  p.text('足球经理的推断', x + 54, y + 4, { size: 16, weight: 'bold', color: c.text });
  p.text('FOOTBALL INTELLIGENCE', x + 54, y + 27, { size: 9, color: c.muted, mono: true });
  p.roundRect(x + w - 74, y + 8, 74, 26, 13, c.cardAlt, c.border);
  p.text('比赛中心', x + w - 37, y + 15, { size: 10, color: c.secondary, align: 'center' });
  y += 62;

  y = app.write('MATCHDAY · 多联赛概率分析', x, y, w, { size: 11, color: c.accent, mono: true }) + 12;
  y = app.write('让每一次推断，', x, y, w, { size: 30, weight: 'bold', color: c.text, lineHeight: 39 });
  y = app.write('都有迹可循。', x, y, w, { size: 30, weight: 'bold', color: c.accent, lineHeight: 39 }) + 12;
  y = app.write('看见概率，也看见不确定性。从官方对阵到赛后回测，保留完整证据与安全边界。', x, y, w, { size: 13, color: c.secondary, lineHeight: 22 }) + 16;

  y = button(app, '进入今日比赛中心  →', x, y, w, 46, { type: 'page', page: 'daily' }, { primary: true }) + 10;
  const half = (w - 10) / 2;
  button(app, '探索数据流水线', x, y, half, 44, { type: 'page', page: 'pipeline' });
  button(app, '复制每日命令', x + half + 10, y, half, 44, { type: 'copy', text: 'bash scripts/daily_automation.sh', success: '每日命令已复制' });
  y += 58;

  const modelHeight = 248;
  card(app, y, modelHeight, 20);
  pitch(app, x + 12, y + 12, w - 24, modelHeight - 24);
  p.text('SCORE MATRIX', x + 16, y + 16, { size: 9, color: c.muted, mono: true });
  p.text('poisson-dc-v1', x + 16, y + 36, { size: 16, weight: 'bold', color: c.text, mono: true });
  p.roundRect(x + w - 105, y + 16, 89, 26, 13, c.cardAlt, c.border);
  p.text('模型 V1 · 可复现', x + w - 60, y + 23, { size: 8, color: c.accent, align: 'center' });
  p.text('比分概率矩阵 · 0—10 球网格', x + 16, y + 72, { size: 11, color: c.secondary });
  const gap = 5;
  const cell = (w - 32 - gap * 6) / 7;
  app.matrix.forEach((item, index) => {
    const row = Math.floor(index / 7);
    const column = index % 7;
    const color = item.hot ? c.accent : `rgba(185,245,74,${Math.max(0.12, item.opacity * 0.55)})`;
    p.roundRect(x + 16 + column * (cell + gap), y + 98 + row * 14, cell, 9, 3, color);
  });
  p.text('P  完整概率分布', x + 16, y + 216, { size: 11, color: c.secondary });
  p.text('V  追加而非覆盖', x + w / 2, y + 216, { size: 11, color: c.secondary });
  y += modelHeight + 20;

  const statW = w / 4;
  project.stats.forEach((item, index) => {
    const sx = x + index * statW;
    if (index) p.line(sx, y + 4, sx, y + 92, c.border);
    p.text(item.value, sx + 10, y + 8, { size: 24, weight: 'bold', color: c.text, mono: true });
    p.text(item.unit, sx + 45, y + 17, { size: 9, color: c.accent });
    p.paragraph(item.label, sx + 10, y + 45, statW - 18, { size: 10, weight: 'bold', color: c.text, lineHeight: 16 });
    p.paragraph(item.detail, sx + 10, y + 67, statW - 18, { size: 8, color: c.muted, lineHeight: 13 });
  });
  p.line(x, y, x + w, y, c.border);
  p.line(x, y + 104, x + w, y + 104, c.border);
  y += 126;

  y = section(app, y, '01 / CORE PRINCIPLES', '四条核心设计原则', '保存推断结果，也保存得出结果所需的上下文。');
  for (const item of project.principles) y = principleCard(app, item, y);
  y = notice(app, y, '足球经理的推断基于赛前数据与概率模型，不构成收益保证或确定性投注建议。信息不足时，明确标记数据缺失与不确定性。');
  y = app.write('Python CLI · SQLite · Poisson / Dixon–Coles\nTaro React H5 · Canvas 2D 微信小游戏', x, y + 10, w, { size: 11, color: c.muted, align: 'center', lineHeight: 19 });
  return y + 12;
};
