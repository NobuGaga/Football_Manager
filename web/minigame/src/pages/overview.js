module.exports = function renderOverview(app, y) {
  const { colors: c, painter: p, width, project } = app;
  const x = app.padding;
  const w = width - app.padding * 2;

  p.roundRect(x, y + 2, 42, 42, 12, c.accent);
  p.text('FM', x + 21, y + 14, { size: 16, weight: 'bold', color: c.accentText, align: 'center', mono: true });
  p.text('足球经理的推断', x + 54, y + 4, { size: 16, weight: 'bold' });
  p.text('PROJECT HUB / CANVAS', x + 54, y + 27, { size: 10, color: c.muted, mono: true });
  y += 62;

  y = app.write('— 足球经理的推断 · 多联赛概率分析', x, y, w, { size: 11, color: c.accent, mono: true }) + 12;
  y = app.write('让每一次推断，', x, y, w, { size: 30, weight: 'bold', lineHeight: 39 });
  y = app.write('都有迹可循。', x, y, w, { size: 30, weight: 'bold', color: c.accent, lineHeight: 39 }) + 12;
  y = app.write('从官方对阵到赛后回测，一条带时间边界、版本记录与安全约束的完整数据流水线。', x, y, w, { size: 14, color: c.secondary, lineHeight: 23 }) + 18;

  const skillTestTextHeight = p.paragraphHeight(project.skillTestText, w - 28, { size: 13, weight: 'bold', lineHeight: 20 });
  const skillTestCardHeight = skillTestTextHeight + 22;
  p.roundRect(x, y, w, skillTestCardHeight, 12, c.cardAlt, c.accent);
  p.paragraph(project.skillTestText, x + 14, y + 11, w - 28, { size: 13, weight: 'bold', color: c.accent, lineHeight: 20 });
  y += skillTestCardHeight + 16;

  y = app.button('查看每日推断  →', x, y, w, 46, { type: 'page', page: 'daily' }, { primary: true }) + 10;
  const half = (w - 10) / 2;
  app.button('探索数据流水线', x, y, half, 44, { type: 'page', page: 'pipeline' });
  app.button('复制每日命令', x + half + 10, y, half, 44, { type: 'copy', text: 'bash scripts/daily_automation.sh', success: '每日命令已复制' });
  y += 58;

  const modelHeight = 238;
  app.card(y, modelHeight);
  p.text('MODEL BASELINE', x + 16, y + 16, { size: 10, color: c.muted, mono: true });
  p.text('poisson-dc-v1', x + 16, y + 36, { size: 16, weight: 'bold', mono: true });
  p.roundRect(x + w - 118, y + 16, 102, 26, 13, c.cardAlt, c.border);
  p.text('● 可复现基线', x + w - 67, y + 23, { size: 10, color: c.accent, align: 'center' });
  p.text('比分概率矩阵 · 示意', x + 16, y + 72, { size: 12, color: c.secondary });
  const matrix = app.matrix;
  const gap = 5;
  const cell = (w - 32 - gap * 6) / 7;
  matrix.forEach((item, index) => {
    const row = Math.floor(index / 7);
    const column = index % 7;
    const color = item.hot ? c.accent : `rgba(200,245,90,${Math.max(0.12, item.opacity * 0.55)})`;
    p.roundRect(x + 16 + column * (cell + gap), y + 98 + row * 14, cell, 9, 3, color);
  });
  p.text('P  完整概率分布', x + 16, y + 210, { size: 11, color: c.secondary });
  p.text('V  追加而非覆盖', x + w / 2, y + 210, { size: 11, color: c.secondary });
  y += modelHeight + 20;

  const statW = (w - 10) / 2;
  project.stats.forEach((item, index) => {
    const column = index % 2;
    const row = Math.floor(index / 2);
    const sx = x + column * (statW + 10);
    const sy = y + row * 112;
    p.roundRect(sx, sy, statW, 102, 14, c.card, c.border);
    p.text(`${item.value} ${item.unit}`, sx + 14, sy + 12, { size: 23, weight: 'bold', color: c.accent, mono: true });
    p.text(item.label, sx + 14, sy + 46, { size: 12, weight: 'bold' });
    p.paragraph(item.detail, sx + 14, sy + 68, statW - 28, { size: 10, color: c.muted, lineHeight: 15 });
  });
  y += 2 * 112 + 12;

  y = app.section(y, '01 / CORE PRINCIPLES', '四条核心设计原则', '保存推断结果，也保存得出结果所需的上下文。');
  for (const item of project.principles) {
    y = app.simpleCard(y, {
      kicker: `${item.number} / ${item.mark}`,
      title: item.title,
      body: item.description,
    });
  }
  y = app.notice(y, '足球经理的推断基于赛前数据与概率模型，不构成收益保证或确定性投注建议。信息不足时，明确标记数据缺失与不确定性。');
  y = app.write('Python CLI · SQLite · Poisson / Dixon–Coles\nTaro React H5 · Canvas 2D 微信小游戏', x, y + 10, w, { size: 11, color: c.muted, align: 'center', lineHeight: 19 });
  return y + 12;
};
