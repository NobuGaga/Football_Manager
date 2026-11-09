module.exports = function renderModel(app, y) {
  const { painter: p, colors: c, width, project } = app;
  const x = app.padding;
  const w = width - app.padding * 2;
  y = app.section(y, '03 / MODEL & EVALUATION', '输出概率，也输出不确定性', '模型给出可复现基线，回测负责检验长期概率质量。');

  const formulaH = 296;
  p.roundRect(x, y, w, formulaH, 16, c.cardAlt, c.accent);
  p.text('PROBABILITY ENGINE', x + 16, y + 16, { size: 10, color: c.accent, mono: true });
  p.text('poisson-dc-v1', x + w - 16, y + 16, { size: 10, color: c.muted, align: 'right', mono: true });
  p.text('从预期进球开始', x + 16, y + 44, { size: 21, weight: 'bold' });
  p.paragraph('基础比分概率由双方独立 Poisson 分布相乘，再对低比分进行 Dixon–Coles 修正。', x + 16, y + 80, w - 32, { size: 13, color: c.secondary, lineHeight: 21 });
  p.roundRect(x + 16, y + 132, w - 32, 82, 12, c.card);
  p.text('基础比分概率', x + 30, y + 145, { size: 10, color: c.muted });
  p.text('P(H=h, A=a)', x + 30, y + 166, { size: 15, weight: 'bold', color: c.accent, mono: true });
  p.paragraph('= Poisson(h; λhome) × Poisson(a; λaway)', x + 30, y + 190, w - 60, { size: 11, mono: true, lineHeight: 17 });
  p.text('低比分修正与归一化', x + 16, y + 230, { size: 13, weight: 'bold' });
  p.paragraph('0:0、0:1、1:0、1:1 使用 rho 修正，在 0..10 球网格归一化。', x + 16, y + 255, w - 32, { size: 11, color: c.muted, lineHeight: 18 });
  y += formulaH + 20;

  y = app.section(y, 'EXPECTED GOALS', '预期进球的估计顺序', '优先使用信息更完整、时间边界更明确的输入。');
  for (const item of project.priorities) {
    y = app.simpleCard(y, { kicker: item.id, title: item.title, body: item.detail, compact: true });
  }

  const boundaryH = 190;
  app.card(y, boundaryH);
  p.text('TIME INTEGRITY', x + 16, y + 16, { size: 10, color: c.accent, mono: true });
  p.text('只有合法的时间链，才进入回测', x + 16, y + 42, { size: 18, weight: 'bold' });
  p.paragraph('防止未来数据泄漏，也避免赛后信息重写赛前判断。', x + 16, y + 73, w - 32, { size: 12, color: c.secondary, lineHeight: 19 });
  app.button('复制时间边界表达式', x + 16, y + 108, w - 32, 44, { type: 'copy', text: 'input_as_of <= predicted_at <= start_time_utc', success: '时间边界已复制' });
  p.text('input_as_of ≤ predicted_at ≤ start_time_utc', x + w / 2, y + 164, { size: 9, color: c.muted, align: 'center', mono: true });
  y += boundaryH + 20;

  y = app.section(y, 'ROLLING BACKTEST', '评价概率，而非迷信单次命中', '总体与分联赛分别统计，只使用已保存推断和真实赛果。');
  const metricW = (w - 10) / 2;
  project.metrics.forEach((item, index) => {
    const column = index % 2;
    const row = Math.floor(index / 2);
    const mx = x + column * (metricW + 10);
    const my = y + row * 94;
    p.roundRect(mx, my, metricW, 84, 14, c.card, c.border);
    p.text(String(index + 1).padStart(2, '0'), mx + 12, my + 10, { size: 10, color: c.accent, mono: true });
    p.text(item.name, mx + 12, my + 32, { size: 13, weight: 'bold' });
    p.text(item.label, mx + 12, my + 57, { size: 11, color: c.muted });
  });
  y += Math.ceil(project.metrics.length / 2) * 94 + 8;
  return app.notice(y, '这是一套可运行基线，不宣称已完成分联赛参数训练。升班马、小样本、杯赛与阵容不确定信息应通过特征和风险标记体现。');
};
