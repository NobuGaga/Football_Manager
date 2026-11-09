module.exports = function renderPipeline(app, y) {
  const { painter: p, colors: c, width, project } = app;
  const x = app.padding;
  const w = width - app.padding * 2;
  y = app.section(y, '02 / DATA PIPELINE', '从对阵，到可验证的结论', '点击节点，查看每一步的输入、输出与异常边界。');

  const gap = 8;
  const nodeW = (w - gap) / 2;
  project.flowSteps.forEach((item, index) => {
    const row = Math.floor(index / 2);
    const column = index % 2;
    const nx = x + column * (nodeW + gap);
    const ny = y + row * 56;
    app.button(`${String(index + 1).padStart(2, '0')}  ${item.mark}  ${item.short}`, nx, ny, nodeW, 46, { type: 'flow', index }, { selected: app.flowIndex === index, small: true });
  });
  y += Math.ceil(project.flowSteps.length / 2) * 56 + 8;

  const current = project.flowSteps[app.flowIndex];
  const inner = w - 32;
  const titleH = p.paragraphHeight(current.title, inner, { size: 20, weight: 'bold', lineHeight: 29 });
  const descH = p.paragraphHeight(current.description, inner, { size: 13, color: c.secondary, lineHeight: 21 });
  const detailH = p.paragraphHeight(current.detail, inner, { size: 12, color: c.muted, lineHeight: 20 });
  const ioH = p.paragraphHeight(current.output, inner - 32, { size: 12, mono: true, lineHeight: 19 });
  const cardH = 106 + titleH + descH + detailH + ioH;
  app.card(y, cardH);
  p.text(`${current.eyebrow} / ${app.flowIndex + 1}`, x + 16, y + 15, { size: 10, color: c.accent, mono: true });
  p.paragraph(current.title, x + 16, y + 39, inner, { size: 20, weight: 'bold', lineHeight: 29 });
  let cursor = y + 39 + titleH + 10;
  p.paragraph(current.description, x + 16, cursor, inner, { size: 13, color: c.secondary, lineHeight: 21 });
  cursor += descH + 10;
  p.paragraph(current.detail, x + 16, cursor, inner, { size: 12, color: c.muted, lineHeight: 20 });
  cursor += detailH + 14;
  p.rect(x + 16, cursor, inner - 58, 5, c.border);
  p.rect(x + 16, cursor, (inner - 58) * (app.flowIndex + 1) / project.flowSteps.length, 5, c.accent);
  p.text(`${app.flowIndex + 1} / ${project.flowSteps.length}`, x + w - 16, cursor - 7, { size: 11, color: c.muted, align: 'right', mono: true });
  cursor += 20;
  p.text(`INPUT  ${current.input}`, x + 16, cursor, { size: 11, color: c.muted, mono: true });
  cursor += 22;
  p.paragraph(`OUTPUT  ${current.output}`, x + 16, cursor, inner, { size: 11, color: c.accent, mono: true, lineHeight: 19 });
  y += cardH + 12;

  if (app.flowIndex < project.flowSteps.length - 1) {
    y = app.button('查看下一节点  →', x, y, w, 46, { type: 'flow-next' }, { primary: true }) + 12;
  } else {
    p.roundRect(x, y, w, 48, 14, c.cardAlt, c.accent);
    p.text('✓ 已查看完整数据流水线', x + w / 2, y + 15, { size: 13, weight: 'bold', color: c.accent, align: 'center' });
    y += 60;
  }

  y = app.section(y, 'SQLITE SCHEMA', '08 张核心数据表', '职责清晰，关联可审计；历史快照和推断版本不被覆盖。');
  const tagH = Math.ceil(project.tables.length / 2) * 42 + 20;
  app.card(y, tagH);
  project.tables.forEach((item, index) => {
    const column = index % 2;
    const row = Math.floor(index / 2);
    const tx = x + 12 + column * ((w - 24) / 2);
    const ty = y + 12 + row * 42;
    p.roundRect(tx, ty, (w - 32) / 2, 32, 8, c.cardAlt);
    p.text(item, tx + 8, ty + 9, { size: 9, color: c.secondary, mono: true });
  });
  y += tagH + 14;
  return app.notice(y, 'business_date 是体彩营业日；数据库存储 UTC 开赛时间，同时保留原始执行时区，避免跨日比赛错配。', 'i');
};
