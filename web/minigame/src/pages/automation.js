module.exports = function renderAutomation(app, y) {
  const { painter: p, colors: c, width, project } = app;
  const x = app.padding;
  const w = width - app.padding * 2;
  y = app.section(y, '04 / DAILY AUTOMATION', '每天一条安全、完整的执行链', 'Kimi Claw 定时执行并推送预测策略，GitHub 仅保存项目版本。');

  const tabW = w / 3;
  project.commandModes.forEach((item, index) => {
    app.button(item.label, x + index * tabW, y, tabW - 6, 42, { type: 'mode', id: item.id }, { selected: app.commandMode === item.id, small: true });
  });
  y += 56;

  const current = project.commandModes.find((item) => item.id === app.commandMode);
  const inner = w - 32;
  const titleH = p.paragraphHeight(current.title, inner, { size: 19, weight: 'bold', lineHeight: 28 });
  const descH = p.paragraphHeight(current.description, inner, { size: 12, color: c.secondary, lineHeight: 20 });
  const commandH = p.paragraphHeight(current.command, inner - 32, { size: 11, mono: true, lineHeight: 19 });
  const cardH = 152 + titleH + descH + commandH;
  app.card(y, cardH);
  p.roundRect(x + 16, y + 14, 88, 25, 12, c.cardAlt, c.border);
  p.text(current.badge, x + 60, y + 21, { size: 10, color: c.accent, align: 'center' });
  p.paragraph(current.title, x + 16, y + 53, inner, { size: 19, weight: 'bold', lineHeight: 28 });
  let cursor = y + 53 + titleH + 10;
  p.paragraph(current.description, x + 16, cursor, inner, { size: 12, color: c.secondary, lineHeight: 20 });
  cursor += descH + 14;
  p.roundRect(x + 16, cursor, inner, 54 + commandH, 12, '#090d0a', c.border);
  p.text('TERMINAL', x + 30, cursor + 13, { size: 10, color: c.muted, mono: true });
  app.button('复制', x + w - 80, cursor + 8, 50, 34, { type: 'copy', text: current.command, success: '运行命令已复制' }, { small: true });
  p.paragraph(current.command, x + 30, cursor + 43, inner - 32, { size: 11, color: c.accent, mono: true, lineHeight: 19 });
  y += cardH + 12;
  y = app.notice(y, '正式任务要求 Git 工作区干净；关联失败时不提交半成品。本页面只展示命令，不会在你的设备上执行。') + 6;

  y = app.section(y, 'DAILY RUNBOOK', '每日执行顺序', '每一步都保留明确输入、输出和失败边界。');
  const rowH = 42;
  const runbookH = project.automationSteps.length * rowH + 22;
  app.card(y, runbookH);
  project.automationSteps.forEach((item, index) => {
    const ry = y + 12 + index * rowH;
    p.circle(x + 29, ry + 14, 12, index === project.automationSteps.length - 1 ? c.accent : c.cardAlt, c.border);
    p.text(String(index + 1), x + 29, ry + 7, { size: 10, weight: 'bold', color: index === project.automationSteps.length - 1 ? c.accentText : c.secondary, align: 'center', mono: true });
    if (index < project.automationSteps.length - 1) p.line(x + 29, ry + 27, x + 29, ry + rowH + 2, c.border);
    p.paragraph(item, x + 52, ry + 5, w - 68, { size: 12, color: c.secondary, lineHeight: 19 });
  });
  y += runbookH + 20;

  y = app.section(y, 'SAFETY BOUNDARIES', '自动化安全边界', '命令展示与真实执行严格分离。');
  for (const item of project.safetyRules) {
    y = app.simpleCard(y, { kicker: item.mark, title: item.title, body: item.description, compact: true });
  }
  return y;
};
