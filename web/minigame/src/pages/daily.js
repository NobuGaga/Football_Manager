function matchCard(app, match, y) {
  const { painter: p, colors: c, width } = app;
  const x = app.padding;
  const w = width - app.padding * 2;
  const inner = w - 28;
  const teams = `${match.homeTeam}  VS  ${match.awayTeam}`;
  const teamsHeight = p.paragraphHeight(teams, inner, { size: 18, weight: 'bold', lineHeight: 27 });
  const methodHeight = p.paragraphHeight(match.methodText, inner - 110, { size: 11, lineHeight: 17 });
  const risk = `${match.primaryRisk}${match.remainingRiskCount > 0 ? ` 等 ${match.remainingRiskCount + 1} 项风险` : ''}`;
  const riskHeight = p.paragraphHeight(risk, inner, { size: 11, color: c.risk, lineHeight: 18 });
  const height = 250 + teamsHeight + methodHeight + riskHeight;

  app.card(y, height);
  p.text(match.matchCode, x + 14, y + 14, { size: 12, weight: 'bold', color: c.accent, mono: true });
  p.text(match.startTimeText, x + w - 14, y + 14, { size: 10, color: c.muted, align: 'right', mono: true });
  p.paragraph(`${match.league} · ${match.aiModel} / ${match.strategyCategory}`, x + 14, y + 40, inner, { size: 11, color: c.secondary, lineHeight: 17 });
  p.paragraph(teams, x + 14, y + 68, inner, { size: 18, weight: 'bold', lineHeight: 27 });
  let cursor = y + 68 + teamsHeight + 12;
  p.text('最高概率倾向', x + 14, cursor, { size: 11, color: c.muted });
  p.text(match.tendency, x + w - 14, cursor, { size: 13, weight: 'bold', color: c.accent, align: 'right' });
  cursor += 27;

  const boxGap = 7;
  const boxW = (inner - boxGap * 2) / 3;
  const probabilities = [
    ['主胜', match.homePercent],
    ['平局', match.drawPercent],
    ['客胜', match.awayPercent],
  ];
  probabilities.forEach(([label, value], index) => {
    const bx = x + 14 + index * (boxW + boxGap);
    p.roundRect(bx, cursor, boxW, 54, 10, c.cardAlt);
    p.text(label, bx + boxW / 2, cursor + 8, { size: 10, color: c.muted, align: 'center' });
    p.text(value, bx + boxW / 2, cursor + 27, { size: 14, weight: 'bold', align: 'center', mono: true });
  });
  cursor += 66;

  const scores = match.topScores.map((item) => `${item.score} · ${item.probabilityText}`).join('    ');
  p.text('比分参考', x + 14, cursor, { size: 11, color: c.muted });
  p.paragraph(scores, x + 85, cursor, inner - 71, { size: 11, color: c.text, mono: true, lineHeight: 17 });
  cursor += 25;
  p.text(`完整度 ${match.completenessText}`, x + 14, cursor, { size: 11, color: c.secondary });
  p.paragraph(match.methodText, x + 124, cursor, inner - 110, { size: 11, color: c.secondary, align: 'right', lineHeight: 17 });
  cursor += Math.max(22, methodHeight + 5);
  p.paragraph(risk, x + 14, cursor, inner, { size: 11, color: c.risk, lineHeight: 18 });
  return y + height + 12;
}

module.exports = function renderDaily(app, y) {
  const { painter: p, colors: c, width, dailyReport } = app;
  const x = app.padding;
  const w = width - app.padding * 2;
  y = app.button('←  返回概览', x, y, 126, 40, { type: 'page', page: 'overview' }, { small: true }) + 16;
  y = app.section(y, 'DAILY PROBABILITY REPORT', '每日推断', `${dailyReport.reportDate} · ${dailyReport.matchCount} 场 / ${dailyReport.predictionCount} 条策略 · 由 Python 每日任务打包`);
  y = app.notice(y, `${dailyReport.disclaimer} 数据完整度较低时仅作弱参考。`);

  if (app.matches.length) {
    for (const match of app.matches) y = matchCard(app, match, y);
  } else {
    app.card(y, 96);
    p.paragraph('暂无可展示的每日推断，请先运行每日任务。', x + 18, y + 28, w - 36, { size: 14, color: c.secondary, align: 'center', lineHeight: 23 });
    y += 108;
  }
  y = app.write(`数据生成于 ${dailyReport.generatedAt}\n本页面为打包时的报告快照，不实时连接数据库。`, x, y + 8, w, { size: 11, color: c.muted, align: 'center', lineHeight: 19 });
  return y + 12;
};
