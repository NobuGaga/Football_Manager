const { colors: c, card, button, section, notice, probabilityBar } = require('./stadium-theme.js');

function headerMetrics(app, match) {
  const p = app.painter;
  const inner = app.width - app.padding * 2 - 28;
  const leagueW = inner - 72 - 88 - 20;
  const leagueH = p.paragraphHeight(match.league, leagueW, { size: 9, lineHeight: 14 });
  const modelH = p.paragraphHeight(`${match.aiModel} / ${match.strategyCategory}`, inner, { size: 8, lineHeight: 12 });
  return {
    leagueW,
    leagueH,
    modelH,
    height: 12 + Math.max(25, leagueH) + 5 + modelH + 10,
  };
}

function matchTopline(app, match, y, metrics) {
  const p = app.painter;
  const x = app.padding;
  const w = app.width - app.padding * 2;
  const inner = w - 28;
  const top = y + 12;
  p.roundRect(x + 14, top, 72, 25, 12, c.cardAlt);
  p.text(match.matchCode, x + 50, top + 6, { size: 10, weight: 'bold', color: c.accent, align: 'center' });
  p.paragraph(match.league, x + 96, top + 5, metrics.leagueW, { size: 9, color: c.secondary, lineHeight: 14 });
  p.text(match.startTimeText, x + w - 14, top + 6, { size: 9, color: c.muted, align: 'right', mono: true });
  p.paragraph(`${match.aiModel} / ${match.strategyCategory}`, x + 14, top + Math.max(25, metrics.leagueH) + 5, inner, { size: 8, color: c.muted, lineHeight: 12 });
}

function mainMetrics(app, match, compact) {
  const p = app.painter;
  const inner = app.width - app.padding * 2 - 28;
  const teamW = inner - 126;
  const size = compact ? 13 : 16;
  const lineHeight = compact ? 19 : 23;
  const homeH = p.paragraphHeight(match.homeTeam, teamW, { size, weight: 'bold', lineHeight });
  const awayH = p.paragraphHeight(match.awayTeam, teamW, { size, weight: 'bold', lineHeight });
  return { teamW, size, lineHeight, homeH, awayH, height: Math.max(homeH + awayH + 4, 50) };
}

function matchMain(app, match, y, metrics, compact) {
  const p = app.painter;
  const x = app.padding;
  const w = app.width - app.padding * 2;
  p.paragraph(match.homeTeam, x + 14, y, metrics.teamW, { size: metrics.size, weight: 'bold', color: c.text, lineHeight: metrics.lineHeight });
  p.paragraph(match.awayTeam, x + 14, y + metrics.homeH + 4, metrics.teamW, { size: metrics.size, weight: 'bold', color: c.text, lineHeight: metrics.lineHeight });
  p.text(`最高 · ${match.tendencyLabel}`, x + w - 14, y + 3, { size: 9, weight: 'bold', color: c.accent, align: 'right' });
  p.text(match.tendencyPercent, x + w - 14, y + 22, { size: compact ? 23 : 26, weight: 'bold', color: c.accent, align: 'right', mono: true });
}

function detailMetrics(app, match, compact) {
  const p = app.painter;
  const inner = app.width - app.padding * 2 - 28;
  const size = compact ? 9 : 10;
  const scores = match.topScores.map((item) => `${item.score} · ${item.probabilityText}`).join('    ');
  const risk = `${match.primaryRisk}${match.remainingRiskCount > 0 ? ` 等 ${match.remainingRiskCount + 1} 项风险` : ''}`;
  return {
    scores,
    risk,
    size,
    scoreH: p.paragraphHeight(scores, inner - 78, { size, mono: true, lineHeight: 16 }),
    methodH: p.paragraphHeight(match.methodText, inner - 106, { size, align: 'right', lineHeight: 16 }),
    riskH: p.paragraphHeight(risk, inner, { size, lineHeight: 17 }),
  };
}

function detailsHeight(metrics, compact) {
  return 12
    + Math.max(compact ? 22 : 25, metrics.scoreH) + 8
    + Math.max(compact ? 20 : 23, metrics.methodH) + 7
    + metrics.riskH + 14;
}

function drawDetails(app, match, cursor, metrics, compact) {
  const p = app.painter;
  const x = app.padding;
  const w = app.width - app.padding * 2;
  const inner = w - 28;
  p.line(x + 14, cursor, x + w - 14, cursor, '#315d37');
  cursor += 12;
  p.text('比分参考', x + 14, cursor, { size: metrics.size, color: c.muted });
  p.paragraph(metrics.scores, x + 92, cursor, inner - 78, { size: metrics.size, color: c.text, mono: true, lineHeight: 16 });
  cursor += Math.max(compact ? 22 : 25, metrics.scoreH) + 8;
  p.text(`完整度 ${match.completenessText}`, x + 14, cursor, { size: metrics.size, weight: 'bold', color: c.risk });
  p.paragraph(match.methodText, x + 120, cursor, inner - 106, { size: metrics.size, color: c.risk, align: 'right', lineHeight: 16 });
  cursor += Math.max(compact ? 20 : 23, metrics.methodH) + 7;
  p.paragraph(metrics.risk, x + 14, cursor, inner, { size: metrics.size, color: '#d8b978', lineHeight: 17 });
}

function matchCard(app, match, y, compact) {
  const p = app.painter;
  const x = app.padding;
  const w = app.width - app.padding * 2;
  const inner = w - 28;
  const header = headerMetrics(app, match);
  const main = mainMetrics(app, match, compact);
  const details = detailMetrics(app, match, compact);
  const mainTop = y + header.height;
  const barY = mainTop + main.height + (compact ? 8 : 12);
  const detailsY = barY + (compact ? 36 : 42);
  const height = detailsY - y + detailsHeight(details, compact);
  card(app, y, height, compact ? 14 : 20);
  p.rect(x, y, 3, height, compact ? '#527a4c' : c.accent);

  matchTopline(app, match, y, header);
  matchMain(app, match, mainTop, main, compact);
  probabilityBar(app, match, x + 14, barY, inner);
  const probabilities = [
    ['主胜', match.homePercent],
    ['平局', match.drawPercent],
    ['客胜', match.awayPercent],
  ];
  probabilities.forEach(([label, value], index) => {
    const active = match.tendency.startsWith(label);
    p.text(`${label} ${value}`, x + 14 + (index + 0.5) * inner / 3, barY + 15, {
      size: compact ? 9 : 10,
      weight: active ? 'bold' : 'normal',
      color: active ? c.accent : c.secondary,
      align: 'center',
    });
  });
  drawDetails(app, match, detailsY, details, compact);
  return y + height + (compact ? 10 : 12);
}

module.exports = function renderDaily(app, y) {
  const { painter: p, width, dailyReport } = app;
  const x = app.padding;
  const w = width - app.padding * 2;
  y = button(app, '←  返回概览', x, y, 126, 40, { type: 'page', page: 'overview' }, { small: true }) + 16;
  y = section(app, y, 'MATCHDAY PROBABILITY REPORT', '每日推断', `${dailyReport.reportDate} · ${dailyReport.matchCount} 场 / ${dailyReport.predictionCount} 条策略 · 由 Python 每日任务打包`);
  y = notice(app, y, `${dailyReport.disclaimer} 数据完整度较低时仅作弱参考。`);

  if (app.matches.length) {
    app.matches.forEach((match, index) => {
      y = matchCard(app, match, y, index > 0);
    });
  } else {
    card(app, y, 96, 16);
    p.paragraph('暂无可展示的每日推断，请先运行每日任务。', x + 18, y + 28, w - 36, { size: 14, color: c.secondary, align: 'center', lineHeight: 23 });
    y += 108;
  }
  y = app.write(`数据生成于 ${dailyReport.generatedAt}\n本页面为打包时的报告快照，不实时连接数据库。`, x, y + 8, w, { size: 11, color: c.muted, align: 'center', lineHeight: 19 });
  return y + 12;
};
