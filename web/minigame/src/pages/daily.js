const { colors: c, card, button, section, notice, probabilityBar } = require('./stadium-theme.js');

function matchTopline(app, match, y) {
  const p = app.painter;
  const x = app.padding;
  const w = app.width - app.padding * 2;
  const inner = w - 28;
  p.roundRect(x + 14, y + 12, 72, 25, 12, c.cardAlt);
  p.text(match.matchCode, x + 50, y + 18, { size: 10, weight: 'bold', color: c.accent, align: 'center' });
  p.text(match.startTimeText, x + w - 14, y + 17, { size: 9, color: c.muted, align: 'right', mono: true });
  return p.paragraph(`${match.league} · ${match.aiModel} / ${match.strategyCategory}`, x + 14, y + 44, inner, { size: 10, color: c.secondary, lineHeight: 16 });
}

function detailMetrics(app, match) {
  const p = app.painter;
  const inner = app.width - app.padding * 2 - 28;
  const scores = match.topScores.map((item) => `${item.score} · ${item.probabilityText}`).join('    ');
  const risk = `${match.primaryRisk}${match.remainingRiskCount > 0 ? ` 等 ${match.remainingRiskCount + 1} 项风险` : ''}`;
  return {
    scores,
    risk,
    scoreH: p.paragraphHeight(scores, inner - 78, { size: 10, mono: true, lineHeight: 16 }),
    methodH: p.paragraphHeight(match.methodText, inner - 106, { size: 10, align: 'right', lineHeight: 16 }),
    riskH: p.paragraphHeight(risk, inner, { size: 10, lineHeight: 17 }),
  };
}

function drawDetails(app, match, cursor, metrics) {
  const p = app.painter;
  const x = app.padding;
  const w = app.width - app.padding * 2;
  const inner = w - 28;
  p.line(x + 14, cursor, x + w - 14, cursor, '#315d37');
  cursor += 13;
  p.text('比分参考', x + 14, cursor, { size: 10, color: c.muted });
  p.paragraph(metrics.scores, x + 92, cursor, inner - 78, { size: 10, color: c.text, mono: true, lineHeight: 16 });
  cursor += Math.max(25, metrics.scoreH) + 10;
  p.text(`完整度 ${match.completenessText}`, x + 14, cursor, { size: 10, weight: 'bold', color: c.risk });
  p.paragraph(match.methodText, x + 120, cursor, inner - 106, { size: 10, color: c.risk, align: 'right', lineHeight: 16 });
  cursor += Math.max(23, metrics.methodH) + 9;
  p.paragraph(metrics.risk, x + 14, cursor, inner, { size: 10, color: '#d8b978', lineHeight: 17 });
  return cursor + metrics.riskH + 15;
}

function featuredMatch(app, match, y) {
  const p = app.painter;
  const x = app.padding;
  const w = app.width - app.padding * 2;
  const inner = w - 28;
  const teamW = (inner - 92) / 2;
  const homeH = p.paragraphHeight(match.homeTeam, teamW, { size: 13, weight: 'bold', lineHeight: 20 });
  const awayH = p.paragraphHeight(match.awayTeam, teamW, { size: 13, weight: 'bold', lineHeight: 20 });
  const teamH = Math.max(homeH, awayH);
  const metrics = detailMetrics(app, match);
  const leagueH = p.paragraphHeight(`${match.league} · ${match.aiModel} / ${match.strategyCategory}`, inner, { size: 10, lineHeight: 16 });
  const teamTop = y + 44 + leagueH + 16;
  const afterTeams = teamTop + 50 + teamH + 16;
  let height = afterTeams - y + 48;
  height += Math.max(25, metrics.scoreH) + 10;
  height += Math.max(23, metrics.methodH) + 9;
  height += metrics.riskH + 28;
  card(app, y, height, 20);

  matchTopline(app, match, y);
  const leftCenter = x + 14 + teamW / 2;
  const rightCenter = x + w - 14 - teamW / 2;
  const center = x + w / 2;
  for (const [badgeX, name, nameX] of [
    [leftCenter, match.homeTeam, x + 14],
    [rightCenter, match.awayTeam, x + w - 14 - teamW],
  ]) {
    p.circle(badgeX, teamTop + 21, 21, c.cardAlt, c.accent);
    p.text(String(name || '待').slice(0, 1), badgeX, teamTop + 10, { size: 17, weight: 'bold', color: c.accent, align: 'center' });
    p.paragraph(name, nameX, teamTop + 50, teamW, { size: 13, weight: 'bold', color: c.text, align: 'center', lineHeight: 20 });
  }
  p.text('VS', center, teamTop + 3, { size: 14, weight: 'bold', color: '#769773', align: 'center', mono: true });
  p.text('最高倾向', center, teamTop + 27, { size: 8, color: c.muted, align: 'center' });
  p.paragraph(match.tendency, center - 45, teamTop + 47, 90, { size: 10, weight: 'bold', color: c.accent, align: 'center', lineHeight: 16 });

  probabilityBar(app, match, x + 14, afterTeams, inner);
  const probabilities = [
    ['主胜', match.homePercent],
    ['平局', match.drawPercent],
    ['客胜', match.awayPercent],
  ];
  probabilities.forEach(([label, value], index) => {
    const active = match.tendency.startsWith(label);
    p.text(`${label} ${value}`, x + 14 + (index + 0.5) * inner / 3, afterTeams + 18, {
      size: 10,
      weight: active ? 'bold' : 'normal',
      color: active ? c.accent : c.secondary,
      align: 'center',
    });
  });
  drawDetails(app, match, afterTeams + 48, metrics);
  return y + height + 12;
}

function compactMatch(app, match, y) {
  const p = app.painter;
  const x = app.padding;
  const w = app.width - app.padding * 2;
  const inner = w - 28;
  const teamW = inner - 128;
  const homeH = p.paragraphHeight(`主 ${match.homeTeam}`, teamW, { size: 14, weight: 'bold', lineHeight: 21 });
  const awayH = p.paragraphHeight(`客 ${match.awayTeam}`, teamW, { size: 14, weight: 'bold', lineHeight: 21 });
  const tendencyH = p.paragraphHeight(match.tendency, 112, { size: 17, weight: 'bold', align: 'right', lineHeight: 23 });
  const metrics = detailMetrics(app, match);
  const leagueH = p.paragraphHeight(`${match.league} · ${match.aiModel} / ${match.strategyCategory}`, inner, { size: 10, lineHeight: 16 });
  const mainTop = y + 44 + leagueH + 12;
  const mainH = Math.max(homeH + awayH + 4, tendencyH + 20, 48);
  const barY = mainTop + mainH + 8;
  let height = barY - y + 42;
  height += Math.max(25, metrics.scoreH) + 10;
  height += Math.max(23, metrics.methodH) + 9;
  height += metrics.riskH + 28;
  card(app, y, height, 14);
  p.rect(x, y + 20, 3, Math.max(0, height - 40), c.accent);

  matchTopline(app, match, y);
  p.paragraph(`主 ${match.homeTeam}`, x + 14, mainTop, teamW, { size: 14, weight: 'bold', color: c.text, lineHeight: 21 });
  p.paragraph(`客 ${match.awayTeam}`, x + 14, mainTop + homeH + 4, teamW, { size: 14, weight: 'bold', color: c.text, lineHeight: 21 });
  p.text('最高倾向', x + w - 14, mainTop + 1, { size: 8, color: c.muted, align: 'right' });
  p.paragraph(match.tendency, x + w - 126, mainTop + 20, 112, { size: 17, weight: 'bold', color: c.accent, align: 'right', lineHeight: 23 });
  probabilityBar(app, match, x + 14, barY, inner);
  const probabilities = [
    ['主胜', match.homePercent],
    ['平局', match.drawPercent],
    ['客胜', match.awayPercent],
  ];
  probabilities.forEach(([label, value], index) => {
    const active = match.tendency.startsWith(label);
    p.text(`${label} ${value}`, x + 14 + (index + 0.5) * inner / 3, barY + 16, {
      size: 9,
      weight: active ? 'bold' : 'normal',
      color: active ? c.accent : c.secondary,
      align: 'center',
    });
  });
  drawDetails(app, match, barY + 42, metrics);
  return y + height + 12;
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
      y = index === 0 ? featuredMatch(app, match, y) : compactMatch(app, match, y);
    });
  } else {
    card(app, y, 96, 16);
    p.paragraph('暂无可展示的每日推断，请先运行每日任务。', x + 18, y + 28, w - 36, { size: 14, color: c.secondary, align: 'center', lineHeight: 23 });
    y += 108;
  }
  y = app.write(`数据生成于 ${dailyReport.generatedAt}\n本页面为打包时的报告快照，不实时连接数据库。`, x, y + 8, w, { size: 11, color: c.muted, align: 'center', lineHeight: 19 });
  return y + 12;
};
