const { colors: c, card, button, probabilityBar } = require('./stadium-theme.js');

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
  p.roundRect(x + 14, top, 72, 25, 12, '#244e29');
  p.text(match.matchCode, x + 50, top + 6, { size: 10, weight: 'bold', color: c.accent, align: 'center' });
  p.paragraph(match.league, x + 96, top + 5, metrics.leagueW, { size: 9, color: c.secondary, lineHeight: 14 });
  p.text(match.startTimeText, x + w - 14, top + 6, { size: 9, color: c.muted, align: 'right', mono: true });
  p.paragraph(`${match.aiModel} / ${match.strategyCategory}`, x + 14, top + Math.max(25, metrics.leagueH) + 5, inner, { size: 8, color: c.muted, lineHeight: 12 });
}

function compactMainMetrics(app, match) {
  const p = app.painter;
  const inner = app.width - app.padding * 2 - 28;
  const teamW = inner - 126;
  const homeH = p.paragraphHeight(match.homeTeam, teamW, { size: 13, weight: 'bold', lineHeight: 19 });
  const awayH = p.paragraphHeight(match.awayTeam, teamW, { size: 13, weight: 'bold', lineHeight: 19 });
  return { teamW, homeH, awayH, height: Math.max(homeH + awayH + 4, 50) };
}

function compactMain(app, match, y, metrics) {
  const p = app.painter;
  const x = app.padding;
  const w = app.width - app.padding * 2;
  p.paragraph(match.homeTeam, x + 14, y, metrics.teamW, { size: 13, weight: 'bold', color: c.text, lineHeight: 19 });
  p.paragraph(match.awayTeam, x + 14, y + metrics.homeH + 4, metrics.teamW, { size: 13, weight: 'bold', color: c.text, lineHeight: 19 });
  p.text(`最高 · ${match.tendencyLabel}`, x + w - 14, y + 3, { size: 9, weight: 'bold', color: c.accent, align: 'right' });
  p.text(match.tendencyPercent, x + w - 14, y + 22, { size: 23, weight: 'bold', color: c.accent, align: 'right', mono: true });
}

function scoreboardMetrics(app, match) {
  const p = app.painter;
  const inner = app.width - app.padding * 2 - 28;
  const teamW = (inner - 92) / 2;
  const homeH = p.paragraphHeight(match.homeTeam, teamW, { size: 13, weight: 'bold', lineHeight: 20 });
  const awayH = p.paragraphHeight(match.awayTeam, teamW, { size: 13, weight: 'bold', lineHeight: 20 });
  const teamH = Math.max(homeH, awayH);
  return { teamW, teamH, height: 50 + teamH + 52 };
}

function scoreboard(app, match, y, metrics) {
  const p = app.painter;
  const x = app.padding;
  const w = app.width - app.padding * 2;
  const leftCenter = x + 14 + metrics.teamW / 2;
  const rightCenter = x + w - 14 - metrics.teamW / 2;
  const center = x + w / 2;
  for (const [badgeX, name, nameX] of [
    [leftCenter, match.homeTeam, x + 14],
    [rightCenter, match.awayTeam, x + w - 14 - metrics.teamW],
  ]) {
    p.circle(badgeX, y + 21, 21, c.cardAlt, c.accent);
    p.text(String(name || '待').slice(0, 1), badgeX, y + 10, { size: 17, weight: 'bold', color: c.accent, align: 'center' });
    p.paragraph(name, nameX, y + 50, metrics.teamW, { size: 13, weight: 'bold', color: c.text, align: 'center', lineHeight: 20 });
  }
  p.text('VS', center, y + 3, { size: 14, weight: 'bold', color: '#769773', align: 'center', mono: true });
  p.text('最高倾向', center, y + 27, { size: 8, color: c.muted, align: 'center' });
  const pillW = 54;
  p.roundRect(center - pillW / 2, y + 46, pillW, 25, 13, c.accent);
  p.text(match.tendencyLabel, center, y + 52, { size: 10, weight: 'bold', color: c.accentText, align: 'center' });

  const probabilityY = y + 50 + metrics.teamH + 12;
  const others = [
    ['主胜', '主', match.homePercent],
    ['平局', '平', match.drawPercent],
    ['客胜', '客', match.awayPercent],
  ].filter(([label]) => label !== match.tendencyLabel);
  p.text(`${match.tendencyLabel}  ${match.tendencyPercent}`, x + 14, probabilityY, { size: 21, weight: 'bold', color: c.accent });
  p.text(others.map(([, label, value]) => `${label} ${value}`).join(' / '), x + w - 14, probabilityY + 9, { size: 9, color: c.secondary, align: 'right', mono: true });
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
  if (!compact) {
    const trackW = 62;
    p.roundRect(x + 14, cursor + 20, trackW, 4, 2, '#365337');
    p.roundRect(x + 14, cursor + 20, trackW * match.dataCompleteness, 4, 2, c.risk);
  }
  p.paragraph(match.methodText, x + 120, cursor, inner - 106, { size: metrics.size, color: c.risk, align: 'right', lineHeight: 16 });
  cursor += Math.max(compact ? 20 : 23, metrics.methodH) + 7;
  p.paragraph(`!  ${metrics.risk}`, x + 14, cursor, inner, { size: metrics.size, color: '#d8b978', lineHeight: 17 });
}

function matchCard(app, match, y, compact) {
  const p = app.painter;
  const x = app.padding;
  const w = app.width - app.padding * 2;
  const inner = w - 28;
  const header = headerMetrics(app, match);
  const main = compact ? compactMainMetrics(app, match) : scoreboardMetrics(app, match);
  const details = detailMetrics(app, match, compact);
  const mainTop = y + header.height;
  const barY = mainTop + main.height + (compact ? 8 : 2);
  const detailsY = barY + (compact ? 36 : 24);
  const height = detailsY - y + detailsHeight(details, compact);
  card(app, y, height, compact ? 18 : 20);

  matchTopline(app, match, y, header);
  if (compact) compactMain(app, match, mainTop, main);
  else scoreboard(app, match, mainTop, main);
  probabilityBar(app, match, x + 14, barY, inner);
  if (compact) {
    const probabilities = [
      ['主胜', match.homePercent],
      ['平局', match.drawPercent],
      ['客胜', match.awayPercent],
    ];
    probabilities.forEach(([label, value], index) => {
      const active = match.tendency.startsWith(label);
      p.text(`${label} ${value}`, x + 14 + (index + 0.5) * inner / 3, barY + 15, {
        size: 9,
        weight: active ? 'bold' : 'normal',
        color: active ? c.accent : c.secondary,
        align: 'center',
      });
    });
  }
  drawDetails(app, match, detailsY, details, compact);
  return y + height + (compact ? 10 : 12);
}

function matchdayNotice(app, y) {
  const p = app.painter;
  const x = app.padding;
  const w = app.width - app.padding * 2;
  const body = '低完整度仅作弱参考，不构成收益保证或确定性投注建议。';
  const bodyH = p.paragraphHeight(body, w - 28, { size: 9, lineHeight: 15 });
  const height = 38 + bodyH + 14;
  p.roundRect(x, y, w, height, 13, '#1b4628', '#4c7040');
  p.text('概率估计仅供分析', x + 14, y + 12, { size: 11, weight: 'bold', color: '#d9e7ce' });
  p.paragraph(body, x + 14, y + 36, w - 28, { size: 9, color: '#b6c9a9', lineHeight: 15 });
  return y + height + 16;
}

module.exports = function renderDaily(app, y) {
  const { painter: p, width, dailyReport } = app;
  const x = app.padding;
  const w = width - app.padding * 2;
  const headerY = y;
  button(app, '←', x, headerY, 40, 40, { type: 'page', page: 'overview' }, { small: true });
  p.text('今日比赛中心', x + 50, headerY + 5, { size: 18, weight: 'bold', color: c.text });
  const dateText = `${dailyReport.reportDate.slice(5).replace('-', ' 月 ')} 日`;
  p.roundRect(x + w - 82, headerY + 6, 82, 28, 14, '#255b35');
  p.text(dateText, x + w - 41, headerY + 13, { size: 9, color: '#d9ebcf', align: 'center' });
  y += 54;
  p.text(`${dailyReport.matchCount} 场 / ${dailyReport.predictionCount} 条策略`, x, y, { size: 12, weight: 'bold', color: c.secondary });
  y += 30;
  y = matchdayNotice(app, y);

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
