function percent(value) {
  return `${(value * 100).toFixed(1)}%`;
}

function createMatrix() {
  return Array.from({ length: 49 }, (_, index) => {
    const row = Math.floor(index / 7);
    const column = index % 7;
    const opacity = Math.max(0.12, 0.9 - Math.abs(row - 2) * 0.12 - Math.abs(column - 2) * 0.12);
    return { id: index, opacity, hot: index === 24 };
  });
}

function decorateMatches(report) {
  return report.matches.map((match) => {
    const directions = [
      { label: '主胜', value: match.probabilities.home_win },
      { label: '平局', value: match.probabilities.draw },
      { label: '客胜', value: match.probabilities.away_win },
    ];
    const highest = directions.reduce((current, item) => item.value > current.value ? item : current);
    const riskFlags = match.riskFlags || [];
    return {
      ...match,
      strategyKey: `${match.fixtureKey}-${match.aiModel}-${match.strategyId}`,
      startTimeText: match.startTimeLocal ? match.startTimeLocal.replace('T', ' ').slice(0, 16) : '待确认',
      homePercent: percent(match.probabilities.home_win),
      drawPercent: percent(match.probabilities.draw),
      awayPercent: percent(match.probabilities.away_win),
      tendency: `${highest.label} ${percent(highest.value)}`,
      completenessText: `${Math.round(match.dataCompleteness * 100)}%`,
      methodText: match.method === 'odds_only_fallback' ? '赔率降级估计 · 弱参考' : match.method || '模型推断',
      topScores: match.topScores.map((item) => ({ ...item, probabilityText: percent(item.probability) })),
      primaryRisk: riskFlags[0] || '无附加风险',
      remainingRiskCount: Math.max(riskFlags.length - 1, 0),
    };
  });
}

function boundedIndex(index, length) {
  return Math.max(0, Math.min(Number(index), length - 1));
}

module.exports = { boundedIndex, createMatrix, decorateMatches, percent };
