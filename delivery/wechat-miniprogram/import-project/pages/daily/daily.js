const { dailyReport } = require('../../data/daily-report.js');

function percent(value) {
  return `${(value * 100).toFixed(1)}%`;
}

Page({
  data: {
    reportDate: dailyReport.reportDate,
    generatedAt: dailyReport.generatedAt,
    matchCount: dailyReport.matchCount,
    disclaimer: dailyReport.disclaimer,
    matches: [],
  },

  onLoad() {
    const matches = dailyReport.matches.map((match) => {
      const directions = [
        { label: '主胜', value: match.probabilities.home_win },
        { label: '平局', value: match.probabilities.draw },
        { label: '客胜', value: match.probabilities.away_win },
      ];
      const highest = directions.reduce((current, item) => item.value > current.value ? item : current);
      return {
        ...match,
        startTimeText: match.startTimeLocal ? match.startTimeLocal.replace('T', ' ').slice(0, 16) : '待确认',
        homePercent: percent(match.probabilities.home_win),
        drawPercent: percent(match.probabilities.draw),
        awayPercent: percent(match.probabilities.away_win),
        tendency: `${highest.label} ${percent(highest.value)}`,
        completenessText: `${Math.round(match.dataCompleteness * 100)}%`,
        methodText: match.method === 'odds_only_fallback' ? '赔率降级估计 · 弱参考' : match.method || '模型推断',
        topScores: match.topScores.map((item) => ({ ...item, probabilityText: percent(item.probability) })),
        primaryRisk: match.riskFlags[0] || '无附加风险',
        remainingRiskCount: Math.max(0, match.riskFlags.length - 1),
      };
    });
    this.setData({ matches });
  },
});
