const { stats, principles } = require('../../data/project');
const { copyText } = require('../../utils/clipboard');

Page({
  data: {
    stats,
    principles,
    matrix: [],
  },

  onLoad() {
    const matrix = Array.from({ length: 49 }, (_, index) => {
      const row = Math.floor(index / 7);
      const column = index % 7;
      const opacity = Math.max(0.12, 0.9 - Math.abs(row - 2) * 0.12 - Math.abs(column - 2) * 0.12);
      return { opacity, hot: index === 24 };
    });
    this.setData({ matrix });
  },

  goDaily() {
    wx.navigateTo({ url: '/pages/daily/daily' });
  },

  goPipeline() {
    wx.switchTab({ url: '/pages/pipeline/pipeline' });
  },

  copyDailyCommand() {
    copyText('bash scripts/daily_automation.sh', '每日命令已复制');
  },
});
