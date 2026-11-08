const { priorities, metrics } = require('../../data/project');
const { copyText } = require('../../utils/clipboard');

Page({
  data: { priorities, metrics },

  copyTimeBoundary() {
    copyText('input_as_of <= predicted_at <= start_time_utc', '时间边界已复制');
  },
});
