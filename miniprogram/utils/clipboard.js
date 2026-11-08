function copyText(data, title = '已复制') {
  wx.setClipboardData({
    data,
    success: () => wx.showToast({ title, icon: 'none' }),
    fail: () => wx.showToast({ title: '复制失败，请稍后重试', icon: 'none' }),
  });
}

module.exports = { copyText };
