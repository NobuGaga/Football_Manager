const { MiniGameApp } = require('./src/app.js');

const profile = wx.getStorageSync('FM_RENDER_PROFILE') === 1;
const app = new MiniGameApp(wx, null, { profile });
app.start();
