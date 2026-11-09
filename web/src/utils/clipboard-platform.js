import Taro from '@tarojs/taro';

const h5Clipboard = {
  setClipboardData({ data, success, fail }) {
    if (!navigator.clipboard || typeof navigator.clipboard.writeText !== 'function') {
      return Promise.reject(new Error('当前浏览器不支持异步剪贴板'));
    }
    return navigator.clipboard.writeText(data).then(success).catch(fail);
  },
};

export const clipboardPlatform = process.env.TARO_ENV === 'h5' ? h5Clipboard : Taro;
