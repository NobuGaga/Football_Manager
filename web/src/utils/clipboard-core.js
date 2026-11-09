function writeClipboard(platform, text) {
  return new Promise((resolve) => {
    let settled = false;
    const settle = (success) => {
      if (!settled) {
        settled = true;
        resolve(success);
      }
    };

    try {
      const request = platform.setClipboardData({
        data: text,
        options: { hideDefaultMessage: true },
        success: () => settle(true),
        fail: () => settle(false),
      });
      if (request && typeof request.then === 'function') {
        request.then((result) => {
          if (result && typeof result.errMsg === 'string') {
            settle(result.errMsg.endsWith(':ok'));
          }
        }).catch(() => settle(false));
      }
    } catch {
      settle(false);
    }
  });
}

module.exports = { writeClipboard };
