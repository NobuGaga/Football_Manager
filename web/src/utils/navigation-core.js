const PAGE_PATHS = {
  daily: '/pages/daily/index',
  pipeline: '/pages/pipeline/index',
};

function createNavigator(platform) {
  return {
    goDaily: () => platform.navigateTo({ url: PAGE_PATHS.daily }),
    goPipeline: () => platform.switchTab({ url: PAGE_PATHS.pipeline }),
  };
}

module.exports = { createNavigator, PAGE_PATHS };
