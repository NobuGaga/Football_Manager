export default defineAppConfig({
  pages: [
    'pages/overview/index',
    'pages/daily/index',
    'pages/pipeline/index',
    'pages/model/index',
    'pages/automation/index',
    'pages/structure/index',
  ],
  sitemapLocation: 'sitemap.json',
  window: {
    navigationBarBackgroundColor: '#0b100d',
    navigationBarTextStyle: 'white',
    navigationBarTitleText: '足球经理的推断',
    backgroundColor: '#0b100d',
    backgroundTextStyle: 'dark',
  },
  tabBar: {
    color: '#829080',
    selectedColor: '#c8f55a',
    backgroundColor: '#101712',
    borderStyle: 'black',
    list: [
      { pagePath: 'pages/overview/index', text: '概览' },
      { pagePath: 'pages/pipeline/index', text: '流水线' },
      { pagePath: 'pages/model/index', text: '模型' },
      { pagePath: 'pages/automation/index', text: '自动化' },
      { pagePath: 'pages/structure/index', text: '结构' },
    ],
  },
});
