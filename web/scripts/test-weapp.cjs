const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { execFileSync } = require('node:child_process');

const root = path.resolve(__dirname, '..');
const output = path.join(root, 'dist', 'weapp');
const pages = ['overview', 'daily', 'pipeline', 'model', 'automation', 'structure'];

execFileSync(process.execPath, [
  path.join(root, 'node_modules', '@tarojs', 'cli', 'bin', 'taro'),
  'build',
  '--type',
  'weapp',
], { cwd: root, stdio: 'inherit' });

for (const file of ['app.js', 'app.json', 'app.wxss', 'base.wxml', 'comp.wxml', 'sitemap.json']) {
  assert.ok(fs.existsSync(path.join(output, file)) && fs.statSync(path.join(output, file)).size > 0, `缺少小程序产物 ${file}`);
}
for (const page of pages) {
  for (const extension of ['js', 'json', 'wxml']) {
    const file = path.join(output, 'pages', page, `index.${extension}`);
    assert.ok(fs.existsSync(file) && fs.statSync(file).size > 0, `页面 ${page} 缺少 ${extension}`);
  }
}
const appConfig = JSON.parse(fs.readFileSync(path.join(output, 'app.json'), 'utf8'));
assert.deepEqual(appConfig.pages, pages.map((page) => `pages/${page}/index`));
assert.equal(appConfig.sitemapLocation, 'sitemap.json');
assert.equal(appConfig.tabBar.list.length, 5);
assert.ok(!fs.existsSync(path.join(output, 'game.js')), '小程序产物不应包含 game.js');
assert.ok(!fs.existsSync(path.join(output, 'game.json')), '小程序产物不应包含 game.json');

const projectConfig = JSON.parse(fs.readFileSync(path.join(root, 'project.config.json'), 'utf8'));
assert.equal(projectConfig.compileType, 'miniprogram');
assert.equal(projectConfig.miniprogramRoot, './dist/weapp/');
const packageJson = require('../package.json');
assert.ok(packageJson.scripts['build:weapp'].includes('taro build --type weapp'));
assert.ok(packageJson.scripts['dev:weapp'].includes('--watch'));
assert.ok(!packageJson.scripts['build:minigame']);
assert.ok(!packageJson.scripts['dev:minigame']);

console.log('WEAPP_BUILD_OK app_json six_pages tabbar wxml wxss sitemap');
