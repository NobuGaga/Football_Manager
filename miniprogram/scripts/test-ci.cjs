const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { defaultVersion, parseRobot, validateCompiledProject } = require('./upload-ci.cjs');

assert.equal(parseRobot(), 1);
assert.equal(parseRobot(''), 1);
assert.equal(parseRobot('1'), 1);
assert.equal(parseRobot('30'), 30);

for (const value of ['0', '31', '-1', '1.5', 'abc']) {
  assert.throws(() => parseRobot(value), /WECHAT_ROBOT/);
}

assert.equal(defaultVersion(new Date('2026-11-08T12:34:56Z')), '1.0.261108123456');
assert.match(defaultVersion(), /^\d+\.\d+\.\d+$/);

const fixture = fs.mkdtempSync(path.join(os.tmpdir(), 'football-manager-ci-'));
try {
  assert.throws(() => validateCompiledProject(fixture), /微信小程序编译产物/);
  const output = path.join(fixture, 'dist', 'weapp');
  fs.mkdirSync(path.join(output, 'pages', 'overview'), { recursive: true });
  fs.mkdirSync(path.join(output, 'pages', 'daily'), { recursive: true });
  const pages = [
    'pages/overview/index',
    'pages/daily/index',
    'pages/pipeline/index',
    'pages/model/index',
    'pages/automation/index',
    'pages/structure/index',
  ];
  fs.writeFileSync(path.join(output, 'app.json'), JSON.stringify({
    pages,
    tabBar: { list: pages.slice(0, 5).map((pagePath) => ({ pagePath })) },
    sitemapLocation: 'sitemap.json',
  }));
  fs.writeFileSync(path.join(output, 'app.js'), 'App({});\n');
  fs.writeFileSync(path.join(output, 'app.wxss'), 'page {}\n');
  fs.writeFileSync(path.join(output, 'sitemap.json'), '{}\n');
  for (const page of ['overview', 'daily']) {
    fs.writeFileSync(path.join(output, 'pages', page, 'index.js'), 'Page({});\n');
    fs.writeFileSync(path.join(output, 'pages', page, 'index.wxml'), '<view />\n');
  }
  assert.throws(() => validateCompiledProject(fixture), /project.config.json/);
  fs.writeFileSync(path.join(fixture, 'project.config.json'), JSON.stringify({ compileType: 'game' }));
  assert.throws(() => validateCompiledProject(fixture), /compileType/);
  fs.writeFileSync(path.join(fixture, 'project.config.json'), JSON.stringify({ compileType: 'miniprogram' }));
  assert.equal(validateCompiledProject(fixture), fixture);
} finally {
  fs.rmSync(fixture, { recursive: true, force: true });
}

console.log('MINIPROGRAM_CI_CONFIG_OK default_robot robot_range local_version weapp_output_validation');
