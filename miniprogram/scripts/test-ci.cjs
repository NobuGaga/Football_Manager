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
  assert.throws(() => validateCompiledProject(fixture), /Taro 微信小程序编译产物/);
  const output = path.join(fixture, 'dist', 'weapp');
  fs.mkdirSync(path.join(output, 'pages', 'daily'), { recursive: true });
  const pages = ['overview', 'daily', 'pipeline', 'model', 'automation', 'structure'].map((page) => `pages/${page}/index`);
  fs.writeFileSync(path.join(output, 'app.json'), JSON.stringify({ pages }));
  fs.writeFileSync(path.join(output, 'app.js'), '');
  fs.writeFileSync(path.join(output, 'pages', 'daily', 'index.wxml'), '');
  assert.throws(() => validateCompiledProject(fixture), /project.config.json/);
  fs.writeFileSync(path.join(fixture, 'project.config.json'), '{}');
  assert.equal(validateCompiledProject(fixture), fixture);
} finally {
  fs.rmSync(fixture, { recursive: true, force: true });
}

console.log('CI_CONFIG_OK default_robot robot_range local_version taro_output_validation');
