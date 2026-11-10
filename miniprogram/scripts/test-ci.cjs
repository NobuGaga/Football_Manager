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
  assert.throws(() => validateCompiledProject(fixture), /微信小游戏编译产物/);
  const output = path.join(fixture, 'dist', 'weapp');
  fs.mkdirSync(path.join(output, 'src'), { recursive: true });
  fs.mkdirSync(path.join(output, 'data'), { recursive: true });
  fs.writeFileSync(path.join(output, 'game.json'), JSON.stringify({ deviceOrientation: 'portrait' }));
  fs.writeFileSync(path.join(output, 'game.js'), "require('./src/app.js');\n");
  fs.writeFileSync(path.join(output, 'src', 'app.js'), 'module.exports = {};\n');
  fs.writeFileSync(path.join(output, 'data', 'daily-report.js'), 'exports.dailyReport = {};\n');
  assert.throws(() => validateCompiledProject(fixture), /project.config.json/);
  fs.writeFileSync(path.join(fixture, 'project.config.json'), JSON.stringify({ compileType: 'miniprogram' }));
  assert.throws(() => validateCompiledProject(fixture), /compileType/);
  fs.writeFileSync(path.join(fixture, 'project.config.json'), JSON.stringify({ compileType: 'game' }));
  assert.equal(validateCompiledProject(fixture), fixture);
} finally {
  fs.rmSync(fixture, { recursive: true, force: true });
}

console.log('MINIGAME_CI_CONFIG_OK default_robot robot_range local_version game_output_validation');
