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
  fs.mkdirSync(path.join(output, 'src', 'pages'), { recursive: true });
  fs.mkdirSync(path.join(output, 'data'), { recursive: true });
  fs.mkdirSync(path.join(output, 'utils'), { recursive: true });
  fs.writeFileSync(path.join(output, 'game.json'), JSON.stringify({ deviceOrientation: 'portrait' }));
  fs.writeFileSync(path.join(output, 'game.js'), "require('./src/app.js');\n");
  for (const file of ['app.js', 'draw.js']) {
    fs.writeFileSync(path.join(output, 'src', file), 'module.exports = {};\n');
  }
  for (const page of ['overview', 'daily', 'pipeline', 'model', 'automation', 'structure']) {
    fs.writeFileSync(path.join(output, 'src', 'pages', `${page}.js`), 'module.exports = {};\n');
  }
  for (const file of ['daily-report.js', 'project.js']) {
    fs.writeFileSync(path.join(output, 'data', file), 'module.exports = {};\n');
  }
  for (const file of ['report.js', 'clipboard-core.js']) {
    fs.writeFileSync(path.join(output, 'utils', file), 'module.exports = {};\n');
  }
  assert.throws(() => validateCompiledProject(fixture), /project.config.json/);
  fs.writeFileSync(path.join(fixture, 'project.config.json'), JSON.stringify({
    compileType: 'miniprogram',
    miniprogramRoot: './dist/weapp/',
  }));
  assert.throws(() => validateCompiledProject(fixture), /compileType/);
  fs.writeFileSync(path.join(fixture, 'project.config.json'), JSON.stringify({
    compileType: 'game',
    miniprogramRoot: './dist/weapp/',
  }));
  assert.equal(validateCompiledProject(fixture), fixture);
  fs.writeFileSync(path.join(output, 'app.json'), '{}\n');
  assert.throws(() => validateCompiledProject(fixture), /混入了微信小程序文件/);
  fs.rmSync(path.join(output, 'app.json'));
  assert.equal(validateCompiledProject(fixture), fixture);
} finally {
  fs.rmSync(fixture, { recursive: true, force: true });
}

console.log('MINIGAME_CI_CONFIG_OK default_robot robot_range local_version game_output_validation no_weapp_mix');
