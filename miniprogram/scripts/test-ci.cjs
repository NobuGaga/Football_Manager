const assert = require('node:assert/strict');
const { defaultVersion, parseRobot } = require('./upload-ci.cjs');

assert.equal(parseRobot(), 1);
assert.equal(parseRobot(''), 1);
assert.equal(parseRobot('1'), 1);
assert.equal(parseRobot('30'), 30);

for (const value of ['0', '31', '-1', '1.5', 'abc']) {
  assert.throws(() => parseRobot(value), /WECHAT_ROBOT/);
}

assert.equal(defaultVersion(new Date('2026-11-08T12:34:56Z')), '1.0.261108123456');
assert.match(defaultVersion(), /^\d+\.\d+\.\d+$/);

console.log('CI_CONFIG_OK default_robot robot_range local_version');
