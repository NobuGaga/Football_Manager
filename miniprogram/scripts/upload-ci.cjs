const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const ci = require('miniprogram-ci');
const { dailyReport } = require('../../web/src/data/daily-report.js');

const FRONTEND_ROOT = path.resolve(__dirname, '../../web');

function parseRobot(value) {
  const robot = Number(value || '1');
  if (!Number.isInteger(robot) || robot < 1 || robot > 30) {
    throw new Error('WECHAT_ROBOT 必须是 1 到 30 之间的整数。');
  }
  return robot;
}

function defaultVersion(now = new Date()) {
  return `1.0.${now.toISOString().replace(/\D/g, '').slice(2, 14)}`;
}

function validateCompiledProject(frontendRoot = FRONTEND_ROOT) {
  const outputRoot = path.join(frontendRoot, 'dist', 'weapp');
  const requiredFiles = ['game.json', 'game.js', 'src/app.js', 'data/daily-report.js'];
  for (const file of requiredFiles) {
    if (!fs.existsSync(path.join(outputRoot, file))) {
      throw new Error(`缺少微信小游戏编译产物 ${file}，请先运行 npm --prefix web run build:weapp`);
    }
  }
  if (fs.statSync(path.join(outputRoot, 'game.js')).size === 0) {
    throw new Error('微信小游戏入口 game.js 不能为空');
  }
  const gameConfig = JSON.parse(fs.readFileSync(path.join(outputRoot, 'game.json'), 'utf8'));
  if (!gameConfig.deviceOrientation) {
    throw new Error('game.json 缺少 deviceOrientation');
  }
  const projectConfigPath = path.join(frontendRoot, 'project.config.json');
  if (!fs.existsSync(projectConfigPath)) {
    throw new Error('缺少 web/project.config.json，无法创建微信 CI 项目');
  }
  const projectConfig = JSON.parse(fs.readFileSync(projectConfigPath, 'utf8'));
  if (projectConfig.compileType !== 'game') {
    throw new Error('web/project.config.json 的 compileType 必须为 game');
  }
  return frontendRoot;
}

async function main() {
  const appid = process.env.WECHAT_APPID;
  const privateKey = process.env.WECHAT_PRIVATE_KEY;
  const version = process.env.WECHAT_VERSION || defaultVersion();
  const robot = parseRobot(process.env.WECHAT_ROBOT);

  if (!appid || !appid.startsWith('wx')) {
    throw new Error('缺少有效的 WECHAT_APPID，请在 Kimi Claw 运行环境中配置。');
  }
  if (!privateKey) {
    throw new Error('缺少 WECHAT_PRIVATE_KEY，请在 Kimi Claw 运行环境中配置代码上传私钥。');
  }
  if (!/^\d+\.\d+\.\d+$/.test(version)) {
    throw new Error('WECHAT_VERSION 必须使用 x.y.z 格式，例如 1.0.1。');
  }

  const projectPath = validateCompiledProject();
  const privateKeyPath = path.join(os.tmpdir(), `wechat-upload-${process.pid}.key`);
  fs.writeFileSync(privateKeyPath, privateKey.replace(/\\n/g, '\n'), { mode: 0o600 });

  try {
    const project = new ci.Project({
      appid,
      type: 'miniGame',
      projectPath,
      privateKeyPath,
      ignores: ['node_modules/**/*', 'src/**/*', 'config/**/*', 'dist/h5/**/*', 'scripts/**/*'],
    });
    const desc = process.env.WECHAT_UPLOAD_DESC || `Kimi Claw 每日更新 ${dailyReport.reportDate}`;

    await ci.upload({
      project,
      version,
      desc,
      robot,
      setting: {
        es6: true,
        enhance: true,
        minify: true,
      },
      onProgressUpdate: () => {},
    });
    console.log(`Canvas 微信小游戏开发版本上传完成：${version}（CI 机器人 ${robot}）`);
    console.log(`每日报告：${dailyReport.reportDate}，共 ${dailyReport.matchCount} 场`);
    console.log('若该机器人尚未绑定体验版，请在微信公众平台完成一次性设置；后续同机器人上传会自动更新体验版。');
  } finally {
    fs.rmSync(privateKeyPath, { force: true });
  }
}

if (require.main === module) {
  main().catch((error) => {
    console.error(`微信小游戏上传失败：${error.message}`);
    process.exitCode = 1;
  });
}

module.exports = { FRONTEND_ROOT, defaultVersion, parseRobot, validateCompiledProject };
