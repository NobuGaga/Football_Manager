const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const ci = require('miniprogram-ci');
const { validateCompiledProject } = require('./upload-ci.cjs');

async function main() {
  const appid = process.env.WECHAT_APPID;
  const privateKey = process.env.WECHAT_PRIVATE_KEY;
  if (!appid || !appid.startsWith('wx')) {
    throw new Error('缺少有效的 WECHAT_APPID，请在 Kimi Claw 运行环境中配置。');
  }
  if (!privateKey) {
    throw new Error('缺少 WECHAT_PRIVATE_KEY，请在 Kimi Claw 运行环境中配置代码上传私钥。');
  }

  const projectPath = validateCompiledProject();
  const privateKeyPath = path.join(os.tmpdir(), `wechat-preview-${process.pid}.key`);
  const qrCodeOutput = process.env.WECHAT_PREVIEW_OUTPUT || path.join(os.tmpdir(), 'football-manager-wechat-preview.jpg');
  fs.writeFileSync(privateKeyPath, privateKey.replace(/\\n/g, '\n'), { mode: 0o600 });
  fs.mkdirSync(path.dirname(qrCodeOutput), { recursive: true });

  try {
    const project = new ci.Project({
      appid,
      type: 'miniGame',
      projectPath,
      privateKeyPath,
      ignores: ['node_modules/**/*', 'src/**/*', 'config/**/*', 'dist/h5/**/*', 'scripts/**/*'],
    });
    const desc = process.env.WECHAT_UPLOAD_DESC || 'Kimi Claw 自用预览';

    await ci.preview({
      project,
      desc,
      setting: {
        es6: true,
        enhance: true,
        minify: true,
      },
      qrcodeFormat: 'image',
      qrcodeOutputDest: qrCodeOutput,
      onProgressUpdate: () => {},
    });

    if (!fs.existsSync(qrCodeOutput) || fs.statSync(qrCodeOutput).size === 0) {
      throw new Error('微信 CI 未生成有效的预览二维码文件。');
    }
    console.log(`Canvas 微信小游戏预览二维码已生成：${qrCodeOutput}`);
  } finally {
    fs.rmSync(privateKeyPath, { force: true });
  }
}

main().catch((error) => {
  console.error(`微信小游戏预览生成失败：${error.message}`);
  process.exitCode = 1;
});
