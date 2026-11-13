const fs = require('node:fs');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const source = path.join(root, 'minigame');
const output = path.join(root, 'dist', 'weapp');
const sharedFiles = [
  ['src/data/project.js', 'data/project.js'],
  ['generated/daily-report.js', 'data/daily-report.js'],
  ['src/utils/report.js', 'utils/report.js'],
  ['src/utils/clipboard-core.js', 'utils/clipboard-core.js'],
];

function build() {
  if (!fs.existsSync(path.join(source, 'game.js')) || !fs.existsSync(path.join(source, 'game.json'))) {
    throw new Error('缺少 web/minigame/game.js 或 game.json');
  }
  fs.rmSync(output, { recursive: true, force: true });
  fs.mkdirSync(output, { recursive: true });
  fs.cpSync(source, output, { recursive: true });
  for (const [from, to] of sharedFiles) {
    const target = path.join(output, to);
    fs.mkdirSync(path.dirname(target), { recursive: true });
    fs.copyFileSync(path.join(root, from), target);
  }
  const gameConfig = JSON.parse(fs.readFileSync(path.join(output, 'game.json'), 'utf8'));
  if (!['portrait', 'landscape', 'landscapeLeft', 'landscapeRight'].includes(gameConfig.deviceOrientation)) {
    throw new Error('game.json 必须配置有效的 deviceOrientation');
  }
  if (fs.statSync(path.join(output, 'game.js')).size === 0) {
    throw new Error('game.js 不能为空');
  }
  console.log(`MINIGAME_BUILD_OK ${path.relative(root, output)} game.js game.json shared-data`);
}

build();

if (process.argv.includes('--watch')) {
  const watched = [
    source,
    path.join(root, 'src', 'data'),
    path.join(root, 'generated'),
    path.join(root, 'src', 'utils'),
  ];
  let timer = null;
  const rebuild = () => {
    clearTimeout(timer);
    timer = setTimeout(() => {
      try {
        build();
      } catch (error) {
        console.error(`MINIGAME_BUILD_FAILED ${error.message}`);
      }
    }, 120);
  };
  for (const target of watched) fs.watch(target, { recursive: true }, rebuild);
  console.log('MINIGAME_WATCHING 修改源码或共享数据后自动重建');
}
