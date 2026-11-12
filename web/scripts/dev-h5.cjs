const { spawn } = require('node:child_process');
const { parseArgs } = require('node:util');

function parseDevArgs(argv) {
  const { values } = parseArgs({
    args: argv,
    options: {
      host: { type: 'string', default: '0.0.0.0' },
      port: { type: 'string', default: '10086' },
    },
    strict: true,
  });
  const port = Number(values.port);
  if (!values.host || !Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error('dev:h5 需要有效的 --host 和 1–65535 范围内的 --port');
  }
  return { host: values.host, port: String(port) };
}

function main() {
  const { host, port } = parseDevArgs(process.argv.slice(2));
  const child = spawn(
    process.execPath,
    [require.resolve('@tarojs/cli/bin/taro'), 'build', '--type', 'h5', '--watch', '--port', port],
    {
      env: { ...process.env, TARO_DEV_SERVER_HOST: host },
      stdio: 'inherit',
    },
  );
  const forward = (signal) => child.kill(signal);
  process.on('SIGINT', () => forward('SIGINT'));
  process.on('SIGTERM', () => forward('SIGTERM'));
  child.on('error', (error) => {
    console.error(error);
    process.exitCode = 1;
  });
  child.on('exit', (code, signal) => {
    process.exitCode = code ?? (signal ? 1 : 0);
  });
}

if (require.main === module) main();

module.exports = { parseDevArgs };
