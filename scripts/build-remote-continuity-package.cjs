// Windows fixture packaging only. Installs nothing and copies no application data.
const fs = require('node:fs');
const path = require('node:path');
const { createRequire, isBuiltin } = require('node:module');
const { execFileSync } = require('node:child_process');
const esbuild = require('../node_modules/tsx/node_modules/esbuild');
if (process.platform !== 'win32') throw new Error('The ECHO fixture package requires Windows native dependencies');
const directory = path.resolve('output/community-continuity/remote-package');
fs.mkdirSync(directory, { recursive: true });
esbuild.buildSync({ entryPoints: ['scripts/remote-continuity-worker.ts'], bundle: true, platform: 'node', format: 'cjs',
  external: ['classic-level', '@fails-components/webtransport', '@fails-components/webtransport-transport-http3-quiche'],
  outfile: path.join(directory, 'worker.cjs') });
esbuild.buildSync({ entryPoints: ['scripts/public-quorum-worker.ts'], bundle: true, platform: 'node', format: 'cjs',
  external: ['classic-level', '@fails-components/webtransport', '@fails-components/webtransport-transport-http3-quiche'],
  outfile: path.join(directory, 'public-worker.cjs') });
const versions = new Map();
function copy(name, from) {
  if (isBuiltin(name)) return;
  const resolver = createRequire(path.join(from, 'package.json'));
  let source = path.dirname(resolver.resolve(name));
  while (!fs.existsSync(path.join(source, 'package.json')) || JSON.parse(fs.readFileSync(path.join(source, 'package.json'))).name !== name) {
    const parent = path.dirname(source);
    if (parent === source) throw new Error(`Cannot locate package ${name}`);
    source = parent;
  }
  const pkg = JSON.parse(fs.readFileSync(path.join(source, 'package.json')));
  if (versions.has(name)) {
    if (versions.get(name) !== pkg.version) throw new Error(`Dependency version conflict for ${name}`);
    return;
  }
  versions.set(name, pkg.version);
  fs.cpSync(source, path.join(directory, 'node_modules', name), { recursive: true });
  for (const dependency of Object.keys(pkg.dependencies || {})) copy(dependency, source);
}
copy('classic-level', process.cwd());
const quote = value => `'${value.replaceAll("'", "''")}'`;
execFileSync('powershell', ['-NoProfile', '-NonInteractive', '-Command',
  `Compress-Archive -LiteralPath ${quote(path.join(directory, 'node_modules'))} -DestinationPath ${quote(path.join(directory, 'dependencies.zip'))} -Force -ErrorAction Stop`], { windowsHide: true });
fs.writeFileSync(path.join(directory, 'dependencies.json'), JSON.stringify(Object.fromEntries(versions), null, 2));
console.log(`Built task-owned ECHO fixture package in ${directory}`);
