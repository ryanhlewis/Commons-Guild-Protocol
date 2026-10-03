// Temporary Windows fixture bundle; installs no service or startup task.
const path = require('node:path');
const fs = require('node:fs');
const esbuild = require('../node_modules/tsx/node_modules/esbuild');
const directory = path.resolve('output/community-continuity/remote-package');
if (!fs.existsSync(path.join(directory, 'dependencies.zip'))) throw Error('Build the existing remote continuity dependency package first');
esbuild.buildSync({ entryPoints: ['scripts/consensus-network-worker.ts'], bundle: true, platform: 'node', format: 'cjs',
  external: ['classic-level', '@fails-components/webtransport', '@fails-components/webtransport-transport-http3-quiche'],
  outfile: path.join(directory, 'consensus-worker.cjs') });
console.log('Built bounded consensus network worker');
