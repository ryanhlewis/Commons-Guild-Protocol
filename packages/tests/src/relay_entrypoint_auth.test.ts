import { it, expect } from 'vitest';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { generatePrivateKey, getPublicKey } from '@cgp/core';
import { WebSocketPubSubHub } from '@cgp/relay/src/pubsub_ws';
it.each(['single', 'sharded', 'redundant'])('relay entrypoint authenticates %s pubsub using dedicated environment token', async (mode) => {
    const token = randomBytes(24).toString('hex'), root = await fs.mkdtemp(path.join(os.tmpdir(), 'cgp-cli-auth-'));
    const hubs = [new WebSocketPubSubHub(0, { host: '127.0.0.1', authToken: token }), new WebSocketPubSubHub(0, { host: '127.0.0.1', authToken: token })];
    for (const hub of hubs)
        while (!(hub as any).wss.address())
            await new Promise(r => setTimeout(r, 10));
    const urls = hubs.map(h => `ws://127.0.0.1:${(h as any).wss.address().port}`), keys = [generatePrivateKey(), generatePrivateKey(), generatePrivateKey()];
    const child = spawn(process.execPath, ['--import', 'tsx', 'packages/relay/src/index.ts'], { cwd: process.cwd(), windowsHide: true, stdio: 'ignore', env: { ...process.env, CGP_RELAY_HOST: '127.0.0.1', CGP_RELAY_PORT: '0', CGP_RELAY_DB: path.join(root, 'db'), CGP_RELAY_DEFAULT_PLUGINS: '0', CGP_RELAY_PRIVATE_KEY_HEX: Buffer.from(keys[0]).toString('hex'), CGP_RELAY_WRITE_QUORUM_CONFIG: JSON.stringify({ epoch: 'cli-auth', members: keys.map(getPublicKey), requiredVotes: 2 }), CGP_RELAY_PUBSUB_URL: mode === 'single' ? urls[0] : '', CGP_RELAY_PUBSUB_URLS: mode === 'single' ? '' : urls.join(','), CGP_RELAY_PUBSUB_MODE: mode, CGP_PUBSUB_TOKEN: 'deliberately-wrong-legacy-token', CGP_RELAY_PUBSUB_AUTH_TOKEN: token } });
    try {
        let ready = false;
        for (let i = 0; i < 200; i++) {
            const sizes = hubs.map(h => (h as any).subscriptions.size);
            ready = mode === 'redundant' ? sizes.every(n => n > 0) : sizes.some(n => n > 0);
            if (ready)
                break;
            await new Promise(r => setTimeout(r, 25));
        }
        expect(ready).toBe(true);
    }
    finally {
        const exited = new Promise<void>(r => child.once('exit', () => r()));
        child.kill();
        await exited;
        await Promise.all(hubs.map(h => h.close()));
    }
}, 15000);
