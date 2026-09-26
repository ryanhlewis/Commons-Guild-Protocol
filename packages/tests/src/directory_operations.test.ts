import { afterEach, describe, expect, it } from 'vitest';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { generatePrivateKey, getPublicKey, hashObject, sign, directoryRegistrationPayload } from '@cgp/core';
import { verifyDirectoryLookupProof } from '@cgp/directory';
import { startDirectory } from '../../../scripts/run-directory';

const roots: string[] = [];
const running: Awaited<ReturnType<typeof startDirectory>>[] = [];
afterEach(async () => { for (const service of running.splice(0)) await service.close(); for (const root of roots.splice(0)) await fs.rm(root, { recursive: true, force: true }); });
async function fixture() {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'cgp-directory-ops-')); roots.push(root);
    const keyFile = path.join(root, 'operator-key'); const metricsFile = path.join(root, 'metrics-token');
    await fs.writeFile(keyFile, Buffer.from(generatePrivateKey()).toString('hex'), { mode: 0o600 });
    await fs.writeFile(metricsFile, 'disposable-metrics-token-with-enough-entropy', { mode: 0o600 });
    const config = { dbPath: path.join(root, 'db'), operatorKeyFile: keyFile, metricsTokenFile: metricsFile, port: 0 };
    const start = async (overrides = {}) => {
        const runtime = await startDirectory({ ...config, ...overrides }); running.push(runtime);
        return { runtime, url: `http://127.0.0.1:${(runtime.server.address() as { port: number }).port}` };
    };
    return { root, config, start };
}
async function registration(handle = 'ops/fixture') {
    const key = generatePrivateKey(); const pub = getPublicKey(key); const timestamp = Date.now();
    const relays = ['wss://relay.example/'];
    return { handle, guildId: 'fixture-guild', guildPubkey: pub, timestamp, relays,
        signature: await sign(key, hashObject(directoryRegistrationPayload(handle, 'fixture-guild', pub, timestamp, relays))) };
}
describe('operational directory server', () => {
    it('preserves the operator identity and signed directory data across restart', async () => {
        const { start } = await fixture(); const first = await start();
        const input = await registration();
        expect((await fetch(`${first.url}/register`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(input) })).status).toBe(200);
        const operator = first.runtime.service.operatorPubkey;
        await first.runtime.close();
        const second = await start();
        expect(second.runtime.service.operatorPubkey).toBe(operator);
        const lookup = await (await fetch(`${second.url}/lookup?handle=${input.handle}`)).json();
        expect(verifyDirectoryLookupProof(lookup, { expectedHandle: input.handle, trustedOperatorPubkeys: [operator], maxSnapshotAgeMs: 60000 })).toBe(true);
        expect((await fetch(`${second.url}/readyz`)).status).toBe(200);
        expect((await fetch(`${second.url}/healthz`)).status).toBe(200);
        expect((await fetch(`${second.url}/metrics`)).status).toBe(401);
        const metrics = await fetch(`${second.url}/metrics`, { headers: { authorization: 'Bearer disposable-metrics-token-with-enough-entropy' } });
        expect(await metrics.text()).toContain('cgp_directory_entries 1');
    });
    it('bounds registration bodies and ignores spoofed forwarded addresses for rate limits', async () => {
        const { start } = await fixture(); const { url } = await start({ registrationsPerMinute: 2, maxBodyBytes: 128 });
        expect((await fetch(`${url}/register`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ body: 'x'.repeat(256) }) })).status).toBe(413);
        expect((await fetch(`${url}/register`, { method: 'POST', headers: { 'content-type': 'application/json', 'x-forwarded-for': '192.0.2.1' }, body: '{}' })).status).toBe(400);
        const limited = await fetch(`${url}/register`, { method: 'POST', headers: { 'content-type': 'application/json', 'x-forwarded-for': '192.0.2.2' }, body: '{}' });
        expect(limited.status).toBe(429); expect(limited.headers.get('retry-after')).toBeTruthy();
    });
    it('fails closed on missing operator identity and exposed unauthenticated metrics', async () => {
        const { config } = await fixture();
        await expect(startDirectory({ ...config, operatorKeyFile: '' })).rejects.toThrow('operatorKeyFile');
        await expect(startDirectory({ ...config, host: '0.0.0.0', metricsTokenFile: undefined })).rejects.toThrow('metrics token');
        await fs.writeFile(config.operatorKeyFile, '0'.repeat(64));
        await expect(startDirectory(config)).rejects.toThrow();
    });
});
