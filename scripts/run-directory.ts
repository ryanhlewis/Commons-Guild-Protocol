/** Production directory entrypoint. It never generates an operator identity. */
import fs from 'node:fs/promises';
import path from 'node:path';
import { timingSafeEqual } from 'node:crypto';
import express from 'express';
import { DirectoryService } from '@cgp/directory';
import { getPublicKey } from '@cgp/core';

export interface DirectoryRuntimeConfig {
    dbPath: string;
    operatorKeyFile: string;
    metricsTokenFile?: string;
    host?: string;
    port?: number;
    registrationsPerMinute?: number;
    globalRegistrationsPerMinute?: number;
    maxPendingRegistrations?: number;
    maxTrackedAddresses?: number;
    maxBodyBytes?: number;
}

function bounded(value: number | undefined, fallback: number, maximum: number, name: string) {
    const selected = value ?? fallback;
    if (!Number.isSafeInteger(selected) || selected < 1 || selected > maximum) throw new Error(`Invalid ${name}`);
    return selected;
}

export async function startDirectory(config: DirectoryRuntimeConfig) {
    if (!config.dbPath || !config.operatorKeyFile) throw new Error('dbPath and operatorKeyFile are required; operator identities must survive restart.');
    const rawKey = (await fs.readFile(config.operatorKeyFile, 'utf8')).trim();
    if (!/^[a-f0-9]{64}$/i.test(rawKey)) throw new Error('Operator key file must contain one 32-byte hexadecimal private key.');
    getPublicKey(Buffer.from(rawKey, 'hex')); // Validate the scalar before opening storage.
    const metricsToken = config.metricsTokenFile ? (await fs.readFile(config.metricsTokenFile, 'utf8')).trim() : '';
    const host = config.host ?? '127.0.0.1';
    if (!['127.0.0.1', '::1', 'localhost'].includes(host) && !metricsToken) throw new Error('A metrics token file is required for a non-loopback listener.');
    if (metricsToken && (metricsToken.length < 24 || metricsToken.length > 512)) throw new Error('Metrics token must contain 24–512 characters.');
    const perAddress = bounded(config.registrationsPerMinute, 30, 10000, 'registrationsPerMinute');
    const globalLimit = bounded(config.globalRegistrationsPerMinute, 1200, 100000, 'globalRegistrationsPerMinute');
    const maxPending = bounded(config.maxPendingRegistrations, 32, 1024, 'maxPendingRegistrations');
    const maxAddresses = bounded(config.maxTrackedAddresses, 10000, 100000, 'maxTrackedAddresses');
    const maxBody = bounded(config.maxBodyBytes, 32 * 1024, 256 * 1024, 'maxBodyBytes');
    if (!Number.isSafeInteger(config.port ?? 3000) || (config.port ?? 3000) < 0 || (config.port ?? 3000) > 65535) throw new Error('Invalid port');
    const service = new DirectoryService(path.resolve(config.dbPath), { operatorPrivateKey: Buffer.from(rawKey, 'hex') });
    try { await service.getSnapshot(); } // Open/rebuild before accepting traffic.
    catch (error) { await service.close(); throw error; }
    const app = express();
    app.disable('x-powered-by');
    app.set('trust proxy', false); // Arbitrary forwarded IP headers never bypass limits.
    app.use((_req, res, next) => { res.setHeader('Cache-Control', 'no-store'); next(); });
    let accepted = 0, rejected = 0, pending = 0;
    let windowStart = Date.now(), windowTotal = 0;
    const addresses = new Map<string, number>();
    const route = (handler: (req: express.Request, res: express.Response) => Promise<unknown>) =>
        (req: express.Request, res: express.Response, next: express.NextFunction) => { void handler(req, res).catch(next); };
    const ready = async (_req: express.Request, res: express.Response) => {
        const snapshot = await service.getSnapshot();
        res.json({ status: 'ready', operatorPubkey: snapshot.operatorPubkey, entries: snapshot.size });
    };
    app.get('/healthz', (_req, res) => res.json({ status: 'healthy' }));
    app.get('/readyz', route(ready));
    app.get('/metrics', route(async (req, res) => {
        if (metricsToken) {
            const supplied = Buffer.from(req.get('authorization') ?? '');
            const expected = Buffer.from(`Bearer ${metricsToken}`);
            if (supplied.length !== expected.length || !timingSafeEqual(supplied, expected)) return res.status(401).end();
        }
        const snapshot = await service.getSnapshot();
        res.type('text/plain').send([
            '# TYPE cgp_directory_entries gauge', `cgp_directory_entries ${snapshot.size}`,
            '# TYPE cgp_directory_registrations_total counter',
            `cgp_directory_registrations_total{result="accepted"} ${accepted}`,
            `cgp_directory_registrations_total{result="rejected"} ${rejected}`,
            '# TYPE cgp_directory_pending_registrations gauge', `cgp_directory_pending_registrations ${pending}`, ''
        ].join('\n'));
    }));
    app.post('/register', (req, res, next) => {
        if (Date.now() - windowStart >= 60000) { windowStart = Date.now(); windowTotal = 0; addresses.clear(); }
        const address = req.socket.remoteAddress ?? 'unknown';
        const count = addresses.get(address) ?? 0;
        if (count >= perAddress || windowTotal >= globalLimit || pending >= maxPending || (!addresses.has(address) && addresses.size >= maxAddresses)) {
            rejected++; res.setHeader('Retry-After', Math.max(1, Math.ceil((windowStart + 60000 - Date.now()) / 1000)));
            return res.status(429).json({ error: 'Registration rate limit exceeded' });
        }
        addresses.set(address, count + 1); windowTotal++; pending++;
        // Count slow/parsing requests too; release exactly once on disconnect or response.
        let released = false;
        const release = () => { if (!released) { released = true; pending--; } };
        res.once('close', release); res.once('finish', release); next();
    }, express.json({ limit: maxBody, strict: true }), route(async (req, res) => {
        try {
            const { handle, guildId, guildPubkey, signature, timestamp, relays, deviceAuthorization } = req.body ?? {};
            await service.register(handle, guildId, guildPubkey, signature, timestamp, relays, deviceAuthorization);
            accepted++; res.json({ success: true });
        } catch {
            rejected++; res.status(400).json({ error: 'Invalid directory registration' });
        }
    }));
    app.get('/lookup', route(async (req, res) => {
        if (typeof req.query.handle !== 'string' || req.query.handle.length > 64) return res.status(400).json({ error: 'Invalid handle' });
        const lookup = await service.getLookupProof(req.query.handle);
        if (!lookup) return res.status(404).json({ error: 'Not found' });
        res.json({ ...lookup, root: lookup.snapshot.root });
    }));
    app.get('/root', route(async (_req, res) => {
        const snapshot = await service.getSnapshot(); res.json({ root: snapshot.root, snapshot });
    }));
    app.use((error: { type?: string }, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
        const status = error?.type === 'entity.too.large' ? 413 : error?.type === 'entity.parse.failed' ? 400 : 503;
        res.status(status).json({ error: status === 503 ? 'Directory unavailable' : 'Invalid request body' });
    });
    const server = app.listen(config.port ?? 3000, host);
    try { await new Promise<void>((resolve, reject) => { server.once('listening', resolve); server.once('error', reject); }); }
    catch (error) { await service.close(); throw error; }
    server.requestTimeout = 10000; server.headersTimeout = 10000; server.keepAliveTimeout = 5000; server.maxConnections = 1000;
    let closed = false;
    return { server, service, async close() {
        if (closed) return; closed = true;
        server.closeIdleConnections();
        await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
        await service.close();
    } };
}

async function main() {
    const configPath = process.argv[2];
    if (!configPath) throw new Error('Usage: tsx scripts/run-directory.ts <private-runtime-config.json>');
    const config = JSON.parse(await fs.readFile(configPath, 'utf8')) as DirectoryRuntimeConfig;
    const runtime = await startDirectory(config);
    console.log(JSON.stringify({ service: 'cgp-directory', address: runtime.server.address(), operatorPubkey: runtime.service.operatorPubkey }));
    for (const signal of ['SIGINT', 'SIGTERM'] as const) process.once(signal, () => { void runtime.close().then(() => process.exit(0)); });
}

if (typeof require !== 'undefined' && require.main === module) void main().catch(error => {
    console.error(error instanceof Error ? error.message : 'Directory startup failed'); process.exitCode = 1;
});
