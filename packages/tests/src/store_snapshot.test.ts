import { it, expect } from 'vitest';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { Level } from 'level';
import { LevelStore } from '@cgp/relay/src/store_level';
import { backupRelayStore, restoreRelayStore } from '@cgp/relay/src/store_snapshot';
const hash = (s: string) => createHash('sha256').update(s).digest('hex');
it('offline full-store restore preserves consensus fences, authority, indexes and unknown future records', async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'cgp-store-snapshot-')), source = path.join(root, 'source'), target = path.join(root, 'restored'), file = path.join(root, 'snapshot.json');
    const db = new Level<string, string>(source);
    await db.put('future:opaque', 'preserve exactly');
    await db.close();
    const store = new LevelStore(source);
    await store.putWriteVoteFence('epoch:g:head', 'proposal-a');
    await store.putSequencerState('epoch:g', { term: 7, votedFor: 'relay-a' } as any);
    await store.putDeviceAuthorityPin('account', { generation: 3, authorityPublicKey: 'authority', revocationEpoch: 9 } as any);
    await store.close();
    const backup = await backupRelayStore(source, file);
    expect(backup.records).toBe(4);
    await restoreRelayStore(file, target, backup.sha256, source);
    const recovered = new LevelStore(target);
    expect(await recovered.getWriteVoteFence('epoch:g:head')).toBe('proposal-a');
    expect(await recovered.getSequencerState('epoch:g')).toEqual({ term: 7, votedFor: 'relay-a' });
    expect(await recovered.getDeviceAuthorityPin('account')).toEqual({ generation: 3, authorityPublicKey: 'authority', revocationEpoch: 9 });
    await recovered.close();
    const read = new Level<string, string>(target);
    expect(await read.get('future:opaque')).toBe('preserve exactly');
    await read.close();
    await expect(restoreRelayStore(file, target, backup.sha256, source)).rejects.toThrow();
    await expect(restoreRelayStore(file, source, backup.sha256, source)).rejects.toThrow('distinct');
});
it.each(['digest', 'source', 'truncated', 'duplicate'])('rejects %s before creating restore target', async (kind) => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'cgp-store-negative-')), source = path.join(root, 'source'), target = path.join(root, 'target'), file = path.join(root, 'snapshot.json');
    const db = new Level<string, string>(source);
    await db.put('key', 'value');
    await db.close();
    const backup = await backupRelayStore(source, file);
    let sha = backup.sha256;
    if (kind === 'digest')
        sha = '0'.repeat(64);
    if (kind === 'truncated')
        await fs.writeFile(file, (await fs.readFile(file, 'utf8')).slice(0, -1));
    if (kind === 'duplicate') {
        const snapshot = JSON.parse(await fs.readFile(file, 'utf8'));
        snapshot.entries.push(snapshot.entries[0]);
        snapshot.records++;
        const text = JSON.stringify(snapshot);
        await fs.writeFile(file, text);
        sha = hash(text);
    }
    await expect(restoreRelayStore(file, target, sha, kind === 'source' ? source + 'wrong' : source)).rejects.toThrow();
    await expect(fs.access(target)).rejects.toThrow();
});
it('refuses a live store backup and incomplete target startup', async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'cgp-store-lock-')), source = path.join(root, 'source');
    const db = new Level<string, string>(source);
    await db.put('key', 'value');
    await expect(backupRelayStore(source, path.join(root, 'snapshot.json'))).rejects.toThrow();
    await db.close();
    await fs.writeFile(path.join(source, 'RESTORE-INCOMPLETE'), 'marker');
    expect(() => new LevelStore(source)).toThrow('incomplete');
});
it('restored full snapshot retains the actual no-double-vote fence after quorum loss', async () => {
    const { RelayWriteQuorumCoordinator } = await import('@cgp/relay/src/write_quorum');
    const { generatePrivateKey, getPublicKey } = await import('@cgp/core');
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'cgp-fence-restore-')), source = path.join(root, 'source'), target = path.join(root, 'restored'), file = path.join(root, 'snapshot.json');
    const keys = Array.from({ length: 3 }, () => generatePrivateKey()), identity = { privateKey: keys[0], publicKey: getPublicKey(keys[0]) }, config = { epoch: 'restore-fence', members: keys.map(getPublicKey), requiredVotes: 2, voteTimeoutMs: 100 };
    const transport = { publish: () => { }, subscribe: () => () => { } };
    const store = new LevelStore(source);
    const proposal: any = { guildId: 'g', headSeq: -1, headHash: null, body: { type: 'GUILD_CREATE', guildId: 'g', name: 'first' }, author: 'synthetic', signature: 'synthetic', createdAt: Date.now() };
    const first = new RelayWriteQuorumCoordinator(config, identity, store, transport);
    await expect(first.authorize(proposal)).rejects.toThrow('1/2 votes');
    await first.close();
    await store.close();
    const backup = await backupRelayStore(source, file);
    await restoreRelayStore(file, target, backup.sha256, source);
    const recovered = new LevelStore(target), next = new RelayWriteQuorumCoordinator(config, identity, recovered, transport);
    await expect(next.authorize({ ...proposal, body: { ...proposal.body, name: 'competing' } })).rejects.toThrow('already voted');
    await next.close();
    await recovered.close();
});
it('refuses binary keyspace instead of silently corrupting future records', async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'cgp-binary-snapshot-')), source = path.join(root, 'source');
    const db = new Level<Buffer, Buffer>(source, { keyEncoding: 'buffer', valueEncoding: 'buffer' });
    await db.put(Buffer.from([255]), Buffer.from([254]));
    await db.close();
    await expect(backupRelayStore(source, path.join(root, 'snapshot.json'))).rejects.toThrow();
});
