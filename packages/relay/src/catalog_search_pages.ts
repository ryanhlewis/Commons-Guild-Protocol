/** Bounded, query-scoped search snapshots. A cursor keeps the same ordering
 * even when publications arrive between pages; repeated searches reuse scoring.
 */
export function createCatalogSearchPages<T>(rank: (query: string) => T[]) {
  let serial = 0;
  const snapshots = new Map<string, { query: string; generation: number; expires: number; rows: T[] }>();
  const latest = new Map<string, string>();
  return (query: string, limit: number, cursor: string | null, generation: number) => {
    const now = Date.now();
    for (const [id, snapshot] of snapshots) if (snapshot.expires <= now) snapshots.delete(id);
    let id: string, offset = 0;
    if (cursor) {
      if (cursor.length > 100 || !/^[A-Za-z0-9_-]+$/.test(cursor)) return { ok: false as const, status: 400, error: 'Invalid search cursor.' };
      const decoded = Buffer.from(cursor, 'base64url').toString('utf8');
      const match = /^([a-z0-9-]+):(\d+)$/.exec(decoded);
      if (!match || !Number.isSafeInteger(Number(match[2]))) return { ok: false as const, status: 400, error: 'Invalid search cursor.' };
      id = match[1]; offset = Number(match[2]);
      const snapshot = snapshots.get(id);
      if (!snapshot) return { ok: false as const, status: 409, error: 'Search snapshot expired. Restart this search.' };
      if (snapshot.query !== query || offset > snapshot.rows.length) return { ok: false as const, status: 400, error: 'Invalid search cursor.' };
    } else {
      id = latest.get(query) ?? '';
      if (!snapshots.has(id) || snapshots.get(id)!.generation !== generation) {
        id = `${now.toString(36)}-${(++serial).toString(36)}`;
        snapshots.set(id, { query, generation, expires: now + 300_000, rows: rank(query) });
        latest.set(query, id);
        while (snapshots.size > 128) snapshots.delete(snapshots.keys().next().value!);
        while (latest.size > 128) latest.delete(latest.keys().next().value!);
      }
    }
    const rows = snapshots.get(id)!.rows;
    const selected = rows.slice(offset, offset + limit), next = offset + selected.length;
    return { ok: true as const, total: rows.length, selected, page: { limit, count: selected.length, hasMore: next < rows.length,
      nextCursor: next < rows.length ? Buffer.from(`${id}:${next}`).toString('base64url') : null } };
  };
}
