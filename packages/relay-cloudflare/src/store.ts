import type { ChannelId, GuildEvent, GuildId } from "@cgp/core";

export interface HistoryQuery {
  guildId: GuildId;
  channelId?: ChannelId;
  beforeSeq?: number;
  afterSeq?: number;
  limit?: number;
  includeStructural?: boolean;
}

export interface LogRangeQuery {
  guildId: GuildId;
  afterSeq?: number;
  limit?: number;
}

export interface WorkerRelayStore {
  getLog(guildId: GuildId): Promise<GuildEvent[]>;
  getLastEvent(guildId: GuildId): Promise<GuildEvent | undefined>;
  getHistory(query: HistoryQuery): Promise<GuildEvent[]>;
  getLogRange(query: LogRangeQuery): Promise<GuildEvent[]>;
  append(guildId: GuildId, event: GuildEvent): Promise<void>;
}

const DEFAULT_HISTORY_LIMIT = 100;
const MAX_HISTORY_LIMIT = 500;
const DEFAULT_RANGE_LIMIT = 5000;
const MAX_RANGE_LIMIT = 10000;

function normalizeLimit(value: unknown, fallback: number, max: number) {
  const parsed = Math.floor(Number(value));
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return fallback;
  }
  return Math.min(max, parsed);
}

function normalizeSeq(value: unknown) {
  const parsed = Math.floor(Number(value));
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : undefined;
}

function eventChannelId(event: GuildEvent) {
  const body = event.body as unknown as Record<string, unknown>;
  return typeof body.channelId === "string" ? body.channelId : "";
}

function eventTargetChannelId(event: GuildEvent) {
  const body = event.body as unknown as Record<string, unknown>;
  const target = body.target && typeof body.target === "object"
    ? body.target as unknown as Record<string, unknown>
    : undefined;
  return typeof target?.channelId === "string" ? target.channelId : "";
}

function eventChannelIds(event: GuildEvent) {
  return [eventChannelId(event), eventTargetChannelId(event)].filter(Boolean);
}

function shouldIncludeStructuralEvent(event: GuildEvent, query: HistoryQuery) {
  if (!query.includeStructural) {
    return false;
  }
  if (event.body.type === "GUILD_CREATE") {
    return true;
  }
  if (event.body.type !== "CHANNEL_CREATE") {
    return false;
  }
  return !query.channelId || eventChannelId(event) === query.channelId;
}

function matchesHistoryQuery(event: GuildEvent, query: HistoryQuery) {
  if (query.beforeSeq !== undefined && event.seq >= query.beforeSeq) {
    return false;
  }
  if (query.afterSeq !== undefined && event.seq <= query.afterSeq) {
    return false;
  }
  if (!query.channelId) {
    return true;
  }
  if (eventChannelId(event) === query.channelId || eventTargetChannelId(event) === query.channelId) {
    return true;
  }
  return shouldIncludeStructuralEvent(event, query);
}

export function selectHistoryEvents(log: GuildEvent[], query: HistoryQuery) {
  const limit = normalizeLimit(query.limit, DEFAULT_HISTORY_LIMIT, MAX_HISTORY_LIMIT);
  const selected: GuildEvent[] = [];
  if (query.afterSeq !== undefined) {
    for (const event of log) {
      if (!matchesHistoryQuery(event, query)) {
        continue;
      }
      selected.push(event);
      if (selected.length >= limit) {
        break;
      }
    }
    return selected;
  }

  for (let index = log.length - 1; index >= 0; index -= 1) {
    const event = log[index];
    if (!event || !matchesHistoryQuery(event, query)) {
      continue;
    }
    selected.push(event);
    if (selected.length >= limit) {
      break;
    }
  }
  selected.reverse();
  return selected;
}

export class D1RelayStore implements WorkerRelayStore {
  private initPromise?: Promise<void>;

  constructor(private readonly db: D1Database) {}

  async getLog(guildId: GuildId) {
    await this.init();
    const result = await this.db.prepare(
      "SELECT event_json FROM events WHERE guild_id = ? ORDER BY seq ASC"
    ).bind(guildId).all<{ event_json: string }>();
    return result.results.map((row) => JSON.parse(row.event_json) as GuildEvent);
  }

  async getLastEvent(guildId: GuildId) {
    await this.init();
    const row = await this.db.prepare(
      "SELECT event_json FROM events WHERE guild_id = ? ORDER BY seq DESC LIMIT 1"
    ).bind(guildId).first<{ event_json: string }>();
    return row ? JSON.parse(row.event_json) as GuildEvent : undefined;
  }

  async getHistory(query: HistoryQuery) {
    await this.init();
    const limit = normalizeLimit(query.limit, DEFAULT_HISTORY_LIMIT, MAX_HISTORY_LIMIT);
    const beforeSeq = normalizeSeq(query.beforeSeq);
    const afterSeq = normalizeSeq(query.afterSeq);
    const params: unknown[] = [query.guildId];
    const where = ["guild_id = ?"];

    if (beforeSeq !== undefined) {
      where.push("seq < ?");
      params.push(beforeSeq);
    }
    if (afterSeq !== undefined) {
      where.push("seq > ?");
      params.push(afterSeq);
    }

    const canUseChannelIndex = query.channelId && !query.includeStructural;
    if (canUseChannelIndex) {
      where.push("primary_channel_id = ?");
      params.push(query.channelId);
    }

    const order = afterSeq !== undefined ? "ASC" : "DESC";
    const rowLimit = canUseChannelIndex ? limit : Math.min(5000, Math.max(limit * 8, limit));
    params.push(rowLimit);
    const result = await this.db.prepare(
      `SELECT event_json FROM events WHERE ${where.join(" AND ")} ORDER BY seq ${order} LIMIT ?`
    ).bind(...params).all<{ event_json: string }>();
    const rows = result.results.map((row) => JSON.parse(row.event_json) as GuildEvent);
    const ordered = order === "DESC" ? rows.reverse() : rows;
    return canUseChannelIndex
      ? ordered
      : selectHistoryEvents(ordered, { ...query, limit });
  }

  async getLogRange(query: LogRangeQuery) {
    await this.init();
    const limit = normalizeLimit(query.limit, DEFAULT_RANGE_LIMIT, MAX_RANGE_LIMIT);
    const afterSeq = normalizeSeq(query.afterSeq);
    const result = afterSeq === undefined
      ? await this.db.prepare(
        "SELECT event_json FROM events WHERE guild_id = ? ORDER BY seq ASC LIMIT ?"
      ).bind(query.guildId, limit).all<{ event_json: string }>()
      : await this.db.prepare(
        "SELECT event_json FROM events WHERE guild_id = ? AND seq > ? ORDER BY seq ASC LIMIT ?"
      ).bind(query.guildId, afterSeq, limit).all<{ event_json: string }>();
    return result.results.map((row) => JSON.parse(row.event_json) as GuildEvent);
  }

  async append(guildId: GuildId, event: GuildEvent) {
    await this.init();
    const channels = eventChannelIds(event);
    const primaryChannel = channels[0] || null;
    await this.db.prepare(
      "INSERT INTO events (guild_id, seq, event_id, event_json, primary_channel_id, created_at) VALUES (?, ?, ?, ?, ?, ?)"
    ).bind(
      guildId,
      event.seq,
      event.id,
      JSON.stringify(event),
      primaryChannel,
      event.createdAt
    ).run();
  }

  private init() {
    if (!this.initPromise) {
      this.initPromise = this.db.exec(`
        CREATE TABLE IF NOT EXISTS events (
          guild_id TEXT NOT NULL,
          seq INTEGER NOT NULL,
          event_id TEXT NOT NULL,
          event_json TEXT NOT NULL,
          primary_channel_id TEXT,
          created_at INTEGER,
          PRIMARY KEY (guild_id, seq)
        );
        CREATE INDEX IF NOT EXISTS events_guild_channel_seq ON events (guild_id, primary_channel_id, seq);
      `).then(() => undefined);
    }
    return this.initPromise;
  }
}

export class DurableObjectSqlRelayStore implements WorkerRelayStore {
  private initComplete = false;

  constructor(private readonly sql: SqlStorage) {}

  async getLog(guildId: GuildId) {
    this.init();
    return [...this.sql.exec<{ event_json: string }>(
      "SELECT event_json FROM events WHERE guild_id = ? ORDER BY seq ASC",
      guildId
    )].map((row) => JSON.parse(row.event_json) as GuildEvent);
  }

  async getLastEvent(guildId: GuildId) {
    this.init();
    const row = [...this.sql.exec<{ event_json: string }>(
      "SELECT event_json FROM events WHERE guild_id = ? ORDER BY seq DESC LIMIT 1",
      guildId
    )][0];
    return row ? JSON.parse(row.event_json) as GuildEvent : undefined;
  }

  async getHistory(query: HistoryQuery) {
    this.init();
    const limit = normalizeLimit(query.limit, DEFAULT_HISTORY_LIMIT, MAX_HISTORY_LIMIT);
    const beforeSeq = normalizeSeq(query.beforeSeq);
    const afterSeq = normalizeSeq(query.afterSeq);
    const params: unknown[] = [query.guildId];
    const where = ["guild_id = ?"];

    if (beforeSeq !== undefined) {
      where.push("seq < ?");
      params.push(beforeSeq);
    }
    if (afterSeq !== undefined) {
      where.push("seq > ?");
      params.push(afterSeq);
    }

    const canUseChannelIndex = query.channelId && !query.includeStructural;
    if (canUseChannelIndex) {
      where.push("primary_channel_id = ?");
      params.push(query.channelId);
    }

    const order = afterSeq !== undefined ? "ASC" : "DESC";
    const rowLimit = canUseChannelIndex ? limit : Math.min(5000, Math.max(limit * 8, limit));
    params.push(rowLimit);
    const rows = [...this.sql.exec<{ event_json: string }>(
      `SELECT event_json FROM events WHERE ${where.join(" AND ")} ORDER BY seq ${order} LIMIT ?`,
      ...params
    )].map((row) => JSON.parse(row.event_json) as GuildEvent);
    const ordered = order === "DESC" ? rows.reverse() : rows;
    return canUseChannelIndex
      ? ordered
      : selectHistoryEvents(ordered, { ...query, limit });
  }

  async getLogRange(query: LogRangeQuery) {
    this.init();
    const limit = normalizeLimit(query.limit, DEFAULT_RANGE_LIMIT, MAX_RANGE_LIMIT);
    const afterSeq = normalizeSeq(query.afterSeq);
    const rows = afterSeq === undefined
      ? [...this.sql.exec<{ event_json: string }>(
        "SELECT event_json FROM events WHERE guild_id = ? ORDER BY seq ASC LIMIT ?",
        query.guildId,
        limit
      )]
      : [...this.sql.exec<{ event_json: string }>(
        "SELECT event_json FROM events WHERE guild_id = ? AND seq > ? ORDER BY seq ASC LIMIT ?",
        query.guildId,
        afterSeq,
        limit
      )];
    return rows.map((row) => JSON.parse(row.event_json) as GuildEvent);
  }

  async append(guildId: GuildId, event: GuildEvent) {
    this.init();
    const channels = eventChannelIds(event);
    this.sql.exec(
      "INSERT INTO events (guild_id, seq, event_id, event_json, primary_channel_id, created_at) VALUES (?, ?, ?, ?, ?, ?)",
      guildId,
      event.seq,
      event.id,
      JSON.stringify(event),
      channels[0] || null,
      event.createdAt
    );
  }

  private init() {
    if (this.initComplete) {
      return;
    }
    this.sql.exec(`
      CREATE TABLE IF NOT EXISTS events (
        guild_id TEXT NOT NULL,
        seq INTEGER NOT NULL,
        event_id TEXT NOT NULL,
        event_json TEXT NOT NULL,
        primary_channel_id TEXT,
        created_at INTEGER,
        PRIMARY KEY (guild_id, seq)
      );
      CREATE INDEX IF NOT EXISTS events_guild_channel_seq ON events (guild_id, primary_channel_id, seq);
    `);
    this.initComplete = true;
  }
}
