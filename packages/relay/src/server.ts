import { WebSocketServer, WebSocket } from "ws";
import {
    GuildEvent,
    computeEventId,
    GuildId,
    ChannelId,
    createInitialState,
    applyEvent,
    GuildState,
    verify,
    hashObject,
    generatePrivateKey,
    getPublicKey,
    serializeState,
    sign,
    EventBody,
    Message,
    Checkpoint,
    EphemeralPolicyUpdate,
    validateEvent
} from "@cgp/core";
import { LevelStore } from "./store_level";
import { Store } from "./store";
import { RelayPlugin, RelayPluginContext } from "./plugins";

interface Subscription {
    guildId: GuildId;
    channels?: ChannelId[];
}

export class RelayServer {
    private wss: WebSocketServer;
    private store: Store;
    private plugins: RelayPlugin[];
    private pluginCtx: RelayPluginContext;
    private pluginsReady: Promise<void>;
    private subscriptions = new Map<WebSocket, Map<string, Subscription>>();
    private messageQueues = new Map<WebSocket, Promise<void>>();
    private stateCache = new Map<GuildId, GuildState>();
    private mutexes = new Map<GuildId, Promise<void>>();

    private pruneTimer: NodeJS.Timeout;
    private checkpointTimer: NodeJS.Timeout;
    private keyPair: { publicKey: string; privateKey: Uint8Array };

    constructor(port: number, dbPathOrStore: string | Store = "./relay-db", plugins: RelayPlugin[] = []) {
        if (typeof dbPathOrStore === "string") {
            this.store = new LevelStore(dbPathOrStore);
        } else {
            this.store = dbPathOrStore;
        }
        this.plugins = plugins;
        this.wss = new WebSocketServer({ port });

        // Generate a random keypair for the relay (in production, load from config)
        const privateKey = generatePrivateKey();
        this.keyPair = {
            privateKey,
            publicKey: getPublicKey(privateKey)
        };
        console.log(`Relay started with public key: ${this.keyPair.publicKey}`);

        this.pluginCtx = {
            relayPublicKey: this.keyPair.publicKey,
            store: this.store,
            publishAsRelay: async (body: EventBody, createdAt?: number) => {
                return this.publishAsRelay(body, createdAt);
            },
            getLog: async (guildId: GuildId) => {
                return await this.store.getLog(guildId);
            }
        };
        this.pluginsReady = this.initPlugins();

        this.wss.on("connection", (socket) => {
            this.subscriptions.set(socket, new Map());
            this.messageQueues.set(socket, Promise.resolve());

            socket.on("message", (data) => {
                const currentQueue = this.messageQueues.get(socket) || Promise.resolve();
                const nextTask = currentQueue.then(async () => {
                    try {
                        const raw = data.toString();
                        const [kind, payload] = JSON.parse(raw);
                        await this.handleMessage(socket, kind, payload);
                    } catch (e: any) {
                        console.error("Error handling message", e);
                        if (e instanceof SyntaxError) {
                            console.error("Invalid payload:", data.toString());
                            socket.send(JSON.stringify(["ERROR", { code: "INVALID_FRAME", message: "Parse error" }]));
                        } else {
                            socket.send(JSON.stringify(["ERROR", { code: "INTERNAL_ERROR", message: e.message }]));
                        }
                    }
                });
                this.messageQueues.set(socket, nextTask);
            });

            socket.on("close", () => {
                this.subscriptions.delete(socket);
                this.messageQueues.delete(socket);
            });
        });

        // Run prune every 60 seconds
        this.pruneTimer = setInterval(() => {
            this.prune().catch(err => console.error("Prune failed:", err));
        }, 60000);

        // Run checkpoint every 60 seconds
        this.checkpointTimer = setInterval(() => {
            this.createCheckpoints().catch(err => console.error("Checkpoint failed:", err));
        }, 60000);

        console.log(`Relay listening on port ${port}`);
    }

    public getPort(): number {
        const addr = this.wss.address();
        if (!addr || typeof addr === "string") return NaN;
        return addr.port;
    }

    private async initPlugins() {
        for (const plugin of this.plugins) {
            try {
                await plugin.onInit?.(this.pluginCtx);
            } catch (e: any) {
                console.error(`Relay plugin ${plugin.name} failed to init:`, e);
            }
        }
    }

    private async publishAsRelay(body: EventBody, createdAt = Date.now()): Promise<GuildEvent | undefined> {
        const author = this.keyPair.publicKey;
        const unsignedForSig = { body, author, createdAt };
        const signature = await sign(this.keyPair.privateKey, hashObject(unsignedForSig));
        const fullEvent = await this.appendSequencedEvent({ body, author, signature, createdAt });
        if (fullEvent) {
            await this.runOnEventAppended(fullEvent);
        }
        return fullEvent;
    }

    public async createCheckpoints() {
        const guilds = await this.store.getGuildIds();

        for (const guildId of guilds) {
            const events = await this.store.getLog(guildId);
            if (events.length === 0) continue;

            // Rebuild state
            let state: GuildState;
            try {
                state = createInitialState(events[0]);
                for (let i = 1; i < events.length; i++) {
                    state = applyEvent(state, events[i]);
                }
            } catch (e) {
                console.error(`Failed to rebuild state for guild ${guildId} during checkpoint:`, e);
                continue;
            }

            // Create checkpoint event
            const lastEvent = events[events.length - 1];
            if (lastEvent.body.type === "CHECKPOINT") {
                continue;
            }

            const serializedState = serializeState(state);
            const stateHash = hashObject(serializedState);

            const body: Checkpoint = {
                type: "CHECKPOINT",
                guildId,
                rootHash: stateHash,
                seq: lastEvent.seq + 1,
                state: serializedState
            };

            const fullEvent: GuildEvent = {
                id: "",
                seq: lastEvent.seq + 1,
                prevHash: lastEvent.id,
                createdAt: Date.now(),
                author: this.keyPair.publicKey,
                body,
                signature: ""
            };

            fullEvent.id = computeEventId(fullEvent);
            fullEvent.signature = await sign(this.keyPair.privateKey, hashObject({ body, author: fullEvent.author, createdAt: fullEvent.createdAt }));

            await this.store.append(guildId, fullEvent);
            console.log(`Relay created checkpoint for guild ${guildId} at seq ${fullEvent.seq}`);
            this.broadcast(guildId, fullEvent);
        }
    }

    public async prune() {
        const guilds = await this.store.getGuildIds();
        const now = Date.now();

        for (const guildId of guilds) {
            const events = await this.store.getLog(guildId);
            if (events.length === 0) continue;

            let state: GuildState;
            try {
                state = createInitialState(events[0]);
                for (let i = 1; i < events.length; i++) {
                    state = applyEvent(state, events[i]);
                }
            } catch (e) {
                console.error(`Failed to rebuild state for guild ${guildId} during prune:`, e);
                continue;
            }

            const seqsToDelete: number[] = [];
            for (const event of events) {
                if (event.body.type === "MESSAGE") {
                    const body = event.body as Message;
                    const channel = state.channels.get(body.channelId);
                    if (channel && channel.retention && channel.retention.mode === "ttl" && channel.retention.seconds) {
                        const ageSeconds = (now - event.createdAt) / 1000;
                        if (ageSeconds > channel.retention.seconds) {
                            seqsToDelete.push(event.seq);
                        }
                    }
                }
            }

            for (const seq of seqsToDelete) {
                await this.store.deleteEvent(guildId, seq);
            }
        }
    }

    private async handleMessage(socket: WebSocket, kind: string, payload: unknown) {
        await this.pluginsReady;

        for (const plugin of this.plugins) {
            try {
                const handled = await plugin.onFrame?.({ socket, kind, payload }, this.pluginCtx);
                if (handled) return;
            } catch (e: any) {
                console.error(`Relay plugin ${plugin.name} failed handling frame ${kind}:`, e);
                socket.send(JSON.stringify(["ERROR", { code: "PLUGIN_ERROR", message: e.message || String(e) }]));
                return;
            }
        }

        if (kind === "HELLO") {
            const pluginList = this.plugins.map(p => ({
                name: p.name,
                metadata: p.metadata,
                inputs: p.inputs
            }));
            socket.send(JSON.stringify(["HELLO_OK", {
                protocol: "cgp/0.1",
                relayName: "Reference Relay",
                plugins: pluginList
            }]));
        } else if (kind === "PLUGIN_CONFIG") {
            const p = payload as { pluginName: string; config: any };
            const plugin = this.plugins.find(pl => pl.name === p.pluginName);
            if (plugin) {
                try {
                    await plugin.onConfig?.({ socket, config: p.config }, this.pluginCtx);
                    socket.send(JSON.stringify(["PLUGIN_CONFIG_OK", { pluginName: p.pluginName }]));
                } catch (e: any) {
                    socket.send(JSON.stringify(["ERROR", { code: "PLUGIN_CONFIG_ERROR", message: `Plugin configuration failed: ${e.message || String(e)}` }]));
                }
            } else {
                socket.send(JSON.stringify(["ERROR", { code: "PLUGIN_NOT_FOUND", message: `Plugin ${p.pluginName} not found` }]));
            }
        } else if (kind === "SUB") {
            const p = payload as { subId: string; guildId: GuildId; channels?: ChannelId[] };
            const { subId, guildId, channels } = p;
            const subs = this.subscriptions.get(socket);
            if (subs) {
                subs.set(subId, { guildId, channels });
            }

            // Send snapshot
            const events = await this.store.getLog(guildId);
            socket.send(JSON.stringify(["SNAPSHOT", { subId, guildId, events, endSeq: events.length - 1 }]));

        } else if (kind === "PUBLISH") {
            const p = payload as { body: EventBody; author: string; signature: string; createdAt: number };
            const { body, author, signature, createdAt } = p;

            const fullEvent = await this.appendSequencedEvent({ body, author, signature, createdAt }, socket);
            if (fullEvent) {
                await this.runOnEventAppended(fullEvent, socket);
            }
        }
    }

    private async appendSequencedEvent(
        p: { body: EventBody; author: string; signature: string; createdAt: number },
        socket?: WebSocket
    ): Promise<GuildEvent | undefined> {
        const { body, author, signature, createdAt } = p;
        const targetGuildId = body.guildId;

        // Mutex: Serialize processing per guild to prevent race conditions on seq/prevHash
        const processEvent = async () => {
            const lastEvent = await this.store.getLastEvent(targetGuildId);

            const seq = lastEvent ? lastEvent.seq + 1 : 0;
            const prevHash = lastEvent ? lastEvent.id : null;

            const fullEvent: GuildEvent = {
                id: "",
                seq,
                prevHash,
                createdAt,
                author,
                body,
                signature
            };

            fullEvent.id = computeEventId({ ...fullEvent });

            const unsignedForSig = { body, author, createdAt };
            const msgHash = hashObject(unsignedForSig);

            if (!verify(author, msgHash, signature)) {
                console.error(`Invalid signature for event ${fullEvent.id}`);
                socket?.send(JSON.stringify(["ERROR", { code: "INVALID_SIGNATURE", message: "Signature verification failed" }]));
                return undefined;
            }

            if (seq > 0) {
                let state = this.stateCache.get(targetGuildId);

                if (!state || state.headSeq !== seq - 1) {
                    // Cache miss or out of sync, rebuild from history
                    const history = await this.store.getLog(targetGuildId);
                    if (history.length === 0) {
                        console.error("Validation error: Missing history for non-genesis event");
                        return undefined;
                    }

                    state = createInitialState(history[0]);
                    for (let i = 1; i < history.length; i++) {
                        state = applyEvent(state, history[i]);
                    }
                    this.stateCache.set(targetGuildId, state);
                }

                try {
                    validateEvent(state, fullEvent);
                    // Optimistically apply to cache
                    const newState = applyEvent(state, fullEvent);
                    this.stateCache.set(targetGuildId, newState);
                } catch (e: any) {
                    console.error(`Validation failed for guild ${targetGuildId}: ${e.message}`);
                    console.error(`Event body:`, JSON.stringify(body));
                    socket?.send(JSON.stringify(["ERROR", { code: "VALIDATION_FAILED", message: e.message }]));
                    return undefined;
                }
            } else {
                if (body.type !== "GUILD_CREATE") {
                    console.error("Validation failed: First event must be GUILD_CREATE");
                    socket?.send(JSON.stringify(["ERROR", { code: "VALIDATION_FAILED", message: "First event must be GUILD_CREATE" }]));
                    return undefined;
                }
                // Initialize cache for new guild
                const state = createInitialState(fullEvent);
                this.stateCache.set(targetGuildId, state);
            }

            await this.store.append(targetGuildId, fullEvent);
            // console.log(`Relay appended event ${fullEvent.id} type ${body.type} seq ${fullEvent.seq}`);

            this.broadcast(targetGuildId, fullEvent);
            return fullEvent;
        };

        // Execute with lock
        const prev = this.mutexes.get(targetGuildId) || Promise.resolve();
        const next = prev.catch(() => { }).then(processEvent);
        this.mutexes.set(targetGuildId, next);
        return await next;
    }

    private async runOnEventAppended(event: GuildEvent, socket?: WebSocket) {
        for (const plugin of this.plugins) {
            try {
                await plugin.onEventAppended?.({ event, socket }, this.pluginCtx);
            } catch (e: any) {
                console.error(`Relay plugin ${plugin.name} failed in onEventAppended:`, e);
            }
        }
    }


    private broadcast(guildId: GuildId, event: GuildEvent) {
        let count = 0;
        for (const [socket, subs] of this.subscriptions) {
            for (const sub of subs.values()) {
                if (sub.guildId === guildId) {
                    socket.send(JSON.stringify(["EVENT", event]));
                    count++;
                }
            }
        }
        // console.log(`Broadcasted event ${event.seq} to ${count} clients`);
    }

    public async close() {
        clearInterval(this.pruneTimer);
        clearInterval(this.checkpointTimer);
        this.wss.close();
        await this.pluginsReady;
        for (const plugin of this.plugins) {
            try {
                await plugin.onClose?.(this.pluginCtx);
            } catch (e: any) {
                console.error(`Relay plugin ${plugin.name} failed to close:`, e);
            }
        }
        await this.store.close();
    }
}
