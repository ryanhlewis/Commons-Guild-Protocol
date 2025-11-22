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
    EphemeralPolicyUpdate
} from "@cgp/core";
import { LevelStore } from "./store_level";

interface Subscription {
    guildId: GuildId;
    channels?: ChannelId[];
}

export class RelayServer {
    private wss: WebSocketServer;
    private store: LevelStore;
    private subscriptions = new Map<WebSocket, Map<string, Subscription>>();
    private messageQueues = new Map<WebSocket, Promise<void>>();

    private pruneTimer: NodeJS.Timeout;
    private checkpointTimer: NodeJS.Timeout;
    private keyPair: { publicKey: string; privateKey: Uint8Array };

    constructor(port: number, dbPath: string = "./relay-db") {
        this.store = new LevelStore(dbPath);
        this.wss = new WebSocketServer({ port });

        // Generate a random keypair for the relay (in production, load from config)
        const privateKey = generatePrivateKey();
        this.keyPair = {
            privateKey,
            publicKey: getPublicKey(privateKey)
        };
        console.log(`Relay started with public key: ${this.keyPair.publicKey}`);

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
                    } catch (e) {
                        console.error("Error handling message", e);
                        socket.send(JSON.stringify(["ERROR", { code: "INVALID_FRAME", message: "Parse error" }]));
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

            for (const event of events) {
                if (event.body.type === "MESSAGE") {
                    const body = event.body as Message;
                    const channel = state.channels.get(body.channelId);
                    if (channel && channel.retention && channel.retention.mode === "ttl" && channel.retention.seconds) {
                        const ageSeconds = (now - event.createdAt) / 1000;
                        if (ageSeconds > channel.retention.seconds) {
                            await this.store.deleteEvent(guildId, event.seq);
                        }
                    }
                }
            }
        }
    }

    private async handleMessage(socket: WebSocket, kind: string, payload: unknown) {
        if (kind === "HELLO") {
            socket.send(JSON.stringify(["HELLO_OK", { protocol: "cgp/0.1", relayName: "Reference Relay" }]));
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

            const targetGuildId = body.guildId;
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
                socket.send(JSON.stringify(["ERROR", { code: "INVALID_SIGNATURE", message: "Signature verification failed" }]));
                return;
            }

            if (seq > 0) {
                const history = await this.store.getLog(targetGuildId);
                if (history.length === 0) {
                    console.error("Validation error: Missing history for non-genesis event");
                    return;
                }

                let state = createInitialState(history[0]);
                for (let i = 1; i < history.length; i++) {
                    state = applyEvent(state, history[i]);
                }

                try {
                    this.validateEvent(state, fullEvent);
                } catch (e: any) {
                    console.error(`Validation failed for guild ${targetGuildId}: ${e.message}`);
                    socket.send(JSON.stringify(["ERROR", { code: "VALIDATION_FAILED", message: e.message }]));
                    return;
                }
            } else {
                if (body.type !== "GUILD_CREATE") {
                    console.error("Validation failed: First event must be GUILD_CREATE");
                    return;
                }
            }

            await this.store.append(targetGuildId, fullEvent);
            console.log(`Relay appended event ${fullEvent.id} type ${body.type}`);

            this.broadcast(targetGuildId, fullEvent);
        }
    }

    private validateEvent(state: GuildState, event: GuildEvent) {
        const { body, author } = event;

        const isOwner = state.ownerId === author;
        const member = state.members.get(author);
        const isAdmin = member?.roles.has("admin") || member?.roles.has("owner");
        const hasPermission = isOwner || isAdmin;

        switch (body.type) {
            case "CHANNEL_CREATE":
            case "ROLE_ASSIGN":
            case "ROLE_REVOKE":
            case "BAN_USER":
            case "UNBAN_USER":
            case "EPHEMERAL_POLICY_UPDATE":
                if (!hasPermission) {
                    throw new Error(`User ${author} does not have permission for ${body.type}`);
                }
                break;
            case "MESSAGE":
                const msgBody = body as Message;
                if (!state.channels.has(msgBody.channelId)) {
                    throw new Error(`Channel ${msgBody.channelId} does not exist`);
                }
                if (state.bans.has(author)) {
                    throw new Error(`User ${author} is banned`);
                }
                break;
        }
    }

    private broadcast(guildId: GuildId, event: GuildEvent) {
        for (const [socket, subs] of this.subscriptions) {
            for (const sub of subs.values()) {
                if (sub.guildId === guildId) {
                    socket.send(JSON.stringify(["EVENT", event]));
                }
            }
        }
    }

    public async close() {
        clearInterval(this.pruneTimer);
        clearInterval(this.checkpointTimer);
        this.wss.close();
        await this.store.close();
    }
}
