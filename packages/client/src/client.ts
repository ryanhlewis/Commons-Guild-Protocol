import { EventEmitter } from "events";
import { WebSocket } from "ws";
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
    sign,
    EventBody,
    GuildCreate,
    ChannelCreate,
    Message,
    RoleAssign,
    BanUser,
    EditMessage,
    DeleteMessage,
    ForkFrom,
    EphemeralPolicyUpdate,
    EphemeralPolicy,
    validateEvent,
    getSharedSecret,
    encrypt,
    decrypt,
    generateSymmetricKey
} from "@cgp/core";

export class CgpClient extends EventEmitter {
    private relays: string[];
    private sockets: WebSocket[] = [];
    private keyPair?: { pub: string; priv: Uint8Array };
    private state = new Map<GuildId, GuildState>();
    private seenEvents = new Set<string>();
    private seenEventsQueue: string[] = []; // Track insertion order for efficient cleanup
    private readonly MAX_SEEN_EVENTS = 1000;
    private readonly CLEANUP_THRESHOLD = 900;
    private pendingEvents = new Map<GuildId, Map<number, GuildEvent>>();
    private groupKeys = new Map<GuildId, string>(); // Hex encoded symmetric keys
    private server: any;

    constructor(config: { relays: string[]; keyPair?: { pub: string; priv: Uint8Array } }) {
        super();
        this.relays = config.relays;
        this.keyPair = config.keyPair;
    }

    async connect() {
        const promises = this.relays.map(url => this.connectToRelay(url));
        await Promise.all(promises);
    }

    private async connectToRelay(url: string, retryCount = 0) {
        return new Promise<void>((resolve) => {
            const ws = new WebSocket(url);

            ws.onopen = () => {
                console.log(`Connected to relay ${url}`);
                ws.send(JSON.stringify(["HELLO", { protocol: "cgp/0.1" }]));
                this.sockets.push(ws);
                resolve();
            };

            ws.onmessage = (msg) => this.handleMessage(msg.data.toString());

            ws.onerror = (err) => {
                console.error(`WS Error on ${url}`, err);
            };

            ws.onclose = () => {
                console.log(`Disconnected from relay ${url}`);
                const index = this.sockets.indexOf(ws);
                if (index > -1) {
                    this.sockets.splice(index, 1);
                }

                const delay = Math.min(1000 * Math.pow(2, retryCount), 30000);
                console.log(`Reconnecting to ${url} in ${delay}ms...`);
                setTimeout(() => {
                    this.connectToRelay(url, retryCount + 1);
                }, delay);
            };

            ws.addEventListener('error', () => resolve());
        });
    }

    async listen(port: number) {
        const { WebSocketServer } = await import("ws");
        const wss = new WebSocketServer({ port });

        console.log(`Client listening on port ${port}`);

        wss.on("connection", (socket) => {
            console.log(`Incoming connection on port ${port}`);
            this.sockets.push(socket);
            socket.on("message", (data) => this.handleMessage(data.toString(), socket));
            socket.on("close", () => {
                const index = this.sockets.indexOf(socket);
                if (index > -1) this.sockets.splice(index, 1);
            });
        });
        this.server = wss;
    }

    async connectToPeer(url: string) {
        return new Promise<void>((resolve, reject) => {
            console.log(`Connecting to peer ${url}`);
            const ws = new WebSocket(url);
            ws.onopen = () => {
                console.log(`Connected to peer ${url}`);
                ws.send(JSON.stringify(["HELLO", { protocol: "cgp/0.1", mode: "p2p" }]));
                this.sockets.push(ws);
                console.log(`Peer 1 sockets count: ${this.sockets.length}`);
                resolve();
            };
            ws.onmessage = (msg) => this.handleMessage(msg.data.toString(), ws);
            ws.onerror = (err) => {
                console.error(`Peer WS Error ${url}`, err);
                reject(err);
            };
        });
    }
    private messageQueue = Promise.resolve();

    private async handleMessage(data: string, socket?: WebSocket) {
        // Chain the processing to ensure sequential execution
        this.messageQueue = this.messageQueue.then(async () => {
            try {
                const parsed = JSON.parse(data);
                if (!Array.isArray(parsed) || parsed.length < 2) return;
                const [kind, payload] = parsed as [string, unknown];

                if (kind === "EVENT") {
                    const event = payload as GuildEvent;
                    const unsignedForSig = { body: event.body, author: event.author, createdAt: event.createdAt };
                    const msgHash = hashObject(unsignedForSig);

                    if (this.seenEvents.has(event.id)) return;
                    this.seenEvents.add(event.id);
                    this.seenEventsQueue.push(event.id);
                    
                    // Efficient cleanup: Remove oldest entries when threshold is exceeded
                    if (this.seenEventsQueue.length > this.MAX_SEEN_EVENTS) {
                        // Remove oldest entries until we're back to CLEANUP_THRESHOLD
                        const toRemove = this.seenEventsQueue.length - this.CLEANUP_THRESHOLD;
                        for (let i = 0; i < toRemove; i++) {
                            const oldId = this.seenEventsQueue[i];
                            this.seenEvents.delete(oldId);
                        }
                        // Keep only the recent entries in the queue
                        this.seenEventsQueue = this.seenEventsQueue.slice(toRemove);
                    }

                    if (!verify(event.author, msgHash, event.signature)) {
                        console.log(`Client: Invalid signature for event ${event.id}`);
                        return;
                    }

                    // console.log(`Client received EVENT seq ${event.seq} type ${event.body.type}`);
                    await this.updateState(event);
                    this.emit("event", event);

                    // Gossip: Forward to all other peers
                    let gossipCount = 0;
                    this.sockets.forEach(s => {
                        if (s !== socket && s.readyState === WebSocket.OPEN) {
                            s.send(data);
                            gossipCount++;
                        }
                    });
                    // console.log(`Gossiped event ${event.seq} to ${gossipCount} peers`);

                } else if (kind === "SNAPSHOT") {
                    const p = payload as { events: GuildEvent[]; guildId: GuildId };
                    const { events, guildId } = p;
                    // console.log(`Client received SNAPSHOT for ${guildId} with ${events.length} events`);
                    if (events.length > 0) {
                        let state = createInitialState(events[0]);
                        // Extract key from initial event if present
                        await this.extractKey(events[0]);

                        for (let i = 1; i < events.length; i++) {
                            state = applyEvent(state, events[i]);
                            await this.extractKey(events[i]);
                        }
                        this.state.set(guildId, state);
                    }
                    events.forEach((e: GuildEvent) => this.emit("event", e));

                } else if (kind === "PUBLISH") {
                    const p = payload as { body: EventBody; author: string; signature: string; createdAt: number };
                    const { body, author, signature, createdAt } = p;

                    const unsignedForSig = { body, author, createdAt };
                    const msgHash = hashObject(unsignedForSig);

                    if (!verify(author, msgHash, signature)) {
                        console.log(`Client: Invalid signature for PUBLISH from peer`);
                        return;
                    }

                    let seq = 0;
                    let prevHash: string | null = null;

                    const targetGuildId = body.guildId;
                    const state = this.state.get(targetGuildId);

                    if (state) {
                        seq = state.headSeq + 1;
                        prevHash = state.headHash;
                    } else if (body.type !== "GUILD_CREATE") {
                        console.log("Client received PUBLISH for unknown guild and not GUILD_CREATE");
                        return;
                    }

                    const event: GuildEvent = {
                        id: "",
                        seq,
                        prevHash,
                        createdAt,
                        author,
                        body,
                        signature
                    };
                    event.id = computeEventId(event);

                    await this.updateState(event);
                    this.emit("event", event);

                    // Gossip: Forward as EVENT to all peers
                    const eventFrame = JSON.stringify(["EVENT", event]);
                    let gossipCount = 0;
                    this.sockets.forEach(s => {
                        if (s.readyState === WebSocket.OPEN) {
                            s.send(eventFrame);
                            gossipCount++;
                        }
                    });
                    // console.log(`Converted PUBLISH to EVENT and gossiped to ${gossipCount} peers`);

                } else if (kind === "HELLO") {
                    // Handshake, ignore for now
                } else if (kind === "SUB") {
                    // Subscription, ignore for now
                } else if (kind === "ERROR") {
                    console.error("Client received ERROR:", payload);
                }

            } catch (e) {
                console.error("Error handling message:", e);
            }
        }).catch(err => console.error("Message queue error:", err));
    }

    private async updateState(event: GuildEvent) {
        const guildId = event.body.guildId;
        let state = this.state.get(guildId);

        await this.extractKey(event);

        if (!state) {
            if (event.body.type === "GUILD_CREATE") {
                try {
                    // validateEvent({} as any, event); // Skip validation for genesis event
                    // Actually createInitialState validates type
                    state = createInitialState(event);
                    this.state.set(guildId, state);
                } catch (e) {
                    console.error("Failed to apply initial event", e);
                }
            } else {
                return;
            }
        } else {
            if (event.seq === state.headSeq + 1 && event.prevHash === state.headHash) {
                try {
                    validateEvent(state, event);
                    state = applyEvent(state, event);
                    this.state.set(guildId, state);
                } catch (e) {
                    console.error("Failed to apply event", e);
                }
            } else {
                console.log(`Gap or Fork detected: Expected ${state.headSeq + 1}/${state.headHash}, got ${event.seq}/${event.prevHash}`);
            }
        }
    }

    private async extractKey(event: GuildEvent) {
        if (!this.keyPair) return;
        const body = event.body;
        let encryptedGroupKey: string | undefined;
        let targetUser: string | undefined;

        if (body.type === "GUILD_CREATE") {
            encryptedGroupKey = body.encryptedGroupKey;
            targetUser = event.author;
        } else if (body.type === "ROLE_ASSIGN") {
            encryptedGroupKey = body.encryptedGroupKey;
            targetUser = body.userId;
        }

        if (encryptedGroupKey && targetUser === this.keyPair.pub) {
            try {
                const shared = getSharedSecret(this.keyPair.priv, event.author);
                const { ciphertext, iv } = JSON.parse(encryptedGroupKey);
                const keyHex = await decrypt(shared, ciphertext, iv);
                this.groupKeys.set(body.guildId, keyHex);
            } catch (e) {
                console.error("Failed to decrypt group key", e);
            }
        }
    }

    async createGuild(name: string, description?: string, access: "public" | "private" = "public"): Promise<GuildId> {
        const { pub, priv } = this.keyPair!;
        const createdAt = Date.now();

        let encryptedGroupKey: string | undefined;
        if (access === "private") {
            const groupKey = generateSymmetricKey();
            this.groupKeys.set("", groupKey); // Temp placeholder, will update with ID

            // Encrypt for self
            const shared = getSharedSecret(priv, pub);
            const res = await encrypt(shared, groupKey);
            encryptedGroupKey = JSON.stringify(res);
        }

        const body: GuildCreate = {
            type: "GUILD_CREATE",
            guildId: "",
            name,
            description,
            access,
            encryptedGroupKey
        };
        const tempId = hashObject({ author: pub, name, createdAt });
        body.guildId = tempId;

        if (access === "private") {
            const key = this.groupKeys.get("");
            if (key) {
                this.groupKeys.delete("");
                this.groupKeys.set(tempId, key);
            }
        }

        await this.subscribe(tempId);
        await this.publish(body);
        return tempId;
    }

    async createChannel(guildId: GuildId, name: string, kind: "text" | "voice" | "ephemeral-text", ephemeralPolicy?: EphemeralPolicy): Promise<ChannelId> {
        const channelId = hashObject(name + Date.now());
        const body: ChannelCreate = { type: "CHANNEL_CREATE", guildId, channelId, name, kind, retention: ephemeralPolicy };
        await this.publish(body);
        return channelId;
    }

    async sendMessage(guildId: GuildId, channelId: ChannelId, content: string): Promise<string> {
        let finalContent = content;
        let iv: string | undefined;
        let encrypted: boolean | undefined;

        const state = this.state.get(guildId);
        const groupKey = this.groupKeys.get(guildId);

        if (groupKey) {
            // Group Encryption
            const result = await encrypt(Buffer.from(groupKey, "hex"), content);
            finalContent = result.ciphertext;
            iv = result.iv;
            encrypted = true;
        } else if (state && state.access === "private" && state.members.size === 2) {
            // Pairwise Fallback (for legacy DMs or if key missing)
            const { pub, priv } = this.keyPair!;
            let otherUser: string | undefined;
            for (const [userId] of state.members) {
                if (userId !== pub) {
                    otherUser = userId;
                    break;
                }
            }

            if (otherUser) {
                const shared = getSharedSecret(priv, otherUser);
                const result = await encrypt(shared, content);
                finalContent = result.ciphertext;
                iv = result.iv;
                encrypted = true;
            }
        }

        const messageId = hashObject(content + Date.now());
        const body: Message = {
            type: "MESSAGE",
            guildId,
            channelId,
            messageId,
            content: finalContent,
            iv,
            encrypted
        };
        await this.publish(body);
        return messageId;
    }

    async assignRole(guildId: GuildId, userId: string, role: string): Promise<void> {
        const { pub, priv } = this.keyPair!;
        let encryptedGroupKey: string | undefined;

        const groupKey = this.groupKeys.get(guildId);
        if (groupKey) {
            const shared = getSharedSecret(priv, userId);
            const res = await encrypt(shared, groupKey);
            encryptedGroupKey = JSON.stringify(res);
        }

        const body: RoleAssign = { type: "ROLE_ASSIGN", guildId, userId, roleId: role, encryptedGroupKey };
        await this.publish(body);
    }

    async banUser(guildId: GuildId, userId: string): Promise<void> {
        const body: BanUser = { type: "BAN_USER", guildId, userId };
        await this.publish(body);
    }

    async editMessage(guildId: GuildId, channelId: ChannelId, messageId: string, content: string): Promise<void> {
        const body: EditMessage = { type: "EDIT_MESSAGE", guildId, channelId, messageId, newContent: content };
        await this.publish(body);
    }

    async deleteMessage(guildId: GuildId, channelId: ChannelId, messageId: string, reason?: string): Promise<void> {
        const body: DeleteMessage = { type: "DELETE_MESSAGE", guildId, channelId, messageId, reason };
        await this.publish(body);
    }

    async forkGuild(originalGuildId: GuildId, parentSeq: number, parentRootHash: string, newName: string): Promise<GuildId> {
        const { pub } = this.keyPair!;
        const createdAt = Date.now();
        const newGuildId = hashObject({ author: pub, name: newName, createdAt });

        const createBody: GuildCreate = { type: "GUILD_CREATE", guildId: newGuildId, name: newName };
        await this.publish(createBody);

        const forkBody: ForkFrom = {
            type: "FORK_FROM",
            guildId: newGuildId,
            parentGuildId: originalGuildId,
            parentSeq,
            parentRootHash,
            note: `Fork of ${originalGuildId}`
        };
        await this.publish(forkBody);

        return newGuildId;
    }

    async setEphemeralPolicy(guildId: GuildId, channelId: ChannelId, duration: number): Promise<void> {
        const body: EphemeralPolicyUpdate = {
            type: "EPHEMERAL_POLICY_UPDATE",
            guildId,
            channelId,
            retention: { mode: "ttl", seconds: duration }
        };
        await this.publish(body);
    }

    async subscribe(guildId: GuildId): Promise<void> {
        const subId = Math.random().toString(36).slice(2);
        const frame = JSON.stringify(["SUB", { subId, guildId }]);
        this.sockets.forEach(s => {
            if (s.readyState === WebSocket.OPEN) {
                s.send(frame);
            }
        });
    }

    async publish(body: EventBody): Promise<string> {
        if (!this.keyPair) throw new Error("No keypair");
        const { pub, priv } = this.keyPair;
        const createdAt = Date.now();

        const unsigned = { body, author: pub, createdAt };
        const signature = await sign(priv, hashObject(unsigned));

        if (this.relays.length === 0) {
            const targetGuildId = body.guildId || (body.type === "GUILD_CREATE" ? computeEventId({ ...unsigned, id: "", seq: 0, prevHash: null, signature } as any) : null);
            if (!targetGuildId) throw new Error("Cannot determine guildId for P2P publish");

            let state = this.state.get(targetGuildId);
            const seq = state ? state.headSeq + 1 : 0;
            const prevHash = state ? state.headHash : null;

            const event: GuildEvent = {
                id: "", seq, prevHash, createdAt, author: pub, body, signature
            };
            event.id = computeEventId(event);

            await this.updateState(event);
            this.emit("event", event);

            const eventFrame = JSON.stringify(["EVENT", event]);
            let gossipCount = 0;
            this.sockets.forEach(s => {
                if (s.readyState === WebSocket.OPEN) {
                    s.send(eventFrame);
                    gossipCount++;
                }
            });
            // console.log(`P2P Publish: Gossiped event ${event.seq} to ${gossipCount} peers`);
            return event.id;
        }

        const payload = { body, author: pub, signature, createdAt };
        const frame = JSON.stringify(["PUBLISH", payload]);

        this.sockets.forEach(s => {
            if (s.readyState === WebSocket.OPEN) {
                s.send(frame);
            }
        });

        return "";
    }

    getGuildState(guildId: GuildId): GuildState | undefined {
        return this.state.get(guildId);
    }

    async decryptMessage(event: GuildEvent): Promise<string> {
        if (!this.keyPair) throw new Error("No keypair");
        const body = event.body as Message;
        if (!body.encrypted) return body.content;

        // Try Group Key first
        const groupKey = this.groupKeys.get(body.guildId);
        if (groupKey) {
            try {
                // console.log(`Attempting group decryption for msg ${body.messageId}`);
                return await decrypt(Buffer.from(groupKey, "hex"), body.content, body.iv!);
            } catch (e) {
                // console.log(`Group decryption failed for msg ${body.messageId}, trying pairwise...`);
            }
        }

        // Fallback to Pairwise
        const shared = getSharedSecret(this.keyPair.priv, event.author);
        return await decrypt(shared, body.content, body.iv!);
    }

    close() {
        this.sockets.forEach(s => s.close());
        this.sockets = [];
        if (this.server) {
            this.server.close();
        }
    }
}
