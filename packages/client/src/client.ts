import WebSocket from "ws"; // In browser this is native, but for Node/Tests we need 'ws'
import { EventEmitter } from "events";
import {
    GuildEvent,
    GuildId,
    ChannelId,
    sign,
    hashObject,
    computeEventId,
    GuildState,
    createInitialState,
    applyEvent,
    verify,
    EventBody,
    GuildCreate,
    ChannelCreate,
    Message,
    RoleAssign,
    BanUser,
    EditMessage,
    DeleteMessage,
    ForkFrom,
    EphemeralPolicy,
    EphemeralPolicyUpdate,
    validateEvent
} from "@cgp/core";

export class CgpClient extends EventEmitter {
    private relays: string[];
    private sockets: WebSocket[] = [];
    private keyPair?: { pub: string; priv: Uint8Array };
    private state = new Map<GuildId, GuildState>();
    private seenEvents = new Set<string>();
    private pendingEvents = new Map<GuildId, Map<number, GuildEvent>>();
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

    private handleMessage(data: string, socket?: WebSocket) {
        try {
            const parsed = JSON.parse(data);
            if (!Array.isArray(parsed) || parsed.length < 2) return;
            const [kind, payload] = parsed as [string, unknown];

            console.log(`Client received ${kind}`);

            if (kind === "EVENT") {
                const event = payload as GuildEvent;
                const unsignedForSig = { body: event.body, author: event.author, createdAt: event.createdAt };
                const msgHash = hashObject(unsignedForSig);

                if (this.seenEvents.has(event.id)) return;
                this.seenEvents.add(event.id);
                if (this.seenEvents.size > 1000) {
                    const it = this.seenEvents.values();
                    for (let i = 0; i < 100; i++) {
                        const val = it.next().value;
                        if (val) this.seenEvents.delete(val);
                    }
                }

                if (!verify(event.author, msgHash, event.signature)) {
                    console.log(`Client: Invalid signature for event ${event.id}`);
                    return;
                }

                this.updateState(event);
                this.emit("event", event);

                // Gossip: Forward to all other peers
                let gossipCount = 0;
                this.sockets.forEach(s => {
                    if (s !== socket && s.readyState === WebSocket.OPEN) {
                        s.send(data);
                        gossipCount++;
                    }
                });
                console.log(`Gossiped event ${event.seq} to ${gossipCount} peers`);

            } else if (kind === "SNAPSHOT") {
                const p = payload as { events: GuildEvent[]; guildId: GuildId };
                const { events, guildId } = p;
                if (events.length > 0) {
                    let state = createInitialState(events[0]);
                    for (let i = 1; i < events.length; i++) {
                        state = applyEvent(state, events[i]);
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

                this.updateState(event);
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
                console.log(`Converted PUBLISH to EVENT and gossiped to ${gossipCount} peers`);

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
    }

    private updateState(event: GuildEvent) {
        const guildId = event.body.guildId;
        let state = this.state.get(guildId);

        if (!state) {
            if (event.body.type === "GUILD_CREATE") {
                state = createInitialState(event);
                this.state.set(guildId, state);
            } else {
                return;
            }
        } else {
            if (event.seq === state.headSeq + 1 && event.prevHash === state.headHash) {
                validateEvent(state, event);
                state = applyEvent(state, event);
                this.state.set(guildId, state);
            }
        }
    }

    async createGuild(name: string, description?: string, access: "public" | "private" = "public"): Promise<GuildId> {
        const { pub } = this.keyPair!;
        const createdAt = Date.now();
        const body: GuildCreate = {
            type: "GUILD_CREATE",
            guildId: "",
            name,
            description,
            access
        };
        const tempId = hashObject({ author: pub, name, createdAt });
        body.guildId = tempId;

        await this.publish(body);
        return tempId;
    }

    async createChannel(guildId: GuildId, name: string, kind: "text" | "voice"): Promise<ChannelId> {
        const channelId = hashObject(name + Date.now());
        const body: ChannelCreate = { type: "CHANNEL_CREATE", guildId, channelId, name, kind };
        await this.publish(body);
        return channelId;
    }

    async sendMessage(guildId: GuildId, channelId: ChannelId, content: string): Promise<void> {
        const body: Message = { type: "MESSAGE", guildId, channelId, messageId: hashObject(content + Date.now()), content };
        await this.publish(body);
    }

    async assignRole(guildId: GuildId, userId: string, role: string): Promise<void> {
        const body: RoleAssign = { type: "ROLE_ASSIGN", guildId, userId, roleId: role };
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

    async deleteMessage(guildId: GuildId, channelId: ChannelId, messageId: string): Promise<void> {
        const body: DeleteMessage = { type: "DELETE_MESSAGE", guildId, channelId, messageId };
        await this.publish(body);
    }

    async forkGuild(originalGuildId: GuildId, newName: string): Promise<GuildId> {
        const { pub } = this.keyPair!;
        const createdAt = Date.now();
        const newGuildId = hashObject({ author: pub, name: newName, createdAt });

        const createBody: GuildCreate = { type: "GUILD_CREATE", guildId: newGuildId, name: newName };
        await this.publish(createBody);

        const forkBody: ForkFrom = {
            type: "FORK_FROM",
            guildId: newGuildId,
            parentGuildId: originalGuildId,
            parentSeq: 0,
            parentRootHash: ""
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

            this.updateState(event);
            this.emit("event", event);

            const eventFrame = JSON.stringify(["EVENT", event]);
            let gossipCount = 0;
            this.sockets.forEach(s => {
                if (s.readyState === WebSocket.OPEN) {
                    s.send(eventFrame);
                    gossipCount++;
                }
            });
            console.log(`P2P Publish: Gossiped event ${event.seq} to ${gossipCount} peers`);
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

    close() {
        this.sockets.forEach(s => s.close());
        if (this.server) this.server.close();
    }
}
