import WebSocket from "ws"; // In browser this is native, but for Node/Tests we need 'ws'
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
    EphemeralPolicy
} from "@cgp/core";

export class CgpClient {
    private relays: string[];
    private sockets: WebSocket[] = [];
    private listeners = new Map<string, (event: GuildEvent) => void>();
    private keyPair?: { pub: string; priv: Uint8Array };
    private state = new Map<GuildId, GuildState>();

    constructor(config: { relays: string[]; keyPair?: { pub: string; priv: Uint8Array } }) {
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
            this.sockets.push(socket);
            socket.on("message", (data) => this.handleMessage(data.toString()));
            socket.on("close", () => {
                const index = this.sockets.indexOf(socket);
                if (index > -1) this.sockets.splice(index, 1);
            });
        });
    }

    async connectToPeer(url: string) {
        return new Promise<void>((resolve) => {
            const ws = new WebSocket(url);
            ws.onopen = () => {
                console.log(`Connected to peer ${url}`);
                ws.send(JSON.stringify(["HELLO", { protocol: "cgp/0.1", mode: "p2p" }]));
                this.sockets.push(ws);
                resolve();
            };
            ws.onmessage = (msg) => this.handleMessage(msg.data.toString());
            ws.onerror = (err) => console.error(`Peer WS Error ${url}`, err);
        });
    }

    private handleMessage(data: string, socket?: WebSocket) {
        try {
            const parsed = JSON.parse(data);
            if (!Array.isArray(parsed) || parsed.length < 2) return;
            const [kind, payload] = parsed as [string, unknown];

            if (kind === "EVENT") {
                const event = payload as GuildEvent;
                const unsignedForSig = { body: event.body, author: event.author, createdAt: event.createdAt };
                const msgHash = hashObject(unsignedForSig);

                if (!verify(event.author, msgHash, event.signature)) {
                    console.error(`Client: Invalid signature for event ${event.id}`);
                    return;
                }

                this.updateState(event);
                this.emit("event", event);

                // Gossip: Forward to all other peers
                this.sockets.forEach(s => {
                    if (s !== socket && s.readyState === WebSocket.OPEN) {
                        s.send(data);
                    }
                });

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
                    console.error(`Client: Invalid signature for PUBLISH from peer`);
                    return;
                }

                const event: GuildEvent = {
                    id: "",
                    seq: 0,
                    prevHash: "",
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
                this.sockets.forEach(s => {
                    if (s.readyState === WebSocket.OPEN) {
                        s.send(eventFrame);
                    }
                });

            } else if (kind === "HELLO") {
                if (socket && socket.readyState === WebSocket.OPEN) {
                    socket.send(JSON.stringify(["HELLO_OK", { protocol: "cgp/0.1", agent: "CgpClient" }]));
                }
            } else if (kind === "SUB") {
                const p = payload as { subId: string; guildId: GuildId };
                const { subId, guildId } = p;
                const state = this.state.get(guildId);
                if (state && socket && socket.readyState === WebSocket.OPEN) {
                    // Send empty snapshot for now as we don't store event history
                    socket.send(JSON.stringify(["SNAPSHOT", { subId, guildId, events: [], endSeq: 0 }]));
                }
            } else if (kind === "ERROR") {
                console.error("Client received ERROR:", payload);
            }
        } catch (e) {
            console.error("Client parse error", e);
        }
    }

    private updateState(event: GuildEvent) {
        const guildId = event.body.guildId || (event.body.type === "GUILD_CREATE" ? event.id : null);
        if (!guildId) return;

        let state = this.state.get(guildId);
        if (!state) {
            if (event.body.type === "GUILD_CREATE") {
                state = createInitialState(event);
                this.state.set(guildId, state);
            }
        } else {
            try {
                const newState = applyEvent(state, event);
                this.state.set(guildId, newState);
            } catch (e) {
                console.error("Failed to apply event to local state", e);
            }
        }
    }

    getGuildState(guildId: GuildId): GuildState | undefined {
        return this.state.get(guildId);
    }

    subscribe(guildId: GuildId, channels?: ChannelId[]) {
        const subId = Math.random().toString(36).slice(2);
        const frame = JSON.stringify(["SUB", { subId, guildId, channels }]);
        this.sockets.forEach(ws => ws.send(frame));
        return subId;
    }

    async createGuild(name: string) {
        if (!this.keyPair) throw new Error("No keys");

        const partialBody = {
            type: "GUILD_CREATE",
            name,
            timestamp: Date.now(),
            nonce: Math.random()
        };

        const guildId = hashObject(partialBody);

        const body: GuildCreate = {
            type: "GUILD_CREATE",
            guildId,
            name,
        };

        this.subscribe(guildId);
        await this.publish(body);
        return guildId;
    }

    async publish(body: EventBody) {
        if (!this.keyPair) throw new Error("No keys");

        const createdAt = Date.now();
        const author = this.keyPair.pub;

        const unsignedForSig = { body, author, createdAt };
        const msgHash = hashObject(unsignedForSig);
        const signature = await sign(this.keyPair.priv, msgHash);

        const payload = {
            body,
            author,
            createdAt,
            signature
        };

        this.sockets.forEach(ws => ws.send(JSON.stringify(["PUBLISH", payload])));
    }

    async createChannel(guildId: GuildId, name: string, kind: "text" | "voice" | "ephemeral-text", retention?: EphemeralPolicy) {
        const channelId = hashObject({ guildId, name, kind, timestamp: Date.now() });
        const body: ChannelCreate = {
            type: "CHANNEL_CREATE",
            guildId,
            channelId,
            name,
            kind,
            retention
        };
        await this.publish(body);
        return channelId;
    }

    async sendMessage(guildId: GuildId, channelId: ChannelId, content: string) {
        const messageId = hashObject({ guildId, channelId, content, timestamp: Date.now() });
        const body: Message = {
            type: "MESSAGE",
            guildId,
            channelId,
            messageId,
            content
        };
        await this.publish(body);
        return messageId;
    }

    async assignRole(guildId: GuildId, userId: string, roleId: string) {
        const body: RoleAssign = {
            type: "ROLE_ASSIGN",
            guildId,
            userId,
            roleId
        };
        await this.publish(body);
    }

    async banUser(guildId: GuildId, userId: string, reason?: string) {
        const body: BanUser = {
            type: "BAN_USER",
            guildId,
            userId,
            reason
        };
        await this.publish(body);
    }

    async editMessage(guildId: GuildId, channelId: ChannelId, messageId: string, newContent: string) {
        const body: EditMessage = {
            type: "EDIT_MESSAGE",
            guildId,
            channelId,
            messageId,
            newContent
        };
        await this.publish(body);
    }

    async deleteMessage(guildId: GuildId, channelId: ChannelId, messageId: string, reason?: string) {
        const body: DeleteMessage = {
            type: "DELETE_MESSAGE",
            guildId,
            channelId,
            messageId,
            reason
        };
        await this.publish(body);
    }

    async forkGuild(parentGuildId: GuildId, parentSeq: number, parentRootHash: string, name: string) {
        const newGuildId = await this.createGuild(name);

        const body: ForkFrom = {
            type: "FORK_FROM",
            guildId: newGuildId,
            parentGuildId,
            parentSeq,
            parentRootHash,
            note: `Fork of ${parentGuildId}`
        };
        await this.publish(body);
        return newGuildId;
    }

    on(event: string, cb: (data: GuildEvent) => void) {
        if (event === "event") {
            const id = Math.random().toString(36);
            this.listeners.set(id, cb);
            return () => this.listeners.delete(id);
        }
        return () => { };
    }

    private emit(event: string, data: GuildEvent) {
        if (event === "event") {
            this.listeners.forEach(cb => cb(data));
        }
    }

    close() {
        this.sockets.forEach(ws => ws.close());
    }
}
