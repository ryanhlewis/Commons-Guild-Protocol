import type { WebSocket } from "ws";
import type { GuildEvent, EventBody, GuildId, SerializableMember } from "@cgp/core";
import type { Store } from "./store";

export interface RelayPluginContext {
    relayPublicKey: string;
    store: Store;
    publishAsRelay: (body: EventBody, createdAt?: number) => Promise<GuildEvent | undefined>;
    getLog: (guildId: GuildId) => Promise<GuildEvent[]>;
}


export interface PluginInputSchema {
    name: string;
    type: "string" | "number" | "boolean" | "object";
    required: boolean;
    sensitive?: boolean;
    description: string;
    placeholder?: string;
}

export interface PluginMetadata {
    name: string;
    description?: string;
    icon?: string; // URL or base64
    version?: string;
}

export interface RelayPlugin {
    name: string;
    metadata?: PluginMetadata;
    inputs?: PluginInputSchema[];

    onInit?: (ctx: RelayPluginContext) => void | Promise<void>;
    onConfig?: (args: { socket: WebSocket; config: any }, ctx: RelayPluginContext) => void | Promise<void>;
    onGetMembers?: (args: { guildId: string; socket?: WebSocket }, ctx: RelayPluginContext) => Promise<SerializableMember[] | undefined>;
    onFrame?: (args: { socket: WebSocket; kind: string; payload: unknown }, ctx: RelayPluginContext) => boolean | Promise<boolean>;
    onEventAppended?: (args: { event: GuildEvent; socket?: WebSocket }, ctx: RelayPluginContext) => void | Promise<void>;
    onClose?: (ctx: RelayPluginContext) => void | Promise<void>;
}

