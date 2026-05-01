import fs from "fs";
import path from "path";
import { generatePrivateKey, getPublicKey } from "@cgp/core";
import { normalizeProfile } from "./profile";

const positionalArgs = collectPositionalArgs();

function collectPositionalArgs() {
    const args = process.argv.slice(2);
    const positional: string[] = [];
    for (let index = 0; index < args.length; index += 1) {
        const arg = args[index];
        if (arg.startsWith("--")) {
            if (!arg.includes("=") && args[index + 1] && !args[index + 1].startsWith("--")) {
                index += 1;
            }
            continue;
        }
        positional.push(arg);
    }
    return positional;
}

function argValue(name: string, fallback?: string) {
    const prefix = `--${name}=`;
    const match = process.argv.find((arg) => arg.startsWith(prefix));
    if (match) return match.slice(prefix.length);
    const index = process.argv.indexOf(`--${name}`);
    if (index >= 0) return process.argv[index + 1];
    return fallback;
}

function safeName(value: string, label: string) {
    if (!/^[a-zA-Z0-9_.-]+$/.test(value)) {
        throw new Error(`${label} may only contain letters, numbers, dot, underscore, and dash`);
    }
    return value;
}

function safeImageName(value: string) {
    if (!/^[a-zA-Z0-9_.:/-]+$/.test(value)) {
        throw new Error("image contains unsupported characters");
    }
    return value;
}

function resolveInside(root: string, candidate: string, label: string) {
    const resolvedRoot = path.resolve(root);
    const resolved = path.resolve(candidate);
    const relative = path.relative(resolvedRoot, resolved);
    if (relative.startsWith("..") || path.isAbsolute(relative)) {
        throw new Error(`${label} must stay inside ${resolvedRoot}`);
    }
    return resolved;
}

function readProfile() {
    const profileName = safeName(argValue("profile", positionalArgs[0] || "smoke")!, "profile");
    const profilePath = path.resolve("loadnet", "profiles", profileName.endsWith(".json") ? profileName : `${profileName}.json`);
    const raw = fs.existsSync(profilePath) ? JSON.parse(fs.readFileSync(profilePath, "utf8")) : { name: profileName };
    const profile = normalizeProfile(raw);
    const wireFormat = argValue("wire-format");
    if (wireFormat !== undefined) {
        if (wireFormat !== "json" && wireFormat !== "binary-json" && wireFormat !== "binary-v1" && wireFormat !== "binary-v2") {
            throw new Error("wire-format must be json, binary-json, binary-v1, or binary-v2");
        }
        profile.wireFormat = wireFormat;
    }
    return profile;
}

function service(name: string, spec: Record<string, unknown>) {
    return { [name]: spec };
}

function defaultSubnetForServiceCount(serviceCount: number) {
    if (serviceCount > 2000) return "172.31.240.0/20";
    if (serviceCount > 1000) return "172.31.240.0/21";
    if (serviceCount > 500) return "172.31.240.0/22";
    if (serviceCount > 240) return "172.31.240.0/20";
    return "172.31.240.0/24";
}

function defaultLoadnetMediaProviders(maxBytes: number) {
    return [
        {
            id: "loadnet-meme-ipfs",
            kind: "ipfs",
            label: "Loadnet meme IPFS host",
            priority: 90,
            maxBytes,
            acceptsMimeTypes: ["image/*", "video/*"],
            acceptsTags: ["meme"],
            rejectsTags: ["adult", "nsfw", "porn", "illegal"],
            adult: "deny",
            retention: "operator-defined",
            mission: "free community meme pinning"
        },
        {
            id: "loadnet-anime-ipfs",
            kind: "ipfs",
            label: "Loadnet anime art IPFS host",
            priority: 80,
            maxBytes,
            acceptsMimeTypes: ["image/*", "video/*"],
            acceptsTags: ["anime", "art"],
            rejectsTags: ["adult", "nsfw", "porn", "illegal"],
            adult: "deny",
            retention: "operator-defined",
            mission: "anime and art media pinning"
        },
        {
            id: "loadnet-dog-ipfs",
            kind: "ipfs",
            label: "Loadnet dog photo IPFS host",
            priority: 75,
            maxBytes,
            acceptsMimeTypes: ["image/*"],
            acceptsTags: ["dog", "photo"],
            rejectsTags: ["adult", "nsfw", "porn", "illegal"],
            adult: "deny",
            retention: "operator-defined",
            mission: "pet and photo pinning"
        },
        {
            id: "loadnet-nsfw-opt-in-ipfs",
            kind: "ipfs",
            label: "Loadnet opt-in adult media IPFS host",
            priority: 70,
            maxBytes,
            acceptsMimeTypes: ["image/*", "video/*"],
            acceptsTags: ["adult", "nsfw", "porn"],
            requiresTags: ["adult"],
            adult: "only",
            retention: "operator-defined",
            mission: "explicitly opt-in adult media pinning"
        },
        {
            id: "loadnet-general-ipfs",
            kind: "ipfs",
            label: "Loadnet general IPFS host",
            priority: 50,
            maxBytes,
            acceptsMimeTypes: ["image/*", "video/*", "audio/*", "application/octet-stream"],
            acceptsTags: ["general", "image", "file", "photo", "art"],
            rejectsTags: ["adult", "nsfw", "porn", "illegal"],
            adult: "deny",
            retention: "best-effort",
            mission: "general encrypted media fallback"
        },
        {
            id: "loadnet-https-origin",
            kind: "https",
            label: "Loadnet external object storage",
            priority: 10,
            maxBytes,
            acceptsMimeTypes: ["*/*"],
            adult: "allow",
            retention: "operator-defined",
            mission: "bring-your-own origin fallback"
        }
    ];
}

function main() {
    const profile = readProfile();
    profile.name = safeName(profile.name, "profile.name");
    const output = path.resolve(argValue("output", positionalArgs[1] || "loadnet/docker-compose.generated.yml")!);
    const scratchRoot = path.resolve("loadnet", "run-data");
    const dataDir = resolveInside(scratchRoot, argValue("data-dir", positionalArgs[2] || scratchRoot)!, "data-dir");
    const resultsDir = path.resolve("loadnet", "results");
    const keepData = process.argv.includes("--keep-data");
    const composePath = (value: string) => value.replace(/\\/g, "/");
    const image = safeImageName(argValue("image", "cgp-loadnet:local")!);
    const serviceCount = profile.pubSubShards + profile.relays + 1 + profile.subscriberWorkers + profile.publisherWorkers + profile.fullClientWorkers + 1;
    const subnet = argValue("subnet", defaultSubnetForServiceCount(serviceCount))!;
    const runId = safeName(argValue("run-id", process.env.LOADNET_RUN_ID || `${profile.name}-${Date.now()}`)!, "run-id");
    const pubSubToken = process.env.CGP_PUBSUB_TOKEN || "loadnet-pubsub-token";
    const wireFormat = profile.wireFormat;
    const relayUrls = Array.from({ length: profile.relays }, (_, relay) => `ws://relay-${relay}:7447`);
    const pubSubUrls = Array.from({ length: profile.pubSubShards }, (_, shard) => `ws://pubsub-${shard}:7600`);
    const distributePublishers = process.env.LOADNET_DISTRIBUTE_PUBLISHERS === "1";
    const disruptiveProfile = profile.relayRestartCount > 0 || profile.pubSubRestartCount > 0 || profile.partitionCount > 0;
    const publisherTimeoutMs = process.env.LOADNET_PUBLISH_TIMEOUT_MS || (disruptiveProfile ? "5000" : "10000");
    const mediaProvidersJson = process.env.CGP_MEDIA_PROVIDERS_JSON || JSON.stringify(defaultLoadnetMediaProviders(profile.fullClientMediaBytes));
    const mediaPolicyEnabled = profile.fullClientMediaEvery > 0 ? "1" : (process.env.CGP_RELAY_MEDIA_POLICY_ENABLED || "0");
    const defaultClientRelayWindow = profile.relays > 8 ? 4 : profile.relays;
    const clientRelayWindowInput = Number(process.env.LOADNET_CLIENT_RELAY_WINDOW ?? defaultClientRelayWindow);
    const clientRelayWindow = Math.max(
        1,
        Math.min(profile.relays, Number.isFinite(clientRelayWindowInput) ? Math.floor(clientRelayWindowInput) : defaultClientRelayWindow)
    );
    const rotatedRelayUrls = (start: number) => {
        if (relayUrls.length === 0) {
            return [];
        }
        return Array.from({ length: relayUrls.length }, (_, offset) => relayUrls[(start + offset) % relayUrls.length]);
    };
    const clientRelayUrls = (start: number) => {
        if (relayUrls.length === 0) {
            return [];
        }
        return Array.from({ length: clientRelayWindow }, (_, offset) => relayUrls[(start + offset) % relayUrls.length]);
    };
    const relayKeys = Array.from({ length: profile.relays }, () => {
        const privateKey = generatePrivateKey();
        return {
            privateKeyHex: Buffer.from(privateKey).toString("hex"),
            publicKey: getPublicKey(privateKey)
        };
    });
    const trustedRelayPublicKeys = relayKeys.map((key) => key.publicKey).join(",");

    const services: Record<string, unknown> = {};

    for (let shard = 0; shard < profile.pubSubShards; shard += 1) {
        services[`pubsub-${shard}`] = {
            image,
            build: { context: "..", dockerfile: "loadnet/Dockerfile" },
            command: ["pubsub"],
            environment: {
                LOADNET_PUBSUB_PORT: "7600",
                LOADNET_PUBSUB_SHARD: String(shard),
                LOADNET_PUBSUB_SHARDS: String(profile.pubSubShards),
                LOADNET_RUN_ID: runId,
                LOADNET_TRACE_PUBSUB: process.env.LOADNET_TRACE_PUBSUB || "0",
                LOADNET_WIRE_FORMAT: wireFormat,
                CGP_PUBSUB_WIRE_FORMAT: wireFormat,
                CGP_PUBSUB_TOKEN: pubSubToken,
                CGP_PUBSUB_RETAIN_ENVELOPES: process.env.CGP_PUBSUB_RETAIN_ENVELOPES || "100000",
                CGP_PUBSUB_RETAIN_DIR: "/data/pubsub-retain",
                LOADNET_LATENCY_MS: String(profile.latencyMs),
                LOADNET_JITTER_MS: String(profile.jitterMs),
                LOADNET_LOSS_PERCENT: String(profile.lossPercent)
            },
            cap_add: ["NET_ADMIN"],
            networks: ["loadnet"],
            volumes: ["loadnet-data:/data"]
        };
    }

    for (let index = 0; index < profile.relays; index += 1) {
        const peerUrls = Array.from({ length: profile.relays }, (_, relay) => relay)
            .filter((relay) => relay !== index)
            .map((relay) => `ws://relay-${relay}:7447`)
            .join(",");
        services[`relay-${index}`] = {
            image,
            command: ["relay"],
            environment: {
                LOADNET_RELAY_INDEX: String(index),
                LOADNET_RUN_ID: runId,
                CGP_RELAY_PORT: "7447",
                CGP_RELAY_DB: `/data/relay-${index}`,
                CGP_RELAY_PUBSUB_URLS: pubSubUrls.join(","),
                CGP_RELAY_PEERS: peerUrls,
                LOADNET_WIRE_FORMAT: wireFormat,
                CGP_RELAY_WIRE_FORMAT: wireFormat,
                CGP_PUBSUB_WIRE_FORMAT: wireFormat,
                CGP_RELAY_PRIVATE_KEY_HEX: relayKeys[index].privateKeyHex,
                CGP_RELAY_TRUSTED_PEER_READ_PUBLIC_KEYS: trustedRelayPublicKeys,
                CGP_RELAY_PEER_CATCHUP_INTERVAL_MS: process.env.CGP_RELAY_PEER_CATCHUP_INTERVAL_MS || "2000",
                CGP_PUBSUB_TOKEN: pubSubToken,
                CGP_RELAY_DEFAULT_PLUGINS: "0",
                CGP_RELAY_MEDIA_POLICY_ENABLED: mediaPolicyEnabled,
                CGP_MEDIA_PROVIDERS_JSON: mediaProvidersJson,
                CGP_MEDIA_MAX_ATTACHMENT_BYTES: String(profile.fullClientMediaBytes),
                CGP_RELAY_CHECKPOINT_INTERVAL_MS: "0",
                CGP_RELAY_PUBSUB_REPLAY_LIMIT: process.env.CGP_RELAY_PUBSUB_REPLAY_LIMIT || "100000",
                CGP_RELAY_VERBOSE_LOGS: process.env.CGP_RELAY_VERBOSE_LOGS || "0",
                CGP_RELAY_STORAGE_SOFT_LIMIT_BYTES: String(profile.relayStorageSoftLimitBytes),
                CGP_RELAY_STORAGE_HARD_LIMIT_BYTES: String(profile.relayStorageHardLimitBytes),
                CGP_RELAY_STORAGE_ESTIMATE_INTERVAL_MS: String(profile.relayStorageEstimateIntervalMs),
                LOADNET_LATENCY_MS: String(profile.latencyMs),
                LOADNET_JITTER_MS: String(profile.jitterMs),
                LOADNET_LOSS_PERCENT: String(profile.lossPercent)
            },
            depends_on: Array.from({ length: profile.pubSubShards }, (_, shard) => `pubsub-${shard}`),
            cap_add: ["NET_ADMIN"],
            networks: ["loadnet"],
            volumes: ["loadnet-data:/data"]
        };
    }

    services.seed = {
        image,
        command: ["seed"],
        environment: {
            LOADNET_RELAYS: relayUrls.join(","),
            LOADNET_RUN_ID: runId,
            LOADNET_CHANNELS: String(profile.channels),
            LOADNET_WIRE_FORMAT: wireFormat,
            CGP_CLIENT_WIRE_FORMAT: wireFormat,
            LOADNET_PROFILE: JSON.stringify(profile),
            LOADNET_LATENCY_MS: String(profile.latencyMs),
            LOADNET_JITTER_MS: String(profile.jitterMs),
            LOADNET_LOSS_PERCENT: String(profile.lossPercent)
        },
        depends_on: Array.from({ length: profile.relays }, (_, index) => `relay-${index}`),
        cap_add: ["NET_ADMIN"],
        networks: ["loadnet"],
        volumes: ["loadnet-data:/data"]
    };

    for (let index = 0; index < profile.subscriberWorkers; index += 1) {
        services[`subscriber-${index}`] = {
            image,
            command: ["subscriber"],
            environment: {
                LOADNET_WORKER_ID: String(index),
                LOADNET_RUN_ID: runId,
                LOADNET_RELAY_URL: `ws://relay-${index % profile.relays}:7447`,
                LOADNET_RELAYS: relayUrls.join(","),
                LOADNET_SUBSCRIBERS: String(profile.subscribersPerWorker),
                LOADNET_CHURN_EVERY_MS: String(profile.churnEveryMs),
                LOADNET_SLOW_CONSUMER_PERCENT: String(profile.slowConsumerPercent),
                LOADNET_SLOW_CONSUMER_DELAY_MS: String(profile.slowConsumerDelayMs),
                LOADNET_BACKFILL_ON_RECONNECT: profile.churnEveryMs > 0 ? "1" : "0",
                LOADNET_FINAL_BACKFILL: process.env.LOADNET_FINAL_BACKFILL ?? "1",
                LOADNET_WIRE_FORMAT: wireFormat,
                CGP_CLIENT_WIRE_FORMAT: wireFormat,
                LOADNET_PROFILE: JSON.stringify(profile),
                LOADNET_LATENCY_MS: String(profile.latencyMs),
                LOADNET_JITTER_MS: String(profile.jitterMs),
                LOADNET_LOSS_PERCENT: String(profile.lossPercent)
            },
            depends_on: ["seed"],
            cap_add: ["NET_ADMIN"],
            networks: ["loadnet"],
            volumes: ["loadnet-data:/data"]
        };
    }

    for (let index = 0; index < profile.publisherWorkers; index += 1) {
        const publisherRelayUrls = distributePublishers
            ? clientRelayUrls(index % Math.max(1, profile.relays))
            : clientRelayUrls(0);
        const publisherWriteRelays = process.env.LOADNET_WRITE_RELAYS ||
            (distributePublishers ? (publisherRelayUrls[0] ?? "ws://relay-0:7447") : (relayUrls[0] ?? "ws://relay-0:7447"));
        services[`publisher-${index}`] = {
            image,
            command: ["publisher"],
            environment: {
                LOADNET_WORKER_ID: String(index),
                LOADNET_RUN_ID: runId,
                LOADNET_RELAY_URL: publisherRelayUrls[0] ?? "ws://relay-0:7447",
                LOADNET_RELAYS: publisherRelayUrls.join(","),
                LOADNET_WRITE_RELAYS: publisherWriteRelays,
                LOADNET_MESSAGES: String(profile.messagesPerPublisher),
                LOADNET_BATCH_SIZE: String(profile.batchSize),
                LOADNET_PUBLISH_TIMEOUT_MS: publisherTimeoutMs,
                LOADNET_EXPECTED_READY: String(profile.subscriberWorkers),
                LOADNET_WIRE_FORMAT: wireFormat,
                CGP_CLIENT_WIRE_FORMAT: wireFormat,
                LOADNET_PROFILE: JSON.stringify(profile),
                LOADNET_LATENCY_MS: String(profile.latencyMs),
                LOADNET_JITTER_MS: String(profile.jitterMs),
                LOADNET_LOSS_PERCENT: String(profile.lossPercent)
            },
            depends_on: ["seed", ...Array.from({ length: profile.subscriberWorkers }, (_, worker) => `subscriber-${worker}`)],
            cap_add: ["NET_ADMIN"],
            networks: ["loadnet"],
            volumes: ["loadnet-data:/data"]
        };
    }

    for (let index = 0; index < profile.fullClientWorkers; index += 1) {
        const fullClientRelayUrls = clientRelayUrls(index % Math.max(1, profile.relays));
        const fullClientWriteRelays = process.env.LOADNET_WRITE_RELAYS ||
            (distributePublishers ? (fullClientRelayUrls[0] ?? "ws://relay-0:7447") : (relayUrls[0] ?? "ws://relay-0:7447"));
        services[`full-client-${index}`] = {
            image,
            command: ["full-client"],
            environment: {
                LOADNET_WORKER_ID: String(index),
                LOADNET_RUN_ID: runId,
                LOADNET_RELAY_URL: fullClientRelayUrls[0] ?? "ws://relay-0:7447",
                LOADNET_RELAYS: fullClientRelayUrls.join(","),
                LOADNET_WRITE_RELAYS: fullClientWriteRelays,
                LOADNET_FULL_CLIENTS: String(profile.fullClientsPerWorker),
                LOADNET_FULL_CLIENT_ACTIVE_SIGNERS: String(profile.fullClientActiveSigners),
                LOADNET_FULL_CLIENT_ACTIONS: String(profile.fullClientActionsPerWorker),
                LOADNET_FULL_CLIENT_BATCH_SIZE: String(profile.fullClientBatchSize),
                LOADNET_FULL_CLIENT_OBSERVERS: String(profile.fullClientObserverSockets),
                LOADNET_FULL_CLIENT_FINAL_BACKFILL: profile.fullClientFinalBackfill ? "1" : "0",
                LOADNET_FULL_CLIENT_MEDIA_EVERY: String(profile.fullClientMediaEvery),
                LOADNET_FULL_CLIENT_MEDIA_BYTES: String(profile.fullClientMediaBytes),
                LOADNET_FULL_CLIENT_MEDIA_TAGS: profile.fullClientMediaTags.join(","),
                LOADNET_EXPECTED_READY: String(profile.subscriberWorkers),
                LOADNET_PUBLISH_TIMEOUT_MS: publisherTimeoutMs,
                LOADNET_WIRE_FORMAT: wireFormat,
                CGP_CLIENT_WIRE_FORMAT: wireFormat,
                LOADNET_PROFILE: JSON.stringify(profile),
                LOADNET_LATENCY_MS: String(profile.latencyMs),
                LOADNET_JITTER_MS: String(profile.jitterMs),
                LOADNET_LOSS_PERCENT: String(profile.lossPercent)
            },
            depends_on: ["seed", ...Array.from({ length: profile.subscriberWorkers }, (_, worker) => `subscriber-${worker}`)],
            cap_add: ["NET_ADMIN"],
            networks: ["loadnet"],
            volumes: ["loadnet-data:/data"]
        };
    }

    services.collector = {
        image,
        command: ["collector"],
        environment: {
            LOADNET_EXPECTED_METRICS: String(profile.publisherWorkers + profile.subscriberWorkers + profile.fullClientWorkers),
            LOADNET_RUN_ID: runId,
            LOADNET_WIRE_FORMAT: wireFormat,
            CGP_CLIENT_WIRE_FORMAT: wireFormat,
            LOADNET_PROFILE: JSON.stringify(profile),
            LOADNET_RELAYS: relayUrls.join(",")
        },
        depends_on: [
            ...Array.from({ length: profile.publisherWorkers }, (_, worker) => `publisher-${worker}`),
            ...Array.from({ length: profile.subscriberWorkers }, (_, worker) => `subscriber-${worker}`),
            ...Array.from({ length: profile.fullClientWorkers }, (_, worker) => `full-client-${worker}`)
        ],
        networks: ["loadnet"],
        volumes: ["loadnet-data:/data", `${composePath(resultsDir)}:/results`]
    };

    const logging = {
        driver: "json-file",
        options: {
            "max-size": process.env.LOADNET_LOG_MAX_SIZE || "10m",
            "max-file": process.env.LOADNET_LOG_MAX_FILE || "3"
        }
    };
    for (const spec of Object.values(services)) {
        if (spec && typeof spec === "object") {
            (spec as Record<string, unknown>).logging = logging;
        }
    }

    const compose = {
        name: `cgp-loadnet-${profile.name}`,
        "x-loadnet-run-id": runId,
        services,
        networks: {
            loadnet: {
                driver: "bridge",
                ipam: { config: [{ subnet }] }
            }
        },
        volumes: {
            "loadnet-data": {
                driver: "local",
                driver_opts: {
                    type: "none",
                    o: "bind",
                    device: composePath(dataDir)
                }
            }
        }
    };

    fs.mkdirSync(path.dirname(output), { recursive: true });
    if (!keepData && fs.existsSync(dataDir)) {
        fs.rmSync(dataDir, { recursive: true, force: true });
    }
    fs.mkdirSync(dataDir, { recursive: true });
    fs.mkdirSync(resultsDir, { recursive: true });
    fs.writeFileSync(output, toYaml(compose));
    console.log(`wrote ${output}`);
}

function scalar(value: unknown): string {
    if (typeof value === "number" || typeof value === "boolean") return String(value);
    return `'${String(value).replace(/'/g, "''")}'`;
}

function toYaml(value: any, indent = 0): string {
    const pad = " ".repeat(indent);
    if (Array.isArray(value)) {
        return value.map((item) => `${pad}- ${typeof item === "object" && item !== null ? `\n${toYaml(item, indent + 2)}` : scalar(item)}`).join("\n") + "\n";
    }
    if (value && typeof value === "object") {
        return Object.entries(value).map(([key, item]) => {
            if (item && typeof item === "object") {
                return `${pad}${key}:\n${toYaml(item, indent + 2)}`;
            }
            return `${pad}${key}: ${scalar(item)}`;
        }).join("\n") + "\n";
    }
    return `${pad}${scalar(value)}\n`;
}

main();
