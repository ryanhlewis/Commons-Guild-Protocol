import fs from "node:fs";
import path from "node:path";

interface LoadnetSummary {
    profile?: {
        relays: number;
        subscriberWorkers: number;
        subscribersPerWorker: number;
        publisherWorkers: number;
        messagesPerPublisher: number;
        latencyMs: number;
        jitterMs: number;
        lossPercent: number;
        churnEveryMs?: number;
        relayRestartCount?: number;
        pubSubRestartCount?: number;
        partitionCount?: number;
        slowConsumerPercent?: number;
    };
    metrics: Array<Record<string, any>>;
    metricsCount?: number;
    expectedMetrics?: number;
    collectorComplete?: boolean;
    netem?: Array<Record<string, any>>;
    totals: {
        published: number;
        received: number;
        expectedReceived: number;
    };
    relayHeads?: {
        validCount?: number;
        invalidCount?: number;
        errorCount?: number;
        conflictCount?: number;
        results?: Array<{ head?: { headSeq?: number; headHash?: string } }>;
    };
}

function argValue(name: string, fallback?: string) {
    const prefix = `--${name}=`;
    const found = process.argv.find((arg) => arg.startsWith(prefix));
    if (found) return found.slice(prefix.length);
    const index = process.argv.indexOf(`--${name}`);
    return index >= 0 ? process.argv[index + 1] : fallback;
}

function numberArg(name: string, fallback: number) {
    const value = Number(argValue(name));
    return Number.isFinite(value) ? value : fallback;
}

function latestSummaryPath() {
    const resultsDir = path.resolve("loadnet", "results");
    const files = fs.existsSync(resultsDir)
        ? fs.readdirSync(resultsDir)
            .filter((name) => /^summary-\d+\.json$/.test(name))
            .map((name) => path.join(resultsDir, name))
            .sort((left, right) => fs.statSync(right).mtimeMs - fs.statSync(left).mtimeMs)
        : [];
    return files[0];
}

function fail(message: string): never {
    console.error(`Loadnet summary gate failed: ${message}`);
    process.exit(1);
}

const summaryPath = argValue("summary", process.argv[2]) ?? latestSummaryPath();
if (!summaryPath) fail("missing --summary <path> and no loadnet/results/summary-*.json exists");
const resolvedSummaryPath = path.resolve(summaryPath);
if (!fs.existsSync(resolvedSummaryPath)) fail(`summary file not found: ${resolvedSummaryPath}`);

const summary = JSON.parse(fs.readFileSync(resolvedSummaryPath, "utf8")) as LoadnetSummary;
const isChaosProfile = Boolean(summary.profile && (
    Number(summary.profile.churnEveryMs ?? 0) > 0 ||
    Number(summary.profile.relayRestartCount ?? 0) > 0 ||
    Number(summary.profile.pubSubRestartCount ?? 0) > 0 ||
    Number(summary.profile.partitionCount ?? 0) > 0
));
const minDeliveryRatio = numberArg("min-delivery-ratio", 1);
const maxPublisherP99Ms = numberArg("max-publisher-p99-ms", isChaosProfile ? 60_000 : 30_000);
const maxRelayHeadLag = numberArg("max-relay-head-lag", 0);
const defaultMaxDuplicateRatio = isChaosProfile ? 0.01 : 0;
const maxDuplicateRatio = numberArg("max-duplicate-ratio", defaultMaxDuplicateRatio);
const publisherMetrics = summary.metrics.filter((metric) => metric.role === "publisher");
const subscriberMetrics = summary.metrics.filter((metric) => metric.role === "subscriber");
const expectsSubscriberDelivery = summary.profile
    ? summary.profile.subscriberWorkers * summary.profile.subscribersPerWorker > 0
    : summary.totals.expectedReceived > 0;

if (summary.collectorComplete === false) {
    fail(`collector timed out with ${summary.metricsCount ?? summary.metrics.length}/${summary.expectedMetrics ?? "unknown"} metrics`);
}
if (summary.expectedMetrics !== undefined && summary.metrics.length < summary.expectedMetrics) {
    fail(`missing metrics: ${summary.metrics.length}/${summary.expectedMetrics}`);
}
if (summary.profile) {
    if (publisherMetrics.length !== summary.profile.publisherWorkers) {
        fail(`publisher metrics count ${publisherMetrics.length} does not match profile ${summary.profile.publisherWorkers}`);
    }
    if (subscriberMetrics.length !== summary.profile.subscriberWorkers) {
        fail(`subscriber metrics count ${subscriberMetrics.length} does not match profile ${summary.profile.subscriberWorkers}`);
    }
    const expectedMessages = summary.profile.publisherWorkers * summary.profile.messagesPerPublisher;
    if (summary.totals.published !== expectedMessages) {
        fail(`published ${summary.totals.published} does not match profile expected ${expectedMessages}`);
    }
    const netemRequired = summary.profile.latencyMs > 0 || summary.profile.jitterMs > 0 || summary.profile.lossPercent > 0;
    if (netemRequired) {
        const expectedNetem = 1 + summary.profile.relays + 1 + summary.profile.subscriberWorkers + summary.profile.publisherWorkers;
        const applied = (summary.netem ?? []).filter((entry) => entry.required === true && entry.applied === true).length;
        if (applied < expectedNetem) {
            fail(`netem applied for ${applied}/${expectedNetem} required services`);
        }
    }
    for (const metric of subscriberMetrics) {
        const duplicateDeliveries = Number(metric.duplicateDeliveries ?? 0);
        const received = Math.max(1, Number(metric.received ?? 0));
        const duplicateRatio = duplicateDeliveries / received;
        if (duplicateRatio > maxDuplicateRatio) {
            fail(`subscriber worker ${metric.workerId} duplicate ratio ${duplicateRatio.toFixed(6)} exceeds ${maxDuplicateRatio}`);
        }
        const underReceivedSubscribers = Number(metric.underReceivedSubscribers ?? 0);
        if (underReceivedSubscribers > 0) {
            fail(`subscriber worker ${metric.workerId} reported underReceivedSubscribers=${underReceivedSubscribers}`);
        }
        const expectedBySubscriber = Array.isArray(metric.expectedBySubscriber) ? metric.expectedBySubscriber : [];
        const perSubscriberReceived = Array.isArray(metric.perSubscriberReceived) ? metric.perSubscriberReceived : [];
        if (expectedBySubscriber.length > 0 || perSubscriberReceived.length > 0) {
            if (expectedBySubscriber.length !== summary.profile.subscribersPerWorker) {
                fail(`subscriber worker ${metric.workerId} expectedBySubscriber length ${expectedBySubscriber.length} does not match profile ${summary.profile.subscribersPerWorker}`);
            }
            if (perSubscriberReceived.length !== summary.profile.subscribersPerWorker) {
                fail(`subscriber worker ${metric.workerId} perSubscriberReceived length ${perSubscriberReceived.length} does not match profile ${summary.profile.subscribersPerWorker}`);
            }
            for (let index = 0; index < summary.profile.subscribersPerWorker; index += 1) {
                const expected = Number(expectedBySubscriber[index] ?? 0);
                const received = Number(perSubscriberReceived[index] ?? -1);
                if (received !== expected) {
                    fail(`subscriber worker ${metric.workerId} socket ${index} received ${received}/${expected}`);
                }
            }
        }
    }
    if (summary.profile.churnEveryMs > 0) {
        const reconnects = subscriberMetrics.reduce((sum, metric) => sum + Number(metric.reconnects ?? 0), 0);
        if (reconnects <= 0) {
            fail("churn profile did not record any subscriber reconnects");
        }
        const backfillDeliveredMessages = subscriberMetrics.reduce((sum, metric) => sum + Number(metric.backfillDeliveredMessages ?? 0), 0);
        if (backfillDeliveredMessages <= 0) {
            fail("churn profile did not record any recovery backfill deliveries");
        }
    }
    if (summary.profile.slowConsumerPercent > 0) {
        const slowConsumers = subscriberMetrics.reduce((sum, metric) => sum + Number(metric.slowConsumers ?? 0), 0);
        if (slowConsumers <= 0) {
            fail("slow-consumer profile did not record any slow consumers");
        }
    }
}

if (expectsSubscriberDelivery && summary.totals.expectedReceived <= 0) {
    fail("expectedReceived is zero");
}
const deliveryRatio = summary.totals.expectedReceived > 0
    ? summary.totals.received / summary.totals.expectedReceived
    : 1;
if (expectsSubscriberDelivery && deliveryRatio < minDeliveryRatio) {
    fail(`delivery ratio ${deliveryRatio.toFixed(6)} below ${minDeliveryRatio}`);
}

for (const publisher of publisherMetrics) {
    const p99 = Number(publisher.batchLatencyMs?.p99 ?? 0);
    if (p99 > maxPublisherP99Ms) {
        fail(`publisher ${publisher.workerId} p99 batch latency ${p99}ms exceeds ${maxPublisherP99Ms}ms`);
    }
}

if (!summary.relayHeads) {
    fail("relay head consistency results are missing");
}
if (Number(summary.relayHeads.invalidCount ?? 0) > 0) {
    fail(`relay head invalidCount=${summary.relayHeads.invalidCount}`);
}
if (Number(summary.relayHeads.conflictCount ?? 0) > 0) {
    fail(`relay head conflictCount=${summary.relayHeads.conflictCount}`);
}
if (Number(summary.relayHeads.errorCount ?? 0) > 0) {
    fail(`relay head errorCount=${summary.relayHeads.errorCount}`);
}
if (Number(summary.relayHeads.validCount ?? 0) <= 0) {
    fail("relay head validCount is zero");
}
const relayHeadSeqs = (summary.relayHeads.results ?? [])
    .map((result) => Number(result.head?.headSeq))
    .filter((seq) => Number.isFinite(seq));
if (relayHeadSeqs.length > 0) {
    const minHeadSeq = Math.min(...relayHeadSeqs);
    const maxHeadSeq = Math.max(...relayHeadSeqs);
    if (maxHeadSeq - minHeadSeq > maxRelayHeadLag) {
        fail(`relay head lag ${maxHeadSeq - minHeadSeq} exceeds ${maxRelayHeadLag} seqs`);
    }
}

console.log(JSON.stringify({
    ok: true,
    summary: resolvedSummaryPath,
    published: summary.totals.published,
    received: summary.totals.received,
    expectedReceived: summary.totals.expectedReceived,
    deliveryRatio: Number(deliveryRatio.toFixed(6)),
    relayHeadValidCount: summary.relayHeads.validCount,
    relayHeadLag: relayHeadSeqs.length > 0 ? Math.max(...relayHeadSeqs) - Math.min(...relayHeadSeqs) : null,
    maxDuplicateRatio
}, null, 2));
