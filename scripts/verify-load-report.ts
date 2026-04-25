import fs from "node:fs";
import path from "node:path";

interface Metric {
    name: string;
    count?: number;
    ms: number;
    ratePerSec?: number;
    extra?: Record<string, unknown>;
}

interface LoadReport {
    metrics: Metric[];
    memory: NodeJS.MemoryUsage;
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

function fail(message: string): never {
    console.error(`Load report gate failed: ${message}`);
    process.exit(1);
}

const reportPath = argValue("report", process.argv[2]);
if (!reportPath) {
    fail("missing --report <path>");
}

const report = JSON.parse(fs.readFileSync(path.resolve(reportPath), "utf8")) as LoadReport;
const metricByName = new Map(report.metrics.map((metric) => [metric.name, metric]));

function requiredMetric(name: string) {
    const found = metricByName.get(name);
    if (!found) fail(`missing metric ${name}`);
    return found;
}

const maxRssMb = numberArg("max-rss-mb", 512);
const minMessageAppendRate = numberArg("min-message-append-rate", 10_000);
const minFanoutDeliveryRate = numberArg("min-fanout-delivery-rate", 1_000);

const rssMb = report.memory.rss / 1024 / 1024;
if (rssMb > maxRssMb) {
    fail(`RSS ${rssMb.toFixed(1)}MB exceeds ${maxRssMb}MB`);
}

const appendMessages = metricByName.get("storage.append-messages");
if (appendMessages && (appendMessages.ratePerSec ?? 0) < minMessageAppendRate) {
    fail(`message append rate ${appendMessages.ratePerSec}/s is below ${minMessageAppendRate}/s`);
}

if (metricByName.has("fanout.delivery")) {
    const delivery = requiredMetric("fanout.delivery");
    const received = Number(delivery.extra?.received ?? delivery.count ?? 0);
    if (delivery.count !== undefined && received !== delivery.count) {
        fail(`fanout delivered ${received}/${delivery.count}`);
    }
    if ((delivery.ratePerSec ?? 0) < minFanoutDeliveryRate) {
        fail(`fanout delivery rate ${delivery.ratePerSec}/s is below ${minFanoutDeliveryRate}/s`);
    }
}

console.log(JSON.stringify({
    ok: true,
    report: path.resolve(reportPath),
    rssMb: Number(rssMb.toFixed(1)),
    messageAppendRate: appendMessages?.ratePerSec,
    fanoutDeliveryRate: metricByName.get("fanout.delivery")?.ratePerSec
}, null, 2));
