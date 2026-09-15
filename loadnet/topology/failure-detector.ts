export type FailureState = "alive" | "suspect" | "dead" | "draining" | "left";

export interface AccrualFailureDetectorOptions {
    sampleWindow?: number;
    minimumSamples?: number;
    initialIntervalMs?: number;
    minimumStdDeviationMs?: number;
    acceptablePauseMs?: number;
    suspectPhi?: number;
    deadPhi?: number;
    hardLeaseMs?: number;
}

export interface FailureAssessment {
    relayId: string;
    state: FailureState;
    phi: number;
    elapsedMs: number;
    meanIntervalMs: number;
    stdDeviationMs: number;
    samples: number;
}

interface FailureHistory {
    intervals: number[];
    lastHeartbeatAt?: number;
    explicitState?: "draining" | "left";
}

const DEFAULT_OPTIONS: Required<AccrualFailureDetectorOptions> = {
    sampleWindow: 64,
    minimumSamples: 4,
    initialIntervalMs: 1_000,
    minimumStdDeviationMs: 100,
    acceptablePauseMs: 250,
    suspectPhi: 3,
    deadPhi: 8,
    hardLeaseMs: 10_000
};

function normalCdf(value: number) {
    const sign = value < 0 ? -1 : 1;
    const x = Math.abs(value) / Math.sqrt(2);
    const t = 1 / (1 + 0.3275911 * x);
    const polynomial = (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t;
    const erf = sign * (1 - polynomial * Math.exp(-x * x));
    return (1 + erf) / 2;
}

function distribution(history: FailureHistory, options: Required<AccrualFailureDetectorOptions>) {
    const samples = history.intervals;
    if (samples.length < options.minimumSamples) {
        return {
            mean: options.initialIntervalMs,
            stdDeviation: Math.max(options.minimumStdDeviationMs, options.initialIntervalMs / 4)
        };
    }
    const mean = samples.reduce((sum, sample) => sum + sample, 0) / samples.length;
    const variance = samples.reduce((sum, sample) => sum + (sample - mean) ** 2, 0) / Math.max(1, samples.length - 1);
    return { mean, stdDeviation: Math.max(options.minimumStdDeviationMs, Math.sqrt(variance)) };
}

export class AccrualFailureDetector {
    private readonly options: Required<AccrualFailureDetectorOptions>;
    private readonly histories = new Map<string, FailureHistory>();

    constructor(options: AccrualFailureDetectorOptions = {}) {
        this.options = { ...DEFAULT_OPTIONS, ...options };
        if (this.options.suspectPhi >= this.options.deadPhi) throw new Error("suspectPhi must be lower than deadPhi");
    }

    heartbeat(relayId: string, now: number) {
        const history = this.histories.get(relayId) ?? { intervals: [] };
        if (history.lastHeartbeatAt !== undefined && now > history.lastHeartbeatAt) {
            history.intervals.push(now - history.lastHeartbeatAt);
            if (history.intervals.length > this.options.sampleWindow) {
                history.intervals.splice(0, history.intervals.length - this.options.sampleWindow);
            }
        }
        history.lastHeartbeatAt = now;
        if (history.explicitState !== "left") history.explicitState = undefined;
        this.histories.set(relayId, history);
    }

    markDraining(relayId: string) {
        const history = this.histories.get(relayId) ?? { intervals: [] };
        history.explicitState = "draining";
        this.histories.set(relayId, history);
    }

    markLeft(relayId: string) {
        const history = this.histories.get(relayId) ?? { intervals: [] };
        history.explicitState = "left";
        this.histories.set(relayId, history);
    }

    assess(relayId: string, now: number, localHealthMultiplier = 1): FailureAssessment {
        const history = this.histories.get(relayId) ?? { intervals: [] };
        const { mean, stdDeviation } = distribution(history, this.options);
        const elapsedMs = history.lastHeartbeatAt === undefined ? Number.POSITIVE_INFINITY : Math.max(0, now - history.lastHeartbeatAt);
        const health = Math.max(1, localHealthMultiplier);
        const adjustedElapsed = Math.max(0, elapsedMs - this.options.acceptablePauseMs * health);
        const z = (adjustedElapsed - mean) / (stdDeviation * health);
        const tailProbability = Math.max(1e-12, 1 - normalCdf(z));
        const phi = Number.isFinite(elapsedMs) ? Math.max(0, -Math.log10(tailProbability)) : 12;
        let state: FailureState = "alive";
        if (history.explicitState) state = history.explicitState;
        else if (elapsedMs >= this.options.hardLeaseMs * health || phi >= this.options.deadPhi) state = "dead";
        else if (phi >= this.options.suspectPhi) state = "suspect";
        return {
            relayId,
            state,
            phi,
            elapsedMs,
            meanIntervalMs: mean,
            stdDeviationMs: stdDeviation,
            samples: history.intervals.length
        };
    }
}
