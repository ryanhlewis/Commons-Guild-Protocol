export interface ForwardQueueMetadata {
    scopeId: string;
    generation: number;
    epoch: number;
}

interface ForwardQueueEntry<T> {
    value: T;
    metadata: ForwardQueueMetadata;
}

export interface ForwardQueueEnqueueResult {
    accepted: boolean;
    droppedSuperseded: number;
}

export class RealtimeForwardQueue<T> {
    private entries: Array<ForwardQueueEntry<T>> = [];
    private head = 0;
    private readonly activeByScope = new Map<string, { generation: number; epoch: number }>();

    get length() {
        return this.entries.length - this.head;
    }

    enqueue(value: T, metadata: ForwardQueueMetadata): ForwardQueueEnqueueResult {
        const active = this.activeByScope.get(metadata.scopeId);
        if (
            active &&
            (metadata.generation < active.generation ||
                (metadata.generation === active.generation && metadata.epoch < active.epoch))
        ) {
            return { accepted: false, droppedSuperseded: 1 };
        }
        let droppedSuperseded = 0;
        if (
            !active ||
            metadata.generation > active.generation ||
            (metadata.generation === active.generation && metadata.epoch > active.epoch)
        ) {
            const nextEntries: Array<ForwardQueueEntry<T>> = [];
            for (let index = this.head; index < this.entries.length; index += 1) {
                const entry = this.entries[index];
                if (
                    entry.metadata.scopeId === metadata.scopeId &&
                    (entry.metadata.generation < metadata.generation ||
                        (entry.metadata.generation === metadata.generation && entry.metadata.epoch < metadata.epoch))
                ) {
                    droppedSuperseded += 1;
                } else {
                    nextEntries.push(entry);
                }
            }
            this.entries = nextEntries;
            this.head = 0;
            this.activeByScope.set(metadata.scopeId, { generation: metadata.generation, epoch: metadata.epoch });
        }
        this.entries.push({ value, metadata });
        return { accepted: true, droppedSuperseded };
    }

    dequeue() {
        if (this.head >= this.entries.length) return undefined;
        const value = this.entries[this.head++].value;
        if (this.head >= 4096 && this.head * 2 >= this.entries.length) {
            this.entries = this.entries.slice(this.head);
            this.head = 0;
        }
        return value;
    }
}
