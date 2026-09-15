export class RealtimePacketWindow {
    private readonly keys = new Set<string>();
    private readonly order: string[] = [];

    constructor(private readonly capacity = 65536) {
        if (!Number.isInteger(capacity) || capacity < 1) throw new Error("Packet window capacity must be positive");
    }

    get size() {
        return this.keys.size;
    }

    accept(key: string) {
        if (this.keys.has(key)) return false;
        this.keys.add(key);
        this.order.push(key);
        if (this.order.length > this.capacity) {
            const expired = this.order.shift();
            if (expired !== undefined) this.keys.delete(expired);
        }
        return true;
    }
}
