export class CompactingQueue<T> {
    private values: Array<T | undefined> = [];
    private head = 0;

    get length() {
        return this.values.length - this.head;
    }

    enqueue(value: T) {
        this.values.push(value);
    }

    dequeue(): T | undefined {
        if (this.head >= this.values.length) return undefined;
        const value = this.values[this.head];
        this.values[this.head] = undefined;
        this.head += 1;
        if (this.head >= 4096 && this.head * 2 >= this.values.length) {
            this.values = this.values.slice(this.head);
            this.head = 0;
        }
        return value;
    }
}
