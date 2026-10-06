import { it, expect } from 'vitest';
import { runEpochTransitionModel } from '../../../scripts/epoch-transition-model';
it('exhaustively checks bounded joint-quorum transition design and fence-loss negative control', () => {
    const result = runEpochTransitionModel();
    expect(result.ok).toBe(true);
    expect(result.explored).toBeGreaterThan(100000);
    expect(result.unsafeFenceResetCounterexample).toBe(true);
});
