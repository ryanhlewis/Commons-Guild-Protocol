# Historical performance PR review

PR 1 (`copilot/improve-slow-code-performance`, head `9f5c200`) was reviewed against main `eff6cbb` and closed without merging.

- Core state transitions already copy collections on demand and preserve member objects. The old PR's narrower state implementation adds no missing fix.
- Client event deduplication already uses an insertion-order queue with a moving head and periodic compaction. The proposed extra queue is redundant.
- Relay maintenance already uses cached/canonical state reconstruction and invalidates caches after maintenance. The older checkpoint/pruning changes predate this implementation.
- Memory storage already deletes batches in one compaction pass and rebuilds its indexes once. The proposed per-event splice/index-shift loop remains linear per deletion and does not improve batch pruning.
- The remaining directory leaf cache clears and repopulates a shared map across asynchronous database iteration, while the previous Merkle tree remains visible. A concurrent proof request could observe a missing leaf or a leaf inconsistent with that tree. It also still rebuilds the entire tree after every registration, so it does not implement incremental updates.

No runtime changes were imported and no new runtime test result is claimed. The closed PR retains the historical commits; its remote branch was removed. Any future directory optimization should publish the tree and lookup index together and include concurrent registration/proof verification tests.
