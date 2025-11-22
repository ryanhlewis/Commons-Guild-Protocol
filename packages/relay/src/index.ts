import { RelayServer } from "./server";

import { Store, MemoryStore } from "./store";
import { LevelStore } from "./store_level";

export { RelayServer, Store, MemoryStore, LevelStore };

if (require.main === module) {
    const PORT = 7447;
    new RelayServer(PORT);
}
