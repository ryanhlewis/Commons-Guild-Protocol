import { RelayServer } from "./server";

export { RelayServer };

if (require.main === module) {
    const PORT = 7447;
    new RelayServer(PORT);
}
