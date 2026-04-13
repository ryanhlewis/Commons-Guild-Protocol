import { spawn } from "node:child_process";

const child = spawn("tsx", ["watch", "src/index.ts", "--clean"], {
    stdio: "inherit",
    env: process.env,
    shell: true,
});

child.on("exit", (code) => {
    process.exit(code ?? 0);
});
