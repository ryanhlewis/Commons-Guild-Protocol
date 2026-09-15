import { mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";

function argument(name: string) {
  const exact = `--${name}`;
  const prefix = `${exact}=`;
  const direct = process.argv.find((value) => value.startsWith(prefix));
  if (direct) return direct.slice(prefix.length);
  const index = process.argv.indexOf(exact);
  return index >= 0 ? process.argv[index + 1] : undefined;
}

function run(command: string, args: string[]) {
  const result = spawnSync(command, args, {
    cwd: path.resolve("."),
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });
  if (result.stdout) process.stdout.write(result.stdout);
  if (result.stderr) process.stderr.write(result.stderr);
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(" ")} failed with ${result.status}`);
  }
}

const image = argument("image") || "cgp-sfu-authority-gate:local";
const startedAt = Date.now();
if (!process.argv.includes("--skip-build")) {
  run("docker", [
    "build",
    "-t",
    image,
    "-f",
    "loadnet/Dockerfile",
    ".",
  ]);
}

run("docker", [
  "run",
  "--rm",
  "--entrypoint",
  "npx",
  image,
  "vitest",
  "run",
  "packages/tests/src/sfu_authority.test.ts",
  "packages/tests/src/sfu_authority_relay_integration.test.ts",
]);

const report = {
  protocol: "cgp-sfu-authority-hard-gate/1",
  passed: true,
  completedAt: new Date().toISOString(),
  durationMs: Date.now() - startedAt,
  topology: {
    relayAuthorities: 5,
    writeThreshold: 3,
    partition: "2-vs-3",
  },
  assertions: {
    initialCertification: "5/5 replicas with valid 3-of-5 certificates",
    minorityAdvance: "rejected",
    majorityAdvance: "committed",
    partitionHeal: "minority caught up",
    relayDeath: "rotation committed with 4/5 live",
    keySubstitution: "rejected",
    overlapExpiry: "prior epoch rejected after overlap",
  },
};
const resultsDirectory = path.resolve("loadnet", "results");
mkdirSync(resultsDirectory, { recursive: true });
const reportPath = path.join(resultsDirectory, "sfu-authority-hard-gate.json");
writeFileSync(reportPath, `${JSON.stringify(report, null, 2)}\n`);
console.log(`wrote ${reportPath}`);
